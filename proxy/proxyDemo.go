package main

import (
	"net"
	"log"
	"bufio"
	"fmt"
	"io"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
)

type protocolError string

func (pe protocolError) Error() string{
	return fmt.Sprintf("redisProxy: %s",string(pe))
}

type redisClient struct {
	conn		net.Conn
	tcpServer	*tcpServer
	reader		*bufio.Reader
	writer		*bufio.Writer
	bufferSize	int
}

type tcpServer struct {
	redisClients		[]*redisClient
	address			string
}

/*
	返回redis-cli net连接
 */
func (redisClient *redisClient) redisConn() net.Conn {
	return redisClient.conn
}

/*
	关闭redisClient连接
 */
func (redisClient *redisClient) Close() error{
	redisClient.conn.Close()
	return nil
}

/*
	发送字节数据到redis-cli中
	发送单位：[1024]byte
 */
func (redisClient *redisClient) SendBytes(b []byte) ([]byte) {
	length := len(b)
	//fmt.Print(length)
	//分包发送 bufferSize = 1024
	if length <= redisClient.bufferSize {
		_, err := redisClient.conn.Write(b)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	} else {
		_, err := redisClient.conn.Write(b[0:redisClient.bufferSize])
		if err != nil {
			log.Fatal(err)
		}
		return redisClient.SendBytes(b[redisClient.bufferSize:])
	}
}

/*
	获取[]byte数组的长度 - interface{]
 */
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

/*
	获取[]byte数组的长度 - int
 */
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

/*
	逐行读取redis-cli中buffer的数据
	转换为[]byte格式
 */
func (redisClient *redisClient) readLine() ([]byte, error){
	message, err := redisClient.reader.ReadSlice('\n')
	if err != nil{
		redisClient.conn.Close()
	}
	i := len(message) - 2
	if i < 0 || message[i] != '\r' {
		return nil, err
	}
	return message[:i], err
}

/*
	解析从redis-cli读取的数据
	去除 \r\n, *, $ 等字符
 */
func (redisClient *redisClient) readReply() (interface{}, error) {
	line, err := redisClient.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil,  protocolError("short response line")
	}
	switch line[0] {
	case '-':
		return string(line[1:]), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(redisClient.reader, p)
		if err != nil {
			return nil, err
		}
		if line, err := redisClient.readLine(); err != nil {
			return nil, err
		}else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = redisClient.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

/*
	接收redis-cli发送的信息
 */
func (redisClient *redisClient) Receive() (reply interface{}, err error) {
	if reply, err = redisClient.readReply(); err != nil {
		return nil, err
	}
	if err, ok := reply.(error); ok{
		return nil, err
	}
	return
}

/*
	创建TCP-server
 */
func New(address string) *tcpServer {
	log.Println("Creating server with address", address)
	tcpServer := &tcpServer{
		address:address,
	}
	return tcpServer
}

/*
	将interface{}切割为[]interface{}数组
	如：interface{"get a"}切割为 interface{"get", "a"}
 */
func convertInterfaceToSlice(arr interface{}) []interface{} {
	v := reflect.ValueOf(arr)
	if v.Kind() != reflect.Slice{
		panic("toslice arr not slice")
	}
	l := v.Len()
	ret := make([]interface{}, l)
	for i := 0; i < l; i++ {
		ret[i] = v.Index(i).Interface()
	}
	return ret
}

/*
	从interface中提取command命令
	如：set, get, pop之类
 */
func convertInterfaceToString(key interface{}) (string, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil{
		return "", err
	}
	return string(buf.Bytes()[4:]), nil
}

/*
	从interface中提取返回信息
	如：OK等
 */
func convertInterfaceToBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil{
		return nil, err
	}
	return buf.Bytes()[4:], nil
}

/*
	处理发送给redis-cli的返回信息(长度小于1024)
	如：ok -> *1\r\n$2\r\nOK\r\n
 */
//func parseMessage(message []byte) ([]byte, error) {
//	result := [][]byte{
//		[]byte("*1"),
//		[]byte("$" + strconv.Itoa(len(message))),
//		message,
//		[]byte(""),
//	}
//	charset := []byte("\r\n")
//	res := bytes.Join(result, charset)
//	return res, nil
//}

/*
	处理发送给redis-cli的返回信息(长度大于1024，需要分包处理)
 */
func parseMessage(message []byte) ([]byte, error) {
	length := len(message)
	charset := []byte("\r\n")
	if length > 1024 {
		// 处理长度大于1024字节的数据
		packageNum := (length / 1024) + 1	// 消息包长度
		result := make([][]byte, (packageNum+1)*2)	// 消息总量
		result[0] = []byte("*"+strconv.Itoa(packageNum)) //消息头
		tmp := packageNum	// 数组下标计数器
		counter := 1	// 消息长度计数器
		for i := 0; i < length; i += 1024{
			if counter == packageNum {
				result[packageNum-tmp+1] = []byte("$"+strconv.Itoa(length - 1024*(packageNum-1)))	// 单个包的字节长度
				result[packageNum-tmp+2] = message[i:i+length-1024*(packageNum-1)]	// 单个包的内容（末尾小于等于1024字节）
			} else {
				result[packageNum-tmp+1] = []byte("$"+strconv.Itoa(1024))	// 单个包的字节长度
				result[packageNum-tmp+2] = message[i:i+1024]	// 单个包的内容（1024字节）
			}
			tmp -= 2
			counter += 1
		}
		result[packageNum*2 + 1] = []byte("")	// 消息尾
		fmt.Print(string(bytes.Join(result, charset)))
		return bytes.Join(result, charset), nil	// 消息内容之间加上 /r/n 格式化成[]byte类型数据
	}else {
		// 直接发送单个包
		result := [][]byte{
			[]byte("*1"),
			[]byte("$" + strconv.Itoa(len(message))),
			message,
			[]byte(""),
		}
		res := bytes.Join(result, charset)
		return res, nil
	}
}

/*
	redis命令过滤
*/
func commandFilter(command string) string{
	switch strings.ToUpper(command) {
	case "KEYS":		// keys
		return ""
	case "MIGIRATE":
		return ""
	case "MOVE":
		return ""
	case "OBJECT":
		return ""
	case "DUMP":
		return ""
	case "BLPOP":		// lists
		return ""
	case "BRPOP":
		return ""
	case "BRPOPLPUSH":
		return ""
	case "RPOPLPUSH":
		return ""
	case "PSUBSCRIBE":	// pub/sub
		return ""
	case "PUBLISH":
		return ""
	case "PUBSUBSCRIBE":
		return ""
	case "SUBSCRIBE":
		return ""
	case "UNSUBSCRIBE":
		return ""
	case "DISCARD":		// transctions
		return ""
	case "EXEC":
		return ""
	case "MULTI":
		return ""
	case "UNWATCH":
		return ""
	case "WATCH":
		return ""
	case "SCRIPT":		// scripting
		return ""
	case "EVAL":
		return ""
	case "EVALSHA":
		return ""
	case "BGREWRITEAOF":		// server
		return ""
	case "BGSAVE":
		return ""
	case "CLIENT":
		return ""
	case "CONFIG":
		return ""
	case "DBSIZE":
		return ""
	case "DEBUG":
		return ""
	case "FLUSHALL":
		return ""
	case "FLUSHDB":
		return ""
	case "LASTSAVE":
		return ""
	case "LATENCY":
		return ""
	case "MONITOR":
		return ""
	case "PSYNC":
		return ""
	case "REPLCONF":
		return ""
	case "RESTORE":
		return ""
	case "SAVE":
		return ""
	case "SHUTDOWN":
		return ""
	case "SLAVEOF":
		return ""
	case "SYNC":
		return ""
	case "TIME":
		return ""
	case "SLOTSCHECK":		// SLOT
		return ""
	case "SLOTSDEL":
		return ""
	case "SLOTSINFO":
		return ""
	case "SLOTSMGRTONE":
		return ""
	case "SLOTSMGRTSLOT":
		return ""
	case "SLOTSMGRTTAGONE":
		return ""
	case "SLOTSMGRTTAGSLOT":
		return ""
	case "READONLY":		// cluster
		return ""
	case "READWRITE":
		return ""
	default:
		return command
	}
}


/*
	测试结果：
	1.从redis-cli客户端中接收命令并解析成功
	2.解析后发送到redis单机中，并获取interface{}类型数据
	3.加工返回的interface{}中的[]byte数据result
	4.将加工后的数据返回到redis-cli中
 */
func (tcpServer *tcpServer) Listen() {
	listener, err := net.Listen("tcp", tcpServer.address)
	if err != nil{
		log.Fatal("Error starting TCP server")
	}
	defer listener.Close()

	// for循环监听多组redis-cli客户端
	for {
		conn, _ := listener.Accept()
		redisClient := &redisClient{
			conn:conn,
			tcpServer:tcpServer,
			reader:bufio.NewReader(conn),
			writer:bufio.NewWriter(conn),
			bufferSize:1024,
		}
		c, _ := redis.Dial("tcp","xxxx",redis.DialPassword("d1Iuw3qlDBntyx1w"))

		//go routine异步处理多个redis-cli客户端
		go func() {

			// for循环接收多组redis-cli发来的消息
			for {
				reply, _ := redisClient.Receive() // receive message from client reader and parse to interface{}
				message := convertInterfaceToSlice(reply)
				command, _ := convertInterfaceToString(message[0])
				if commandFilter(command) != ""{	// 过滤命令
					actual, _ := c.Do(command, message[1:]...)
					if actual != nil{	// 判断执行返回结果是否为空
						result, err := convertInterfaceToBytes(actual)
						if err != nil{
							log.Fatal(err)
						}
						res, _ := parseMessage(result)	//处理数据
						redisClient.SendBytes(res)	//将处理好的数据递归发送给redis客户端
					} else{
						result := []byte("")
						res, _ := parseMessage(result)
						redisClient.SendBytes(res)
					}
				}else {	// 不支持命令
					result := []byte("Command not support!")
					res, _ := parseMessage(result)
					redisClient.SendBytes(res)
				}
			}
		} ()
	}
}

const address  = "xxxx"
func main(){
	tcpServer := New(address)
	defer tcpServer.Listen()
}
