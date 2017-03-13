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
 */
func (redisClient *redisClient) SendBytes(b []byte) error{
	_, err := redisClient.conn.Write(b)
	return err
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
func ToSlice(arr interface{}) []interface{} {
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
	处理发送给redis-cli的返回信息
	如：ok -> *1\r\n$2\r\nOK\r\n
 */
func parseMessage(message []byte) ([]byte, error) {
	result := [][]byte{
		[]byte("*1"),
		[]byte("$" + strconv.Itoa(len(message))),
		message,
		[]byte(""),
	}
	charset := []byte("\r\n")
	res := bytes.Join(result, charset)
	return res, nil
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
	//hello := []byte("*1\r\n$2\r\nOK\r\n")
	for {
		conn, _ := listener.Accept()
		redisClient := &redisClient{
			conn:conn,
			tcpServer:tcpServer,
			reader:bufio.NewReader(conn),
			writer:bufio.NewWriter(conn),
		}
		c, _ := redis.Dial("tcp","host:port",redis.DialPassword("xxxx"))
		reply, _ := redisClient.Receive() // receive message from client reader and parse to interface{}
		message := ToSlice(reply)
		command, _ := convertInterfaceToString(message[0])
		actual, _ := c.Do(command, message[1:]...)
		result, err := convertInterfaceToBytes(actual)
		if err != nil{
			log.Fatal(err)
		}
		res, _ := parseMessage(result)
		redisClient.SendBytes(res)
	}
}

const address  = "host:port"
func main(){
	tcpServer := New(address)
	defer tcpServer.Listen()
}
