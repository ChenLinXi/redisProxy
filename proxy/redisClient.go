package proxy

import (
	"net"
	"sync"
	"bufio"
	"time"
	//"io"
	//"strconv"
	//"bytes"
	"fmt"
)

type redisClient struct {
	conn		net.Conn
	tcpServer	*tcpServer
	mu		sync.Mutex
	err		error
	pending		int

	// Read
	br		*bufio.Reader
	readTimeout	time.Duration

	// Write
	bw		*bufio.Writer
	writeTimeout	time.Duration


	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte

	// Scratch space for formatting integers and floats.
	numScratch [40]byte
}

var (
	okReply	interface{} = "OK"
	pongReply interface{} = "PONG"
)

/*
*	获取redis客户端连接
 */
func (redisClient *redisClient) redisConn() net.Conn{
	return redisClient.conn
}

/*
*	关闭redis客户端连接
 */
func (redisClient *redisClient) Close() error{
	redisClient.mu.Lock()
	err := redisClient.err
	if redisClient.err == nil {
		redisClient.err = nil
	}
	redisClient.mu.Unlock()
	redisClient.conn.Close()
	return err
}

/*
*	处理redis客户端异常，关闭redis客户端连接
 */
func (redisClient *redisClient) Fatal(err error) error{
	redisClient.mu.Lock()
	if redisClient.err == nil {
		redisClient.err = err
		redisClient.conn.Close()
	}
	redisClient.mu.Unlock()
	return err
}

/*
*	发送字符串给redis客户端
 */
func (redisClient *redisClient) Send(message string) error{
	_, err := redisClient.conn.Write([]byte(message))
	return err
}

/*
*	发送字节给redis客户端
 */
func (redisClient *redisClient) SendBytes(b []byte) error{
	_, err := redisClient.conn.Write(b)
	return err
}

/*
*	逐行读取redisClient客户端发送的数据
 */
func (redisClient *redisClient) readMessage() {
	reader := bufio.NewReader(redisClient.conn)
	result := make(chan []byte, redisClient.tcpServer.receiveChanSize)
	// 迭代接收buffer中数据
	go func() {
		for {
			message, err := reader.ReadSlice('\n')
			if err != nil {
				redisClient.conn.Close()
				redisClient.tcpServer.onRedisClientConnectionClosed(redisClient, err)
				return
			}
			i := len(message) - 2
			if i < 0 || message[i] != '\r' {
				return
			}
			result <- message[:i]
		}
	}()

	//接收完后 发送数据到tcp服务端
	go func() {
		redisClient.tcpServer.onNewMessage(redisClient, result)
	}()
}

//**** redisClient入口，其中执行业务逻辑
func (redisClient *redisClient) listen() {
	fmt.Println("请在redisClient中执行业务逻辑，并读取redisClient中buffer的信息")
	redisClient.readMessage()
}

/*
*	逐行读取redisClient客户端发送的数据
 */
//func (redisClient *redisClient) readLine() ([]byte, error) {
//	p, err := redisClient.br.ReadSlice('\n')
//	if err == bufio.ErrBufferFull{
//		return nil, protocolError("long response line")
//	}
//	if err != nil {
//		return nil, err
//	}
//	i := len(p) - 2
//	if i < 0 || p[i] != '\r'{
//		return nil, protocolError("bad response line terminator")
//	}
//	return p[:i], nil
//}
//func (redisClient *redisClient) readLine() ([]byte, error) {
//	reader := bufio.NewReader(redisClient.conn)
//	p, err := reader.ReadSlice('\n')
//	if err == bufio.ErrBufferFull{
//		return nil, protocolError("long response line")
//	}
//	if err != nil {
//		return nil, err
//	}
//	i := len(p) - 2
//	if i < 0 || p[i] != '\r' {
//		return nil, protocolError("bad response line terminator")
//	}
//	return p[:i], nil
//}


/*
*	解析redis客户端发送的数据
 */
//func (redisClient *redisClient) readReply() (interface{}, error){
//	line, err := redisClient.readLine()
//	if err != nil {
//		return nil, err
//	}
//	if len(line) == 0 {
//		return nil, protocolError("short response line")
//	}
//	switch line[0] {
//	case '+':
//		switch  {
//		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
//
//			return okReply, nil
//		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
//			return pongReply, nil
//		default:
//			return string(line[1:]), nil
//		}
//	case '-':
//		return string(line[1:]), nil
//	case ':':
//		return parseInt(line[1:])
//	case '$':
//		n, err := parseLen(line[1:])
//		if n < 0 || err != nil {
//			return nil, err
//		}
//		p := make([]byte, n)
//		_, err = io.ReadFull(redisClient.br, p)
//		if err != nil {
//			return nil, err
//		}
//		if line, err := redisClient.readLine(); err != nil {
//			return nil, err
//		} else if len(line) != 0 {
//			return nil, protocolError("bad bulk string format")
//		}
//		return p, nil
//	case '*':
//		n, err := parseLen(line[1:])
//		if n < 0 || err != nil {
//			return nil, err
//		}
//		r := make([]interface{}, n)
//		for i := range r {
//			r[i], err = redisClient.readReply()
//			if err != nil {
//				return nil, err
//			}
//		}
//		return r, nil
//	}
//	return nil, protocolError("unexpected response line")
//}


//func (redisClient *redisClient) Receive() (reply interface{}, err error) {
//	if redisClient.readTimeout != 0 {
//		redisClient.conn.SetDeadline(time.Now().Add(redisClient.readTimeout))
//	}
//	if reply, err = redisClient.readReply(); err != nil {
//		return nil, redisClient.Fatal(err)
//	}
//	redisClient.mu.Lock()
//	if redisClient.pending > 0 {
//		redisClient.pending -= 1
//	}
//	redisClient.mu.Unlock()
//	if err, ok := reply.(error); ok {
//		return nil, err
//	}
//	return
//}
//
//func (redisClient *redisClient) writeLen(prefix byte, n int) error {
//	redisClient.lenScratch[len(redisClient.lenScratch)-1] = '\n'
//	redisClient.lenScratch[len(redisClient.lenScratch)-2] = '\r'
//	i := len(redisClient.lenScratch) - 3
//	for {
//		redisClient.lenScratch[i] = byte('0' + n%10)
//		i -= 1
//		n = n / 10
//		if n == 0 {
//			break
//		}
//	}
//	redisClient.lenScratch[i] = prefix
//	_, err := redisClient.bw.Write(redisClient.lenScratch[i:])
//	return err
//}
//
//func (redisClient *redisClient) writeString(s string) error{
//	redisClient.writeLen('$', len(s))
//	redisClient.bw.WriteString(s)
//	_, err := redisClient.bw.WriteString("\r\n")
//	return err
//}
//
//func (redisClient *redisClient) writeBytes(p []byte) error{
//	redisClient.writeLen('$',len(p))
//	redisClient.bw.Write(p)
//	_, err := redisClient.bw.WriteString("\r\n")
//	return err
//}
//
//func (redisClient *redisClient) writeInt64(n int64) error{
//	return redisClient.writeBytes(strconv.AppendInt(redisClient.numScratch[:0], n, 10))
//}
//
//func (redisClient *redisClient) writeFloat64(n float64) error{
//	return redisClient.writeBytes(strconv.AppendFloat(redisClient.numScratch[:0], n, 'g', -1, 64))
//}
//
//func (redisClient *redisClient) writeCommand(cmd string, args []interface{}) (err error) {
//	redisClient.writeLen('*', 1+len(args))
//	err = redisClient.writeString(cmd)
//	for _, arg := range args {
//		if err != nil {
//			break
//		}
//		switch arg := arg.(type) {
//		case string:
//			err = redisClient.writeString(arg)
//		case []byte:
//			err = redisClient.writeBytes(arg)
//		case int:
//			err = redisClient.writeInt64(int64(arg))
//		case int64:
//			err = redisClient.writeInt64(arg)
//		case float64:
//			err = redisClient.writeFloat64(arg)
//		case bool:
//			if arg {
//				err = redisClient.writeString("1")
//			} else {
//				err = redisClient.writeString("0")
//			}
//		case nil:
//			err = redisClient.writeString("")
//		default:
//			var buf bytes.Buffer
//			fmt.Fprint(&buf, arg)
//			err = redisClient.writeBytes(buf.Bytes())
//		}
//	}
//	return err
//}
