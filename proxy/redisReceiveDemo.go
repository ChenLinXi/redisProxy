package main

import (
	"net"
	"log"
	"bufio"
	"fmt"
	"io"
)

type protocolError string

func (pe protocolError) Error() string{
	return fmt.Sprintf("redisProxy: %s",string(pe))
}

type redisClient struct {
	conn		net.Conn
	tcpServer	*tcpServer
	reader		*bufio.Reader
}

type tcpServer struct {
	redisClients		[]*redisClient
	address			string
}

func (redisClient *redisClient) redisConn() net.Conn {
	return redisClient.conn
}

func (redisClient *redisClient) Close() error{
	redisClient.conn.Close()
	return nil
}

func (redisClient *redisClient) SendBytes(b []byte) error{
	_, err := redisClient.conn.Write(b)
	return err
}

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

// parseLen parses bulk string and array lengths.
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

func (redisClient *redisClient) Receive() (reply interface{}, err error) {
	if reply, err = redisClient.readReply(); err != nil {
		return nil, err
	}
	if err, ok := reply.(error); ok{
		return nil, err
	}
	return
}


func New(address string) *tcpServer {
	log.Println("Creating server with address", address)
	tcpServer := &tcpServer{
		address:address,
	}
	return tcpServer
}

/*
	测试结果：
	从redis-cli客户端中接收命令并解析成功
 */
func (tcpServer *tcpServer) Listen() {
	listener, err := net.Listen("tcp", tcpServer.address)
	if err != nil{
		log.Fatal("Error starting TCP server")
	}
	defer listener.Close()
	hello := []byte("*1\r\n$2OK\r\n")
	for {
		conn, _ := listener.Accept()
		redisClient := &redisClient{
			conn:conn,
			tcpServer:tcpServer,
			reader:bufio.NewReader(conn),
		}
		// send Message to redisClient as response
		redisClient.SendBytes(hello)
		// receive message from client reader and parse to interface{}
		reply, _ := redisClient.Receive()
		fmt.Print(reply)
	}
}

const address  = "xxxx"

func main(){
	tcpServer := New(address)
	defer tcpServer.Listen()
}

