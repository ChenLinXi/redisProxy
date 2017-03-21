package module

import (
	"net"
	"log"
	"bufio"
	"bytes"
	"strings"
)

/*
	tcp server proxy lots of clients
 */
type Server struct {
	clients		[]Client
	address		string
}

/*
	handlerConnection(conn)
 */
func (server *Server) handleConnection(conn net.Conn){
	var filter filter	//引入命令过滤器
	//var utils utils		//引入工具类
	client := &Client{
		conn:conn,
		server:server,
		reader:bufio.NewReaderSize(conn, 1024),
		writer:bufio.NewWriterSize(conn, 1024),
		bufferSize:1024,
	}

	// 创建redis单机连接
	rc, _ := net.Dial("tcp", "xxxx")
	redisConn := &redisConn{
		conn: rc,
		server: server,
		br: bufio.NewReader(rc),
		bw: bufio.NewWriter(rc),
	}

	// 命令过滤器初始化
	f := filter.filter()

	for {
		// 从客户端接收数据
		message, err := client.readAll()
		if err != nil {
			log.Fatal(err)
		}
		if len(message) != 0{
			charset := "\r\n"
			index := bytes.IndexAny(message[8:], charset)
			cmd := string(message[8:8+index])
			_, ok := f[strings.ToUpper(cmd)]
			// 命令过滤器
			if ok{
				// 发送错误信息
				res := []byte("-ERR the command not support\r\n")
				client.SendBytes(res)
			} else {
				// 向redis发送数据
				err = redisConn.SendBytes(message)
				if err != nil {
					log.Fatal(err)
				}
				// 从redis接收数据
				res, _ := redisConn.Receive()
				client.SendBytes(res)
			}
		}
	}
}

/*
	listen tcp server
 */
func (server *Server) Listen() {

	listener, err := net.Listen("tcp", server.address)
	if err != nil{
		log.Fatal("Error starting TCP server")
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.handleConnection(conn)	// 调用处理方法
	}
}
