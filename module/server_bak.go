package module

import (
	"net"
	"log"
	"bufio"
	"reflect"
	"fmt"
)

/*
	tcp server proxy lots of clients
 */

type Server struct {
	clients		[]*Client
	address		string
}


type ScanMessage struct {
	elemType   reflect.Type
	sliceValue reflect.Value
}

/*
	handlerConnection(conn)
 */
func (server *Server) handleConnection(conn net.Conn){
	//var filter filter	//引入命令过滤器
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
		br:bufio.NewReaderSize(conn, 1024),
		bw:bufio.NewWriterSize(conn, 1024),
	}

	for {
		//从客户端接收信息
		message, err := client.readAll()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(message)

		// 发送命令到redis
		go redisConn.SendBytes(message)

		res, _ := redisConn.Receive()
		client.SendBytes(res)
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
