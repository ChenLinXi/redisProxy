package module

import (
	"net"
	"log"
	"bufio"
	"github.com/garyburd/redigo/redis"
	"strings"
)

/*
	tcp server proxy lots of clients
 */

type Server struct {
	clients		[]*Client
	address		string
}

/*
	handlerConnection(conn)
 */
func (server *Server) handleConnection(conn net.Conn){
	var filter filter	//引入命令过滤器
	var utils utils		//引入工具类
	client := &Client{
		conn:conn,
		server:server,
		reader:bufio.NewReader(conn),
		writer:bufio.NewWriter(conn),
		bufferSize:1024,
	}
	c, _ := redis.Dial("tcp","xxxx",redis.DialPassword("xxxx"))
	for {
		reply, _ := client.Receive()
		if reply != nil{
			message := utils.convertInterfaceToSlice(reply)
			command, _ := utils.convertInterfaceToString(message[0])
			f := filter.filter()
			_, ok := f[strings.ToUpper(command)]
			if !ok{
				actual, err := c.Do(command, message[1:]...)
				if actual != nil && err == nil{
					result, err := redis.Bytes(actual, nil)
					if err != nil{
						log.Fatal(err)
					}
					res, _ := utils.parseMessage(result)
					client.SendBytes(res)
				}else {
					result :=  []byte("")
					res, _ :=  utils.parseMessage(result)
					client.SendBytes(res)
				}
			}else {
				result := []byte("ERR command not support")
				res, _ := utils.parseMessage(result)
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
		go server.handleConnection(conn)
	}
}
