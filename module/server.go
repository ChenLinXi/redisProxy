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
	listen tcp server
 */
func (server *Server) Listen() {
	var filter filter	//引入命令过滤器
	var utils utils		//引入工具类
	listener, err := net.Listen("tcp", server.address)
	if err != nil{
		log.Fatal("Error starting TCP server")
	}
	defer listener.Close()
	for {
		conn, _ := listener.Accept()
		client := &Client{
			conn:conn,
			server:server,
			reader:bufio.NewReader(conn),
			writer:bufio.NewWriter(conn),
			bufferSize:1024,
		}
		go func() {
			c, _ := redis.Dial("tcp","10.100.150.31:6011",redis.DialPassword("d1Iuw3qlDBntyx1w"))
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
		}()
	}
}
