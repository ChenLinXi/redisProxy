package proxy

import (
	"testing"
	"log"
	"fmt"
)

const address  = "xxxx"

func TestNew(t *testing.T) {

	tcpServer := New(address, 1024)	//执行tcpServer类函数
	hello := []byte("*1\r\n$2OK\r\n")

	tcpServer.OnNewRedisClient(func(redisClient *redisClient) {
		//redisClient.SendBytes(hello)
		fmt.Println("New redisClient.")
	})

	// 异步处理client消息
	tcpServer.OnNewMessage(func(redisClient *redisClient, message chan []byte) {
		redisClient.SendBytes(hello)
		for {
			fmt.Print(string(<-message))
		}
	})

	tcpServer.OnRedisClientConnectionClosed(func(redisClient *redisClient, err error) {
		fmt.Print("\nredisClient连接中断 ")
		log.Fatal("Err:", err)
	})

	tcpServer.Listen()
}
