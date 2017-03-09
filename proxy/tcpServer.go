package proxy

import (
	"net"
	"log"
)

type tcpServer struct {
	redisClients			[]*redisClient
	address				string
	receiveChanSize			int
	onNewRedisClientCallback	func(redisClient *redisClient)
	onRedisClientConnectionClosed	func(redisClient *redisClient, err error)
	onNewMessage			func(redisClient *redisClient, message chan []byte)
}

/*
*	新连接回调方法
 */
func (tcpServer *tcpServer) OnNewRedisClient(callback func(redisClient *redisClient)){
	tcpServer.onNewRedisClientCallback = callback
}

/*
*	连接关闭回调方法
 */
func (tcpServer *tcpServer) OnRedisClientConnectionClosed(callback func(redisClient *redisClient, err error)){
	tcpServer.onRedisClientConnectionClosed = callback
}

/*
*	新消息回调方法
 */
func (tcpServer *tcpServer) OnNewMessage(callback func(redisClient *redisClient, message chan []byte)){
	tcpServer.onNewMessage = callback
}

/*
*	监听client连接
 */
func (tcpServer *tcpServer) Listen() {
	listener, err := net.Listen("tcp", tcpServer.address)
	if err != nil {
		log.Fatal("Error starting TCP server")
	}
	defer listener.Close()

	for {
		conn, _ := listener.Accept()
		redisClient := &redisClient{
			conn: conn,
			tcpServer: tcpServer,
		}
		go redisClient.listen()	//****循环监听多个redisClient
		tcpServer.onNewRedisClientCallback(redisClient)
	}
}

/*
*	测试结果：
*	从redis客户端接收byte数据，并打印；
*	发送byte数据给redis客户端；
*	测试时，不支持打印log数据，只有在tcp服务器断开连接时才打印
*
 */
func New(address string, rcSize int) *tcpServer {
	log.Println("Creating server with address", address)
	tcpServer := &tcpServer{
		address:address,
		receiveChanSize:rcSize,
	}
	tcpServer.OnNewRedisClient(func(redisClient *redisClient) {})
	tcpServer.OnNewMessage(func(redisClient *redisClient, message chan []byte) {})
	tcpServer.OnRedisClientConnectionClosed(func(redisClient *redisClient, err error) {})
	return tcpServer
}
