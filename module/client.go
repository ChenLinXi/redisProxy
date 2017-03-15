package module

import (
	"net"
	"bufio"
	"log"
	"io"
)

/*
	client
 */

type Client struct {
	conn		net.Conn
	server		*Server
	reader		*bufio.Reader
	writer		*bufio.Writer
	bufferSize	int
}

/*
	client conn
 */
func (client *Client) Conn() net.Conn{
	return client.conn
}


/*
	close conn
 */
func (client *Client) Close() error{
	err := client.conn.Close()
	if err != nil{
		return nil
	}
	return err
}

/*
	send bytes to redis-cli
 */
func (client *Client) SendBytes(b []byte) ([]byte) {
	_, err := client.conn.Write(b)
	if err != nil{
		log.Fatal(err)
	}
	return nil
}

/*
	read a line from client buffer
 */
func (client *Client) readLine() ([]byte, error) {
	message, err := client.reader.ReadSlice('\n')
	if err != nil {
		client.conn.Close()
	}
	i := len(message) - 2
	if i < 0 || message[i] != '\r' {
		return nil, err
	}
	return message[:i], err
}

/*
	read messages based on readLine()
 */
func (client *Client) readReply() (interface{}, error) {
	var utils utils		//引入工具类
	line, err := client.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '-':
		return string(line[1:]), nil
	case ':':
		return utils.parseInt(line[1:])
	case '$':
		n, err := utils.parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(client.reader, p)
		if err != nil {
			return nil, err
		}
		if line, err := client.readLine(); err != nil {
			return nil, err
		}else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := utils.parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = client.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

/*
	返回处理完的数据
 */
func (client *Client) Receive() (reply interface{}, err error) {
	if reply, err = client.readReply(); err != nil{
		return nil, err
	}
	if err, ok := reply.(error); ok{
		return nil, err
	}
	return
}
