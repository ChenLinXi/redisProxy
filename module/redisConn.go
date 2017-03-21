package module

import (
	"net"
	"bufio"
	"sync"
	"time"
	"errors"
)

type redisConn struct {
	conn		net.Conn
	server		*Server
	err 		error
	pending		int
	mu		sync.Mutex

	br		*bufio.Reader
	readTimeout	time.Duration

	bw		*bufio.Writer
	writeTimeout	time.Duration
}

func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) *redisConn {
	return &redisConn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

func (redisConn *redisConn) Conn() net.Conn{
	return redisConn.conn
}

func (redisConn *redisConn) Close() error {
	redisConn.mu.Lock()
	err := redisConn.err
	if redisConn.err == nil {
		redisConn.err = errors.New("redis: closed")
		err = redisConn.conn.Close()
	}
	redisConn.mu.Unlock()
	return err
}

func (redisConn *redisConn) Err() error {
	redisConn.mu.Lock()
	err := redisConn.err
	redisConn.mu.Unlock()
	return err
}

func (redisConn *redisConn) fatal(err error) error {
	redisConn.mu.Lock()
	if redisConn.err == nil {
		redisConn.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		redisConn.conn.Close()
	}
	redisConn.mu.Unlock()
	return err
}

func (redisConn *redisConn) SendBytes(b []byte) error {
	_, err := redisConn.conn.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func (redisConn *redisConn) peek() int {
	redisConn.br.Peek(1)
	length := redisConn.br.Buffered()
	return length
}

func (redisConn *redisConn) Receive() ([]byte, error) {
	msgLen := redisConn.peek()
	b := make([]byte, msgLen)
	n, err := redisConn.br.Read(b)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}

func (redisConn *redisConn) Flush() error {
	if redisConn.writeTimeout != 0 {
		redisConn.conn.SetWriteDeadline(time.Now().Add(redisConn.writeTimeout))
	}
	if err := redisConn.bw.Flush(); err != nil {
		return redisConn.fatal(err)
	}
	return nil
}
