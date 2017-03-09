package redis

import (
	"fmt"
	"sync"
	"net"
	"time"
	"bufio"
	"net/url"
	"errors"
	"strconv"
	"bytes"
	"io"
	"regexp"
)

/*
****************************************
	连接conn对象，拨号参数 类定义
	@Params: net.Conn、error、Timeout、bufferRead/Write
****************************************
 */
type conn struct {

				  // shared
	mu	sync.Mutex
	pending	int	// 连接等待数
	err	error
	conn	net.Conn

				  // read
	readTimeout	time.Duration
	br		*bufio.Reader

				  // write
	writeTimeout	time.Duration
	bw		*bufio.Writer

				  // Scratch space for formatting argument length.
				  // '*' or '$', length, "\r\n"
	lenScratch [32]byte

				  // Scratch space for formatting integers and floats.
	numScratch [40]byte
}

type DialOption struct {
	f func(*dialOptions)
}

/*
	拨号Dial配置参数
 */
type dialOptions struct {
	readTimeout	time.Duration
	writeTimeout	time.Duration
	dial		func(network, addr string) (net.Conn, error)
	db		int
	password	string
	skipVerify	bool
}

/*
****************************************
	拨号方法，设置拨号参数
	返回值：net.Conn连接，error
****************************************
 */
func Dial(network, address string, options ...DialOption) (*conn, error){
	do := dialOptions{
		dial: net.Dial,
	}
	for _, option := range options{
		option.f(&do)
	}

	netConn, err := do.dial(network, address)
	if err != nil{
		return nil, err
	}

	c := &conn{
		conn:		netConn,
		bw:		bufio.NewWriter(netConn),
		br:		bufio.NewReader(netConn),
		readTimeout:	do.readTimeout,
		writeTimeout:	do.writeTimeout,
	}

	if do.password != ""{
		if _, err := c.Do("AUTH", do.password); err!=nil {
			netConn.Close()
			return nil, err
		}
	}

	if do.db != 0 {
		if _, err := c.Do("SELECT", do.db); err != nil {
			netConn.Close()
			return nil, err
		}
	}
	return c, nil
}

var pathDBRegexp = regexp.MustCompile(`/(\d*)\z`)
/*
	URL拨号方法（链接预处理：TLS、password...）
	返回值：回调Dial方法，返回Conn连接
 */
func DialURL(rawurl string, options ...DialOption) (*conn, error){
	u, err := url.Parse(rawurl)
	if err != nil{
		return nil, err
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		host = u.Host
		port = "6379"
	}
	if host == "" {
		host = "localhost"
	}
	address := net.JoinHostPort(host, port)

	if u.User != nil{
		password, isSet := u.User.Password()
		if isSet {
			options = append(options, DialPassword(password))
		}
	}

	match := pathDBRegexp.FindStringSubmatch(u.Path)
	if len(match) == 2 {
		db := 0
		if len(match[1]) > 0 {
			db, err = strconv.Atoi(match[1])
			if err != nil {
				return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
			}
		}
		if db != 0 {
			options = append(options, DialDatabase(db))
		}
	} else if u.Path != "" {
		return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
	}

	return Dial("tcp", address, options...)
}

/*
	超时信息构造函数
	返回值： DialOption配置实体
 */
func DialTimeout(network, address string, connectTimeout, readTimeout, writeTimeout time.Duration) (*conn, error){
	return Dial(network, address,
		DialConnectTimeout(connectTimeout),
		DialReadTimeout(readTimeout),
		DialWriteTimeout(writeTimeout))
}

func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.readTimeout = d
	}}
}

func DialWriteTimeout(d time.Duration) DialOption{
	return DialOption{func(do *dialOptions) {
		do.writeTimeout = d
	}}
}

func DialConnectTimeout(d time.Duration) DialOption{
	return DialOption{func(do *dialOptions) {
		dialer := net.Dialer{Timeout:d}
		do.dial = dialer.Dial
	}}
}

/*
	配置信息构造函数
	返回值： DialOption配置实体
 */
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption{
	return DialOption{func(do *dialOptions) {
		do.dial = dial
	}}
}

func DialDatabase(db int) DialOption{
	return DialOption{func(do *dialOptions){
		do.db = db
	}}
}

func DialPassword(password string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.password = password
	}}
}


/*
****************************************
	创建连接
	返回值：指定配置的连接Conn
****************************************
 */
func NewCon(netConn net.Conn, readTimeout, writeTimeout time.Duration) *conn{
	return &conn{
		conn:		netConn,
		bw:		bufio.NewWriter(netConn),
		br:		bufio.NewReader(netConn),
		readTimeout:	readTimeout,
		writeTimeout:	writeTimeout,
	}
}

/*
	连接Conn构造函数: Close()、fatal()、Err()、writeLen()、writeString、writeBytes、
	writeInt64()、 writeFloat64、 writeCommand()、Do()、Receive()、Flush()、Send()
 */
func (c *conn) Close() error  {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("connection: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil{
		c.err = err
		c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) writeLen(prefix byte, n int) error{
	c.lenScratch[len(c.lenScratch)-1] = '\n'
	c.lenScratch[len(c.lenScratch)-2] = '\r'
	i := len(c.lenScratch) - 3
	for {
		c.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0{
			break
		}
	}
	c.lenScratch[i] = prefix
	_, err := c.bw.Write(c.lenScratch[i:])
	return err
}

func (c *conn) writeString(s string) error {
	c.writeLen('$', len(s))
	c.bw.WriteString(s)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeBytes(p []byte) error {
	c.writeLen('$', len(p))
	c.bw.Write(p)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeInt64(n int64) error{
	return c.writeBytes(strconv.AppendInt(c.numScratch[:0], n, 10))
}

func (c *conn) writeFloat64(n float64) error {
	return c.writeBytes(strconv.AppendFloat(c.numScratch[:0], n, 'g', -1, 64))
}

/*
	执行命令
	返回值：err
 */
func (c *conn) writeCommand(cmd string, args []interface{}) (err error){
	c.writeLen('*', 1+len(args))
	err = c.writeString(cmd)
	for _, arg := range args {
		if err != nil {
			break
		}
		switch arg := arg.(type) {
		case string:
			err = c.writeString(arg)
		case []byte:
			err = c.writeBytes(arg)
		case int:
			err = c.writeInt64(int64(arg))
		case int64:
			err = c.writeInt64(arg)
		case float64:
			err = c.writeFloat64(arg)
		case bool:
			if arg {
				err = c.writeString("1")
			} else {
				err = c.writeString("0")
			}
		case nil:
			err = c.writeString("")
		default:
			var buf bytes.Buffer
			fmt.Fprint(&buf, arg)
			err = c.writeBytes(buf.Bytes())
		}
	}
	return err
}

/*
****************************************
	接收reply部分(基于protocol)
****************************************
 */
type protocolError string

func (pe protocolError) Error() string{
	return fmt.Sprintf("Connection: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

func (c *conn) readLine() ([]byte, error){
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull{
		return nil, protocolError("long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	return p[:i], err
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

// parseInt parses an integer reply.
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0{
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

var (
	okReply		interface{} = "OK"
	pongReply	interface{} = "PONG"
)

/*
	接收后回复消息
 */
func (c *conn) readReply() (interface{}, error){
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O':
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
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
		_, err = io.ReadFull(c.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := c.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r{
			r[i], err = c.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

/*
	发送消息
 */
func (c *conn) Send(cmd string, args ...interface{}) error{
	c.mu.Lock()
	c.pending += 1
	c.mu.Unlock()
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.writeCommand(cmd, args); err != nil {
		return c.fatal(err)
	}
	return nil
}

/*
	清空buffer缓冲区
 */
func (c *conn) Flush() error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.bw.Flush(); err != nil {
		return c.fatal(err)
	}
	return nil
}

/*
	接收消息
 */
func (c *conn) Receive() (reply interface{}, err error) {
	if c.readTimeout != 0 {
		c.conn.SetDeadline(time.Now().Add(c.readTimeout))
	}
	if reply, err = c.readReply(); err != nil {
		return nil, c.fatal(err)
	}
	c.mu.Lock()
	if c.pending > 0 {
		c.pending -= 1
	}
	c.mu.Unlock()
	if err, ok := reply.(error); ok {
		return nil, err
	}
	return
}

/*
	执行
 */
func (c *conn) Do(cmd string, args ...interface{}) (interface{},error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = 0
	c.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	if cmd != ""{
		if err := c.writeCommand(cmd, args); err != nil {
			return nil, c.fatal(err)
		}
	}

	if err := c.bw.Flush(); err != nil {
		return  nil, c.fatal(err)
	}

	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	if cmd == "" {
		reply := make([]interface{}, pending)
		for i :=  range reply{
			r, e := c.readReply()
			if e != nil {
				return  nil, c.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}

	var err error
	var reply interface{}
	for i := 0; i <= pending; i++ {
		var e error
		if reply, e = c.readReply(); e != nil {
			return nil, c.fatal(e)
		}
		if e, ok := reply.(error); ok && err == nil {
			err = e
		}
	}
	return reply, err
}
