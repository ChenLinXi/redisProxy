package module

import (
	"reflect"
	"bytes"
	"encoding/gob"
	"strconv"
)

/*
	1.parse length of []byte
	2.parse int of []byte
	3.convert interface to slice
	4.convert interface to string
	5.parse send message to lots of package
 */


type utils struct {}

/*
	parseLen get length(int) of []byte
 */
func (utils *utils) parseLen(p []byte) (int, error) {
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

/*
	parseInt get length of []byte
 */
func (utils *utils) parseInt(p []byte) (interface{}, error) {
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

/*
	将interface{}切割为[]interface{}数组
 */
func (utils *utils) convertInterfaceToSlice(arr interface{}) []interface{} {
	v := reflect.ValueOf(arr)
	if v.Kind() != reflect.Slice{
		panic("toslice arr not slice")
	}
	l := v.Len()
	ret := make([]interface{}, l)
	for i := 0; i < l; i++ {
		ret[i] = v.Index(i).Interface()
	}
	return ret
}

/*
	从interface中提取command命令
	如：set, get, pop之类
 */
func (utils *utils) convertInterfaceToString(key interface{}) (string, error){
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil{
		return "", err
	}
	return string(buf.Bytes()[4:]), nil
}

/*
	分包
 */
func (utils *utils) parseMessage(message []byte) ([]byte, error){
	length := len(message)
	charset := []byte("\r\n")
	if length > 1024 {
		// 处理长度大于1024字节的数据
		packageNum := (length / 1024) + 1	// 消息包长度
		result := make([][]byte, (packageNum+1)*2)	// 消息总量
		result[0] = []byte("*"+strconv.Itoa(packageNum)) //消息头
		tmp := packageNum	// 数组下标计数器
		counter := 1	// 消息长度计数器
		for i := 0; i < length; i += 1024{
			if counter == packageNum {
				result[packageNum-tmp+1] = []byte("$"+strconv.Itoa(length - 1024*(packageNum-1)))	// 单个包的字节长度
				result[packageNum-tmp+2] = message[i:i+length-1024*(packageNum-1)]	// 单个包的内容（末尾小于等于1024字节）
			} else {
				result[packageNum-tmp+1] = []byte("$"+strconv.Itoa(1024))	// 单个包的字节长度
				result[packageNum-tmp+2] = message[i:i+1024]	// 单个包的内容（1024字节）
			}
			tmp -= 2
			counter += 1
		}
		result[packageNum*2 + 1] = []byte{}	// 消息尾
		return bytes.Join(result, charset), nil	// 消息内容之间加上 /r/n 格式化成[]byte类型数据
	}else {
		// 直接发送单个包
		result := [][]byte{
			[]byte("*1"),
			[]byte("$" + strconv.Itoa(len(message))),
			message,
			[]byte(""),
		}
		res := bytes.Join(result, charset)
		return res, nil
	}
}
