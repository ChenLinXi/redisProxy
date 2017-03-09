package proxy

import "fmt"

type protocolError string

func (pe protocolError) Error() string{
	return fmt.Sprintf("redisProxy: %s",string(pe))
}
