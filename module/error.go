package module

import "fmt"

type protocolError string

func (pe protocolError) Error() string{
	return fmt.Sprintf("proxy: %s", string(pe))
}
