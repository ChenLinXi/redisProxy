package module

import "testing"
import (
	"log"
	"runtime"
)

const address  = "xxxx"

func New(address string) *Server{
	log.Println("Creating server with address", address)
	server := &Server{
		address:address,
	}
	return server
}

func TestServer_Listen(t *testing.T) {
	nCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(nCpu)
	server := New(address)
	defer server.Listen()
}
