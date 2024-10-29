package pgserver

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
)

type Server struct {
	Listener server.ProtocolListener
}

func NewServer(srv *server.Server, host string, port int) (*Server, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	l, err := server.NewListener("tcp", addr, "")
	if err != nil {
		panic(err)
	}
	listener, err := NewListener(
		mysql.ListenerConfig{
			Protocol: "tcp",
			Address:  addr,
			Listener: l,
		},
		srv,
	)
	if err != nil {
		return nil, err
	}
	return &Server{Listener: listener}, nil
}

func (s *Server) Start() {
	s.Listener.Accept()
}
