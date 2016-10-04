package network

import "net"

type Server struct {
	channelMgr *ChannelMgr
	localAddr  *net.TCPAddr
	init       ChannelInit
}

func NewServer(localAddr string, init ChannelInit) (*Server, error) {
	addr, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		return nil, err
	}

	server := &Server{
		localAddr:  addr,
		channelMgr: NewChannelMgr(),
		init:       init,
	}

	return server, nil
}

func (this *Server) Bootstrap() {
	listener, err := net.ListenTCP("tcp", this.localAddr)
	if err != nil {
		panic(err)
	}

	for {
		conn, _ := listener.AcceptTCP()
		chl := NewChannel(conn)
		chl.socket = conn

		this.init.(ChannelInit).Init(chl)

		this.channelMgr.OnConnect(chl)
		chl.pipeline.FireConnect(chl)

		go chl.ReadLoop()
	}
}
