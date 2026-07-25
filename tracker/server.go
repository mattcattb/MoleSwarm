package tracker

import (
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
)

type trackedPeer struct {
	Address  netip.AddrPort
	LastSeen time.Time
	Left     uint64
}

type Server struct {
	mu       sync.Mutex
	interval time.Duration
	swarms   map[protocol.InfoHash]map[protocol.PeerID]trackedPeer
}

func NewServer(interval time.Duration) *Server {
	return &Server{
		interval: interval,
		swarms:   make(map[protocol.InfoHash]map[protocol.PeerID]trackedPeer),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	_, err := decodeAnnounceRequest(request)
	if err != nil {
		return
	}
}
