package tracker

import (
	"bytes"
	"fmt"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
)

const maxPeersPerAnnounce = 50

type trackedPeer struct {
	Address  netip.AddrPort
	LastSeen time.Time
	Left     uint64
}

type Server struct {
	mu       sync.Mutex
	interval time.Duration
	peerTTL  time.Duration
	swarms   map[protocol.InfoHash]map[protocol.PeerID]trackedPeer
}

func NewServer(interval time.Duration) *Server {
	if interval <= 0 {
		interval = 30 * time.Second
	}

	return &Server{
		interval: interval,
		peerTTL:  3 * interval,
		swarms:   make(map[protocol.InfoHash]map[protocol.PeerID]trackedPeer),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		s.writeFailure(w, "announce requests must use GET")
		return
	}

	announce, err := decodeAnnounceRequest(request)
	if err != nil {
		s.writeFailure(w, "invalid announce request")
		return
	}

	address, err := netip.ParseAddrPort(request.RemoteAddr)
	if err != nil {
		s.writeFailure(w, "invalid peer address")
		return
	}

	response := s.announce(announce, address.Addr().Unmap(), time.Now())
	encoded, err := encodeTrackerResponse(response, announce.Compact)
	if err != nil {
		s.writeFailure(w, "unable to encode tracker response")
		return
	}
	s.writeBencoding(w, encoded)
}

func (s *Server) announce(request AnnounceRequest, address netip.Addr, now time.Time) AnnounceResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pruneSwarmLocked(request.InfoHash, now)

	swarm := s.swarms[request.InfoHash]
	if request.Event == StoppedEvent {
		delete(swarm, request.PeerID)
		if len(swarm) == 0 {
			delete(s.swarms, request.InfoHash)
		}
	} else {
		if swarm == nil {
			swarm = make(map[protocol.PeerID]trackedPeer)
			s.swarms[request.InfoHash] = swarm
		}
		swarm[request.PeerID] = trackedPeer{
			Address:  netip.AddrPortFrom(address, request.Port),
			LastSeen: now,
			Left:     request.Left,
		}
	}

	peers := make([]Peer, 0, min(len(swarm), maxPeersPerAnnounce))
	for peerID, peer := range swarm {
		if peerID == request.PeerID {
			continue
		}
		peers = append(peers, Peer{
			PeerID: peerID,
			IP:     peer.Address.Addr(),
			Port:   peer.Address.Port(),
		})
		if len(peers) == maxPeersPerAnnounce {
			break
		}
	}

	return AnnounceResponse{Interval: s.interval, Peers: peers}
}

func (s *Server) pruneSwarmLocked(infoHash protocol.InfoHash, now time.Time) {
	swarm := s.swarms[infoHash]
	for peerID, peer := range swarm {
		if now.Sub(peer.LastSeen) > s.peerTTL {
			delete(swarm, peerID)
		}
	}
	if len(swarm) == 0 {
		delete(s.swarms, infoHash)
	}
}

func (s *Server) writeFailure(w http.ResponseWriter, reason string) {
	s.writeBencoding(w, protocol.DictBencoding(map[string]protocol.Bencoding{
		"failure reason": protocol.StringBencoding(reason),
	}))
}

func (s *Server) writeBencoding(w http.ResponseWriter, value protocol.Bencoding) {
	var body bytes.Buffer
	if err := protocol.Encode(&body, value); err != nil {
		http.Error(w, fmt.Sprintf("encode tracker response: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(body.Bytes())
}
