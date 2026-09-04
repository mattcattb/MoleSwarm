package tracker

import (
	"bytes"
	"fmt"
	"net/http"
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
)

const maxPeersPerAnnounce = 50

type trackedPeer struct {
	Address  netip.AddrPort
	LastSeen time.Time
	Left     uint64
}

type Server struct {
	mu                sync.Mutex
	interval          time.Duration
	peerTTL           time.Duration
	swarms            map[protocol.InfoHash]map[protocol.PeerID]trackedPeer
	allowedInfoHashes map[protocol.InfoHash]struct{}
}

// Snapshot is an immutable observation of the tracker's current swarms.
type Snapshot struct {
	IntervalSeconds int64           `json:"intervalSeconds"`
	PeerTTLSeconds  int64           `json:"peerTtlSeconds"`
	Swarms          []SwarmSnapshot `json:"swarms"`
}

type Config struct {
	AnnounceInterval  time.Duration
	PeerTTL           time.Duration
	AllowedInfoHashes []protocol.InfoHash
}

func (c Config) Validate() error {
	if c.AnnounceInterval <= 0 {
		return fmt.Errorf("announce interval must be positive")
	}
	if c.AnnounceInterval%time.Second != 0 {
		return fmt.Errorf("announce interval must be a whole number of seconds")
	}
	if c.PeerTTL != 0 && c.PeerTTL < c.AnnounceInterval {
		return fmt.Errorf("peer TTL must be at least the announce interval")
	}
	return nil
}

type SwarmSnapshot struct {
	InfoHash string         `json:"infoHash"`
	Peers    []PeerSnapshot `json:"peers"`
}

type PeerSnapshot struct {
	PeerID    string    `json:"peerId"`
	Address   string    `json:"address"`
	LastSeen  time.Time `json:"lastSeen"`
	BytesLeft uint64    `json:"bytesLeft"`
}

func NewServer(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if config.PeerTTL == 0 {
		config.PeerTTL = 3 * config.AnnounceInterval
	}

	var allowedInfoHashes map[protocol.InfoHash]struct{}
	if config.AllowedInfoHashes != nil {
		allowedInfoHashes = make(map[protocol.InfoHash]struct{}, len(config.AllowedInfoHashes))
		for _, infoHash := range config.AllowedInfoHashes {
			allowedInfoHashes[infoHash] = struct{}{}
		}
	}

	return &Server{
		interval:          config.AnnounceInterval,
		peerTTL:           config.PeerTTL,
		swarms:            make(map[protocol.InfoHash]map[protocol.PeerID]trackedPeer),
		allowedInfoHashes: allowedInfoHashes,
	}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		s.writeFailure(w, "announce requests must use GET")
		return
	}

	announce, err := parseAnnounceRequest(request)
	if err != nil {
		s.writeFailure(w, "invalid announce request")
		return
	}
	if !s.allows(announce.InfoHash) {
		s.writeFailure(w, "torrent is not allowed")
		return
	}

	address, err := netip.ParseAddrPort(request.RemoteAddr)
	if err != nil {
		s.writeFailure(w, "invalid peer address")
		return
	}

	response := s.announce(announce, address.Addr().Unmap(), time.Now())
	encoded, err := encodeAnnounceResponse(response, announce.Compact)
	if err != nil {
		s.writeFailure(w, "unable to encode tracker response")
		return
	}
	s.writeBencoding(w, encoded)
}

func (s *Server) allows(infoHash protocol.InfoHash) bool {
	if s.allowedInfoHashes == nil {
		return true
	}
	_, allowed := s.allowedInfoHashes[infoHash]
	return allowed
}

// Snapshot copies tracker state while holding the swarm mutex. Expired peers
// are pruned first so an observer cannot keep displaying peers past their TTL.
func (s *Server) Snapshot() Snapshot {
	return s.snapshot(time.Now())
}

func (s *Server) snapshot(now time.Time) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	for infoHash := range s.swarms {
		s.pruneSwarmLocked(infoHash, now)
	}

	infoHashes := make(map[protocol.InfoHash]struct{}, len(s.swarms)+len(s.allowedInfoHashes))
	for infoHash := range s.swarms {
		infoHashes[infoHash] = struct{}{}
	}
	for infoHash := range s.allowedInfoHashes {
		infoHashes[infoHash] = struct{}{}
	}

	swarms := make([]SwarmSnapshot, 0, len(infoHashes))
	for infoHash := range infoHashes {
		peers := make([]PeerSnapshot, 0, len(s.swarms[infoHash]))
		for peerID, peer := range s.swarms[infoHash] {
			peers = append(peers, PeerSnapshot{
				PeerID:    fmt.Sprintf("%x", peerID),
				Address:   peer.Address.String(),
				LastSeen:  peer.LastSeen,
				BytesLeft: peer.Left,
			})
		}
		sort.Slice(peers, func(i, j int) bool {
			return peers[i].PeerID < peers[j].PeerID
		})
		swarms = append(swarms, SwarmSnapshot{
			InfoHash: fmt.Sprintf("%x", infoHash),
			Peers:    peers,
		})
	}
	sort.Slice(swarms, func(i, j int) bool {
		return swarms[i].InfoHash < swarms[j].InfoHash
	})

	return Snapshot{
		IntervalSeconds: int64(s.interval / time.Second),
		PeerTTLSeconds:  int64(s.peerTTL / time.Second),
		Swarms:          swarms,
	}
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
