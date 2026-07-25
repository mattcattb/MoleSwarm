package torrent

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"

	"github.com/mattcattb/go-torrent/protocol"
	"github.com/mattcattb/go-torrent/tracker"
)

// Client owns the process-wide BitTorrent identity and routes incoming peers to
// the torrent session identified by their handshake.
type Client struct {
	peerID protocol.PeerID

	mu         sync.RWMutex
	torrents   map[protocol.InfoHash]*Torrent
	listenPort uint16
}

func NewClient() (*Client, error) {
	var peerID protocol.PeerID
	if _, err := rand.Read(peerID[:]); err != nil {
		return nil, fmt.Errorf("generate peer ID: %w", err)
	}

	return &Client{
		peerID:   peerID,
		torrents: make(map[protocol.InfoHash]*Torrent),
	}, nil
}

func (c *Client) PeerID() protocol.PeerID {
	return c.peerID
}

// AddTorrent attaches one swarm session to this process-level client.
func (c *Client) AddTorrent(torrent *Torrent) error {
	if torrent == nil {
		return fmt.Errorf("add torrent: torrent is nil")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	infoHash := torrent.Meta.InfoHash
	if _, exists := c.torrents[infoHash]; exists {
		return fmt.Errorf("add torrent: info hash %x is already registered", infoHash)
	}

	c.torrents[infoHash] = torrent
	return nil
}

// RemoveTorrent stops one swarm without affecting the client listener or other
// registered swarms.
func (c *Client) RemoveTorrent(infoHash protocol.InfoHash) error {
	c.mu.Lock()
	torrent, exists := c.torrents[infoHash]
	if exists {
		delete(c.torrents, infoHash)
	}
	c.mu.Unlock()

	if !exists {
		return fmt.Errorf("remove torrent: info hash %x is not registered", infoHash)
	}
	return torrent.Close()
}

// RunTorrent announces one registered swarm, connects the returned peers, and
// runs its transfer loop until the context is canceled.
func (c *Client) RunTorrent(ctx context.Context, infoHash protocol.InfoHash) error {
	torrent, exists := c.torrent(infoHash)
	if !exists {
		return fmt.Errorf("run torrent: info hash %x is not registered", infoHash)
	}

	response, err := c.announce(ctx, torrent, tracker.StartedEvent)
	if err != nil {
		return err
	}

	for _, candidate := range response.Peers {
		if len(torrent.Peers) >= maxPeers {
			break
		}
		if err := c.connect(ctx, torrent, candidate); err != nil {
			continue
		}
	}

	return torrent.Run(ctx)
}

func (c *Client) Serve(ctx context.Context, listener net.Listener) error {
	c.setListenPort(listener)
	defer c.closeTorrents()

	var pendingMu sync.Mutex
	pending := make(map[net.Conn]struct{})
	stopping := false
	closePending := func() {
		pendingMu.Lock()
		defer pendingMu.Unlock()

		stopping = true
		for conn := range pending {
			_ = conn.Close()
		}
	}
	defer closePending()

	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopped:
		}
	}()
	defer close(stopped)
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept peer connection: %w", err)
		}

		pendingMu.Lock()
		if stopping {
			pendingMu.Unlock()
			_ = conn.Close()
			continue
		}
		pending[conn] = struct{}{}
		pendingMu.Unlock()

		go func(conn net.Conn) {
			defer func() {
				pendingMu.Lock()
				delete(pending, conn)
				pendingMu.Unlock()
			}()
			c.routeIncomingPeer(ctx, conn)
		}(conn)
	}
}

func (c *Client) setListenPort(listener net.Listener) {
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address.Port < 0 || address.Port > 65535 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.listenPort = uint16(address.Port)
}

func (c *Client) routeIncomingPeer(ctx context.Context, conn net.Conn) {
	handshake, err := protocol.ReadHandshake(conn)
	if err != nil {
		_ = conn.Close()
		return
	}

	torrent, exists := c.torrent(handshake.InfoHash)
	if !exists {
		_ = conn.Close()
		return
	}

	if err := c.validateHandshake(torrent, handshake); err != nil {
		_ = conn.Close()
		return
	}
	if err := c.sendPeerHandshake(torrent, conn); err != nil {
		_ = conn.Close()
		return
	}

	peer := CreatePeer(conn, handshake)
	if !torrent.registerPeer(ctx, peer) {
		_ = conn.Close()
	}
}

func (c *Client) torrent(infoHash protocol.InfoHash) (*Torrent, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	torrent, exists := c.torrents[infoHash]
	return torrent, exists
}

func (c *Client) announce(ctx context.Context, torrent *Torrent, event tracker.AnnounceEvent) (tracker.AnnounceResponse, error) {
	c.mu.RLock()
	port := c.listenPort
	c.mu.RUnlock()

	return tracker.Announce(ctx, torrent.Meta.Announce, tracker.AnnounceRequest{
		Port:       port,
		Uploaded:   torrent.uploaded,
		Downloaded: torrent.downloaded,
		Left:       torrent.BytesLeft(),
		InfoHash:   torrent.Meta.InfoHash,
		PeerID:     c.peerID,
		Compact:    true,
		Event:      event,
	})
}

func (c *Client) connect(ctx context.Context, torrent *Torrent, candidate tracker.Peer) error {
	conn, err := DialPeer(ctx, candidate)
	if err != nil {
		return err
	}
	if err := c.sendPeerHandshake(torrent, conn); err != nil {
		_ = conn.Close()
		return err
	}

	handshake, err := protocol.ReadHandshake(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := c.validateHandshake(torrent, handshake); err != nil {
		_ = conn.Close()
		return err
	}

	peer := CreatePeer(conn, handshake)
	if !torrent.registerPeer(ctx, peer) {
		_ = conn.Close()
	}
	return nil
}

func (c *Client) sendPeerHandshake(torrent *Torrent, conn net.Conn) error {
	return protocol.WriteHandshake(conn, protocol.Handshake{
		InfoHash: torrent.Meta.InfoHash,
		PeerID:   c.peerID,
		Protocol: protocol.PeerProtocol,
	})
}

func (c *Client) validateHandshake(torrent *Torrent, handshake protocol.Handshake) error {
	if handshake.InfoHash != torrent.Meta.InfoHash {
		return fmt.Errorf("handshake info hash does not match torrent info hash")
	}
	if handshake.PeerID == c.peerID {
		return fmt.Errorf("peer ID is the same as the client peer ID")
	}
	if handshake.Protocol != protocol.PeerProtocol {
		return fmt.Errorf("invalid peer protocol used")
	}
	return nil
}

func (c *Client) closeTorrents() {
	c.mu.RLock()
	torrents := make([]*Torrent, 0, len(c.torrents))
	for _, torrent := range c.torrents {
		torrents = append(torrents, torrent)
	}
	c.mu.RUnlock()

	for _, torrent := range torrents {
		_ = torrent.Close()
	}
}
