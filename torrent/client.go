package torrent

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/mattcattb/MoleSwarm/protocol"
)

var (
	ErrTorrentNotFound       = errors.New("torrent not found")
	ErrTorrentAlreadyRunning = errors.New("torrent already running")
	ErrTorrentNotRunning     = errors.New("torrent not running")
	ErrTorrentStopped        = errors.New("torrent stopped")
)

// Client owns the process-wide BitTorrent identity and routes incoming peers to
// the torrent session identified by their handshake.
type Client struct {
	peerID protocol.PeerID

	mu            sync.RWMutex
	torrents      map[protocol.InfoHash]*Torrent
	listenPort    uint16
	listenAddress net.Addr
}

type ClientStatus struct {
	PeerID             string   `json:"peerId"`
	ListenAddress      string   `json:"listenAddress"`
	RegisteredTorrents int      `json:"registeredTorrents"`
	InfoHashes         []string `json:"infoHashes"`
}

func (c *Client) buildStatus() ClientStatus {

	c.mu.RLock()
	defer c.mu.RUnlock()

	infoHashes := make([]string, 0)

	for _, t := range c.torrents {
		infoHashes = append(infoHashes, string(t.Meta.InfoHash[:]))
	}

	return ClientStatus{
		PeerID:             string(c.peerID[:]),
		ListenAddress:      c.listenAddress.String(),
		RegisteredTorrents: len(c.torrents),
		InfoHashes:         infoHashes,
	}

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

// PauseTorrentDownload pauses one running torrent without stopping the client.
func (c *Client) PauseTorrentDownload(ctx context.Context, infoHash protocol.InfoHash) error {

	torrent, exists := c.getTorrent(infoHash)

	if !exists {
		return fmt.Errorf(
			"pause torrent: info hash %x is not registered",
			infoHash,
		)
	}

	_, err := torrent.SetPaused(ctx, true)

	return err
}

// ResumeTorrentDownload resumes peer discovery and transfers for one paused torrent.
func (c *Client) ResumeTorrentDownload(ctx context.Context, infoHash protocol.InfoHash) error {

	torrent, exists := c.getTorrent(infoHash)

	if !exists {
		return fmt.Errorf(
			"pause torrent: info hash %x is not registered",
			infoHash,
		)

	}

	_, err := torrent.SetPaused(ctx, false)
	return err
}

// AddTorrent registers one torrent with this process-level client.
func (c *Client) RegisterTorrent(torrent *Torrent) error {
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

// RemoveTorrent stops one torrent without affecting the peer listener or other
// registered torrents.
func (c *Client) UnregisterTorrent(ctx context.Context, infoHash protocol.InfoHash) error {
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

// RunTorrent announces one registered torrent, connects the returned peers, and
// runs its transfer loop until the context is canceled.

func (c *Client) RunTorrent(ctx context.Context, infoHash protocol.InfoHash) error {
	torrent, exists := c.getTorrent(infoHash)
	if !exists {
		return fmt.Errorf("run torrent: info hash %x is not registered", infoHash)
	}

	config := torrentRunConfig{peerID: c.peerID, port: c.listenPort}

	return torrent.run(ctx, config)
}

// ListenForPeers creates the client's peer listener and records its advertised
// port before any torrent announces to a tracker.
func (c *Client) ListenForPeers(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for peers: %w", err)
	}

	c.setListenPort(listener)
	return listener, nil
}

func (c *Client) ServePeers(ctx context.Context, listener net.Listener) error {

	var pendingMu sync.Mutex
	pending := make(map[net.Conn]struct{})
	stopped := make(chan struct{})

	// watch for cancellation to close listener
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopped:
		}
	}()

	defer close(stopped)
	defer listener.Close()

	var handlers sync.WaitGroup

	defer func() {

		pendingMu.Lock()

		connections := make([]net.Conn, 0, len(pending))
		for conn := range pending {
			connections = append(connections, conn)
		}

		pendingMu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}

		handlers.Wait()

	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept peer connection: %w", err)
		}

		pendingMu.Lock()
		pending[conn] = struct{}{}
		pendingMu.Unlock()
		handlers.Add(1)

		go func() {
			defer func() {
				handlers.Done()
				pendingMu.Lock()
				delete(pending, conn)
				pendingMu.Unlock()
			}()
			err := c.handleIncomingConnection(ctx, conn)

			if err != nil {
				_ = conn.Close()
			}
		}()
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

func (c *Client) handleIncomingConnection(ctx context.Context, conn net.Conn) error {
	handshake, err := protocol.ReadHandshake(conn)
	if err != nil {
		return fmt.Errorf("read incomg peer handshake %w", err)
	}

	infoHash := handshake.InfoHash

	torrent, exists := c.getTorrent(infoHash)
	if !exists {
		return fmt.Errorf(
			"%w: %x",
			ErrTorrentNotFound,
			handshake.InfoHash,
		)

	}

	return torrent.acceptIncomingPeer(ctx, conn, handshake, c.peerID)

}

func (c *Client) getTorrent(infoHash protocol.InfoHash) (*Torrent, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	torrent, exists := c.torrents[infoHash]
	return torrent, exists
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
