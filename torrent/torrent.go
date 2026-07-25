package torrent

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/mattcattb/go-torrent/protocol"
)

type Torrent struct {
	Meta protocol.MetaInfo

	pieces  PieceSet
	file    *os.File
	pending map[protocol.BlockRequest]*peer

	peers      map[protocol.PeerID]*peer
	peerEvents chan peerEvent
	commands   chan torrentCommand

	TrackerID string

	uploaded   uint64
	downloaded uint64

	lifecycleMu sync.Mutex
	running     bool
	cancel      context.CancelFunc
	done        chan struct{}
	closeErr    error
}

type torrentCommand struct {
	stats chan<- torrentStats
}

type torrentStats struct {
	uploaded   uint64
	downloaded uint64
	left       uint64
}

func NewTorrent(meta protocol.MetaInfo) (*Torrent, error) {
	pieces, err := NewPieceSet(meta.Info)
	if err != nil {
		return nil, err
	}

	return &Torrent{
		Meta:       meta,
		pieces:     pieces,
		pending:    make(map[protocol.BlockRequest]*peer),
		peers:      make(map[protocol.PeerID]*peer),
		peerEvents: make(chan peerEvent),
		commands:   make(chan torrentCommand),
	}, nil
}

func OpenTorrent(meta protocol.MetaInfo, outputPath string) (*Torrent, error) {
	torrent, err := NewTorrent(meta)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(meta.Info.Length); err != nil {
		file.Close()
		return nil, err
	}

	torrent.file = file
	return torrent, nil
}

const maxPeers = int(5)

func (t *Torrent) pendingCount(peer *peer) int {

	count := 0

	for _, assignedPeer := range t.pending {
		if assignedPeer == peer {
			count += 1
		}
	}

	return count
}

func (t *Torrent) registerPeer(ctx context.Context, peer *peer) bool {
	if peer == nil || len(t.peers) >= maxPeers {
		return false
	}
	if _, exists := t.peers[peer.id]; exists {
		return false
	}
	t.peers[peer.id] = peer
	go peer.readLoop(ctx, t.peerEvents)
	go peer.writeLoop(ctx, t.peerEvents)

	return true
}

func (t *Torrent) Close() error {
	t.lifecycleMu.Lock()
	if !t.running {
		err := t.closeFile()
		t.lifecycleMu.Unlock()
		return err
	}

	cancel := t.cancel
	done := t.done
	t.lifecycleMu.Unlock()

	cancel()
	<-done

	t.lifecycleMu.Lock()
	err := t.closeErr
	t.lifecycleMu.Unlock()
	return err
}

func (t *Torrent) attachPeer(ctx context.Context, peer *peer) bool {
	t.lifecycleMu.Lock()
	running := t.running
	done := t.done
	t.lifecycleMu.Unlock()
	if !running {
		return false
	}

	select {
	case t.peerEvents <- peerEvent{peer: peer, Connected: true}:
		return true
	case <-ctx.Done():
		return false
	case <-done:
		return false
	}
}

func (t *Torrent) stats(ctx context.Context) (torrentStats, error) {
	t.lifecycleMu.Lock()
	running := t.running
	done := t.done
	t.lifecycleMu.Unlock()
	if !running {
		return torrentStats{}, fmt.Errorf("torrent is not running")
	}

	response := make(chan torrentStats, 1)
	select {
	case t.commands <- torrentCommand{stats: response}:
	case <-ctx.Done():
		return torrentStats{}, ctx.Err()
	case <-done:
		return torrentStats{}, fmt.Errorf("torrent stopped before reporting stats")
	}

	select {
	case stats := <-response:
		return stats, nil
	case <-ctx.Done():
		return torrentStats{}, ctx.Err()
	case <-done:
		return torrentStats{}, fmt.Errorf("torrent stopped before reporting stats")
	}
}

func (t *Torrent) run(ctx context.Context, ready chan<- error) (err error) {
	runCtx, cancel := context.WithCancel(ctx)

	t.lifecycleMu.Lock()
	if t.running {
		t.lifecycleMu.Unlock()
		ready <- fmt.Errorf("torrent is already running")
		cancel()
		return fmt.Errorf("torrent is already running")
	}
	t.running = true
	t.cancel = cancel
	t.done = make(chan struct{})
	t.closeErr = nil
	t.lifecycleMu.Unlock()
	ready <- nil

	defer func() {
		cancel()
		t.closePeers()

		t.lifecycleMu.Lock()
		t.closeErr = t.closeFile()
		t.running = false
		t.cancel = nil
		close(t.done)
		if err == nil && t.closeErr != nil {
			err = t.closeErr
		}
		t.lifecycleMu.Unlock()
	}()

	for {
		select {
		case event := <-t.peerEvents:
			if event.Connected {
				if !t.registerPeer(runCtx, event.peer) && event.peer != nil && event.peer.conn != nil {
					_ = event.peer.conn.Close()
				}
				continue
			}
			if event.Err != nil {
				t.removePeer(event.peer)
				continue
			}

			if err := t.handlePeerMessage(event.peer, event.Message); err != nil {
				t.removePeer(event.peer)
			}

		case command := <-t.commands:
			command.stats <- torrentStats{
				uploaded:   t.uploaded,
				downloaded: t.downloaded,
				left:       t.pieces.BytesLeft(),
			}

		case <-runCtx.Done():
			return runCtx.Err()
		}
	}
}

const maxPendingPerPeer = 8

func (t *Torrent) fillRequestWindow(peer *peer) error {

	if peer == nil {
		return nil
	}

	if peer.state.PeerChoking || !peer.state.AmInterested {
		return nil
	}

	for t.pendingCount(peer) < maxPendingPerPeer {
		// keep going for a peer until 8 requests are returned OR a error or non ok status is up
		req, ok, err := t.reserveNextRequest(peer)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		if err := peer.sendMessage(protocol.Request{Block: req}); err != nil {
			delete(t.pending, req)
			return err
		}
	}

	return nil
}

func (t *Torrent) reserveNextRequest(peer *peer) (protocol.BlockRequest, bool, error) {
	for index := 0; index < t.pieces.Count(); index++ {
		pieceIndex := uint32(index)
		if t.pieces.IsComplete(pieceIndex) || !peer.hasPiece(pieceIndex) {
			continue
		}

		request, ok, err := t.pieces.nextMissingBlock(pieceIndex, func(request protocol.BlockRequest) bool {
			_, pending := t.pending[request]
			return pending
		})
		if err != nil || !ok {
			if err != nil {
				return protocol.BlockRequest{}, false, err
			}
			continue
		}

		t.pending[request] = peer
		return request, true, nil
	}

	return protocol.BlockRequest{}, false, nil
}

func (t *Torrent) receiveBlock(peer *peer, message protocol.Piece) (bool, error) {
	request := protocol.BlockRequest{
		PieceIndex: message.PieceIndex,
		Begin:      message.Begin,
		Length:     uint32(len(message.Data)),
	}

	assignedPeer, pending := t.pending[request]
	if !pending || assignedPeer != peer {
		return false, fmt.Errorf("unexpected block for piece %d at offset %d", message.PieceIndex, message.Begin)
	}

	assembled, err := t.pieces.StoreBlock(request, message.Data)
	if err != nil {
		return false, err
	}

	delete(t.pending, request)
	t.downloaded += uint64(len(message.Data))

	if !assembled {
		return false, nil
	}

	if err := t.pieces.VerifyAndWrite(message.PieceIndex, t.file); err != nil {
		return false, err
	}

	return true, nil
}

func (t *Torrent) serveBlock(peer *peer, request protocol.BlockRequest) error {
	if peer == nil || peer.state.AmChoking {
		return nil
	}

	data, err := t.pieces.ReadBlock(request, t.file)
	if err != nil {
		return err
	}
	if err := peer.sendMessage(protocol.Piece{
		PieceIndex: request.PieceIndex,
		Begin:      request.Begin,
		Data:       data,
	}); err != nil {
		return err
	}

	t.uploaded += uint64(len(data))
	return nil
}

func (t *Torrent) handlePeerMessage(peer *peer, message protocol.Message) error {
	if peer == nil {
		return fmt.Errorf("peer message has no peer")
	}

	switch message := message.(type) {
	case protocol.KeepAlive:
		return nil

	case protocol.Choke:
		peer.state.PeerChoking = true
		t.releasePendingRequests(peer)
		return nil

	case protocol.Unchoke:
		peer.state.PeerChoking = false
		return t.fillRequestWindow(peer)

	case protocol.Interested:
		peer.state.PeerInterested = true
		return nil

	case protocol.NotInterested:
		peer.state.PeerInterested = false
		return nil

	case protocol.Have:
		t.setPeerPiece(peer, message.PieceIndex)

		if err := t.updateInterest(peer); err != nil {
			return err
		}
		return t.fillRequestWindow(peer)

	case protocol.Bitfield:
		peer.bitfield = append(peer.bitfield[:0], message.Bits...)
		if err := t.updateInterest(peer); err != nil {
			return err
		}
		return t.fillRequestWindow(peer)

	case protocol.Request:
		return t.serveBlock(peer, message.Block)

	case protocol.CancelRequest:
		return nil

	case protocol.Piece:
		completed, err := t.receiveBlock(peer, message)
		if err != nil {
			return err
		}
		if completed {
			t.broadcastMessage(protocol.Have{PieceIndex: message.PieceIndex})
		}

		return t.fillRequestWindow(peer)
	}

	return nil
}

func (t *Torrent) updateInterest(peer *peer) error {
	interested := false
	for index := 0; index < t.pieces.Count(); index++ {
		pieceIndex := uint32(index)
		if !t.pieces.IsComplete(pieceIndex) && peer.hasPiece(pieceIndex) {
			interested = true
			break
		}
	}

	if interested == peer.state.AmInterested {
		return nil
	}

	if interested {

		if err := peer.sendMessage(protocol.Interested{}); err != nil {
			return err
		}
	} else if err := peer.sendMessage(protocol.NotInterested{}); err != nil {
		return err
	}

	peer.state.AmInterested = interested
	return nil
}

func (t *Torrent) setPeerPiece(peer *peer, index uint32) {
	byteIndex := index / 8
	if int(byteIndex) >= len(peer.bitfield) {
		peer.bitfield = append(peer.bitfield, make([]byte, int(byteIndex)+1-len(peer.bitfield))...)
	}

	peer.bitfield[byteIndex] |= byte(1 << (7 - index%8))
}

func (t *Torrent) releasePendingRequests(peer *peer) {
	for request, assignedPeer := range t.pending {
		if assignedPeer == peer {
			delete(t.pending, request)
		}
	}
}

func (t *Torrent) broadcastMessage(message protocol.Message) {
	for _, peer := range t.peers {
		if err := peer.sendMessage(message); err != nil {
			t.removePeer(peer)
		}
	}
}

func (t *Torrent) removePeer(peer *peer) {
	if peer == nil {
		return
	}

	t.releasePendingRequests(peer)
	if current, ok := t.peers[peer.id]; ok && current == peer {
		delete(t.peers, peer.id)
	}
	if peer.conn != nil {
		_ = peer.conn.Close()
	}
}

func (t *Torrent) closePeers() {
	for _, peer := range t.peers {
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
	}
	t.peers = make(map[protocol.PeerID]*peer)
	t.pending = make(map[protocol.BlockRequest]*peer)
}

func (t *Torrent) closeFile() error {
	if t.file == nil {
		return nil
	}

	err := t.file.Close()
	t.file = nil
	return err
}
