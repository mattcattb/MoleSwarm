package torrent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
)

type Torrent struct {
	Meta protocol.MetaInfo

	// torrent run manages
	pieces     PieceSet
	file       *os.File
	pending    map[protocol.BlockRequest]*peer
	peers      map[protocol.PeerID]*peer
	uploaded   uint64
	downloaded uint64
	paused     bool
	TrackerID  string

	// consumed by torrent.run
	peerEvents       chan peerEvent
	pauseRequests    chan pauseRequest
	snapshotRequests chan snapshotRequest

	// lifecycle mu protects
	lifecycleMu sync.Mutex
	running     bool
	cancel      context.CancelFunc
	done        chan struct{}
	closeErr    error
}

type pauseRequest struct {
	paused bool
	reply  chan<- pauseResult
}

type pauseResult struct {
	changed bool
	err     error
}

func NewTorrent(meta protocol.MetaInfo) (*Torrent, error) {
	pieces, err := NewPieceSet(meta.Info)
	if err != nil {
		return nil, err
	}

	return &Torrent{
		Meta:             meta,
		pieces:           pieces,
		pending:          make(map[protocol.BlockRequest]*peer),
		peers:            make(map[protocol.PeerID]*peer),
		peerEvents:       make(chan peerEvent),
		snapshotRequests: make(chan snapshotRequest, 1),
		pauseRequests:    make(chan pauseRequest, 1),
	}, nil
}

func OpenDownload(meta protocol.MetaInfo, outputPath string) (*Torrent, error) {
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

// OpenSeed opens existing data for upload and verifies every piece before the
// session is allowed to advertise it to peers.
func OpenSeed(meta protocol.MetaInfo, dataPath string) (*Torrent, error) {
	torrent, err := NewTorrent(meta)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	if err := torrent.pieces.VerifyExisting(file); err != nil {
		_ = file.Close()
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
	<-done // waits until all operations are done!

	t.lifecycleMu.Lock()
	err := t.closeErr
	t.lifecycleMu.Unlock()
	return err
}

var errTorrentAlreadyRunning = errors.New("torrent already running")

func (t *Torrent) SetPaused(ctx context.Context, paused bool) (bool, error) {
	t.lifecycleMu.Lock()
	running := t.running // if torrent is running or not
	done := t.done       // if torrent is completed or not
	t.lifecycleMu.Unlock()
	if !running {
		return false, fmt.Errorf("torrent is not running")
	}

	reply := make(chan pauseResult, 1)

	request := pauseRequest{
		paused: paused,
		reply:  reply,
	}

	torrentCompletedBeforeError := fmt.Errorf("torrent stopped before changing lifecycle state")

	select {
	case t.pauseRequests <- request:
	case <-ctx.Done():
		return false, ctx.Err()
	case <-done:
		return false, torrentCompletedBeforeError
	}

	select {
	case reply := <-reply:
		return reply.changed, reply.err
	case <-ctx.Done():
		return false, ctx.Err()
	case <-done:
		return false, torrentCompletedBeforeError
	}
}

type torrentRunConfig struct {
	peerID protocol.PeerID
	port   uint16
}

func (t *Torrent) beginRun(cancel context.CancelFunc) error {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()

	if t.running {
		return errTorrentAlreadyRunning
	}

	t.running = true
	t.cancel = cancel
	t.done = make(chan struct{})
	t.closeErr = nil
	return nil
}

func (t *Torrent) finishRun(runErr error) error {
	t.closePeers()
	fileErr := t.closeFile()

	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()

	t.running = false
	t.cancel = nil
	t.closeErr = fileErr

	close(t.done)

	if runErr == nil && fileErr != nil {
		return fileErr
	}

	return runErr
}

func (t *Torrent) run(ctx context.Context, config torrentRunConfig) (err error) {
	runCtx, cancel := context.WithCancel(ctx)

	if err := t.beginRun(cancel); err != nil {
		cancel()
		return err
	}

	defer func() {
		cancel()
		err = t.finishRun(err)
	}()

	resp, err := t.announce(runCtx, config, tracker.StartedEvent)

	if err != nil {
		return err
	}

	t.connectTrackerPeers(runCtx, config, resp.Peers)

	interval := normalizeTrackerInterval(resp.Interval)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	reannounceC := timer.C

	for {
		select {

		case event := <-t.peerEvents:
			t.handlePeerEvent(runCtx, event)

		case request := <-t.snapshotRequests:
			request.reply <- t.buildSnapshot()
		case setPause := <-t.pauseRequests:
			changed := t.applyPaused(setPause.paused)
			if !changed {
				setPause.reply <- pauseResult{}
				continue
			}

			if setPause.paused {
				timer.Stop()
				reannounceC = nil
				_, announceErr := t.announce(runCtx, config, tracker.StoppedEvent)
				setPause.reply <- pauseResult{changed: true, err: announceErr}
				continue
			}

			resp, announceErr := t.announce(runCtx, config, tracker.StartedEvent)
			if announceErr == nil {
				interval = normalizeTrackerInterval(resp.Interval)
				t.connectTrackerPeers(runCtx, config, resp.Peers)
				timer.Reset(interval)
				reannounceC = timer.C
			}
			setPause.reply <- pauseResult{changed: true, err: announceErr}

		case <-reannounceC:
			resp, announceErr := t.announce(runCtx, config, "")

			if announceErr == nil {
				interval = normalizeTrackerInterval(resp.Interval)
				t.connectTrackerPeers(runCtx, config, resp.Peers)
			}

			timer.Reset(interval)
			reannounceC = timer.C

		case <-runCtx.Done():
			return runCtx.Err()
		}
	}
}

func (t *Torrent) transferState() TransferState {
	if t.paused {
		return TransferPaused
	}
	if t.pieces.Complete() {
		return TransferCompleted
	}
	return TransferDownloading
}

const maxPendingPerPeer = 8

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

func (t *Torrent) activatePeer(ctx context.Context, peer *peer) error {
	if peer == nil {
		return errors.New("peer is nil")
	}

	if t.paused {
		return errors.New("torrent is paused")
	}

	if len(t.peers) >= maxPeers {
		return errors.New("peer limit reached")
	}

	if _, exists := t.peers[peer.id]; exists {
		return errors.New("peer is already active")
	}

	if !peer.tryEnqueue(protocol.Bitfield{
		Bits: t.PiecesBitfield(),
	}) {
		return errPeerWriteQueueFull
	}

	t.peers[peer.id] = peer

	go peer.readLoop(ctx, t.peerEvents)
	go peer.writeLoop(ctx, t.peerEvents)

	return nil

}

// establish tcp handshake + return peer
func (t *Torrent) connectPeer(ctx context.Context, config torrentRunConfig, candidate tracker.Peer) (*peer, error) {

	conn, err := dialPeer(ctx, candidate)

	if err != nil {
		return nil, err
	}

	if err := writePeerHandshake(conn, t.Meta.InfoHash, config.peerID); err != nil {
		_ = conn.Close()
		return nil, err
	}

	handshake, err := protocol.ReadHandshake(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if err := validatePeerHandshake(handshake, t.Meta.InfoHash, config.peerID); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return newPeer(conn, handshake), nil
}

func (t *Torrent) removePeer(peer *peer) {
	if peer == nil {
		return
	}

	t.releasePendingRequests(peer)
	if current, ok := t.peers[peer.id]; ok && current == peer {
		delete(t.peers, peer.id)
	}
	t.closePeer(peer)
}

func (t *Torrent) closePeers() {
	for _, peer := range t.peers {
		t.closePeer(peer)
	}
	t.peers = make(map[protocol.PeerID]*peer)
	t.pending = make(map[protocol.BlockRequest]*peer)
}

func (t *Torrent) closePeer(peer *peer) {

	if peer == nil {
		return
	}

	if peer.conn != nil {
		_ = peer.conn.Close()
	}
}

func (t *Torrent) closeFile() error {
	if t.file == nil {
		return nil
	}

	err := t.file.Close()
	t.file = nil
	return err
}

func (t *Torrent) applyPaused(paused bool) bool {

	changed := t.paused != paused

	if !changed {
		return false
	}

	t.paused = paused

	if paused {
		t.closePeers()
	}

	return true
}

func (t *Torrent) connectTrackerPeers(ctx context.Context, config torrentRunConfig, candidates []tracker.Peer) {

	for _, candidate := range candidates {
		// if err := t.conn

		if len(t.peers) >= maxPeers {
			return
		}

		peer, err := t.connectPeer(ctx, config, candidate)

		if err != nil {
			continue
		}

		if err := t.activatePeer(ctx, peer); err != nil {
			t.closePeer(peer)
		}

	}
}

func (t *Torrent) acceptIncomingPeer(ctx context.Context, conn net.Conn, handshake protocol.Handshake, peerId protocol.PeerID) error {
	t.lifecycleMu.Lock()
	running := t.running
	done := t.done
	t.lifecycleMu.Unlock()

	if !running {
		return ErrTorrentNotRunning
	}

	if err := validatePeerHandshake(handshake, t.Meta.InfoHash, peerId); err != nil {
		return err
	}

	if err := writePeerHandshake(conn, t.Meta.InfoHash, peerId); err != nil {
		return err
	}

	peer := newPeer(conn, handshake)

	peer.incoming = true

	select {
	case t.peerEvents <- peerEvent{
		kind: peerArrived,
		peer: peer,
	}:
		return nil

	case <-ctx.Done():
		return ctx.Err()

	case <-done:
		return ErrTorrentStopped
	}

}
