package torrent

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"

	"github.com/mattcattb/MoleSwarm/protocol"
)

var (
	ErrAlreadyRunning = errors.New("torrent already running")
	ErrNotRunning     = errors.New("torrent not running")
	ErrStopped        = errors.New("torrent stopped")
)

type Torrent struct {
	Meta protocol.MetaInfo

	// torrent run manages
	pieces       PieceSet
	file         *os.File
	pending      map[protocol.BlockRequest]*peer
	peers        map[protocol.PeerID]*peer
	uploaded     uint64
	downloaded   uint64
	downloadMode downloadMode
	TrackerID    string

	// consumed by torrent.run
	peerEvents           chan peerEvent
	downloadModeRequests chan downloadModeRequest
	statusRequests       chan statusRequest

	// lifecycle mu protects
	lifecycleMu sync.Mutex
	running     bool
	cancel      context.CancelFunc
	done        chan struct{}
	closeErr    error
}

func New(meta protocol.MetaInfo) (*Torrent, error) {
	pieces, err := NewPieceSet(meta.Info)
	if err != nil {
		return nil, err
	}

	return &Torrent{
		Meta:                 meta,
		pieces:               pieces,
		pending:              make(map[protocol.BlockRequest]*peer),
		peers:                make(map[protocol.PeerID]*peer),
		peerEvents:           make(chan peerEvent),
		statusRequests:       make(chan statusRequest, 1),
		downloadModeRequests: make(chan downloadModeRequest, 1),
	}, nil
}

func OpenDownload(meta protocol.MetaInfo, outputPath string) (*Torrent, error) {
	torrent, err := New(meta)
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
	torrent, err := New(meta)
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

const maxPendingPerPeer = 8

func (t *Torrent) releasePendingRequests(peer *peer) {
	for request, assignedPeer := range t.pending {
		if assignedPeer == peer {
			delete(t.pending, request)
		}
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

func (t *Torrent) changeDownloadMode(mode downloadMode) error {
	if t.downloadMode == mode {
		return nil
	}

	switch t.downloadMode {
	case downloadModePaused:
		// pause
		t.downloadMode = downloadModePaused
		t.broadcastMessage(protocol.NotInterested{})
	case downloadModeProgress:
		t.downloadMode = downloadModeProgress

		t.broadcastMessage(protocol.Interested{})
		// ! TODO maybe refil areas here? depends if we pause the blocks and areas here
	}
	return nil
}

func (t *Torrent) AcceptIncomingPeer(ctx context.Context, conn net.Conn, handshake protocol.Handshake, peerId protocol.PeerID) error {
	t.lifecycleMu.Lock()
	running := t.running
	done := t.done
	t.lifecycleMu.Unlock()

	if !running {
		return ErrAlreadyRunning
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
		return ErrStopped
	}

}
