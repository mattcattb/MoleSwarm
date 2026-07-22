package torrent

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
)

type Torrent struct {
	Meta MetaInfo

	pieces  PieceSet
	file    *os.File
	pending map[BlockRequest]*Peer

	Peers      map[PeerID]*Peer
	peerEvents chan PeerEvent

	PeerID     PeerID
	listenPort uint16
	TrackerID  string

	uploaded   uint64
	downloaded uint64
}

func NewTorrent(meta MetaInfo, port uint16) (*Torrent, error) {
	pieces, err := NewPieceSet(meta.Info)
	if err != nil {
		return nil, err
	}

	var peerID PeerID
	if _, err := rand.Read(peerID[:]); err != nil {
		return nil, fmt.Errorf("generate peer ID: %w", err)
	}

	return &Torrent{
		Meta:       meta,
		pieces:     pieces,
		pending:    make(map[BlockRequest]*Peer),
		Peers:      make(map[PeerID]*Peer),
		peerEvents: make(chan PeerEvent),
		PeerID:     peerID,
		listenPort: port,
	}, nil
}

func OpenTorrent(meta MetaInfo, outputPath string) (*Torrent, error) {
	torrent, err := NewTorrent(meta, 0)
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

func (t *Torrent) Start(ctx context.Context) error {
	response, err := t.announce(ctx, StartedEvent)

	if err != nil {
		return err
	}

	for _, canidate := range response.Peers {
		if len(t.Peers) > maxPeers {
			break
		}
		if err := t.Connect(ctx, canidate); err != nil {
			continue
		}
	}

	return t.Run(ctx)
}

func (t *Torrent) announce(ctx context.Context, event AnnounceEvent) (TrackerResponse, error) {
	return Announce(ctx, t.Meta.Announce, TrackerAnnounceRequest{
		port:       t.listenPort,
		uploaded:   t.uploaded,
		downloaded: t.downloaded,
		left:       t.BytesLeft(),
		InfoHash:   t.Meta.InfoHash,
		PeerID:     t.PeerID,
		Event:      event,
	})
}

func (t *Torrent) Connect(ctx context.Context, candidate PeerRecord) error {
	// make peer connection then add here?

	conn, err := DialPeer(ctx, candidate)

	if err != nil {
		return err
	}

	peer, err := CompleteHandshake(conn, t.Meta, t.PeerID)

	if err != nil {
		conn.Close()
		return err
	}

	if _, exists := t.Peers[peer.ID]; exists {
		conn.Close()
		return nil
	}

	t.Peers[peer.ID] = peer
	go peer.readLoop(ctx, t.peerEvents)

	return nil

}

func (t *Torrent) Close() error {
	for _, peer := range t.Peers {
		if peer.Conn != nil {
			_ = peer.Conn.Close()
		}
	}

	if t.file == nil {
		return nil
	}

	err := t.file.Close()
	t.file = nil
	return err
}

func (t *Torrent) BytesLeft() uint64 {
	return t.pieces.BytesLeft()
}

func (t *Torrent) Run(ctx context.Context) error {
	for {
		select {
		case event := <-t.peerEvents:
			if event.Err != nil {
				t.removePeer(event.Peer)
				continue
			}

			if err := t.handlePeerMessage(event.Peer, event.Message); err != nil {
				t.removePeer(event.Peer)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (t *Torrent) NextRequest(peer *Peer) error {
	if peer == nil {
		return fmt.Errorf("cannot request a block without a peer")
	}
	if peer.State.PeerChoking || !peer.State.AmInterested {
		return nil
	}

	request, ok, err := t.reserveNextRequest(peer)
	if err != nil || !ok {
		return err
	}

	if err := peer.sendMessage(Request{Block: request}); err != nil {
		delete(t.pending, request)
		return err
	}

	return nil
}

func (t *Torrent) reserveNextRequest(peer *Peer) (BlockRequest, bool, error) {
	for index := 0; index < t.pieces.Count(); index++ {
		pieceIndex := uint32(index)
		if t.pieces.IsComplete(pieceIndex) || !peer.HasPiece(pieceIndex) {
			continue
		}

		request, ok, err := t.pieces.nextMissingBlock(pieceIndex, func(request BlockRequest) bool {
			_, pending := t.pending[request]
			return pending
		})
		if err != nil || !ok {
			if err != nil {
				return BlockRequest{}, false, err
			}
			continue
		}

		t.pending[request] = peer
		return request, true, nil
	}

	return BlockRequest{}, false, nil
}

func (t *Torrent) receiveBlock(peer *Peer, message Piece) (bool, error) {
	request := BlockRequest{
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

func (t *Torrent) serveBlock(peer *Peer, request BlockRequest) error {
	if peer == nil || peer.State.AmChoking {
		return nil
	}

	data, err := t.pieces.ReadBlock(request, t.file)
	if err != nil {
		return err
	}
	if err := peer.sendMessage(Piece{
		PieceIndex: request.PieceIndex,
		Begin:      request.Begin,
		Data:       data,
	}); err != nil {
		return err
	}

	t.uploaded += uint64(len(data))
	return nil
}

func (t *Torrent) handlePeerMessage(peer *Peer, message PeerMessage) error {
	if peer == nil {
		return fmt.Errorf("peer message has no peer")
	}

	switch message := message.(type) {
	case KeepAlive:
		return nil

	case Choke:
		peer.State.PeerChoking = true
		t.releasePendingRequests(peer)
		return nil

	case Unchoke:
		peer.State.PeerChoking = false
		return t.NextRequest(peer)

	case Interested:
		peer.State.PeerInterested = true
		return nil

	case NotInterested:
		peer.State.PeerInterested = false
		return nil

	case Have:
		t.setPeerPiece(peer, message.PieceIndex)

		if err := t.updateInterest(peer); err != nil {
			return err
		}
		return t.NextRequest(peer)

	case Bitfield:
		peer.Bitfield = append(peer.Bitfield[:0], message.Bits...)
		if err := t.updateInterest(peer); err != nil {
			return err
		}
		return t.NextRequest(peer)

	case Request:
		return t.serveBlock(peer, message.Block)

	case CancelRequest:
		return nil

	case Piece:
		completed, err := t.receiveBlock(peer, message)
		if err != nil {
			return err
		}
		if completed {
			t.broadcastMessage(Have{PieceIndex: message.PieceIndex})
		}
		return t.NextRequest(peer)
	}

	return nil
}

func (t *Torrent) updateInterest(peer *Peer) error {
	interested := false
	for index := 0; index < t.pieces.Count(); index++ {
		pieceIndex := uint32(index)
		if !t.pieces.IsComplete(pieceIndex) && peer.HasPiece(pieceIndex) {
			interested = true
			break
		}
	}

	if interested == peer.State.AmInterested {
		return nil
	}

	if interested {
		if err := peer.sendMessage(Interested{}); err != nil {
			return err
		}
	} else if err := peer.sendMessage(NotInterested{}); err != nil {
		return err
	}

	peer.State.AmInterested = interested
	return nil
}

func (t *Torrent) setPeerPiece(peer *Peer, index uint32) {
	byteIndex := index / 8
	if int(byteIndex) >= len(peer.Bitfield) {
		peer.Bitfield = append(peer.Bitfield, make([]byte, int(byteIndex)+1-len(peer.Bitfield))...)
	}

	peer.Bitfield[byteIndex] |= byte(1 << (7 - index%8))
}

func (t *Torrent) releasePendingRequests(peer *Peer) {
	for request, assignedPeer := range t.pending {
		if assignedPeer == peer {
			delete(t.pending, request)
		}
	}
}

func (t *Torrent) broadcastMessage(message PeerMessage) {
	for _, peer := range t.Peers {
		if err := peer.sendMessage(message); err != nil {
			t.removePeer(peer)
		}
	}
}

func (t *Torrent) removePeer(peer *Peer) {
	if peer == nil {
		return
	}

	t.releasePendingRequests(peer)
	if current, ok := t.Peers[peer.ID]; ok && current == peer {
		delete(t.Peers, peer.ID)
	}
	if peer.Conn != nil {
		_ = peer.Conn.Close()
	}
}
