package torrent

import (
	"context"
	"fmt"
	"os"

	"github.com/mattcattb/go-torrent/protocol"
)

type Torrent struct {
	Meta protocol.MetaInfo

	pieces  PieceSet
	file    *os.File
	pending map[protocol.BlockRequest]*Peer

	Peers      map[protocol.PeerID]*Peer
	peerEvents chan PeerEvent

	TrackerID string

	uploaded   uint64
	downloaded uint64
}

func NewTorrent(meta protocol.MetaInfo) (*Torrent, error) {
	pieces, err := NewPieceSet(meta.Info)
	if err != nil {
		return nil, err
	}

	return &Torrent{
		Meta:       meta,
		pieces:     pieces,
		pending:    make(map[protocol.BlockRequest]*Peer),
		Peers:      make(map[protocol.PeerID]*Peer),
		peerEvents: make(chan PeerEvent),
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

func (t *Torrent) pendingCount(peer *Peer) int {

	count := 0

	for _, assignedPeer := range t.pending {
		if assignedPeer == peer {
			count += 1
		}
	}

	return count
}

func (t *Torrent) registerPeer(ctx context.Context, peer *Peer) bool {
	if _, exists := t.Peers[peer.ID]; exists {
		return false
	}
	t.Peers[peer.ID] = peer
	go peer.readLoop(ctx, t.peerEvents)
	go peer.writeLoop(ctx, t.peerEvents)

	return true
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

	if err := peer.sendMessage(protocol.Request{Block: request}); err != nil {
		delete(t.pending, request)
		return err
	}

	return nil
}

const maxPendingPerPeer = 8

func (t *Torrent) fillRequestWindow(peer *Peer) error {

	if peer == nil {
		return nil
	}

	if peer.State.PeerChoking || !peer.State.AmInterested {
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

func (t *Torrent) reserveNextRequest(peer *Peer) (protocol.BlockRequest, bool, error) {
	for index := 0; index < t.pieces.Count(); index++ {
		pieceIndex := uint32(index)
		if t.pieces.IsComplete(pieceIndex) || !peer.HasPiece(pieceIndex) {
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

func (t *Torrent) receiveBlock(peer *Peer, message protocol.Piece) (bool, error) {
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

func (t *Torrent) serveBlock(peer *Peer, request protocol.BlockRequest) error {
	if peer == nil || peer.State.AmChoking {
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

func (t *Torrent) handlePeerMessage(peer *Peer, message protocol.Message) error {
	if peer == nil {
		return fmt.Errorf("peer message has no peer")
	}

	switch message := message.(type) {
	case protocol.KeepAlive:
		return nil

	case protocol.Choke:
		peer.State.PeerChoking = true
		t.releasePendingRequests(peer)
		return nil

	case protocol.Unchoke:
		peer.State.PeerChoking = false
		return t.fillRequestWindow(peer)

	case protocol.Interested:
		peer.State.PeerInterested = true
		return nil

	case protocol.NotInterested:
		peer.State.PeerInterested = false
		return nil

	case protocol.Have:
		t.setPeerPiece(peer, message.PieceIndex)

		if err := t.updateInterest(peer); err != nil {
			return err
		}
		return t.fillRequestWindow(peer)

	case protocol.Bitfield:
		peer.Bitfield = append(peer.Bitfield[:0], message.Bits...)
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

		if err := peer.sendMessage(protocol.Interested{}); err != nil {
			return err
		}
	} else if err := peer.sendMessage(protocol.NotInterested{}); err != nil {
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

func (t *Torrent) broadcastMessage(message protocol.Message) {
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
