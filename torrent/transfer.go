package torrent

import (
	"fmt"

	"github.com/mattcattb/MoleSwarm/protocol"
)

func (t *Torrent) handleProtocolMessage(peer *peer, message protocol.Message) error {
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
		if peer.state.AmChoking {
			if err := peer.sendMessage(protocol.Unchoke{}); err != nil {
				return err
			}
			peer.state.AmChoking = false
		}
		return nil

	case protocol.NotInterested:
		peer.state.PeerInterested = false
		return nil

	case protocol.Have:
		peer.setPiece(message.PieceIndex)

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
