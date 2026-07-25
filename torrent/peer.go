package torrent

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/mattcattb/go-torrent/protocol"
	"github.com/mattcattb/go-torrent/tracker"
)

const peerWriteQueueSize = 32

var errPeerWriteQueueFull = errors.New("peer write queue is full")

type peerState struct {
	AmChoking      bool
	AmInterested   bool
	PeerChoking    bool
	PeerInterested bool
}

type peer struct {
	conn     net.Conn
	id       protocol.PeerID
	infoHash protocol.InfoHash
	state    peerState
	bitfield []byte
	outgoing chan protocol.Message
}

func dialPeer(ctx context.Context, peerRecord tracker.Peer) (net.Conn, error) {
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, "tcp", netip.AddrPortFrom(peerRecord.IP, peerRecord.Port).String())
}

func newPeer(conn net.Conn, handshake protocol.Handshake) *peer {
	return &peer{
		conn:     conn,
		id:       handshake.PeerID,
		infoHash: handshake.InfoHash,
		state: peerState{
			AmChoking:   true,
			PeerChoking: true,
		},
		outgoing: make(chan protocol.Message, peerWriteQueueSize),
	}
}

func (p *peer) hasPiece(index uint32) bool {
	byteIndex := index / 8
	if int(byteIndex) >= len(p.bitfield) {
		return false
	}

	mask := byte(1 << (7 - index%8))
	return p.bitfield[byteIndex]&mask != 0
}

func (p *peer) sendMessage(message protocol.Message) error {
	if p.queue(message) {
		return nil
	}
	return errPeerWriteQueueFull
}

type peerEvent struct {
	peer      *peer
	Connected bool
	Message   protocol.Message
	Err       error
}

func (p *peer) readLoop(ctx context.Context, events chan<- peerEvent) {

	for {

		msg, err := protocol.ReadMessage(p.conn)

		event := peerEvent{
			peer:    p,
			Message: msg,
			Err:     err,
		}

		select {
		case events <- event:
		case <-ctx.Done():
			return
		}

		if err != nil {
			return
		}

	}
}

func (p *peer) queue(message protocol.Message) bool {
	select {
	case p.outgoing <- message:
		return true

	default:
		return false
	}
}

func (p *peer) setPieceHave(index uint32) {
	byteIndex := index / 8
	mask := byte(1 << (7 - index%8))
	if int(byteIndex) >= len(p.bitfield) {
		p.bitfield = append(p.bitfield, make([]byte, int(byteIndex)+1-len(p.bitfield))...)
	}
	p.bitfield[byteIndex] |= mask
}

func (p *peer) writeLoop(ctx context.Context, events chan<- peerEvent) {
	for {

		select {
		case message := <-p.outgoing:
			if err := protocol.WriteMessage(p.conn, message); err != nil {
				select {
				case events <- peerEvent{peer: p, Err: err}:
				case <-ctx.Done():

				}
				return
			}

		case <-ctx.Done():
			return
		}

	}
}
