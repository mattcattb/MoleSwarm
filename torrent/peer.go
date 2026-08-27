package torrent

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
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
	incoming bool
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
	if p.tryEnqueue(message) {
		return nil
	}
	return errPeerWriteQueueFull
}

func (t *Torrent) broadcastMessage(message protocol.Message) {
	for _, peer := range t.peers {
		if err := peer.sendMessage(message); err != nil {
			t.removePeer(peer)
		}
	}
}

type peerEventKind uint8

const (
	peerEventUnknown peerEventKind = iota
	peerArrived
	peerMessageReceived
	peerDisconnected
)

type peerEvent struct {
	kind    peerEventKind
	peer    *peer
	Message protocol.Message
	Err     error
}

func (t *Torrent) handlePeerEvent(ctx context.Context, event peerEvent) {

	switch event.kind {

	case peerArrived:

		err := t.activatePeer(ctx, event.peer)

		if err != nil && event.peer != nil && event.peer.conn != nil {
			t.closePeer(event.peer)

		}
	case peerMessageReceived:
		if !t.hasPeer(event.peer) {
			return
		}
		if err := t.handleProtocolMessage(event.peer, event.Message); err != nil {
			t.removePeer(event.peer)
		}
	case peerDisconnected:
		t.removePeer(event.peer)
	}

}

func (p *peer) readLoop(ctx context.Context, events chan<- peerEvent) {

	for {

		msg, err := protocol.ReadMessage(p.conn)
		if err != nil {
			select {
			case events <- peerEvent{
				kind: peerDisconnected,
				peer: p,
				Err:  err,
			}:
			case <-ctx.Done():
			}
			return
		}

		select {
		case events <- peerEvent{
			kind:    peerMessageReceived,
			peer:    p,
			Message: msg,
		}:
		case <-ctx.Done():
			return
		}

	}
}

func (p *peer) setPiece(index uint32) {
	byteIndex := index / 8
	if int(byteIndex) >= len(p.bitfield) {
		p.bitfield = append(p.bitfield, make([]byte, int(byteIndex)+1-len(p.bitfield))...)
	}

	p.bitfield[byteIndex] |= byte(1 << (7 - index%8))
}

func (p *peer) tryEnqueue(message protocol.Message) bool {
	select {
	case p.outgoing <- message:
		return true

	default:
		return false
	}
}

func (p *peer) writeLoop(ctx context.Context, events chan<- peerEvent) {
	for {

		select {
		case message := <-p.outgoing:
			if err := protocol.WriteMessage(p.conn, message); err != nil {
				select {
				case events <- peerEvent{kind: peerDisconnected, peer: p, Err: err}:
				case <-ctx.Done():

				}
				return
			}

		case <-ctx.Done():
			return
		}

	}
}

func (t *Torrent) hasPeer(peer *peer) bool {
	if peer == nil {
		return false
	}
	current, exists := t.peers[peer.id]
	return exists && current == peer
}
