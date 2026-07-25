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

type PeerState struct {
	AmChoking      bool
	AmInterested   bool
	PeerChoking    bool
	PeerInterested bool
}

type Peer struct {
	Conn     net.Conn
	ID       protocol.PeerID
	InfoHash protocol.InfoHash
	State    PeerState
	Bitfield []byte
	outgoing chan protocol.Message
}

func DialPeer(ctx context.Context, peerRecord tracker.Peer) (net.Conn, error) {
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, "tcp", netip.AddrPortFrom(peerRecord.IP, peerRecord.Port).String())
}

func CompleteHandshake(conn net.Conn, info protocol.MetaInfo, peerID protocol.PeerID) (*Peer, error) {

	if err := protocol.WriteHandshake(conn, protocol.Handshake{
		InfoHash: info.InfoHash,
		PeerID:   peerID,
		Protocol: protocol.PeerProtocol,
	}); err != nil {
		return nil, err
	}

	// if connection dropped, does not have the info hash!
	// uhhh maybe we await the other peer handshake? hmm

	handshakeMessage, err := protocol.ReadHandshake(conn)

	if err != nil {
		return nil, err
	}

	return CreatePeer(conn, handshakeMessage), nil
}

func CreatePeer(conn net.Conn, handshake protocol.Handshake) *Peer {

	return &Peer{
		Conn:     conn,
		ID:       handshake.PeerID,
		InfoHash: handshake.InfoHash,
		State: PeerState{
			AmChoking:   true,
			PeerChoking: true,
		},
		outgoing: make(chan protocol.Message, peerWriteQueueSize),
	}
}

func (p *Peer) HasPiece(index uint32) bool {
	byteIndex := index / 8
	if int(byteIndex) >= len(p.Bitfield) {
		return false
	}

	mask := byte(1 << (7 - index%8))
	return p.Bitfield[byteIndex]&mask != 0
}

func (p *Peer) sendMessage(message protocol.Message) error {
	if p.queue(message) {
		return nil
	}
	return errPeerWriteQueueFull
}

type PeerEvent struct {
	Peer    *Peer
	Message protocol.Message
	Err     error
}

func (p *Peer) readLoop(ctx context.Context, events chan<- PeerEvent) {

	for {

		msg, err := protocol.ReadMessage(p.Conn)

		event := PeerEvent{
			Peer:    p,
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

func (p *Peer) queue(message protocol.Message) bool {
	select {
	case p.outgoing <- message:
		return true

	default:
		return false
	}
}

func (p *Peer) setPieceHave(index uint32) {
	byteIndex := index / 8
	mask := byte(1 << (7 - index%8))
	if int(byteIndex) >= len(p.Bitfield) {
		p.Bitfield = append(p.Bitfield, make([]byte, int(byteIndex)+1-len(p.Bitfield))...)
	}
	p.Bitfield[byteIndex] |= mask
}

/*
func (p *Peer) HandleIncomingPieces()

func (p *Peer) CanRequest() bool {

}
*/

func (p *Peer) writeLoop(ctx context.Context, events chan<- PeerEvent) {
	for {

		select {
		case message := <-p.outgoing:
			if err := protocol.WriteMessage(p.Conn, message); err != nil {
				select {
				case events <- PeerEvent{Peer: p, Err: err}:
				case <-ctx.Done():

				}
				return
			}

		case <-ctx.Done():
			return
		}

	}
}
