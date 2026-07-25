package torrent

import (
	"bufio"
	"context"
	"net"
	"net/netip"
)

type PeerState struct {
	AmChoking      bool
	AmInterested   bool
	PeerChoking    bool
	PeerInterested bool
}

type Peer struct {
	Conn     net.Conn
	ID       PeerID
	InfoHash InfoHash
	State    PeerState
	Bitfield []byte
}

func DialPeer(ctx context.Context, peerRecord PeerRecord) (net.Conn, error) {
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, "tcp", netip.AddrPortFrom(peerRecord.IP, peerRecord.Port).String())
}

func CompleteHandshake(conn net.Conn, info MetaInfo, peerId PeerID) (*Peer, error) {

	buffWriter := bufio.NewWriter(conn)

	if err := WritePeerHandshake(buffWriter, PeerHandshakeMessage{
		InfoHash: info.InfoHash,
		PeerID:   peerId,
	}); err != nil {
		return nil, err
	}

	// if connection dropped, does not have the info hash!
	// uhhh maybe we await the other peer handshake? hmm

	r := bufio.NewReader(conn)
	handshakeMessage, err := ReadPeerHandshake(r)

	if err != nil {
		return nil, err
	}

	return &Peer{
		Conn:     conn,
		ID:       handshakeMessage.PeerID,
		InfoHash: handshakeMessage.InfoHash,
		State: PeerState{
			AmChoking:   true,
			PeerChoking: true,
		},
	}, nil
}

func (p *Peer) HasPiece(index uint32) bool {
	byteIndex := index / 8
	if int(byteIndex) >= len(p.Bitfield) {
		return false
	}

	mask := byte(1 << (7 - index%8))
	return p.Bitfield[byteIndex]&mask != 0
}

func (p *Peer) sendMessage(msg PeerMessage) error {
	return WriteMessage(p.Conn, msg)
}

type PeerEvent struct {
	Peer    *Peer
	Message PeerMessage
	Err     error
}

func (p *Peer) readLoop(ctx context.Context, events chan<- PeerEvent) {

	for {

		msg, err := ReadMessage(p.Conn)

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
