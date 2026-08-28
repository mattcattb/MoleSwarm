package torrent

import (
	"context"
	"errors"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
)

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

func (t *Torrent) hasPeer(peer *peer) bool {
	if peer == nil {
		return false
	}
	current, exists := t.peers[peer.id]
	return exists && current == peer
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
func (t *Torrent) connectPeer(ctx context.Context, config RunConfig, candidate tracker.Peer) (*peer, error) {

	conn, err := dialPeer(ctx, candidate)

	if err != nil {
		return nil, err
	}

	if err := writePeerHandshake(conn, t.Meta.InfoHash, config.PeerID); err != nil {
		_ = conn.Close()
		return nil, err
	}

	handshake, err := protocol.ReadHandshake(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if err := validatePeerHandshake(handshake, t.Meta.InfoHash, config.PeerID); err != nil {
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
