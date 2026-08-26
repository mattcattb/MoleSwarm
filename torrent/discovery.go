package torrent

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
)

func (t *Torrent) announce(ctx context.Context, config torrentRunConfig, event tracker.AnnounceEvent) (tracker.AnnounceResponse, error) {

	request := tracker.AnnounceRequest{
		Port:       config.port,
		PeerID:     config.peerID,
		InfoHash:   t.Meta.InfoHash,
		Uploaded:   t.uploaded,
		Downloaded: t.downloaded,
		Left:       t.pieces.BytesLeft(),
		Compact:    true,
		Event:      event,
	}

	response, err := tracker.Announce(ctx, t.Meta.Announce, request)

	if err != nil {
		return tracker.AnnounceResponse{}, err
	}

	return response, nil
}

func normalizeTrackerInterval(interval time.Duration) time.Duration {
	if interval < time.Second {
		return 30 * time.Second
	}

	return interval
}

func resetTrackerTimer() {

}

func writePeerHandshake(conn net.Conn, infoHash protocol.InfoHash, peerId protocol.PeerID) error {

	return protocol.WriteHandshake(conn, protocol.Handshake{Protocol: protocol.PeerProtocol, InfoHash: infoHash, PeerID: peerId})

}

func validatePeerHandshake(handshake protocol.Handshake, expectedHash protocol.InfoHash, localPeedId protocol.PeerID) error {

	if handshake.Protocol != protocol.PeerProtocol {
		return fmt.Errorf("invalid peer protocol")
	}

	if handshake.InfoHash != expectedHash {
		return fmt.Errorf("unexpected info hash")
	}

	if handshake.PeerID == localPeedId {
		return fmt.Errorf("peer ID matches local client")
	}

	return nil
}
