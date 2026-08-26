package torrent

import (
	"context"
	"fmt"
	"sort"
)

type TransferState string

const (
	TransferDownloading TransferState = "downloading"
	TransferPaused      TransferState = "paused"
	TransferCompleted   TransferState = "completed"
)

// Snapshot is an immutable observation of one running torrent session. The
// event loop constructs it so observers never read mutable peer, request, or
// piece state directly.

type snapshotRequest struct {
	reply chan<- Status
}

type Status struct {
	State           TransferState  `json:"state"`
	InfoHash        string         `json:"infoHash"`
	Name            string         `json:"name"`
	Tracker         string         `json:"tracker"`
	TrackerID       string         `json:"trackerId,omitempty"`
	TotalBytes      uint64         `json:"totalBytes"`
	PieceLength     int64          `json:"pieceLength"`
	TotalPieces     int            `json:"totalPieces"`
	CompletePieces  int            `json:"completePieces"`
	BytesLeft       uint64         `json:"bytesLeft"`
	DownloadedBytes uint64         `json:"downloadedBytes"`
	UploadedBytes   uint64         `json:"uploadedBytes"`
	PendingRequests int            `json:"pendingRequests"`
	Peers           []PeerSnapshot `json:"peers"`
}

// PeerSnapshot contains protocol state owned by the torrent event loop.
type PeerSnapshot struct {
	PeerID          string `json:"peerId"`
	Address         string `json:"address"`
	Incoming        bool   `json:"incoming"`
	AmChoking       bool   `json:"amChoking"`
	AmInterested    bool   `json:"amInterested"`
	PeerChoking     bool   `json:"peerChoking"`
	PeerInterested  bool   `json:"peerInterested"`
	AvailablePieces int    `json:"availablePieces"`
	PendingRequests int    `json:"pendingRequests"`
}

func (t *Torrent) Snapshot(ctx context.Context) (Status, error) {
	t.lifecycleMu.Lock()
	running := t.running
	done := t.done
	t.lifecycleMu.Unlock()
	if !running {
		return Status{}, fmt.Errorf("torrent is not running")
	}

	// here we need to request from the torrent a snapshot, and then wait for that snapshot channel to return from that channel the snapshot

	snapshotReply := make(chan Status)

	snapshotRequest := snapshotRequest{
		reply: snapshotReply,
	}

	select {

	case t.snapshotRequests <- snapshotRequest:

	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-done:
		return Status{}, fmt.Errorf("torrent completed before snapshot request")
	}

	// wait for response
	select {
	case resp := <-snapshotReply:

		return resp, nil

	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-done:
		return Status{}, fmt.Errorf("torrent completed before snapshot request")
	}
}

func (t *Torrent) buildSnapshot() Status {
	peers := make([]PeerSnapshot, 0, len(t.peers))
	for _, peer := range t.peers {
		address := ""
		if peer.conn != nil && peer.conn.RemoteAddr() != nil {
			address = peer.conn.RemoteAddr().String()
		}
		peers = append(peers, PeerSnapshot{
			PeerID:          fmt.Sprintf("%x", peer.id),
			Address:         address,
			Incoming:        peer.incoming,
			AmChoking:       peer.state.AmChoking,
			AmInterested:    peer.state.AmInterested,
			PeerChoking:     peer.state.PeerChoking,
			PeerInterested:  peer.state.PeerInterested,
			AvailablePieces: bitCount(peer.bitfield, t.pieces.Count()),
			PendingRequests: t.pendingCount(peer),
		})
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].PeerID < peers[j].PeerID
	})

	return Status{
		State:           t.transferState(),
		InfoHash:        fmt.Sprintf("%x", t.Meta.InfoHash),
		Name:            t.Meta.Info.Name,
		Tracker:         t.Meta.Announce,
		TrackerID:       t.TrackerID,
		TotalBytes:      uint64(t.Meta.Info.Length),
		PieceLength:     t.Meta.Info.PieceLength,
		TotalPieces:     t.pieces.Count(),
		CompletePieces:  t.pieces.CompletedCount(),
		BytesLeft:       t.pieces.BytesLeft(),
		DownloadedBytes: t.downloaded,
		UploadedBytes:   t.uploaded,
		PendingRequests: len(t.pending),
		Peers:           peers,
	}
}
