package torrent

import (
	"fmt"
	"sort"
)

type downloadMode string

const (
	downloadModeProgress downloadMode = "progress"
	downloadModePaused   downloadMode = "paused"
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

type Status struct {
	State           TransferState `json:"state"`
	InfoHash        string        `json:"infoHash"`
	Name            string        `json:"name"`
	Tracker         string        `json:"tracker"`
	TrackerID       string        `json:"trackerId,omitempty"`
	TotalBytes      uint64        `json:"totalBytes"`
	PieceLength     int64         `json:"pieceLength"`
	TotalPieces     int           `json:"totalPieces"`
	CompletePieces  int           `json:"completePieces"`
	BytesLeft       uint64        `json:"bytesLeft"`
	DownloadedBytes uint64        `json:"downloadedBytes"`
	UploadedBytes   uint64        `json:"uploadedBytes"`
	PendingRequests int           `json:"pendingRequests"`
	Peers           []PeerStatus  `json:"peers"`
}

// PeerStatus contains protocol state owned by the torrent event loop.
type PeerStatus struct {
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

func (t *Torrent) buildStatus() Status {
	peers := make([]PeerStatus, 0, len(t.peers))
	for _, peer := range t.peers {
		address := ""
		if peer.conn != nil && peer.conn.RemoteAddr() != nil {
			address = peer.conn.RemoteAddr().String()
		}
		peers = append(peers, PeerStatus{
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

func (t *Torrent) transferState() TransferState {
	if t.downloadMode == downloadModePaused {
		return TransferPaused
	}
	if t.pieces.Complete() {
		return TransferCompleted
	}
	return TransferDownloading
}
