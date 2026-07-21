package torrent

import (
	"net/netip"
	"time"
)

type AnnounceEvent string

type TrackerAnnounceRequest struct {
	InfoHash InfoHash
	PeerID   PeerID

	port       uint16
	uploaded   uint64
	downloaded uint64
	left       uint64
	Compact    bool
	Event      AnnounceEvent
}

type TrackerResponse struct {
	Interval time.Duration
	Peers    []netip.AddrPort
}
