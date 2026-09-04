package protocol

import (
	"io"
	"net/netip"
	"net/url"
	"strconv"
	"time"
)

type AnnounceEvent string

const (
	StartedEvent   AnnounceEvent = "started"
	CompletedEvent AnnounceEvent = "completed"
	StoppedEvent   AnnounceEvent = "stopped"
)

type AnnounceRequest struct {
	InfoHash InfoHash
	PeerID   PeerID

	Port       uint16
	Uploaded   uint64
	Downloaded uint64
	Left       uint64
	Compact    bool
	Event      AnnounceEvent
}

type AnnounceResponse struct {
	Interval time.Duration
	Peers    []AnnouncePeer
}

type AnnouncePeer struct {
	PeerID PeerID
	IP     netip.Addr
	Port   uint16
}

func BuildAnnounceUrl(announceURL string, request AnnounceRequest) (url.URL, error) {
	baseURL, err := url.Parse(announceURL)
	if err != nil {
		return url.URL{}, err
	}

	params := baseURL.Query()
	params.Set("info_hash", string(request.InfoHash[:]))
	params.Set("peer_id", string(request.PeerID[:]))
	params.Set("port", strconv.FormatUint(uint64(request.Port), 10))
	params.Set("uploaded", strconv.FormatUint(request.Uploaded, 10))
	params.Set("downloaded", strconv.FormatUint(request.Downloaded, 10))
	params.Set("left", strconv.FormatUint(request.Left, 10))
	if request.Compact {
		params.Set("compact", "1")
	} else {
		params.Set("compact", "0")
	}
	if request.Event != "" {
		params.Set("event", string(request.Event))
	} else {
		params.Del("event")
	}

	baseURL.RawQuery = params.Encode()
	return *baseURL, nil
}

func ParseAnnounceQuery(query url.Values) (AnnounceRequest, error) {

	// decode annouce request

}

func ReadAnnouceResponse(r io.Reader) (AnnounceResponse, error) {

}

func WriteAnnounceResponse(w io.Writer, response AnnounceResponse, comact bool) error {

}

func WriteAnnonceFailure(w io.Writer, reason string) error {

}
