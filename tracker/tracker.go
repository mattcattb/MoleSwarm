package tracker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
)

type AnnounceEvent string

const (
	StartedEvent   AnnounceEvent = "started"
	CompletedEvent AnnounceEvent = "completed"
	StoppedEvent   AnnounceEvent = "stopped"
)

type AnnounceRequest struct {
	InfoHash protocol.InfoHash
	PeerID   protocol.PeerID

	Port       uint16
	Uploaded   uint64
	Downloaded uint64
	Left       uint64
	Compact    bool
	Event      AnnounceEvent
}

type Peer struct {
	PeerID protocol.PeerID
	IP     netip.Addr
	Port   uint16
}

type AnnounceResponse struct {
	Interval time.Duration
	Peers    []Peer
}

func Announce(ctx context.Context, announceURL string, request AnnounceRequest) (AnnounceResponse, error) {
	formattedURL, err := encodeAnnounceRequest(announceURL, request)

	if err != nil {
		return AnnounceResponse{}, err
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, formattedURL.String(), nil)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("build announce HTTP request: %w", err)
	}

	resp, err := http.DefaultClient.Do(httpRequest)

	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("announce HTTP request: %w", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return AnnounceResponse{}, fmt.Errorf("announce HTTP status: %s", resp.Status)
	}

	const maxTrackerResponseSize = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTrackerResponseSize+1))
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("read tracker response: %w", err)
	}
	if len(body) > maxTrackerResponseSize {
		return AnnounceResponse{}, fmt.Errorf("tracker response exceeds %d bytes", maxTrackerResponseSize)
	}
	bodyBencoding, err := protocol.Decode(bytes.NewReader(body))
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("decode tracker response: %w", err)
	}

	trackerResp, err := decodeTrackerResponse(bodyBencoding)

	if err != nil {
		return AnnounceResponse{}, err
	}

	return trackerResp, nil

}

func encodeAnnounceRequest(announceURL string, request AnnounceRequest) (url.URL, error) {
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

var errInvalidAnnounceRequest = errors.New("invalid announce request")

func parseUint64Query(query url.Values, key string) (uint64, error) {

	if !query.Has(key) {
		return 0, errInvalidAnnounceRequest
	}
	val := query.Get(key)

	// ! incorrect parse uint
	v, err := strconv.ParseUint(val, 10, 50)

	if err != nil {
		return 0, errInvalidAnnounceRequest
	}

	return v, nil
}
func parseUint16Query(query url.Values, key string) (uint16, error) {
	val, err := parseUint64Query(query, key)

	if err != nil {
		return 0, err
	}

	return uint16(val), nil
}

func parseStrQuery(query url.Values, key string) (string, error) {
	val := query.Get(key)
	if len(val) == 0 {
		// error bad formatting
		return "", errInvalidAnnounceRequest
	}

	return val, nil
}

func decodeAnnounceRequest(req *http.Request) (AnnounceRequest, error) {
	qParams := req.URL.Query()

	infoHashStr, err := parseStrQuery(qParams, "info_hash")
	var infoHash protocol.InfoHash

	if err != nil {
		return AnnounceRequest{}, err
	}

	copy(infoHash[:], []byte(infoHashStr))

	peerIdStr, err := parseStrQuery(qParams, "peer_id")

	if err != nil {
		return AnnounceRequest{}, err
	}

	var peerId protocol.PeerID
	copy(peerId[:], []byte(peerIdStr))

	port, err := parseUint16Query(qParams, "port")

	if err != nil {
		return AnnounceRequest{}, err
	}

	uploaded, err := parseUint64Query(qParams, "uploaded")
	if err != nil {
		return AnnounceRequest{}, err
	}
	downloaded, err := parseUint64Query(qParams, "downloaded")
	if err != nil {
		return AnnounceRequest{}, err
	}
	left, err := parseUint64Query(qParams, "left")

	if err != nil {
		return AnnounceRequest{}, err
	}

	event := qParams.Get("event")

	return AnnounceRequest{
		Port:       port,
		Uploaded:   uploaded,
		Downloaded: downloaded,
		Left:       left,
		InfoHash:   infoHash,
		Event:      AnnounceEvent(event),
		PeerID:     peerId,
	}, nil

}

func decodeTrackerResponse(value protocol.Bencoding) (AnnounceResponse, error) {
	body, ok := value.Dict()

	if !ok {
		return AnnounceResponse{}, fmt.Errorf("response bencoding was not a dictionary")
	}

	if failureBc, ok := body.String("failure reason"); ok {
		return AnnounceResponse{}, fmt.Errorf("tracker error response: %s", failureBc)
	}

	intervalInt, ok := body.Int("interval")

	if !ok {
		// interval does not exist
		return AnnounceResponse{}, fmt.Errorf("interval not present in response dict")
	}
	if intervalInt < 0 || intervalInt > math.MaxInt64/int64(time.Second) {
		return AnnounceResponse{}, fmt.Errorf("tracker interval %d is invalid", intervalInt)
	}

	// trackerIdB, ok := body.String("tracker id")
	// string client should send back

	peersValue, ok := body["peers"]
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("peers key is missing")
	}

	peersList := make([]Peer, 0)
	if peersValList, ok := peersValue.List(); ok {
		for _, bval := range peersValList {
			peerRecord, err := parsePeerRecord(bval)

			if err != nil {
				return AnnounceResponse{}, err
			}

			peersList = append(peersList, peerRecord)
		}
	} else if compactPeers, ok := peersValue.String(); ok {
		if len(compactPeers)%6 != 0 {
			return AnnounceResponse{}, fmt.Errorf("compact peers length %d is not divisible by 6", len(compactPeers))
		}

		for offset := 0; offset < len(compactPeers); offset += 6 {
			var rawIP [4]byte
			copy(rawIP[:], compactPeers[offset:offset+4])
			ip := netip.AddrFrom4(rawIP)
			port := binary.BigEndian.Uint16([]byte(compactPeers[offset+4 : offset+6]))
			if port == 0 {
				return AnnounceResponse{}, fmt.Errorf("compact peer has invalid port 0")
			}
			peersList = append(peersList, Peer{IP: ip, Port: port})
		}
	} else {
		return AnnounceResponse{}, fmt.Errorf("peers must be a list or compact string")
	}

	return AnnounceResponse{
		Interval: time.Duration(intervalInt) * time.Second,
		Peers:    peersList,
	}, nil

}

var errInvalidBencodingFormat = errors.New("invalid bencoding format")

func parsePeerRecord(val protocol.Bencoding) (Peer, error) {

	bDict, ok := val.Dict()

	if !ok {
		return Peer{}, errInvalidBencodingFormat
	}

	pId, ok := bDict.String("peer id")

	if !ok || len(pId) != len(protocol.PeerID{}) {
		return Peer{}, errInvalidBencodingFormat
	}

	pIP, ok := bDict.String("ip")
	if !ok {
		return Peer{}, errInvalidBencodingFormat
	}
	ip, err := netip.ParseAddr(pIP)
	if err != nil {
		return Peer{}, errInvalidBencodingFormat
	}

	pPort, ok := bDict.Int("port")
	if !ok || pPort <= 0 || pPort > math.MaxUint16 {
		return Peer{}, errInvalidBencodingFormat
	}

	var peerId protocol.PeerID
	copy(peerId[:], []byte(pId))

	return Peer{
		Port:   uint16(pPort),
		PeerID: peerId,
		IP:     ip,
	}, nil
}

func encodeTrackerResponse(resp AnnounceResponse) (protocol.Bencoding, error) {
	interval := protocol.IntegerBencoding(int64(resp.Interval))

	peerList := make([]protocol.Bencoding, 0)

	for _, pr := range resp.Peers {
		iDict := protocol.BencodingDict{}

		iDict["peer id"] = protocol.StringBencoding(string(pr.PeerID[:]))
		iDict["ip"] = protocol.StringBencoding(pr.IP.String())
		iDict["port"] = protocol.IntegerBencoding(int64(pr.Port))

		peerList = append(peerList, protocol.DictBencoding(iDict))
	}

	return protocol.DictBencoding(map[string]protocol.Bencoding{
		"interval": interval,
		"peers":    protocol.ListBencoding(peerList),
	}), nil
}

type trackedPeer struct {
	Address  netip.AddrPort
	LastSeen time.Time
	Left     uint64
}

type Server struct {
	mu       sync.Mutex
	interval time.Duration
	swarms   map[protocol.InfoHash]map[protocol.PeerID]trackedPeer
}

func NewServer(interval time.Duration) *Server {
	return &Server{
		interval: interval,
		swarms:   make(map[protocol.InfoHash]map[protocol.PeerID]trackedPeer),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, err := decodeAnnounceRequest(r)

	if err != nil {
		// write the announce error?
		// ("announce requese decoding error: %w", err)
		return
	}
	// UHHHH lets use this now to... hmmm...

}
