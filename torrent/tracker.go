package torrent

import (
	"bufio"
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
)

type AnnounceEvent string

const (
	StartedEvent   AnnounceEvent = "started"
	CompletedEvent AnnounceEvent = "completed"
	StoppedEvent   AnnounceEvent = "stopped"
)

type TrackerAnnounceRequest struct {
	InfoHash InfoHash
	PeerID   PeerID

	Port       uint16
	Uploaded   uint64
	Downloaded uint64
	Left       uint64
	Compact    bool
	Event      AnnounceEvent
}

type PeerRecord struct {
	PeerID PeerID
	IP     netip.Addr
	Port   uint16
}

type TrackerResponse struct {
	Interval time.Duration
	Peers    []PeerRecord
}

func Announce(ctx context.Context, announceURL string, request TrackerAnnounceRequest) (TrackerResponse, error) {
	formattedURL, err := encodeAnnounceRequest(announceURL, request)

	if err != nil {
		return TrackerResponse{}, err
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, formattedURL.String(), nil)
	if err != nil {
		return TrackerResponse{}, fmt.Errorf("build announce HTTP request: %w", err)
	}

	resp, err := http.DefaultClient.Do(httpRequest)

	if err != nil {
		return TrackerResponse{}, fmt.Errorf("announce HTTP request: %w", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return TrackerResponse{}, fmt.Errorf("announce HTTP status: %s", resp.Status)
	}

	const maxTrackerResponseSize = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTrackerResponseSize+1))
	if err != nil {
		return TrackerResponse{}, fmt.Errorf("read tracker response: %w", err)
	}
	if len(body) > maxTrackerResponseSize {
		return TrackerResponse{}, fmt.Errorf("tracker response exceeds %d bytes", maxTrackerResponseSize)
	}
	bReader := BReader{r: bufio.NewReader(bytes.NewReader(body))}

	bodyBencoding, err := bReader.decode()
	if err != nil {
		return TrackerResponse{}, fmt.Errorf("decode tracker response: %w", err)
	}
	if _, err := bReader.r.Peek(1); err == nil {
		return TrackerResponse{}, fmt.Errorf("decode tracker response: trailing data")
	} else if !errors.Is(err, io.EOF) {
		return TrackerResponse{}, fmt.Errorf("decode tracker response ending: %w", err)
	}

	trackerResp, err := decodeTrackerResponse(bodyBencoding)

	if err != nil {
		return TrackerResponse{}, err
	}

	return trackerResp, nil

}

func encodeAnnounceRequest(announceURL string, request TrackerAnnounceRequest) (url.URL, error) {
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

var InvalidAnounceRequest = errors.New("invalid response request")

func parseUint64Query(query url.Values, key string) (uint64, error) {

	if !query.Has(key) {
		return 0, InvalidAnounceRequest
	}
	val := query.Get(key)

	// ! incorrect parse uint
	v, err := strconv.ParseUint(val, 10, 50)

	if err != nil {
		return 0, InvalidAnounceRequest
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
		return "", InvalidAnounceRequest
	}

	return val, nil
}

func decodeAnnounceRequest(req *http.Request) (TrackerAnnounceRequest, error) {
	qParams := req.URL.Query()

	infoHashStr, err := parseStrQuery(qParams, "info_hash")
	var infoHash InfoHash

	if err != nil {
		return TrackerAnnounceRequest{}, err
	}

	copy(infoHash[:], []byte(infoHashStr))

	peerIdStr, err := parseStrQuery(qParams, "peer_id")

	if err != nil {
		return TrackerAnnounceRequest{}, err
	}

	var peerId PeerID
	copy(peerId[:], []byte(peerIdStr))

	port, err := parseUint16Query(qParams, "port")

	if err != nil {
		return TrackerAnnounceRequest{}, err
	}

	uploaded, err := parseUint64Query(qParams, "uploaded")
	if err != nil {
		return TrackerAnnounceRequest{}, err
	}
	downloaded, err := parseUint64Query(qParams, "downloaded")
	if err != nil {
		return TrackerAnnounceRequest{}, err
	}
	left, err := parseUint64Query(qParams, "left")

	if err != nil {
		return TrackerAnnounceRequest{}, err
	}

	event := qParams.Get("event")

	return TrackerAnnounceRequest{
		Port:       port,
		Uploaded:   uploaded,
		Downloaded: downloaded,
		Left:       left,
		InfoHash:   infoHash,
		Event:      AnnounceEvent(event),
		PeerID:     peerId,
	}, nil

}

func decodeTrackerResponse(value Bencoding) (TrackerResponse, error) {
	body, ok := value.Dict()

	if !ok {
		return TrackerResponse{}, fmt.Errorf("response bencoding was not a dictionary")
	}

	if failureBc, ok := body.String("failure reason"); ok {
		return TrackerResponse{}, fmt.Errorf("tracker error response: %s", failureBc)
	}

	intervalInt, ok := body.Int("interval")

	if !ok {
		// interval does not exist
		return TrackerResponse{}, fmt.Errorf("interval not present in response dict")
	}
	if intervalInt < 0 || intervalInt > math.MaxInt64/int64(time.Second) {
		return TrackerResponse{}, fmt.Errorf("tracker interval %d is invalid", intervalInt)
	}

	// trackerIdB, ok := body.String("tracker id")
	// string client should send back

	peersValue, ok := body["peers"]
	if !ok {
		return TrackerResponse{}, fmt.Errorf("peers key is missing")
	}

	peersList := make([]PeerRecord, 0)
	if peersValList, ok := peersValue.List(); ok {
		for _, bval := range peersValList {
			peerRecord, err := parsePeerRecord(bval)

			if err != nil {
				return TrackerResponse{}, err
			}

			peersList = append(peersList, peerRecord)
		}
	} else if compactPeers, ok := peersValue.String(); ok {
		if len(compactPeers)%6 != 0 {
			return TrackerResponse{}, fmt.Errorf("compact peers length %d is not divisible by 6", len(compactPeers))
		}

		for offset := 0; offset < len(compactPeers); offset += 6 {
			var rawIP [4]byte
			copy(rawIP[:], compactPeers[offset:offset+4])
			ip := netip.AddrFrom4(rawIP)
			port := binary.BigEndian.Uint16([]byte(compactPeers[offset+4 : offset+6]))
			if port == 0 {
				return TrackerResponse{}, fmt.Errorf("compact peer has invalid port 0")
			}
			peersList = append(peersList, PeerRecord{IP: ip, Port: port})
		}
	} else {
		return TrackerResponse{}, fmt.Errorf("peers must be a list or compact string")
	}

	return TrackerResponse{
		Interval: time.Duration(intervalInt) * time.Second,
		Peers:    peersList,
	}, nil

}

var InvalidBencodingFormat = errors.New("invalid bencoding format")

func parsePeerRecord(val Bencoding) (PeerRecord, error) {

	bDict, ok := val.Dict()

	if !ok {
		return PeerRecord{}, InvalidBencodingFormat
	}

	pId, ok := bDict.String("peer id")

	if !ok || len(pId) != len(PeerID{}) {
		return PeerRecord{}, InvalidBencodingFormat
	}

	pIP, ok := bDict.String("ip")
	if !ok {
		return PeerRecord{}, InvalidBencodingFormat
	}
	ip, err := netip.ParseAddr(pIP)
	if err != nil {
		return PeerRecord{}, InvalidBencodingFormat
	}

	pPort, ok := bDict.Int("port")
	if !ok || pPort <= 0 || pPort > math.MaxUint16 {
		return PeerRecord{}, InvalidBencodingFormat
	}

	var peerId PeerID
	copy(peerId[:], []byte(pId))

	return PeerRecord{
		Port:   uint16(pPort),
		PeerID: peerId,
		IP:     ip,
	}, nil
}

func encodeTrackerResponse(resp TrackerResponse) (Bencoding, error) {
	interval := IntegerBencoding(int64(resp.Interval))

	peerList := make([]Bencoding, 0)

	for _, pr := range resp.Peers {
		iDict := BencodingDict{}

		iDict["peer id"] = StringBencoding(string(pr.PeerID[:]))
		iDict["ip"] = StringBencoding(pr.IP.String())
		iDict["port"] = IntegerBencoding(int64(pr.Port))

		peerList = append(peerList, DictBencoding(iDict))
	}

	return DictBencoding(map[string]Bencoding{
		"interval": interval,
		"peers":    ListBencoding(peerList),
	}), nil
}

type trackedPeer struct {
	Address  netip.AddrPort
	LastSeen time.Time
	Left     uint64
}

type Tracker struct {
	mu       sync.Mutex
	interval time.Duration
	swarms   map[InfoHash]map[PeerID]trackedPeer
}

func NewTracker(interval time.Duration) *Tracker {
	return nil
}

func (t *Tracker) ServeHttp(w http.ResponseWriter, r *http.Request) {
	_, err := decodeAnnounceRequest(r)

	if err != nil {
		// write the announce error?
		// ("announce requese decoding error: %w", err)
		return
	}
	// UHHHH lets use this now to... hmmm...

}
