package torrent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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

	port       uint16
	uploaded   uint64
	downloaded uint64
	left       uint64
	Compact    bool
	Event      AnnounceEvent
}

type PeerRecord struct {
	PeerID PeerID
	IP     netip.Addr
	port   uint16
}

type TrackerResponse struct {
	Interval time.Duration
	Peers    []PeerRecord
}

func Announce(ctx context.Context, announceURL string, request TrackerAnnounceRequest) (TrackerResponse, error) {

	formattedUrl, err := encodeAnnounceRequest(announceURL, request)

	if err != nil {
		return TrackerResponse{}, err
	}

	resp, err := http.Get(formattedUrl.String())

	if err != nil {
		return TrackerResponse{}, fmt.Errorf("announce HTTP request: %w", err)
	}

	defer resp.Body.Close()

	bReader := BReader{r: bufio.NewReader(resp.Body)}

	bodyBencoding, err := bReader.decode()

	trackerResp, err := decodeTrackerResponse(bodyBencoding)

	if err != nil {
		return TrackerResponse{}, err
	}

	return trackerResp, nil

}

func encodeAnnounceRequest(announceUrl string, request TrackerAnnounceRequest) (url.URL, error) {
	baseUrl, err := url.Parse(announceUrl)
	if err != nil {
		return url.URL{}, err
	}

	params := url.Values{}

	params.Add("info_hash", string(request.InfoHash[:]))
	params.Add("peer_id", string(request.PeerID[:]))
	params.Add("port", strconv.FormatUint(uint64(request.port), 10))
	params.Add("uploaded", strconv.Itoa(int(request.uploaded)))
	params.Add("downloaded", strconv.Itoa(int(request.downloaded)))
	params.Add("left", strconv.Itoa(int(request.left)))
	params.Add("event", string(request.Event))

	baseUrl.RawQuery = params.Encode()

	return *baseUrl, nil
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
		port:       port,
		uploaded:   uploaded,
		downloaded: downloaded,
		left:       left,
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

	// trackerIdB, ok := body.String("tracker id")
	// string client should send back

	peersValList, ok := body.List("peers")

	if !ok {
		return TrackerResponse{}, fmt.Errorf("peers key not a list")
	}

	peersList := make([]PeerRecord, 0)

	for _, bval := range peersValList {
		peerRecord, err := parsePeerRecord(bval)

		if err != nil {
			return TrackerResponse{}, err
		}

		peersList = append(peersList, peerRecord)
	}

	return TrackerResponse{
		Interval: time.Duration(intervalInt),
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

	if !ok {
		return PeerRecord{}, InvalidBencodingFormat
	}

	// pIp, ok := bDict.String("ip")

	if !ok {
		return PeerRecord{}, InvalidBencodingFormat
	}
	// pPort, ok := bDict.Int("port")
	if !ok {
		return PeerRecord{}, InvalidBencodingFormat
	}

	var peerId PeerID
	copy(peerId[:], []byte(pId))

	return PeerRecord{}, nil

	/*
		var ip netip.Addr
		copy(ip, pIp)

		return PeerRecord{
			port:   uint16(pPort),
			PeerID: peerId,
			IP:     netip.AddrFrom16(pIp[16]),
		}, nil */
}

func encodeTrackerResponse(resp TrackerResponse) (Bencoding, error) {
	interval := IntegerBencoding(int64(resp.Interval))

	peerList := make([]Bencoding, 0)

	for _, pr := range resp.Peers {
		iDict := BencodingDict{}

		iDict["peer id"] = StringBencoding(string(pr.PeerID[:]))
		iDict["ip"] = StringBencoding(pr.IP.String())
		iDict["port"] = IntegerBencoding(int64(pr.port))

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
