package tracker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
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

	value, err := strconv.ParseUint(query.Get(key), 10, 64)
	if err != nil {
		return 0, errInvalidAnnounceRequest
	}
	return value, nil
}

func parseUint16Query(query url.Values, key string) (uint16, error) {
	value, err := parseUint64Query(query, key)
	if err != nil {
		return 0, err
	}
	return uint16(value), nil
}

func parseStringQuery(query url.Values, key string) (string, error) {
	value := query.Get(key)
	if value == "" {
		return "", errInvalidAnnounceRequest
	}
	return value, nil
}

func decodeAnnounceRequest(request *http.Request) (AnnounceRequest, error) {
	query := request.URL.Query()

	infoHashValue, err := parseStringQuery(query, "info_hash")
	if err != nil || len(infoHashValue) != len(protocol.InfoHash{}) {
		return AnnounceRequest{}, errInvalidAnnounceRequest
	}
	var infoHash protocol.InfoHash
	copy(infoHash[:], infoHashValue)

	peerIDValue, err := parseStringQuery(query, "peer_id")
	if err != nil || len(peerIDValue) != len(protocol.PeerID{}) {
		return AnnounceRequest{}, errInvalidAnnounceRequest
	}
	var peerID protocol.PeerID
	copy(peerID[:], peerIDValue)

	port, err := parseUint16Query(query, "port")
	if err != nil || port == 0 {
		return AnnounceRequest{}, errInvalidAnnounceRequest
	}
	uploaded, err := parseUint64Query(query, "uploaded")
	if err != nil {
		return AnnounceRequest{}, err
	}
	downloaded, err := parseUint64Query(query, "downloaded")
	if err != nil {
		return AnnounceRequest{}, err
	}
	left, err := parseUint64Query(query, "left")
	if err != nil {
		return AnnounceRequest{}, err
	}

	compact := query.Get("compact") == "1"
	event := AnnounceEvent(query.Get("event"))
	if event != "" && event != StartedEvent && event != CompletedEvent && event != StoppedEvent {
		return AnnounceRequest{}, errInvalidAnnounceRequest
	}

	return AnnounceRequest{
		InfoHash:   infoHash,
		PeerID:     peerID,
		Port:       port,
		Uploaded:   uploaded,
		Downloaded: downloaded,
		Left:       left,
		Compact:    compact,
		Event:      event,
	}, nil
}

func decodeTrackerResponse(value protocol.Bencoding) (AnnounceResponse, error) {
	body, ok := value.Dict()
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("response bencoding was not a dictionary")
	}
	if failureReason, ok := body.String("failure reason"); ok {
		return AnnounceResponse{}, fmt.Errorf("tracker error response: %s", failureReason)
	}

	interval, ok := body.Int("interval")
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("interval not present in response dict")
	}
	if interval < 0 || interval > math.MaxInt64/int64(time.Second) {
		return AnnounceResponse{}, fmt.Errorf("tracker interval %d is invalid", interval)
	}

	peersValue, ok := body["peers"]
	if !ok {
		return AnnounceResponse{}, fmt.Errorf("peers key is missing")
	}

	peers := make([]Peer, 0)
	if list, ok := peersValue.List(); ok {
		for _, value := range list {
			peer, err := parsePeer(value)
			if err != nil {
				return AnnounceResponse{}, err
			}
			peers = append(peers, peer)
		}
	} else if compact, ok := peersValue.String(); ok {
		if len(compact)%6 != 0 {
			return AnnounceResponse{}, fmt.Errorf("compact peers length %d is not divisible by 6", len(compact))
		}
		for offset := 0; offset < len(compact); offset += 6 {
			var rawIP [4]byte
			copy(rawIP[:], compact[offset:offset+4])
			port := binary.BigEndian.Uint16([]byte(compact[offset+4 : offset+6]))
			if port == 0 {
				return AnnounceResponse{}, fmt.Errorf("compact peer has invalid port 0")
			}
			peers = append(peers, Peer{IP: netip.AddrFrom4(rawIP), Port: port})
		}
	} else {
		return AnnounceResponse{}, fmt.Errorf("peers must be a list or compact string")
	}

	return AnnounceResponse{Interval: time.Duration(interval) * time.Second, Peers: peers}, nil
}

var errInvalidBencodingFormat = errors.New("invalid bencoding format")

func parsePeer(value protocol.Bencoding) (Peer, error) {
	dict, ok := value.Dict()
	if !ok {
		return Peer{}, errInvalidBencodingFormat
	}

	peerIDValue, ok := dict.String("peer id")
	if !ok || len(peerIDValue) != len(protocol.PeerID{}) {
		return Peer{}, errInvalidBencodingFormat
	}
	ipValue, ok := dict.String("ip")
	if !ok {
		return Peer{}, errInvalidBencodingFormat
	}
	ip, err := netip.ParseAddr(ipValue)
	if err != nil {
		return Peer{}, errInvalidBencodingFormat
	}
	port, ok := dict.Int("port")
	if !ok || port <= 0 || port > math.MaxUint16 {
		return Peer{}, errInvalidBencodingFormat
	}

	var peerID protocol.PeerID
	copy(peerID[:], peerIDValue)
	return Peer{PeerID: peerID, IP: ip, Port: uint16(port)}, nil
}

func encodeTrackerResponse(response AnnounceResponse, compact bool) (protocol.Bencoding, error) {
	if response.Interval < 0 || response.Interval%time.Second != 0 {
		return protocol.Bencoding{}, fmt.Errorf("tracker interval %s is not a whole number of seconds", response.Interval)
	}

	interval := protocol.IntegerBencoding(int64(response.Interval / time.Second))
	if compact {
		peers := make([]byte, 0, len(response.Peers)*6)
		for _, peer := range response.Peers {
			if !peer.IP.Is4() {
				continue
			}
			ip := peer.IP.As4()
			peers = append(peers, ip[:]...)
			var port [2]byte
			binary.BigEndian.PutUint16(port[:], peer.Port)
			peers = append(peers, port[:]...)
		}

		return protocol.DictBencoding(map[string]protocol.Bencoding{
			"interval": interval,
			"peers":    protocol.StringBencoding(string(peers)),
		}), nil
	}

	peers := make([]protocol.Bencoding, 0, len(response.Peers))
	for _, peer := range response.Peers {
		peers = append(peers, protocol.DictBencoding(protocol.BencodingDict{
			"peer id": protocol.StringBencoding(string(peer.PeerID[:])),
			"ip":      protocol.StringBencoding(peer.IP.String()),
			"port":    protocol.IntegerBencoding(int64(peer.Port)),
		}))
	}

	return protocol.DictBencoding(map[string]protocol.Bencoding{
		"interval": interval,
		"peers":    protocol.ListBencoding(peers),
	}), nil
}
