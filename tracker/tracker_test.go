package tracker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
)

func TestAnnounceReturnsDictionaryPeers(t *testing.T) {
	infoHash := protocol.InfoHash{1, 2, 3}
	clientPeerID := protocol.PeerID{4, 5, 6}
	trackedPeerID := protocol.PeerID{7, 8, 9}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, http.MethodGet; got != want {
			t.Errorf("method = %s, want %s", got, want)
		}

		query := r.URL.Query()
		if got, want := query.Get("passkey"), "secret"; got != want {
			t.Errorf("passkey = %q, want %q", got, want)
		}
		if got, want := query.Get("info_hash"), string(infoHash[:]); got != want {
			t.Errorf("info_hash = %x, want %x", got, want)
		}
		if got, want := query.Get("peer_id"), string(clientPeerID[:]); got != want {
			t.Errorf("peer_id = %x, want %x", got, want)
		}
		if got, want := query.Get("port"), "6881"; got != want {
			t.Errorf("port = %q, want %q", got, want)
		}
		if got, want := query.Get("uploaded"), "10"; got != want {
			t.Errorf("uploaded = %q, want %q", got, want)
		}
		if got, want := query.Get("downloaded"), "20"; got != want {
			t.Errorf("downloaded = %q, want %q", got, want)
		}
		if got, want := query.Get("left"), "30"; got != want {
			t.Errorf("left = %q, want %q", got, want)
		}
		if got, want := query.Get("compact"), "0"; got != want {
			t.Errorf("compact = %q, want %q", got, want)
		}
		if got, want := query.Get("event"), string(StartedEvent); got != want {
			t.Errorf("event = %q, want %q", got, want)
		}

		response := []byte("d8:intervali60e5:peersld2:ip9:127.0.0.17:peer id20:")
		response = append(response, trackedPeerID[:]...)
		response = append(response, []byte("4:porti51413eeee")...)
		if _, err := w.Write(response); err != nil {
			t.Errorf("write tracker response: %v", err)
		}
	}))
	defer server.Close()

	response, err := Announce(context.Background(), server.URL+"?passkey=secret&port=1", AnnounceRequest{
		InfoHash:   infoHash,
		PeerID:     clientPeerID,
		Port:       6881,
		Uploaded:   10,
		Downloaded: 20,
		Left:       30,
		Event:      StartedEvent,
	})
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	if got, want := response.Interval, 60*time.Second; got != want {
		t.Fatalf("interval = %s, want %s", got, want)
	}
	if got, want := len(response.Peers), 1; got != want {
		t.Fatalf("peer count = %d, want %d", got, want)
	}
	if got, want := response.Peers[0].PeerID, trackedPeerID; got != want {
		t.Errorf("peer ID = %x, want %x", got, want)
	}
	if got, want := response.Peers[0].IP, netip.MustParseAddr("127.0.0.1"); got != want {
		t.Errorf("peer IP = %s, want %s", got, want)
	}
	if got, want := response.Peers[0].Port, uint16(51413); got != want {
		t.Errorf("peer port = %d, want %d", got, want)
	}
}

func TestAnnounceReturnsCompactPeers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("compact"), "1"; got != want {
			t.Errorf("compact = %q, want %q", got, want)
		}

		response := []byte("d8:intervali30e5:peers6:")
		response = append(response, 192, 0, 2, 1, 0x1a, 0xe1)
		response = append(response, 'e')
		if _, err := w.Write(response); err != nil {
			t.Errorf("write tracker response: %v", err)
		}
	}))
	defer server.Close()

	response, err := Announce(context.Background(), server.URL, AnnounceRequest{
		Compact: true,
	})
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	if got, want := len(response.Peers), 1; got != want {
		t.Fatalf("peer count = %d, want %d", got, want)
	}
	if got, want := response.Peers[0].IP, netip.MustParseAddr("192.0.2.1"); got != want {
		t.Errorf("peer IP = %s, want %s", got, want)
	}
	if got, want := response.Peers[0].Port, uint16(6881); got != want {
		t.Errorf("peer port = %d, want %d", got, want)
	}
}

func TestAnnounceReturnsTrackerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte("d14:failure reason11:not allowede")); err != nil {
			t.Errorf("write tracker response: %v", err)
		}
	}))
	defer server.Close()

	if _, err := Announce(context.Background(), server.URL, AnnounceRequest{}); err == nil {
		t.Fatal("announce succeeded for a tracker failure response")
	}
}

func TestAnnounceHonorsContextCancellation(t *testing.T) {
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
			http.Error(w, "request was not canceled", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-requestStarted
		cancel()
	}()

	_, err := Announce(ctx, server.URL, AnnounceRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("announce error = %v, want context cancellation", err)
	}
}

func TestServerAnnounceReturnsOtherPeersInRequestedFormat(t *testing.T) {
	server := httptest.NewServer(NewServer(30 * time.Second))
	defer server.Close()

	infoHash := protocol.InfoHash{1}
	firstPeerID := protocol.PeerID{2}
	secondPeerID := protocol.PeerID{3}

	first, err := Announce(context.Background(), server.URL, AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   firstPeerID,
		Port:     6881,
		Left:     42,
		Event:    StartedEvent,
	})
	if err != nil {
		t.Fatalf("announce first peer: %v", err)
	}
	if len(first.Peers) != 0 {
		t.Fatalf("first peer received %d peers, want 0", len(first.Peers))
	}

	second, err := Announce(context.Background(), server.URL, AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   secondPeerID,
		Port:     6882,
		Left:     21,
		Event:    StartedEvent,
	})
	if err != nil {
		t.Fatalf("announce second peer: %v", err)
	}
	if got, want := second.Interval, 30*time.Second; got != want {
		t.Fatalf("interval = %s, want %s", got, want)
	}
	if got, want := len(second.Peers), 1; got != want {
		t.Fatalf("peer count = %d, want %d", got, want)
	}
	if got, want := second.Peers[0].PeerID, firstPeerID; got != want {
		t.Errorf("peer ID = %x, want %x", got, want)
	}
	if got, want := second.Peers[0].Port, uint16(6881); got != want {
		t.Errorf("peer port = %d, want %d", got, want)
	}

	compact, err := Announce(context.Background(), server.URL, AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   secondPeerID,
		Port:     6882,
		Left:     21,
		Compact:  true,
	})
	if err != nil {
		t.Fatalf("announce compact response: %v", err)
	}
	if got, want := len(compact.Peers), 1; got != want {
		t.Fatalf("compact peer count = %d, want %d", got, want)
	}
	if got, want := compact.Peers[0].Port, uint16(6881); got != want {
		t.Errorf("compact peer port = %d, want %d", got, want)
	}
}

func TestServerPrunesStalePeersBeforePeerSelection(t *testing.T) {
	server := NewServer(time.Second)
	server.peerTTL = time.Second
	infoHash := protocol.InfoHash{1}
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	server.announce(AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   protocol.PeerID{2},
		Port:     6881,
		Event:    StartedEvent,
	}, netip.MustParseAddr("192.0.2.1"), now)

	response := server.announce(AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   protocol.PeerID{3},
		Port:     6882,
		Event:    StartedEvent,
	}, netip.MustParseAddr("192.0.2.2"), now.Add(2*time.Second))

	if got := len(response.Peers); got != 0 {
		t.Fatalf("peer count after pruning = %d, want 0", got)
	}
}
