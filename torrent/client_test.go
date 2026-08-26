package torrent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
)

func TestClientRoutesRegisteredInfoHashUsingSharedIdentity(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	trackerAnnounced := make(chan struct{}, 1)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		trackerServer.ServeHTTP(w, request)
		select {
		case trackerAnnounced <- struct{}{}:
		default:
		}
	}))
	defer httpTracker.Close()

	firstMeta := protocol.MetaInfo{
		Info:     infoForTest([]byte("first client routing fixture")),
		InfoHash: protocol.InfoHash{1, 2, 3},
	}
	firstSession, err := NewTorrent(firstMeta)
	if err != nil {
		t.Fatalf("new first torrent: %v", err)
	}
	if err := client.RegisterTorrent(firstSession); err != nil {
		t.Fatalf("add first torrent: %v", err)
	}

	routedMeta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest([]byte("second client routing fixture")),
		InfoHash: protocol.InfoHash{4, 5, 6},
	}
	routedSession, err := NewTorrent(routedMeta)
	if err != nil {
		t.Fatalf("new routed torrent: %v", err)
	}
	if err := client.RegisterTorrent(routedSession); err != nil {
		t.Fatalf("add routed torrent: %v", err)
	}

	listener, err := client.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.RunTorrent(ctx, routedMeta.InfoHash)
	}()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.ServePeers(ctx, listener)
	}()

	select {
	case <-trackerAnnounced:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("running torrent did not announce to tracker")
	}

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial client: %v", err)
	}
	defer conn.Close()

	remotePeerID := protocol.PeerID{9, 8, 7}
	if err := protocol.WriteHandshake(conn, protocol.Handshake{
		InfoHash: routedMeta.InfoHash,
		PeerID:   remotePeerID,
		Protocol: protocol.PeerProtocol,
	}); err != nil {
		cancel()
		t.Fatalf("write handshake: %v", err)
	}

	response, err := protocol.ReadHandshake(conn)
	if err != nil {
		cancel()
		t.Fatalf("read handshake: %v", err)
	}
	if response.InfoHash != routedMeta.InfoHash {
		t.Fatalf("response info hash = %x, want %x", response.InfoHash, routedMeta.InfoHash)
	}
	if response.PeerID != client.PeerID() {
		t.Fatalf("response peer ID = %x, want client peer ID %x", response.PeerID, client.PeerID())
	}

	if err := waitForPeerCount(ctx, routedSession, 1); err != nil {
		cancel()
		t.Fatalf("wait for routed peer: %v", err)
	}
	snapshot, err := routedSession.Snapshot(ctx)
	if err != nil {
		cancel()
		t.Fatalf("snapshot routed torrent: %v", err)
	}
	if got, want := snapshot.InfoHash, fmt.Sprintf("%x", routedMeta.InfoHash); got != want {
		t.Errorf("snapshot info hash = %q, want %q", got, want)
	}
	if got, want := snapshot.TotalPieces, 1; got != want {
		t.Errorf("snapshot total pieces = %d, want %d", got, want)
	}
	if got, want := snapshot.CompletePieces, 0; got != want {
		t.Errorf("snapshot complete pieces = %d, want %d", got, want)
	}
	if got, want := snapshot.State, TransferDownloading; got != want {
		t.Errorf("torrent state = %q, want %q", got, want)
	}
	if got, want := len(snapshot.Peers), 1; got != want {
		t.Fatalf("snapshot peer count = %d, want %d", got, want)
	}
	if got, want := snapshot.Peers[0].PeerID, fmt.Sprintf("%x", remotePeerID); got != want {
		t.Errorf("snapshot peer ID = %q, want %q", got, want)
	}
	if !snapshot.Peers[0].Incoming {
		t.Error("snapshot routed peer was not marked incoming")
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error = %v, want context canceled", err)
	}
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("torrent run error = %v, want context canceled", err)
	}
}

func TestClientRejectsUnknownInfoHashWithoutStoppingListener(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	trackerAnnounced := make(chan struct{}, 1)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		trackerServer.ServeHTTP(w, request)
		select {
		case trackerAnnounced <- struct{}{}:
		default:
		}
	}))
	defer httpTracker.Close()

	meta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest([]byte("known torrent")),
		InfoHash: protocol.InfoHash{1, 2, 3},
	}
	session, err := NewTorrent(meta)
	if err != nil {
		t.Fatalf("new torrent: %v", err)
	}
	if err := client.RegisterTorrent(session); err != nil {
		t.Fatalf("add torrent: %v", err)
	}

	listener, err := client.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.ServePeers(ctx, listener)
	}()
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.RunTorrent(ctx, meta.InfoHash)
	}()

	select {
	case <-trackerAnnounced:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("known torrent did not announce to tracker")
	}

	unknownConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial unknown torrent: %v", err)
	}
	if err := protocol.WriteHandshake(unknownConn, protocol.Handshake{
		InfoHash: protocol.InfoHash{9, 9, 9},
		PeerID:   protocol.PeerID{4, 5, 6},
		Protocol: protocol.PeerProtocol,
	}); err != nil {
		cancel()
		t.Fatalf("write unknown handshake: %v", err)
	}
	if err := unknownConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		cancel()
		t.Fatalf("set unknown connection deadline: %v", err)
	}
	if _, err := protocol.ReadHandshake(unknownConn); err == nil {
		cancel()
		t.Fatal("unknown torrent received a handshake response")
	}
	_ = unknownConn.Close()

	knownConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial known torrent: %v", err)
	}
	defer knownConn.Close()
	if err := protocol.WriteHandshake(knownConn, protocol.Handshake{
		InfoHash: meta.InfoHash,
		PeerID:   protocol.PeerID{7, 8, 9},
		Protocol: protocol.PeerProtocol,
	}); err != nil {
		cancel()
		t.Fatalf("write known handshake: %v", err)
	}
	if _, err := protocol.ReadHandshake(knownConn); err != nil {
		cancel()
		t.Fatalf("read known handshake after unknown hash: %v", err)
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error = %v, want context canceled", err)
	}
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
}

func TestClientCancellationClosesPendingHandshake(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listener := &acceptNotifyingListener{
		Listener: tcpListener,
		accepted: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.ServePeers(ctx, listener)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial client: %v", err)
	}
	defer conn.Close()

	<-listener.accepted
	if _, err := conn.Write([]byte{byte(len(protocol.PeerProtocol))}); err != nil {
		cancel()
		t.Fatalf("start handshake: %v", err)
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error = %v, want context canceled", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var response [1]byte
	if _, err := conn.Read(response[:]); err == nil {
		t.Fatal("pending handshake connection remained open after cancellation")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("pending handshake connection remained open after cancellation")
	}
}

func TestClientsDiscoverAndConnectThroughTracker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	trackerAnnounces := make(chan struct{}, 2)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		trackerServer.ServeHTTP(w, request)
		select {
		case trackerAnnounces <- struct{}{}:
		default:
		}
	}))
	defer httpTracker.Close()

	meta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest([]byte("two clients discover each other through a tracker")),
		InfoHash: protocol.InfoHash{1, 2, 3},
	}

	firstClient, err := NewClient()
	if err != nil {
		t.Fatalf("new first client: %v", err)
	}
	firstTorrent, err := NewTorrent(meta)
	if err != nil {
		t.Fatalf("new first torrent: %v", err)
	}
	if err := firstClient.RegisterTorrent(firstTorrent); err != nil {
		t.Fatalf("add first torrent: %v", err)
	}
	firstListener, err := firstClient.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for first client: %v", err)
	}
	firstServeErr := make(chan error, 1)
	go func() {
		firstServeErr <- firstClient.ServePeers(ctx, firstListener)
	}()
	firstRunErr := make(chan error, 1)
	go func() {
		firstRunErr <- firstClient.RunTorrent(ctx, meta.InfoHash)
	}()

	select {
	case <-trackerAnnounces:
	case <-ctx.Done():
		t.Fatalf("first client did not announce: %v", ctx.Err())
	}

	secondClient, err := NewClient()
	if err != nil {
		t.Fatalf("new second client: %v", err)
	}
	secondTorrent, err := NewTorrent(meta)
	if err != nil {
		t.Fatalf("new second torrent: %v", err)
	}
	if err := secondClient.RegisterTorrent(secondTorrent); err != nil {
		t.Fatalf("add second torrent: %v", err)
	}
	secondListener, err := secondClient.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for second client: %v", err)
	}
	secondServeErr := make(chan error, 1)
	go func() {
		secondServeErr <- secondClient.ServePeers(ctx, secondListener)
	}()
	secondRunErr := make(chan error, 1)
	go func() {
		secondRunErr <- secondClient.RunTorrent(ctx, meta.InfoHash)
	}()

	if err := waitForPeerCount(ctx, firstTorrent, 1); err != nil {
		t.Fatalf("first client peer connection: %v", err)
	}
	if err := waitForPeerCount(ctx, secondTorrent, 1); err != nil {
		t.Fatalf("second client peer connection: %v", err)
	}

	cancel()
	for _, result := range []struct {
		name string
		err  error
	}{
		{name: "first client serve", err: <-firstServeErr},
		{name: "first client run", err: <-firstRunErr},
		{name: "second client serve", err: <-secondServeErr},
		{name: "second client run", err: <-secondRunErr},
	} {
		if !errors.Is(result.err, context.Canceled) {
			t.Errorf("%s error = %v, want context canceled", result.name, result.err)
		}
	}
}

func TestTorrentReannouncesAtTrackerInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	announces := make(chan struct{}, 2)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		trackerServer.ServeHTTP(w, request)
		select {
		case announces <- struct{}{}:
		default:
		}
	}))
	defer httpTracker.Close()

	meta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest([]byte("periodic announce fixture")),
		InfoHash: protocol.InfoHash{8, 5},
	}
	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	session, err := NewTorrent(meta)
	if err != nil {
		t.Fatalf("new torrent: %v", err)
	}
	if err := client.RegisterTorrent(session); err != nil {
		t.Fatalf("add torrent: %v", err)
	}
	listener, err := client.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.ServePeers(ctx, listener)
	}()
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.RunTorrent(ctx, meta.InfoHash)
	}()

	for count := 0; count < 2; count++ {
		select {
		case <-announces:
		case <-ctx.Done():
			t.Fatalf("received %d announces, want 2: %v", count, ctx.Err())
		}
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error = %v, want context canceled", err)
	}
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
}

func TestTorrentPauseAndResumeCoordinateTrackerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	announces := make(chan tracker.AnnounceEvent, 8)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		announces <- tracker.AnnounceEvent(request.URL.Query().Get("event"))
		trackerServer.ServeHTTP(w, request)
	}))
	defer httpTracker.Close()

	meta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest([]byte("pause and resume tracker lifecycle")),
		InfoHash: protocol.InfoHash{6, 2},
	}
	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	session, err := NewTorrent(meta)
	if err != nil {
		t.Fatalf("new torrent: %v", err)
	}
	if err := client.RegisterTorrent(session); err != nil {
		t.Fatalf("add torrent: %v", err)
	}
	listener, err := client.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.ServePeers(ctx, listener)
	}()
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.RunTorrent(ctx, meta.InfoHash)
	}()

	if err := waitForTrackerEvent(ctx, announces, tracker.StartedEvent); err != nil {
		t.Fatalf("initial announce: %v", err)
	}
	if err := client.PauseTorrentDownload(ctx, meta.InfoHash); err != nil {
		t.Fatalf("pause torrent: %v", err)
	}
	if err := waitForTrackerEvent(ctx, announces, tracker.StoppedEvent); err != nil {
		t.Fatalf("stopped announce: %v", err)
	}
	paused, err := session.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot paused torrent: %v", err)
	}
	if got, want := paused.State, TransferPaused; got != want {
		t.Errorf("paused state = %q, want %q", got, want)
	}

	select {
	case event := <-announces:
		t.Fatalf("paused torrent announced event %q", event)
	case <-time.After(1200 * time.Millisecond):
	}

	if err := client.ResumeTorrentDownload(ctx, meta.InfoHash); err != nil {
		t.Fatalf("resume torrent: %v", err)
	}
	if err := waitForTrackerEvent(ctx, announces, tracker.StartedEvent); err != nil {
		t.Fatalf("resumed announce: %v", err)
	}
	resumed, err := session.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot resumed torrent: %v", err)
	}
	if got, want := resumed.State, TransferDownloading; got != want {
		t.Errorf("resumed state = %q, want %q", got, want)
	}
	if err := waitForTrackerEvent(ctx, announces, ""); err != nil {
		t.Fatalf("periodic announce after resume: %v", err)
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error = %v, want context canceled", err)
	}
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
}

func TestTorrentPauseDisconnectsPeersAndPreservesVerifiedPieces(t *testing.T) {
	data := []byte("pause keeps verified torrent data")
	trackerAnnounced := make(chan struct{}, 1)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		trackerServer.ServeHTTP(w, request)
		select {
		case trackerAnnounced <- struct{}{}:
		default:
		}
	}))
	defer httpTracker.Close()

	meta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest(data),
		InfoHash: protocol.InfoHash{7, 4},
	}
	dataPath := filepath.Join(t.TempDir(), "seed.txt")
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatalf("write seed data: %v", err)
	}
	session, err := OpenSeed(meta, dataPath)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}

	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.RegisterTorrent(session); err != nil {
		t.Fatalf("register torrent: %v", err)
	}
	listener, err := client.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for peers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.ServePeers(ctx, listener)
	}()
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.RunTorrent(ctx, meta.InfoHash)
	}()

	select {
	case <-trackerAnnounced:
	case <-ctx.Done():
		t.Fatalf("running torrent did not announce: %v", ctx.Err())
	}

	connectIncomingPeer := func(peerID protocol.PeerID) net.Conn {
		t.Helper()
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dial client: %v", err)
		}
		if err := protocol.WriteHandshake(conn, protocol.Handshake{
			InfoHash: meta.InfoHash,
			PeerID:   peerID,
			Protocol: protocol.PeerProtocol,
		}); err != nil {
			_ = conn.Close()
			t.Fatalf("write incoming handshake: %v", err)
		}
		response, err := protocol.ReadHandshake(conn)
		if err != nil {
			_ = conn.Close()
			t.Fatalf("read client handshake: %v", err)
		}
		if response.InfoHash != meta.InfoHash {
			_ = conn.Close()
			t.Fatalf("response info hash = %x, want %x", response.InfoHash, meta.InfoHash)
		}
		return conn
	}

	firstRemote := connectIncomingPeer(protocol.PeerID{1})
	defer firstRemote.Close()
	if err := waitForPeerCount(ctx, session, 1); err != nil {
		t.Fatalf("wait for peer before pause: %v", err)
	}

	if err := client.PauseTorrentDownload(ctx, meta.InfoHash); err != nil {
		t.Fatalf("pause torrent: %v", err)
	}
	paused, err := session.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot paused torrent: %v", err)
	}
	if paused.State != TransferPaused {
		t.Errorf("paused state = %q, want %q", paused.State, TransferPaused)
	}
	if got := len(paused.Peers); got != 0 {
		t.Errorf("paused peer count = %d, want 0", got)
	}
	if got, want := paused.CompletePieces, 1; got != want {
		t.Errorf("paused complete pieces = %d, want %d", got, want)
	}

	pausedRemote := connectIncomingPeer(protocol.PeerID{2})
	defer pausedRemote.Close()
	if err := pausedRemote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set paused peer deadline: %v", err)
	}
	var response [1]byte
	if _, err := pausedRemote.Read(response[:]); err == nil {
		t.Fatal("paused torrent kept an incoming peer connection open")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("paused torrent did not reject the incoming peer")
	}

	if err := client.ResumeTorrentDownload(ctx, meta.InfoHash); err != nil {
		t.Fatalf("resume torrent: %v", err)
	}
	resumed, err := session.Snapshot(ctx)
	if err != nil {
		cancel()
		t.Fatalf("snapshot resumed torrent: %v", err)
	}
	if resumed.State != TransferCompleted {
		t.Errorf("resumed state = %q, want %q", resumed.State, TransferCompleted)
	}
	if got, want := resumed.CompletePieces, 1; got != want {
		t.Errorf("resumed complete pieces = %d, want %d", got, want)
	}

	cancel()
	if err := <-serveErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error = %v, want context canceled", err)
	}
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
}

func TestSeederTransfersVerifiedPieceToLeecherThroughTracker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	trackerAnnounces := make(chan struct{}, 2)
	trackerServer, err := tracker.NewServer(tracker.Config{AnnounceInterval: time.Second})
	if err != nil {
		t.Fatalf("new tracker server: %v", err)
	}
	httpTracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		trackerServer.ServeHTTP(w, request)
		select {
		case trackerAnnounces <- struct{}{}:
		default:
		}
	}))
	defer httpTracker.Close()

	data := []byte("Break My System learns from a verified BitTorrent piece.\n")
	meta := protocol.MetaInfo{
		Announce: httpTracker.URL,
		Info:     infoForTest(data),
		InfoHash: protocol.InfoHash{4, 2},
	}
	seedPath := filepath.Join(t.TempDir(), "seed.txt")
	if err := os.WriteFile(seedPath, data, 0o600); err != nil {
		t.Fatalf("write seed data: %v", err)
	}

	seeder, err := OpenSeed(meta, seedPath)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	seedClient, err := NewClient()
	if err != nil {
		t.Fatalf("new seed client: %v", err)
	}
	if err := seedClient.RegisterTorrent(seeder); err != nil {
		t.Fatalf("add seed torrent: %v", err)
	}
	seedListener, err := seedClient.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for seed: %v", err)
	}
	seedServeErr := make(chan error, 1)
	go func() {
		seedServeErr <- seedClient.ServePeers(ctx, seedListener)
	}()
	seedRunErr := make(chan error, 1)
	go func() {
		seedRunErr <- seedClient.RunTorrent(ctx, meta.InfoHash)
	}()

	select {
	case <-trackerAnnounces:
	case <-ctx.Done():
		t.Fatalf("seed did not announce: %v", ctx.Err())
	}

	outputPath := filepath.Join(t.TempDir(), "downloaded.txt")
	leecher, err := OpenDownload(meta, outputPath)
	if err != nil {
		t.Fatalf("open leecher torrent: %v", err)
	}
	leecherClient, err := NewClient()
	if err != nil {
		t.Fatalf("new leecher client: %v", err)
	}
	if err := leecherClient.RegisterTorrent(leecher); err != nil {
		t.Fatalf("add leecher torrent: %v", err)
	}
	leecherListener, err := leecherClient.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for leecher: %v", err)
	}
	leecherServeErr := make(chan error, 1)
	go func() {
		leecherServeErr <- leecherClient.ServePeers(ctx, leecherListener)
	}()
	leecherRunErr := make(chan error, 1)
	go func() {
		leecherRunErr <- leecherClient.RunTorrent(ctx, meta.InfoHash)
	}()

	if err := waitForBytesLeft(ctx, leecher, 0); err != nil {
		t.Fatalf("leecher did not complete: %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded data: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded data = %q, want %q", got, data)
	}

	cancel()
	for _, result := range []struct {
		name string
		err  error
	}{
		{name: "seed serve", err: <-seedServeErr},
		{name: "seed run", err: <-seedRunErr},
		{name: "leecher serve", err: <-leecherServeErr},
		{name: "leecher run", err: <-leecherRunErr},
	} {
		if !errors.Is(result.err, context.Canceled) {
			t.Errorf("%s error = %v, want context canceled", result.name, result.err)
		}
	}
}

func waitForPeerCount(ctx context.Context, torrent *Torrent, want int) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		snapshot, err := torrent.Snapshot(ctx)
		if err == nil && len(snapshot.Peers) == want {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitForBytesLeft(ctx context.Context, torrent *Torrent, want uint64) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		snapshot, err := torrent.Snapshot(ctx)
		if err == nil && snapshot.BytesLeft == want {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitForTrackerEvent(
	ctx context.Context,
	events <-chan tracker.AnnounceEvent,
	want tracker.AnnounceEvent,
) error {
	select {
	case event := <-events:
		if event != want {
			return fmt.Errorf("announce event = %q, want %q", event, want)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type acceptNotifyingListener struct {
	net.Listener
	accepted chan struct{}
}

func (l *acceptNotifyingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted <- struct{}{}
	}
	return conn, err
}
