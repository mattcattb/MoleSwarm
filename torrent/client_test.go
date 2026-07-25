package torrent

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
)

func TestClientRoutesRegisteredInfoHashUsingSharedIdentity(t *testing.T) {
	client, err := NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	firstMeta := protocol.MetaInfo{
		Info:     infoForTest([]byte("first client routing fixture")),
		InfoHash: protocol.InfoHash{1, 2, 3},
	}
	firstSession, err := NewTorrent(firstMeta)
	if err != nil {
		t.Fatalf("new first torrent: %v", err)
	}
	if err := client.AddTorrent(firstSession); err != nil {
		t.Fatalf("add first torrent: %v", err)
	}

	routedMeta := protocol.MetaInfo{
		Info:     infoForTest([]byte("second client routing fixture")),
		InfoHash: protocol.InfoHash{4, 5, 6},
	}
	routedSession, err := NewTorrent(routedMeta)
	if err != nil {
		t.Fatalf("new routed torrent: %v", err)
	}
	if err := client.AddTorrent(routedSession); err != nil {
		t.Fatalf("add routed torrent: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan error, 1)
	runErr := make(chan error, 1)
	go func() {
		runErr <- routedSession.run(ctx, ready)
	}()
	if err := <-ready; err != nil {
		cancel()
		t.Fatalf("run routed torrent: %v", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.Serve(ctx, listener)
	}()

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

	meta := protocol.MetaInfo{
		Info:     infoForTest([]byte("known torrent")),
		InfoHash: protocol.InfoHash{1, 2, 3},
	}
	session, err := NewTorrent(meta)
	if err != nil {
		t.Fatalf("new torrent: %v", err)
	}
	if err := client.AddTorrent(session); err != nil {
		t.Fatalf("add torrent: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- client.Serve(ctx, listener)
	}()

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
		serveErr <- client.Serve(ctx, listener)
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
