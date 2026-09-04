package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/torrent"
)

func TestFetchMetaInfoVerifiesDigestAndParsesResponse(t *testing.T) {
	metaBytes, infoHash := metainfoForTest(t)
	digest := sha256.Sum256(metaBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got, want := request.Method, http.MethodGet; got != want {
			t.Errorf("method = %s, want %s", got, want)
		}
		_, _ = w.Write(metaBytes)
	}))
	defer server.Close()

	meta, err := fetchMetaInfo(context.Background(), server.URL, fmt.Sprintf("%x", digest))
	if err != nil {
		t.Fatalf("fetch metainfo: %v", err)
	}
	if got, want := meta.InfoHash, infoHash; got != want {
		t.Fatalf("info hash = %x, want %x", got, want)
	}
}

func TestFetchMetaInfoRejectsDigestMismatch(t *testing.T) {
	metaBytes, _ := metainfoForTest(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(metaBytes)
	}))
	defer server.Close()

	_, err := fetchMetaInfo(context.Background(), server.URL, strings.Repeat("0", sha256.Size*2))
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("fetch error = %v, want digest mismatch", err)
	}
}

func TestLoadDownloadMetaInfoRejectsAmbiguousSource(t *testing.T) {
	_, err := loadDownloadMetaInfo(context.Background(), []string{"lesson.torrent"}, "https://example.test/lesson.torrent", strings.Repeat("0", sha256.Size*2))
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("load error = %v, want ambiguous source error", err)
	}
}

func TestCreateWritesValidMetainfoForSourceFile(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "lesson.txt")
	data := bytes.Repeat([]byte("BMS torrent artifact\n"), 2_000)
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	outputPath := filepath.Join(t.TempDir(), "artifact.torrent")

	if err := create([]string{
		"-announce", "https://tracker.example/announce",
		"-output", outputPath,
		"-piece-length", "16384",
		sourcePath,
	}); err != nil {
		t.Fatalf("create torrent: %v", err)
	}

	encoded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read metainfo: %v", err)
	}
	outputInfo, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat metainfo: %v", err)
	}
	if got, want := outputInfo.Mode().Perm(), os.FileMode(0o644); got != want {
		t.Errorf("metainfo permissions = %o, want %o", got, want)
	}
	meta, err := protocol.ReadMetaInfo(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("read generated metainfo: %v", err)
	}
	if got, want := meta.Announce, "https://tracker.example/announce"; got != want {
		t.Fatalf("announce = %q, want %q", got, want)
	}
	if got, want := meta.Info.Name, "lesson.txt"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	if got, want := meta.Info.Length, int64(len(data)); got != want {
		t.Fatalf("length = %d, want %d", got, want)
	}
	if got, want := meta.Info.PieceLength, int64(16*1024); got != want {
		t.Fatalf("piece length = %d, want %d", got, want)
	}
	if got, want := len(meta.Info.PieceHashes), 3; got != want {
		t.Fatalf("piece count = %d, want %d", got, want)
	}
	if got, want := meta.Info.PieceHashes[0], protocol.PieceHash(sha1.Sum(data[:16*1024])); got != want {
		t.Fatalf("first piece hash = %x, want %x", got, want)
	}
}

func TestSeedRejectsSourceDataThatDoesNotMatchMetainfo(t *testing.T) {
	metaBytes, _ := metainfoForTest(t)
	torrentPath := filepath.Join(t.TempDir(), "lesson.torrent")
	if err := os.WriteFile(torrentPath, metaBytes, 0o600); err != nil {
		t.Fatalf("write metainfo: %v", err)
	}
	dataPath := filepath.Join(t.TempDir(), "wrong.txt")
	if err := os.WriteFile(dataPath, []byte("wrong"), 0o600); err != nil {
		t.Fatalf("write source data: %v", err)
	}

	err := seed(context.Background(), []string{"-data", dataPath, torrentPath})
	if !errors.Is(err, torrent.ErrPieceHashMismatch) {
		t.Fatalf("seed error = %v, want hash mismatch", err)
	}
}

func TestTorrentStatusHandlerReportsRunningTorrent(t *testing.T) {
	trackerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("d8:intervali30e5:peers0:e"))
	}))
	defer trackerServer.Close()

	data := []byte("observable torrent")
	meta := protocol.MetaInfo{
		Announce: trackerServer.URL,
		Info: protocol.Info{
			Name:        "observable.txt",
			Length:      int64(len(data)),
			PieceLength: int64(len(data)),
			PieceHashes: []protocol.PieceHash{protocol.PieceHash(sha1.Sum(data))},
		},
		InfoHash: protocol.InfoHash{1, 2, 3},
	}
	dataPath := filepath.Join(t.TempDir(), "observable.txt")
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatalf("write seed data: %v", err)
	}
	activeTorrent, err := torrent.OpenSeed(meta, dataPath)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	client, err := torrent.NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.RegisterTorrent(activeTorrent); err != nil {
		t.Fatalf("add torrent: %v", err)
	}
	listener, err := client.ListenForPeers("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- client.RunTorrent(ctx, meta.InfoHash)
	}()

	var response *httptest.ResponseRecorder
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response = httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil)
		torrentStatusHandler(client, activeTorrent).ServeHTTP(response, request)
		if response.Code == http.StatusOK {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if response == nil || response.Code != http.StatusOK {
		cancel()
		t.Fatalf("snapshot status = %d, want %d", response.Code, http.StatusOK)
	}

	var status torrentStatusResponse
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		cancel()
		t.Fatalf("decode status: %v", err)
	}
	if got, want := status.PeerID, fmt.Sprintf("%x", client.PeerID()); got != want {
		t.Errorf("status peer ID = %q, want %q", got, want)
	}
	if got, want := status.Torrent.CompletePieces, 1; got != want {
		t.Errorf("complete pieces = %d, want %d", got, want)
	}
	if got := status.Torrent.BytesLeft; got != 0 {
		t.Errorf("bytes left = %d, want 0", got)
	}

	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("run torrent error = %v, want context canceled", err)
	}
}

func metainfoForTest(t *testing.T) ([]byte, protocol.InfoHash) {
	t.Helper()
	pieceHash := sha1.Sum([]byte("hello"))
	infoBytes := []byte("d6:lengthi5e4:name5:hello12:piece lengthi5e6:pieces20:")
	infoBytes = append(infoBytes, pieceHash[:]...)
	infoBytes = append(infoBytes, 'e')
	metaBytes := []byte("d8:announce28:http://tracker.test/announce4:info")
	metaBytes = append(metaBytes, infoBytes...)
	metaBytes = append(metaBytes, 'e')
	return metaBytes, protocol.InfoHash(sha1.Sum(infoBytes))
}
