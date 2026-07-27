package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
	"github.com/mattcattb/go-torrent/torrent"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: torrent <create|inspect|seed|download> [options]")
	}

	switch args[0] {
	case "create":
		return create(args[1:])
	case "inspect":
		return inspect(args[1:])
	case "seed":
		return seed(ctx, args[1:])
	case "download":
		return download(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

const (
	defaultPieceLength = 256 * 1024
	maxPieceLength     = 4 * 1024 * 1024
)

func create(args []string) error {
	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	announceURL := flags.String("announce", "", "HTTP(S) tracker announce URL")
	outputPath := flags.String("output", "", "new torrent metainfo path")
	pieceLength := flags.Int64("piece-length", defaultPieceLength, "piece size in bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *announceURL == "" || *outputPath == "" || flags.NArg() != 1 {
		return fmt.Errorf("usage: torrent create -announce <url> -output <torrent-file> [-piece-length <bytes>] <source-file>")
	}
	if err := validateHTTPURL(*announceURL); err != nil {
		return fmt.Errorf("announce URL: %w", err)
	}
	if *pieceLength <= 0 || *pieceLength > maxPieceLength {
		return fmt.Errorf("piece length must be between 1 and %d bytes", maxPieceLength)
	}
	if _, err := os.Stat(*outputPath); err == nil {
		return fmt.Errorf("output metainfo file %q already exists", *outputPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check output metainfo file: %w", err)
	}

	sourcePath := flags.Arg(0)
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}
	if !sourceInfo.Mode().IsRegular() {
		return fmt.Errorf("source file %q is not a regular file", sourcePath)
	}

	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source file: %w", err)
	}
	defer source.Close()
	pieceHashes, err := hashPieces(source, *pieceLength)
	if err != nil {
		return fmt.Errorf("hash source file: %w", err)
	}

	encoded, meta, err := protocol.EncodeMetaInfo(*announceURL, protocol.Info{
		Name:        filepath.Base(sourcePath),
		Length:      sourceInfo.Size(),
		PieceLength: *pieceLength,
		PieceHashes: pieceHashes,
	})
	if err != nil {
		return fmt.Errorf("create metainfo: %w", err)
	}
	if err := writeNewFile(*outputPath, encoded); err != nil {
		return fmt.Errorf("write metainfo: %w", err)
	}

	digest := sha256.Sum256(encoded)
	fmt.Printf("Created:             %s\n", *outputPath)
	fmt.Printf("Name:                %s\n", meta.Info.Name)
	fmt.Printf("Length:              %d bytes\n", meta.Info.Length)
	fmt.Printf("Piece length:        %d bytes\n", meta.Info.PieceLength)
	fmt.Printf("Pieces:              %d\n", len(meta.Info.PieceHashes))
	fmt.Printf("Info hash:           %x\n", meta.InfoHash)
	fmt.Printf("Metainfo SHA-256:    %x\n", digest)
	fmt.Printf("Tracker:             %s\n", meta.Announce)
	return nil
}

func hashPieces(source io.Reader, pieceLength int64) ([]protocol.PieceHash, error) {
	buffer := make([]byte, int(pieceLength))
	pieceHashes := make([]protocol.PieceHash, 0)
	for {
		read, err := io.ReadFull(source, buffer)
		if err == io.EOF {
			return pieceHashes, nil
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		pieceHashes = append(pieceHashes, protocol.PieceHash(sha1.Sum(buffer[:read])))
		if err == io.ErrUnexpectedEOF {
			return pieceHashes, nil
		}
	}
}

func writeNewFile(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	temporary, err := os.CreateTemp(directory, ".metainfo-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("output metainfo file %q already exists", path)
		}
		return err
	}
	return nil
}

func inspect(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: torrent inspect <torrent-file>")
	}

	file, err := os.Open(args[0])
	if err != nil {
		return fmt.Errorf("open torrent file: %w", err)
	}
	defer file.Close()

	meta, err := protocol.ReadMetaInfo(file)
	if err != nil {
		return fmt.Errorf("read torrent metadata: %w", err)
	}

	fmt.Printf("Name:         %s\n", meta.Info.Name)
	fmt.Printf("Length:       %d bytes\n", meta.Info.Length)
	fmt.Printf("Piece length: %d bytes\n", meta.Info.PieceLength)
	fmt.Printf("Pieces:       %d\n", len(meta.Info.PieceHashes))
	fmt.Printf("Info hash:    %x\n", meta.InfoHash)
	fmt.Printf("Tracker:      %s\n", meta.Announce)

	return nil
}

func seed(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	listenAddress := flags.String("listen", ":6881", "peer listen address")
	statusAddress := flags.String("status-listen", "", "optional private HTTP status listen address")
	dataPath := flags.String("data", "", "complete source file to verify and seed")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dataPath == "" || flags.NArg() != 1 {
		return fmt.Errorf("usage: torrent seed -data <source-file> [-listen <address>] [-status-listen <address>] <torrent-file>")
	}

	meta, err := readMetaInfoFile(flags.Arg(0))
	if err != nil {
		return err
	}
	session, err := torrent.OpenSeed(meta, *dataPath)
	if err != nil {
		return fmt.Errorf("open torrent seed: %w", err)
	}

	return runSession(ctx, session, *listenAddress, *statusAddress)
}

func download(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("download", flag.ContinueOnError)
	listenAddress := flags.String("listen", ":6881", "peer listen address")
	statusAddress := flags.String("status-listen", "", "optional private HTTP status listen address")
	outputPath := flags.String("output", "", "new output file path")
	metainfoURL := flags.String("metainfo-url", "", "HTTP(S) URL for a torrent metainfo file")
	metainfoSHA256 := flags.String("metainfo-sha256", "", "expected SHA-256 for -metainfo-url")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *outputPath == "" {
		return fmt.Errorf("usage: torrent download -output <file> [-listen <address>] [-status-listen <address>] (-metainfo-url <url> -metainfo-sha256 <hex> | <torrent-file>)")
	}
	if *metainfoURL == "" && flags.NArg() != 1 {
		return fmt.Errorf("usage: torrent download -output <file> [-listen <address>] [-status-listen <address>] (-metainfo-url <url> -metainfo-sha256 <hex> | <torrent-file>)")
	}
	if *metainfoURL != "" && flags.NArg() != 0 {
		return fmt.Errorf("download accepts either -metainfo-url or a torrent-file path, not both")
	}
	if *metainfoURL == "" && *metainfoSHA256 != "" {
		return fmt.Errorf("-metainfo-sha256 requires -metainfo-url")
	}
	if _, err := os.Stat(*outputPath); err == nil {
		return fmt.Errorf("output file %q already exists", *outputPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check output file: %w", err)
	}

	meta, err := loadDownloadMetaInfo(ctx, flags.Args(), *metainfoURL, *metainfoSHA256)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*outputPath), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	session, err := torrent.OpenTorrent(meta, *outputPath)
	if err != nil {
		return fmt.Errorf("open torrent session: %w", err)
	}
	return runSession(ctx, session, *listenAddress, *statusAddress)
}

func runSession(ctx context.Context, session *torrent.Torrent, listenAddress, statusAddress string) error {
	client, err := torrent.NewClient()
	if err != nil {
		return fmt.Errorf("create torrent client: %w", err)
	}
	if err := client.AddTorrent(session); err != nil {
		return fmt.Errorf("add torrent: %w", err)
	}

	listener, err := client.Listen(listenAddress)
	if err != nil {
		return err
	}

	var statusListener net.Listener
	if statusAddress != "" {
		statusListener, err = net.Listen("tcp", statusAddress)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("listen for torrent status: %w", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 3)
	componentCount := 2
	go func() {
		results <- client.Serve(runCtx, listener)
	}()
	go func() {
		results <- client.RunTorrent(runCtx, session.Meta.InfoHash)
	}()
	if statusListener != nil {
		componentCount++
		go func() {
			results <- serveTorrentStatus(runCtx, statusListener, client, session)
		}()
	}

	select {
	case err := <-results:
		cancel()
		for completed := 1; completed < componentCount; completed++ {
			<-results
		}
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return nil
		}
		return err

	case <-ctx.Done():
		cancel()
		for completed := 0; completed < componentCount; completed++ {
			<-results
		}
		return nil
	}
}

type torrentStatusResponse struct {
	PeerID  string           `json:"peerId"`
	Torrent torrent.Snapshot `json:"torrent"`
}

func torrentStatusHandler(client *torrent.Client, session *torrent.Torrent) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/snapshot", func(w http.ResponseWriter, request *http.Request) {
		snapshot, err := session.Snapshot(request.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(torrentStatusResponse{
			PeerID:  fmt.Sprintf("%x", client.PeerID()),
			Torrent: snapshot,
		}); err != nil {
			http.Error(w, "encode torrent snapshot", http.StatusInternalServerError)
		}
	})
	return mux
}

func serveTorrentStatus(
	ctx context.Context,
	listener net.Listener,
	client *torrent.Client,
	session *torrent.Torrent,
) error {
	server := &http.Server{Handler: torrentStatusHandler(client, session)}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	err := server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil {
		<-shutdownDone
		return ctx.Err()
	}
	return fmt.Errorf("serve torrent status: %w", err)
}

func loadDownloadMetaInfo(ctx context.Context, paths []string, metainfoURL, expectedSHA256 string) (protocol.MetaInfo, error) {
	if metainfoURL != "" {
		if len(paths) != 0 {
			return protocol.MetaInfo{}, fmt.Errorf("download accepts either -metainfo-url or a torrent-file path, not both")
		}
		return fetchMetaInfo(ctx, metainfoURL, expectedSHA256)
	}
	if len(paths) != 1 {
		return protocol.MetaInfo{}, fmt.Errorf("torrent file path is required")
	}

	return readMetaInfoFile(paths[0])
}

func readMetaInfoFile(path string) (protocol.MetaInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return protocol.MetaInfo{}, fmt.Errorf("open torrent file: %w", err)
	}
	defer file.Close()

	meta, err := protocol.ReadMetaInfo(file)
	if err != nil {
		return protocol.MetaInfo{}, fmt.Errorf("read torrent metadata: %w", err)
	}
	return meta, nil
}

func fetchMetaInfo(ctx context.Context, metainfoURL, expectedSHA256 string) (protocol.MetaInfo, error) {
	expectedDigest, err := parseSHA256(expectedSHA256)
	if err != nil {
		return protocol.MetaInfo{}, err
	}

	if err := validateHTTPURL(metainfoURL); err != nil {
		return protocol.MetaInfo{}, err
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, metainfoURL, nil)
	if err != nil {
		return protocol.MetaInfo{}, fmt.Errorf("build metainfo request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return protocol.MetaInfo{}, fmt.Errorf("fetch metainfo: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return protocol.MetaInfo{}, fmt.Errorf("fetch metainfo: unexpected HTTP status %s", response.Status)
	}

	digest := sha256.New()
	meta, err := protocol.ReadMetaInfo(io.TeeReader(response.Body, digest))
	if err != nil {
		return protocol.MetaInfo{}, fmt.Errorf("read metainfo response: %w", err)
	}
	if got := digest.Sum(nil); !bytes.Equal(got, expectedDigest[:]) {
		return protocol.MetaInfo{}, fmt.Errorf("metainfo SHA-256 does not match expected digest")
	}
	return meta, nil
}

func validateHTTPURL(value string) error {
	parsedURL, err := url.ParseRequestURI(value)
	if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return fmt.Errorf("must be an absolute HTTP(S) URL")
	}
	return nil
}

func parseSHA256(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(value) != hex.EncodedLen(len(digest)) {
		return digest, fmt.Errorf("metainfo SHA-256 must be %d hexadecimal characters", hex.EncodedLen(len(digest)))
	}
	if _, err := hex.Decode(digest[:], []byte(value)); err != nil {
		return digest, fmt.Errorf("metainfo SHA-256 must be hexadecimal: %w", err)
	}
	return digest, nil
}
