package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mattcattb/go-torrent/protocol"
	"github.com/mattcattb/go-torrent/tracker"
)

type commandConfig struct {
	ListenAddress string
	StatusAddress string
	Tracker       tracker.Config
}

func loadConfig(args []string, lookupEnv func(string) (string, bool)) (commandConfig, error) {
	config := commandConfig{
		ListenAddress: ":6969",
		Tracker: tracker.Config{
			AnnounceInterval: 30 * time.Second,
		},
	}

	if value, exists := lookupEnv("TRACKER_LISTEN"); exists {
		if value == "" {
			return commandConfig{}, fmt.Errorf("TRACKER_LISTEN is empty")
		}
		config.ListenAddress = value
	} else if port, exists := lookupEnv("PORT"); exists {
		if port == "" {
			return commandConfig{}, fmt.Errorf("PORT is empty")
		}
		config.ListenAddress = ":" + port
	}
	if value, exists := lookupEnv("TRACKER_STATUS_LISTEN"); exists {
		config.StatusAddress = value
	}
	if value, exists := lookupEnv("TRACKER_INTERVAL"); exists {
		interval, err := time.ParseDuration(value)
		if err != nil {
			return commandConfig{}, fmt.Errorf("parse TRACKER_INTERVAL: %w", err)
		}
		config.Tracker.AnnounceInterval = interval
	}
	if value, exists := lookupEnv("TRACKER_PEER_TTL"); exists {
		peerTTL, err := time.ParseDuration(value)
		if err != nil {
			return commandConfig{}, fmt.Errorf("parse TRACKER_PEER_TTL: %w", err)
		}
		config.Tracker.PeerTTL = peerTTL
	}

	var allowedTorrentPaths []string
	var allowedHashValues []string
	allowlistConfigured := false
	if value, exists := lookupEnv("TRACKER_ALLOWED_INFO_HASHES"); exists {
		if strings.TrimSpace(value) == "" {
			return commandConfig{}, fmt.Errorf("TRACKER_ALLOWED_INFO_HASHES is empty")
		}
		allowlistConfigured = true
		for _, hash := range strings.Split(value, ",") {
			hash = strings.TrimSpace(hash)
			if hash == "" {
				return commandConfig{}, fmt.Errorf("TRACKER_ALLOWED_INFO_HASHES contains an empty value")
			}
			allowedHashValues = append(allowedHashValues, hash)
		}
	}

	torrentDirectory := ""
	if value, exists := lookupEnv("TRACKER_TORRENT_DIR"); exists {
		if value == "" {
			return commandConfig{}, fmt.Errorf("TRACKER_TORRENT_DIR is empty")
		}
		torrentDirectory = value
		allowlistConfigured = true
	}

	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.StringVar(&config.ListenAddress, "listen", config.ListenAddress, "HTTP listen address")
	flags.StringVar(&config.StatusAddress, "status-listen", config.StatusAddress, "optional private HTTP status listen address")
	flags.DurationVar(&config.Tracker.AnnounceInterval, "interval", config.Tracker.AnnounceInterval, "recommended client announce interval")
	flags.DurationVar(&config.Tracker.PeerTTL, "peer-ttl", config.Tracker.PeerTTL, "time after which a silent peer expires; zero derives three announce intervals")
	flags.StringVar(&torrentDirectory, "torrent-dir", torrentDirectory, "directory containing allowed .torrent files")
	flags.Func("allow-torrent", "metainfo file for an allowed swarm; may be repeated", func(path string) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return fmt.Errorf("metainfo path is empty")
		}
		allowlistConfigured = true
		allowedTorrentPaths = append(allowedTorrentPaths, path)
		return nil
	})
	flags.Func("allow-info-hash", "hexadecimal info hash for an allowed swarm; may be repeated", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("info hash is empty")
		}
		allowlistConfigured = true
		allowedHashValues = append(allowedHashValues, value)
		return nil
	})
	if err := flags.Parse(args); err != nil {
		return commandConfig{}, err
	}
	if flags.NArg() != 0 {
		return commandConfig{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	if torrentDirectory != "" {
		paths, err := torrentFiles(torrentDirectory)
		if err != nil {
			return commandConfig{}, err
		}
		allowlistConfigured = true
		allowedTorrentPaths = append(allowedTorrentPaths, paths...)
	}

	allowedInfoHashes, err := resolveAllowedInfoHashes(allowedTorrentPaths, allowedHashValues)
	if err != nil {
		return commandConfig{}, err
	}
	if allowlistConfigured {
		config.Tracker.AllowedInfoHashes = allowedInfoHashes
	}
	if err := config.Tracker.Validate(); err != nil {
		return commandConfig{}, fmt.Errorf("tracker config: %w", err)
	}
	return config, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "serve" {
		return fmt.Errorf("usage: tracker serve [options]")
	}

	config, err := loadConfig(args[1:], os.LookupEnv)
	if err != nil {
		return err
	}
	return serve(ctx, config)
}

func serve(ctx context.Context, config commandConfig) error {
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for tracker: %w", err)
	}

	trackerServer, err := tracker.NewServer(config.Tracker)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("create tracker server: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/announce", trackerServer)

	var statusListener net.Listener
	if config.StatusAddress != "" {
		statusListener, err = net.Listen("tcp", config.StatusAddress)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("listen for tracker status: %w", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fmt.Printf("tracker listening on %s; announce URL path is /announce\n", listener.Addr())
	results := make(chan error, 2)
	componentCount := 1
	go func() {
		results <- serveHTTP(runCtx, listener, mux)
	}()

	if statusListener != nil {
		componentCount++
		fmt.Printf("tracker status listening on %s\n", statusListener.Addr())
		go func() {
			results <- serveHTTP(runCtx, statusListener, trackerStatusHandler(trackerServer))
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
		for completed := 0; completed < componentCount; completed++ {
			<-results
		}
		return nil
	}
}

func trackerStatusHandler(server *tracker.Server) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(server.Snapshot()); err != nil {
			http.Error(w, "encode tracker snapshot", http.StatusInternalServerError)
		}
	})
	return mux
}

func serveHTTP(ctx context.Context, listener net.Listener, handler http.Handler) error {
	server := &http.Server{Handler: handler}
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
	return fmt.Errorf("serve HTTP: %w", err)
}

func loadAllowedInfoHashes(paths []string) ([]protocol.InfoHash, error) {
	infoHashes := make([]protocol.InfoHash, 0, len(paths))
	seen := make(map[protocol.InfoHash]struct{}, len(paths))
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open allowed torrent %q: %w", path, err)
		}

		meta, readErr := protocol.ReadMetaInfo(file)
		closeErr := file.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read allowed torrent %q: %w", path, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close allowed torrent %q: %w", path, closeErr)
		}
		if _, exists := seen[meta.InfoHash]; exists {
			continue
		}

		seen[meta.InfoHash] = struct{}{}
		infoHashes = append(infoHashes, meta.InfoHash)
	}
	return infoHashes, nil
}

func parseInfoHash(value string) (protocol.InfoHash, error) {
	var infoHash protocol.InfoHash
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return infoHash, fmt.Errorf("decode info hash %q: %w", value, err)
	}
	if len(decoded) != len(infoHash) {
		return infoHash, fmt.Errorf("info hash %q decodes to %d bytes, want %d", value, len(decoded), len(infoHash))
	}
	copy(infoHash[:], decoded)
	return infoHash, nil
}

func resolveAllowedInfoHashes(paths, hashValues []string) ([]protocol.InfoHash, error) {
	fromFiles, err := loadAllowedInfoHashes(paths)
	if err != nil {
		return nil, err
	}

	seen := make(map[protocol.InfoHash]struct{}, len(fromFiles)+len(hashValues))
	for _, infoHash := range fromFiles {
		seen[infoHash] = struct{}{}
	}
	for _, value := range hashValues {
		infoHash, err := parseInfoHash(value)
		if err != nil {
			return nil, err
		}
		seen[infoHash] = struct{}{}
	}

	infoHashes := make([]protocol.InfoHash, 0, len(seen))
	for infoHash := range seen {
		infoHashes = append(infoHashes, infoHash)
	}
	sort.Slice(infoHashes, func(i, j int) bool {
		return bytes.Compare(infoHashes[i][:], infoHashes[j][:]) < 0
	})
	return infoHashes, nil
}

func torrentFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read torrent directory %q: %w", directory, err)
	}

	paths := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".torrent" {
			continue
		}
		paths = append(paths, filepath.Join(directory, entry.Name()))
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("torrent directory %q contains no .torrent files", directory)
	}
	return paths, nil
}
