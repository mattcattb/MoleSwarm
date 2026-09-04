package main

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
)

func TestLoadAllowedInfoHashesReadsAndDeduplicatesMetaInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lesson.torrent")
	metaBytes, infoHash := metainfoForTest(t)
	if err := os.WriteFile(path, metaBytes, 0o600); err != nil {
		t.Fatalf("write metainfo: %v", err)
	}

	infoHashes, err := loadAllowedInfoHashes([]string{path, path})
	if err != nil {
		t.Fatalf("load allowed info hashes: %v", err)
	}
	if got, want := infoHashes, []protocol.InfoHash{infoHash}; !equalInfoHashes(got, want) {
		t.Fatalf("info hashes = %x, want %x", got, want)
	}
}

func TestLoadAllowedInfoHashesRejectsInvalidMetaInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.torrent")
	if err := os.WriteFile(path, []byte("not a torrent"), 0o600); err != nil {
		t.Fatalf("write invalid metainfo: %v", err)
	}

	if _, err := loadAllowedInfoHashes([]string{path}); err == nil {
		t.Fatal("load allowed info hashes succeeded for invalid metainfo")
	}
}

func TestTrackerStatusHandlerReportsAllowedSwarm(t *testing.T) {
	infoHash := protocol.InfoHash{1, 2, 3}
	server, err := tracker.NewServer(tracker.Config{
		AnnounceInterval:  time.Second,
		AllowedInfoHashes: []protocol.InfoHash{infoHash},
	})
	if err != nil {
		t.Fatalf("new restricted tracker server: %v", err)
	}
	response := httptest.NewRecorder()

	trackerStatusHandler(server).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil),
	)

	if got, want := response.Code, http.StatusOK; got != want {
		t.Fatalf("status code = %d, want %d", got, want)
	}
	var snapshot tracker.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if got, want := len(snapshot.Swarms), 1; got != want {
		t.Fatalf("swarm count = %d, want %d", got, want)
	}
	if got, want := snapshot.Swarms[0].InfoHash, fmt.Sprintf("%x", infoHash); got != want {
		t.Errorf("info hash = %q, want %q", got, want)
	}
}

func TestLoadConfigUsesDefaultsAndLetsFlagsOverrideEnvironment(t *testing.T) {
	environment := map[string]string{
		"PORT":                  "9999",
		"TRACKER_LISTEN":        ":7000",
		"TRACKER_STATUS_LISTEN": ":8000",
		"TRACKER_INTERVAL":      "10s",
		"TRACKER_PEER_TTL":      "40s",
	}

	config, err := loadConfig(
		[]string{"-listen", ":7001", "-interval", "5s"},
		func(key string) (string, bool) {
			value, exists := environment[key]
			return value, exists
		},
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got, want := config.ListenAddress, ":7001"; got != want {
		t.Errorf("listen address = %q, want %q", got, want)
	}
	if got, want := config.StatusAddress, ":8000"; got != want {
		t.Errorf("status address = %q, want %q", got, want)
	}
	if got, want := config.Tracker.AnnounceInterval, 5*time.Second; got != want {
		t.Errorf("announce interval = %s, want %s", got, want)
	}
	if got, want := config.Tracker.PeerTTL, 40*time.Second; got != want {
		t.Errorf("peer TTL = %s, want %s", got, want)
	}
	if config.Tracker.AllowedInfoHashes != nil {
		t.Fatalf("allowed info hashes = %x, want unrestricted nil slice", config.Tracker.AllowedInfoHashes)
	}
}

func TestLoadConfigCombinesAndDeduplicatesAllowedSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lesson.torrent")
	metaBytes, fileInfoHash := metainfoForTest(t)
	if err := os.WriteFile(path, metaBytes, 0o600); err != nil {
		t.Fatalf("write metainfo: %v", err)
	}
	environmentInfoHash := protocol.InfoHash{9, 8, 7}
	environment := map[string]string{
		"TRACKER_ALLOWED_INFO_HASHES": fmt.Sprintf("%x,%x", environmentInfoHash, environmentInfoHash),
	}

	config, err := loadConfig(
		[]string{
			"-allow-info-hash", fmt.Sprintf("%x", environmentInfoHash),
			"-allow-torrent", path,
		},
		func(key string) (string, bool) {
			value, exists := environment[key]
			return value, exists
		},
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got, want := len(config.Tracker.AllowedInfoHashes), 2; got != want {
		t.Fatalf("allowed info hash count = %d, want %d", got, want)
	}
	seen := make(map[protocol.InfoHash]struct{}, len(config.Tracker.AllowedInfoHashes))
	for _, infoHash := range config.Tracker.AllowedInfoHashes {
		seen[infoHash] = struct{}{}
	}
	for _, infoHash := range []protocol.InfoHash{environmentInfoHash, fileInfoHash} {
		if _, exists := seen[infoHash]; !exists {
			t.Errorf("allowed info hashes do not contain %x", infoHash)
		}
	}
}

func TestLoadConfigLoadsTorrentDirectory(t *testing.T) {
	directory := t.TempDir()
	metaBytes, infoHash := metainfoForTest(t)
	if err := os.WriteFile(filepath.Join(directory, "lesson.torrent"), metaBytes, 0o600); err != nil {
		t.Fatalf("write metainfo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "README.md"), []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write ignored file: %v", err)
	}

	config, err := loadConfig(nil, func(key string) (string, bool) {
		if key == "TRACKER_TORRENT_DIR" {
			return directory, true
		}
		return "", false
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got, want := config.Tracker.AllowedInfoHashes, []protocol.InfoHash{infoHash}; !equalInfoHashes(got, want) {
		t.Fatalf("allowed info hashes = %x, want %x", got, want)
	}
}

func TestLoadConfigRejectsInvalidExplicitValues(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		environment map[string]string
		errorText   string
	}{
		{
			name:        "invalid duration",
			environment: map[string]string{"TRACKER_INTERVAL": "soon"},
			errorText:   "parse TRACKER_INTERVAL",
		},
		{
			name:        "invalid info hash",
			environment: map[string]string{"TRACKER_ALLOWED_INFO_HASHES": "not-a-hash"},
			errorText:   "decode info hash",
		},
		{
			name:      "peer TTL shorter than interval",
			args:      []string{"-interval", "10s", "-peer-ttl", "5s"},
			errorText: "peer TTL must be at least",
		},
		{
			name:        "empty torrent directory",
			environment: map[string]string{"TRACKER_TORRENT_DIR": t.TempDir()},
			errorText:   "contains no .torrent files",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadConfig(test.args, func(key string) (string, bool) {
				value, exists := test.environment[key]
				return value, exists
			})
			if err == nil || !strings.Contains(err.Error(), test.errorText) {
				t.Fatalf("load config error = %v, want text %q", err, test.errorText)
			}
		})
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

func equalInfoHashes(got, want []protocol.InfoHash) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
