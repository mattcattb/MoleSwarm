package torrent

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"os"
	"testing"

	"github.com/mattcattb/MoleSwarm/protocol"
)

func TestPieceSetWritesVerifiedPieceAndUpdatesLeft(t *testing.T) {
	data := bytes.Repeat([]byte("a"), int(blockSize)+9)
	pieces, err := NewPieceSet(infoForTest(data))
	if err != nil {
		t.Fatalf("new piece set: %v", err)
	}

	if got, want := pieces.BytesLeft(), uint64(len(data)); got != want {
		t.Fatalf("initial bytes left = %d, want %d", got, want)
	}

	assembled, err := pieces.StoreBlock(protocol.BlockRequest{
		PieceIndex: 0,
		Begin:      0,
		Length:     blockSize,
	}, data[:blockSize])
	if err != nil {
		t.Fatalf("store first block: %v", err)
	}
	if assembled {
		t.Fatal("first block assembled the piece")
	}

	assembled, err = pieces.StoreBlock(protocol.BlockRequest{
		PieceIndex: 0,
		Begin:      blockSize,
		Length:     9,
	}, data[blockSize:])
	if err != nil {
		t.Fatalf("store final block: %v", err)
	}
	if !assembled {
		t.Fatal("final block did not assemble the piece")
	}

	file, err := os.CreateTemp(t.TempDir(), "output")
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	defer file.Close()

	if err := pieces.VerifyAndWrite(0, file); err != nil {
		t.Fatalf("verify and write: %v", err)
	}

	if !pieces.IsComplete(0) {
		t.Fatal("piece was not complete after verification")
	}
	if got := pieces.BytesLeft(); got != 0 {
		t.Fatalf("bytes left = %d, want 0", got)
	}

	got, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("output = %q, want %q", got, data)
	}
}

func TestPieceSetKeepsBytesLeftAfterHashMismatch(t *testing.T) {
	expected := []byte("expected piece data")
	pieces, err := NewPieceSet(infoForTest(expected))
	if err != nil {
		t.Fatalf("new piece set: %v", err)
	}

	wrong := bytes.Repeat([]byte("x"), len(expected))
	assembled, err := pieces.StoreBlock(protocol.BlockRequest{
		PieceIndex: 0,
		Begin:      0,
		Length:     uint32(len(wrong)),
	}, wrong)
	if err != nil || !assembled {
		t.Fatalf("store block = (%t, %v), want (true, nil)", assembled, err)
	}

	file, err := os.CreateTemp(t.TempDir(), "output")
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	defer file.Close()

	err = pieces.VerifyAndWrite(0, file)
	if !errors.Is(err, ErrPieceHashMismatch) {
		t.Fatalf("verify error = %v, want hash mismatch", err)
	}
	if pieces.IsComplete(0) {
		t.Fatal("hash-mismatched piece was marked complete")
	}
	if got, want := pieces.BytesLeft(), uint64(len(expected)); got != want {
		t.Fatalf("bytes left = %d, want %d", got, want)
	}
}

func TestPieceSetVerifyExistingMarksOnlyVerifiedDataComplete(t *testing.T) {
	data := []byte("seed data must be verified before upload")
	pieces, err := NewPieceSet(infoForTest(data))
	if err != nil {
		t.Fatalf("new piece set: %v", err)
	}

	file, err := os.CreateTemp(t.TempDir(), "seed")
	if err != nil {
		t.Fatalf("create seed file: %v", err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		t.Fatalf("write seed data: %v", err)
	}

	if err := pieces.VerifyExisting(file); err != nil {
		t.Fatalf("verify existing data: %v", err)
	}
	if !pieces.Complete() {
		t.Fatal("verified seed data was not marked complete")
	}
}

func TestPieceSetVerifyExistingRejectsHashMismatch(t *testing.T) {
	pieces, err := NewPieceSet(infoForTest([]byte("expected seed data")))
	if err != nil {
		t.Fatalf("new piece set: %v", err)
	}

	file, err := os.CreateTemp(t.TempDir(), "seed")
	if err != nil {
		t.Fatalf("create seed file: %v", err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("wrong seed data!!!")); err != nil {
		t.Fatalf("write seed data: %v", err)
	}

	err = pieces.VerifyExisting(file)
	if !errors.Is(err, ErrPieceHashMismatch) {
		t.Fatalf("verify existing error = %v, want hash mismatch", err)
	}
	if pieces.Complete() {
		t.Fatal("hash-mismatched seed data was marked complete")
	}
}

func TestTorrentReceivesAssignedBlockAndCompletesPiece(t *testing.T) {
	data := []byte("a complete one-block piece")
	torrent, err := NewTorrent(protocol.MetaInfo{Info: infoForTest(data)})
	if err != nil {
		t.Fatalf("new torrent: %v", err)
	}

	file, err := os.CreateTemp(t.TempDir(), "output")
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	defer file.Close()
	torrent.file = file

	peer := &peer{}
	request := protocol.BlockRequest{
		PieceIndex: 0,
		Begin:      0,
		Length:     uint32(len(data)),
	}
	torrent.pending[request] = peer

	completed, err := torrent.receiveBlock(peer, protocol.Piece{
		PieceIndex: 0,
		Begin:      0,
		Data:       data,
	})
	if err != nil {
		t.Fatalf("receive block: %v", err)
	}
	if !completed {
		t.Fatal("piece did not complete")
	}
	if len(torrent.pending) != 0 {
		t.Fatal("pending request was not removed")
	}
	if got := torrent.pieces.BytesLeft(); got != 0 {
		t.Fatalf("bytes left = %d, want 0", got)
	}
}

func infoForTest(data []byte) protocol.Info {
	hash := protocol.PieceHash(sha1.Sum(data))
	return protocol.Info{
		Name:        "fixture",
		Length:      int64(len(data)),
		PieceLength: int64(len(data)),
		PieceHashes: []protocol.PieceHash{hash},
	}
}
