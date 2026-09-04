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
	torrent, err := New(protocol.MetaInfo{Info: infoForTest(data)})
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

func TestFileStorageAssemblesFinalizesAndReadsPiece(t *testing.T) {
	data := []byte("ten-bytes!")
	file, err := os.CreateTemp(t.TempDir(), "storage")
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer file.Close()

	storage, err := newFileStorage(file, infoForTest(data), 4)
	if err != nil {
		t.Fatalf("new file storage: %v", err)
	}
	for begin := uint32(0); ; begin += storage.blockLength {
		request, err := storage.blockRequest(0, begin)
		if err != nil {
			t.Fatalf("block request at %d: %v", begin, err)
		}
		assembled, err := storage.acceptBlock(request, data[begin:begin+request.Length])
		if err != nil {
			t.Fatalf("accept block at %d: %v", begin, err)
		}
		if begin+request.Length == uint32(len(data)) {
			if !assembled {
				t.Fatal("last block did not assemble piece")
			}
			break
		}
		if assembled {
			t.Fatal("piece assembled before its final block")
		}
	}

	job, err := storage.takeFinalizeJob(0)
	if err != nil {
		t.Fatalf("take finalize job: %v", err)
	}
	p, _ := storage.pieceAt(0)
	if p.state != pieceFinalizing || p.buffer != nil {
		t.Fatalf("piece retained buffer while finalizing: state=%v buffer=%v", p.state, p.buffer)
	}

	result := storage.verifyAndWritePiece(job)
	if result.err != nil {
		t.Fatalf("verify and write: %v", result.err)
	}
	if !storage.applyFinalizeResult(result) || !p.complete() {
		t.Fatal("successful finalization did not complete piece")
	}

	request, err := storage.blockRequest(0, 4)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	got, err := storage.readBlock(request)
	if err != nil {
		t.Fatalf("read block: %v", err)
	}
	if !bytes.Equal(got, data[4:8]) {
		t.Fatalf("read block = %q, want %q", got, data[4:8])
	}
}

func TestFileStorageHashFailureReturnsPieceToMissing(t *testing.T) {
	expected := []byte("expected")
	file, err := os.CreateTemp(t.TempDir(), "storage")
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer file.Close()

	storage, err := newFileStorage(file, infoForTest(expected), defaultBlockLength)
	if err != nil {
		t.Fatalf("new file storage: %v", err)
	}
	request, err := storage.blockRequest(0, 0)
	if err != nil {
		t.Fatalf("block request: %v", err)
	}
	if assembled, err := storage.acceptBlock(request, []byte("bad-data")); err != nil || !assembled {
		t.Fatalf("accept wrong block = (%t, %v), want (true, nil)", assembled, err)
	}
	job, err := storage.takeFinalizeJob(0)
	if err != nil {
		t.Fatalf("take finalize job: %v", err)
	}
	result := storage.verifyAndWritePiece(job)
	if !errors.Is(result.err, ErrPieceHashMismatch) {
		t.Fatalf("finalize error = %v, want hash mismatch", result.err)
	}
	if !storage.applyFinalizeResult(result) {
		t.Fatal("hash failure result was not applied")
	}
	p, _ := storage.pieceAt(0)
	if p.state != pieceMissing || p.buffer != nil || p.receivedBlocks != nil {
		t.Fatalf("rejected piece invariant failed: state=%v buffer=%v blocks=%v", p.state, p.buffer, p.receivedBlocks)
	}
}

func TestFileStorageRecheckKeepsOnlyVerifiedExistingPieces(t *testing.T) {
	first := []byte("good")
	second := []byte("data")
	info := protocol.Info{
		Name:        "fixture",
		Length:      8,
		PieceLength: 4,
		PieceHashes: []protocol.PieceHash{protocol.HashPiece(first), protocol.HashPiece(second)},
	}
	file, err := os.CreateTemp(t.TempDir(), "resume")
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	defer file.Close()
	if _, err := file.Write(append(first, []byte("nope")...)); err != nil {
		t.Fatalf("write existing data: %v", err)
	}

	storage, err := newFileStorage(file, info, defaultBlockLength)
	if err != nil {
		t.Fatalf("new file storage: %v", err)
	}
	firstPiece, _ := storage.pieceAt(0)
	secondPiece, _ := storage.pieceAt(1)
	if !firstPiece.complete() || secondPiece.state != pieceMissing {
		t.Fatalf("recheck states = (%v, %v), want (complete, missing)", firstPiece.state, secondPiece.state)
	}
	if got := storage.bytesLeft(); got != 4 {
		t.Fatalf("bytes left = %d, want 4", got)
	}
	if got := storage.bitfield(); !bytes.Equal(got, []byte{0x80}) {
		t.Fatalf("bitfield = %08b, want 10000000", got)
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
