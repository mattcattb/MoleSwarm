package torrent

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mattcattb/MoleSwarm/protocol"
)

var (
	ErrInvalidPiece      = errors.New("invalid piece")
	ErrInvalidBlock      = errors.New("invalid block")
	ErrPieceInvalidState = errors.New("piece invalid state")
)

const defaultBlockLength uint32 = 16 * 1024

// fileStorage ties piece completion state to the file whose bytes that state
// describes. Its state-mutating methods are intended to be called by one
// owner, such as the torrent event loop.
type fileStorage struct {
	file        *os.File
	info        protocol.Info
	blockLength uint32
	pieces      []piece
}

func newFileStorage(file *os.File, info protocol.Info, blockLength uint32) (*fileStorage, error) {
	if file == nil {
		return nil, fmt.Errorf("torrent data file is not open")
	}
	if info.Length < 0 {
		return nil, fmt.Errorf("torrent length cannot be negative")
	}
	if info.PieceLength <= 0 || info.PieceLength > int64(^uint32(0)) {
		return nil, fmt.Errorf("piece length must fit in the wire protocol")
	}
	if blockLength == 0 {
		return nil, fmt.Errorf("block length must be positive")
	}

	expectedPieces := int64(0)
	if info.Length > 0 {
		expectedPieces = (info.Length-1)/info.PieceLength + 1
	}
	if int64(len(info.PieceHashes)) != expectedPieces {
		return nil, fmt.Errorf("torrent has %d piece hashes; expected %d", len(info.PieceHashes), expectedPieces)
	}

	fs := &fileStorage{
		file:        file,
		info:        info,
		blockLength: blockLength,
		pieces:      make([]piece, expectedPieces),
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat torrent data file: %w", err)
	}
	if err := fs.recheck(stat.Size()); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *fileStorage) close() error { return fs.file.Close() }
func (fs *fileStorage) sync() error  { return fs.file.Sync() }
func (fs *fileStorage) count() int   { return len(fs.pieces) }

func (fs *fileStorage) pieceAt(index uint32) (*piece, bool) {
	if index >= uint32(len(fs.pieces)) {
		return nil, false
	}
	return &fs.pieces[index], true
}

func (fs *fileStorage) pieceOffset(index uint32) (int64, error) {
	if _, ok := fs.pieceAt(index); !ok {
		return 0, fmt.Errorf("%w: index %d", ErrInvalidPiece, index)
	}
	return int64(index) * fs.info.PieceLength, nil
}

func (fs *fileStorage) pieceLength(index uint32) (uint32, error) {
	offset, err := fs.pieceOffset(index)
	if err != nil {
		return 0, err
	}
	remaining := fs.info.Length - offset
	if remaining <= 0 {
		return 0, fmt.Errorf("%w: index %d", ErrInvalidPiece, index)
	}
	length := fs.info.PieceLength
	if remaining < length {
		length = remaining
	}
	return uint32(length), nil
}

// recheck synchronizes memory with verified bytes already present in the file.
// Missing, short, or hash-mismatched pieces remain missing and can be fetched.
func (fs *fileStorage) recheck(existingSize int64) error {
	for index := range fs.pieces {
		pieceIndex := uint32(index)
		offset, err := fs.pieceOffset(pieceIndex)
		if err != nil {
			return err
		}
		length, err := fs.pieceLength(pieceIndex)
		if err != nil {
			return err
		}
		if existingSize < offset+int64(length) {
			continue
		}

		data := make([]byte, length)
		n, err := fs.file.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read existing piece %d: %w", pieceIndex, err)
		}
		if n == len(data) && protocol.VerifyPiece(data, fs.info.PieceHashes[index]) {
			fs.pieces[index].markExistingComplete()
		}
	}
	return nil
}

func (fs *fileStorage) blockRequest(index, begin uint32) (protocol.BlockRequest, error) {
	if begin%fs.blockLength != 0 {
		return protocol.BlockRequest{}, fmt.Errorf("%w: offset %d is not aligned", ErrInvalidBlock, begin)
	}
	pieceLength, err := fs.pieceLength(index)
	if err != nil {
		return protocol.BlockRequest{}, err
	}
	length := requestLengthFor(pieceLength, begin, fs.blockLength)
	if length == 0 {
		return protocol.BlockRequest{}, fmt.Errorf("%w: offset %d is outside piece %d", ErrInvalidBlock, begin, index)
	}
	return protocol.BlockRequest{PieceIndex: index, Begin: begin, Length: length}, nil
}

func (fs *fileStorage) nextMissingBlock(index uint32, isPending func(protocol.BlockRequest) bool) (protocol.BlockRequest, bool, error) {
	p, ok := fs.pieceAt(index)
	if !ok {
		return protocol.BlockRequest{}, false, ErrInvalidPiece
	}
	if p.state == pieceMissing {
		length, err := fs.pieceLength(index)
		if err != nil {
			return protocol.BlockRequest{}, false, err
		}
		if err := p.prepare(length, fs.blockLength); err != nil {
			return protocol.BlockRequest{}, false, err
		}
	}
	if p.state != pieceReceiving {
		return protocol.BlockRequest{}, false, nil
	}

	for blockIndex, received := range p.receivedBlocks {
		if received {
			continue
		}
		request, err := fs.blockRequest(index, uint32(blockIndex)*fs.blockLength)
		if err != nil {
			return protocol.BlockRequest{}, false, err
		}
		if isPending == nil || !isPending(request) {
			return request, true, nil
		}
	}
	return protocol.BlockRequest{}, false, nil
}

func (fs *fileStorage) acceptBlock(request protocol.BlockRequest, data []byte) (bool, error) {
	expected, err := fs.blockRequest(request.PieceIndex, request.Begin)
	if err != nil {
		return false, err
	}
	if request != expected || uint32(len(data)) != request.Length {
		return false, ErrInvalidBlock
	}

	p, ok := fs.pieceAt(request.PieceIndex)
	if !ok {
		return false, ErrInvalidPiece
	}
	if p.state == pieceMissing {
		length, err := fs.pieceLength(request.PieceIndex)
		if err != nil {
			return false, err
		}
		if err := p.prepare(length, fs.blockLength); err != nil {
			return false, err
		}
	}
	return p.acceptBlock(request.Begin, data, fs.blockLength)
}

type pieceFinalizeJob struct {
	pieceIndex uint32
	generation uint64
	buffer     []byte
}

type pieceFinalizeResult struct {
	pieceIndex uint32
	generation uint64
	buffer     []byte
	err        error
}

// takeFinalizeJob transfers buffer ownership out of the piece. The caller may
// pass the job to a worker without sharing mutable piece state with it.
func (fs *fileStorage) takeFinalizeJob(index uint32) (pieceFinalizeJob, error) {
	p, ok := fs.pieceAt(index)
	if !ok {
		return pieceFinalizeJob{}, ErrInvalidPiece
	}
	buffer, generation, err := p.takeForFinalization()
	if err != nil {
		return pieceFinalizeJob{}, err
	}
	return pieceFinalizeJob{pieceIndex: index, generation: generation, buffer: buffer}, nil
}

// verifyAndWritePiece performs only worker-safe work: hash an owned buffer and
// write it at its deterministic file offset. It never mutates piece state.
func (fs *fileStorage) verifyAndWritePiece(job pieceFinalizeJob) pieceFinalizeResult {
	result := pieceFinalizeResult{pieceIndex: job.pieceIndex, generation: job.generation, buffer: job.buffer}

	length, err := fs.pieceLength(job.pieceIndex)
	if err != nil || uint32(len(job.buffer)) != length {
		result.err = ErrInvalidPiece
		return result
	}
	if !protocol.VerifyPiece(job.buffer, fs.info.PieceHashes[job.pieceIndex]) {
		result.buffer = nil
		result.err = ErrPieceHashMismatch
		return result
	}
	offset, err := fs.pieceOffset(job.pieceIndex)
	if err != nil {
		result.err = err
		return result
	}
	n, err := fs.file.WriteAt(job.buffer, offset)
	if err != nil {
		result.err = err
		return result
	}
	if n != len(job.buffer) {
		result.err = io.ErrShortWrite
		return result
	}
	result.buffer = nil
	return result
}

// applyFinalizeResult returns false for a stale result. Hash failures reset the
// piece for downloading; storage failures restore its bytes for a write retry.
func (fs *fileStorage) applyFinalizeResult(result pieceFinalizeResult) bool {
	p, ok := fs.pieceAt(result.pieceIndex)
	if !ok {
		return false
	}
	if result.err == nil {
		return p.markComplete(result.generation)
	}
	if errors.Is(result.err, ErrPieceHashMismatch) {
		return p.reject(result.generation)
	}
	return p.restore(result.generation, result.buffer)
}

func (fs *fileStorage) readBlock(request protocol.BlockRequest) ([]byte, error) {
	p, ok := fs.pieceAt(request.PieceIndex)
	if !ok || !p.complete() {
		return nil, fmt.Errorf("%w: piece %d is unavailable", ErrInvalidPiece, request.PieceIndex)
	}
	expected, err := fs.blockRequest(request.PieceIndex, request.Begin)
	if err != nil {
		return nil, err
	}
	if request != expected {
		return nil, ErrInvalidBlock
	}
	offset, err := fs.pieceOffset(request.PieceIndex)
	if err != nil {
		return nil, err
	}
	data := make([]byte, request.Length)
	n, err := fs.file.ReadAt(data, offset+int64(request.Begin))
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

func (fs *fileStorage) completedCount() int {
	count := 0
	for index := range fs.pieces {
		if fs.pieces[index].complete() {
			count++
		}
	}
	return count
}

func (fs *fileStorage) bytesLeft() uint64 {
	var left uint64
	for index := range fs.pieces {
		if fs.pieces[index].complete() {
			continue
		}
		length, err := fs.pieceLength(uint32(index))
		if err != nil {
			panic(err)
		}
		left += uint64(length)
	}
	return left
}

func (fs *fileStorage) complete() bool { return fs.bytesLeft() == 0 }

func (fs *fileStorage) bitfield() []byte {
	bits := make([]byte, (len(fs.pieces)+7)/8)
	for index := range fs.pieces {
		if fs.pieces[index].complete() {
			bits[index/8] |= 1 << (7 - uint(index)%8)
		}
	}
	return bits
}
