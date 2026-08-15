package torrent

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mattcattb/MoleSwarm/protocol"
)

const blockSize uint32 = 16 * 1024

var ErrPieceHashMismatch = errors.New("piece hash did not match")

type PieceState struct {
	Complete bool
	Data     []byte
	Received []bool
}

// PieceSet owns the local, verified view of a torrent's content. It does not
// know which peers exist or which peer owns an outstanding request.
type PieceSet struct {
	info   protocol.Info
	states []PieceState
}

func NewPieceSet(info protocol.Info) (PieceSet, error) {
	if info.Length < 0 {
		return PieceSet{}, fmt.Errorf("torrent length cannot be negative")
	}
	if info.PieceLength <= 0 {
		return PieceSet{}, fmt.Errorf("piece length must be positive")
	}
	if info.PieceLength > int64(^uint32(0)) {
		return PieceSet{}, fmt.Errorf("piece length exceeds wire protocol limit")
	}

	var expectedPieces int64
	if info.Length > 0 {
		expectedPieces = (info.Length-1)/info.PieceLength + 1
	}
	if int64(len(info.PieceHashes)) != expectedPieces {
		return PieceSet{}, fmt.Errorf(
			"torrent has %d piece hashes; expected %d",
			len(info.PieceHashes),
			expectedPieces,
		)
	}

	return PieceSet{
		info:   info,
		states: make([]PieceState, expectedPieces),
	}, nil
}

func (p *PieceSet) Count() int {
	return len(p.states)
}

func (p *PieceSet) CompletedCount() int {
	complete := 0
	for _, state := range p.states {
		if state.Complete {
			complete++
		}
	}
	return complete
}

func (p *PieceSet) IsComplete(index uint32) bool {
	return index < uint32(len(p.states)) && p.states[index].Complete
}

func (p *PieceSet) Complete() bool {
	return p.BytesLeft() == 0
}

func (p *PieceSet) BytesLeft() uint64 {
	var left uint64

	for index, state := range p.states {
		if state.Complete {
			continue
		}

		length, err := p.PieceLength(uint32(index))
		if err != nil {
			panic(err)
		}

		left += uint64(length)
	}

	return left
}

func (p *PieceSet) BytesComplete() uint64 {
	return uint64(p.info.Length) - p.BytesLeft()
}

func (p *PieceSet) PieceLength(index uint32) (uint32, error) {
	if index >= uint32(len(p.states)) {
		return 0, fmt.Errorf("piece index %d out of range", index)
	}

	offset := int64(index) * p.info.PieceLength
	remaining := p.info.Length - offset
	if remaining <= 0 {
		return 0, fmt.Errorf("invalid piece offset for index %d", index)
	}

	length := p.info.PieceLength
	if remaining < length {
		length = remaining
	}

	return uint32(length), nil
}

func (p *PieceSet) PieceOffset(index uint32) (int64, error) {
	if index >= uint32(len(p.states)) {
		return 0, fmt.Errorf("piece index %d out of range", index)
	}

	return int64(index) * p.info.PieceLength, nil
}

func (p *PieceSet) prepare(index uint32) error {
	if p.IsComplete(index) {
		return nil
	}

	piece := &p.states[index]
	if piece.Data != nil {
		return nil
	}

	length, err := p.PieceLength(index)
	if err != nil {
		return err
	}

	piece.Data = make([]byte, length)
	piece.Received = make([]bool, blockCount(length))
	return nil
}

func (p *PieceSet) nextMissingBlock(
	index uint32,
	isPending func(protocol.BlockRequest) bool,
) (protocol.BlockRequest, bool, error) {
	if err := p.prepare(index); err != nil {
		return protocol.BlockRequest{}, false, err
	}

	piece := &p.states[index]
	for blockIndex, received := range piece.Received {
		if received {
			continue
		}

		request, err := p.blockRequest(index, uint32(blockIndex)*blockSize)
		if err != nil {
			return protocol.BlockRequest{}, false, err
		}
		if !isPending(request) {
			return request, true, nil
		}
	}

	return protocol.BlockRequest{}, false, nil
}

func (p *PieceSet) StoreBlock(
	request protocol.BlockRequest,
	data []byte,
) (bool, error) {
	expected, err := p.blockRequest(request.PieceIndex, request.Begin)
	if err != nil {
		return false, err
	}
	if request != expected || uint32(len(data)) != request.Length {
		return false, fmt.Errorf("invalid block range for piece %d", request.PieceIndex)
	}

	if err := p.prepare(request.PieceIndex); err != nil {
		return false, err
	}

	piece := &p.states[request.PieceIndex]
	blockIndex := request.Begin / blockSize
	if piece.Received[blockIndex] {
		return false, fmt.Errorf("block for piece %d at offset %d already received", request.PieceIndex, request.Begin)
	}

	begin := int(request.Begin)
	copy(piece.Data[begin:begin+len(data)], data)
	piece.Received[blockIndex] = true

	return allBlocksReceived(piece), nil
}

func (p *PieceSet) VerifyAndWrite(index uint32, file *os.File) error {
	if file == nil {
		return fmt.Errorf("torrent output file is not open")
	}
	if index >= uint32(len(p.states)) {
		return fmt.Errorf("piece index %d out of range", index)
	}

	piece := &p.states[index]
	if !allBlocksReceived(piece) {
		return fmt.Errorf("piece %d is incomplete", index)
	}

	if protocol.PieceHash(sha1.Sum(piece.Data)) != p.info.PieceHashes[index] {
		piece.Data = nil
		piece.Received = nil
		return fmt.Errorf("piece %d: %w", index, ErrPieceHashMismatch)
	}

	offset, err := p.PieceOffset(index)
	if err != nil {
		return err
	}

	written, err := file.WriteAt(piece.Data, offset)
	if err != nil {
		return err
	}
	if written != len(piece.Data) {
		return io.ErrShortWrite
	}

	piece.Complete = true
	piece.Data = nil
	piece.Received = nil
	return nil
}

// VerifyExisting marks every piece in file as available only after checking it
// against the hashes in the metainfo. It is used before a session seeds data
// that already exists on disk.
func (p *PieceSet) VerifyExisting(file *os.File) error {
	if file == nil {
		return fmt.Errorf("torrent data file is not open")
	}

	for index := range p.states {
		pieceIndex := uint32(index)
		length, err := p.PieceLength(pieceIndex)
		if err != nil {
			return err
		}
		offset, err := p.PieceOffset(pieceIndex)
		if err != nil {
			return err
		}

		data := make([]byte, length)
		read, err := file.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read piece %d: %w", pieceIndex, err)
		}
		if read != len(data) {
			return fmt.Errorf("read piece %d: %w", pieceIndex, io.ErrUnexpectedEOF)
		}
		if protocol.PieceHash(sha1.Sum(data)) != p.info.PieceHashes[pieceIndex] {
			return fmt.Errorf("verify piece %d: %w", pieceIndex, ErrPieceHashMismatch)
		}

		p.states[pieceIndex].Complete = true
	}
	return nil
}

func (p *PieceSet) ReadBlock(request protocol.BlockRequest, file *os.File) ([]byte, error) {
	if file == nil {
		return nil, fmt.Errorf("torrent output file is not open")
	}
	if !p.IsComplete(request.PieceIndex) {
		return nil, fmt.Errorf("piece %d is unavailable", request.PieceIndex)
	}

	expected, err := p.blockRequest(request.PieceIndex, request.Begin)
	if err != nil {
		return nil, err
	}
	if request != expected {
		return nil, fmt.Errorf("invalid block range for piece %d", request.PieceIndex)
	}

	offset, err := p.PieceOffset(request.PieceIndex)
	if err != nil {
		return nil, err
	}

	data := make([]byte, request.Length)
	read, err := file.ReadAt(data, offset+int64(request.Begin))
	if err != nil && err != io.EOF {
		return nil, err
	}
	if read != len(data) {
		return nil, io.ErrUnexpectedEOF
	}

	return data, nil
}

func (p *PieceSet) blockRequest(index, begin uint32) (protocol.BlockRequest, error) {
	if begin%blockSize != 0 {
		return protocol.BlockRequest{}, fmt.Errorf("block offset %d is not aligned", begin)
	}

	pieceLength, err := p.PieceLength(index)
	if err != nil {
		return protocol.BlockRequest{}, err
	}

	length := requestLength(pieceLength, begin)
	if length == 0 {
		return protocol.BlockRequest{}, fmt.Errorf("block offset %d is outside piece %d", begin, index)
	}

	return protocol.BlockRequest{
		PieceIndex: index,
		Begin:      begin,
		Length:     length,
	}, nil
}

func blockCount(pieceLength uint32) int {
	return int((pieceLength-1)/blockSize + 1)
}

func requestLength(pieceLength, begin uint32) uint32 {
	if begin >= pieceLength {
		return 0
	}

	remaining := pieceLength - begin
	if remaining < blockSize {
		return remaining
	}

	return blockSize
}

func allBlocksReceived(piece *PieceState) bool {
	if len(piece.Data) == 0 || len(piece.Received) == 0 {
		return false
	}

	for _, received := range piece.Received {
		if !received {
			return false
		}
	}

	return true
}
