package torrent

type pieceState uint8

const (
	pieceMissing pieceState = iota
	pieceReceiving
	pieceAssembled
	pieceFinalizing
	pieceComplete
)

// piece contains only the in-memory state of one piece. The torrent event loop
// should be its sole owner; finalization transfers ownership of buffer to a
// worker until a result is applied.
type piece struct {
	state          pieceState
	generation     uint64
	buffer         []byte
	receivedBlocks []bool
}

func (p *piece) prepare(length, blockLength uint32) error {
	if p.state != pieceMissing || length == 0 || blockLength == 0 {
		return ErrPieceInvalidState
	}

	p.generation++
	p.state = pieceReceiving
	p.buffer = make([]byte, length)
	p.receivedBlocks = make([]bool, blockCountFor(length, blockLength))
	return nil
}

func (p *piece) acceptBlock(begin uint32, data []byte, blockLength uint32) (bool, error) {
	if p.state != pieceReceiving || p.buffer == nil || len(p.receivedBlocks) == 0 {
		return false, ErrPieceInvalidState
	}
	if blockLength == 0 || begin%blockLength != 0 {
		return false, ErrInvalidBlock
	}

	expectedLength := requestLengthFor(uint32(len(p.buffer)), begin, blockLength)
	if expectedLength == 0 || uint32(len(data)) != expectedLength {
		return false, ErrInvalidBlock
	}

	blockIndex := int(begin / blockLength)
	if blockIndex >= len(p.receivedBlocks) || p.receivedBlocks[blockIndex] {
		return false, ErrInvalidBlock
	}

	copy(p.buffer[int(begin):int(begin)+len(data)], data)
	p.receivedBlocks[blockIndex] = true
	if !p.allBlocksReceived() {
		return false, nil
	}

	p.state = pieceAssembled
	p.receivedBlocks = nil
	return true, nil
}

func (p *piece) allBlocksReceived() bool {
	if p.state != pieceReceiving || p.buffer == nil || len(p.receivedBlocks) == 0 {
		return false
	}
	for _, received := range p.receivedBlocks {
		if !received {
			return false
		}
	}
	return true
}

func (p *piece) takeForFinalization() ([]byte, uint64, error) {
	if p.state != pieceAssembled || p.buffer == nil {
		return nil, 0, ErrPieceInvalidState
	}

	buffer := p.buffer
	p.buffer = nil
	p.state = pieceFinalizing
	return buffer, p.generation, nil
}

func (p *piece) markComplete(generation uint64) bool {
	if p.state != pieceFinalizing || p.generation != generation {
		return false
	}
	p.state = pieceComplete
	return true
}

func (p *piece) reject(generation uint64) bool {
	if p.state != pieceFinalizing || p.generation != generation {
		return false
	}
	p.state = pieceMissing
	p.buffer = nil
	p.receivedBlocks = nil
	return true
}

func (p *piece) restore(generation uint64, buffer []byte) bool {
	if p.state != pieceFinalizing || p.generation != generation || buffer == nil {
		return false
	}
	p.state = pieceAssembled
	p.buffer = buffer
	return true
}

func (p *piece) markExistingComplete() {
	p.state = pieceComplete
	p.buffer = nil
	p.receivedBlocks = nil
}

func (p *piece) complete() bool {
	return p.state == pieceComplete
}

func blockCountFor(pieceLength, blockLength uint32) int {
	return int((pieceLength-1)/blockLength + 1)
}

func requestLengthFor(pieceLength, begin, blockLength uint32) uint32 {
	if blockLength == 0 || begin >= pieceLength {
		return 0
	}
	remaining := pieceLength - begin
	if remaining < blockLength {
		return remaining
	}
	return blockLength
}
