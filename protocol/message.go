package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var maxPeerMessageSize = uint32(500000)

func ReadMessage(r io.Reader) (pm Message, err error) {
	var lengthBytes [4]byte

	if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
		return nil, fmt.Errorf("read peer message length: %w", err)
	}

	length := binary.BigEndian.Uint32(lengthBytes[:])

	if length == 0 {
		return KeepAlive{}, nil
	}

	if length > maxPeerMessageSize {
		return nil, fmt.Errorf("peer message length %d exceeds limit", length)
	}

	body := make([]byte, int(length))
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("read peer error")
	}

	return decodeBody(body)
}

var invalidPayloadLength = errors.New("invalid payload length")

var fixedPayloadLengths = map[MessageID]int{
	ChokeID:         0,
	UnchokeID:       0,
	InterestedID:    0,
	NotInterestedID: 0,
	HaveID:          4,
	RequestID:       12,
	CancelID:        12,
}

func decodeBody(body []byte) (Message, error) {
	if len(body) == 0 {
		return nil, errors.New("peer message len")
	}
	id := MessageID(body[0])
	payload := body[1:]
	if expected, fixed := fixedPayloadLengths[id]; fixed && len(payload) != expected {
		return nil, fmt.Errorf(
			"%w for message %d: got %d bytes, want %d",
			invalidPayloadLength,
			id,
			len(payload),
			expected,
		)
	}

	switch id {
	case InterestedID:
		return Interested{}, nil

	case NotInterestedID:
		return NotInterested{}, nil
	case ChokeID:
		return Choke{}, nil

	case UnchokeID:
		return Unchoke{}, nil
	case HaveID:
		return Have{PieceIndex: binary.BigEndian.Uint32(payload)}, nil
	case RequestID:
		block, err := decodeBlockRequest(payload)
		if err != nil {
			return nil, err
		}

		return Request{Block: block}, nil

	case CancelID:
		block, err := decodeBlockRequest(payload)
		if err != nil {
			return nil, err
		}

		return CancelRequest{Block: block}, nil

	case BitfieldID:
		return Bitfield{Bits: payload}, nil
	case PieceID:
		return decodePiece(payload)
	}

	return nil, fmt.Errorf("unknown MessageID")
}

func decodePiece(payload []byte) (Message, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf(
			"piece payload length is %d; expected at least 8",
			len(payload),
		)
	}

	return Piece{
		PieceIndex: binary.BigEndian.Uint32(payload[0:4]),
		Begin:      binary.BigEndian.Uint32(payload[4:8]),
		Data:       payload[8:],
	}, nil
}

func decodeBlockRequest(body []byte) (BlockRequest, error) {
	if len(body) != 12 {
		return BlockRequest{}, invalidPayloadLength
	}
	pieceIndex := binary.BigEndian.Uint32(body[:4])
	begin := binary.BigEndian.Uint32(body[4:8])
	length := binary.BigEndian.Uint32(body[8:])

	return BlockRequest{PieceIndex: pieceIndex, Begin: begin, Length: length}, nil
}

func WriteMessage(w io.Writer, message Message) error {

	if _, ok := message.(KeepAlive); ok {
		var KeepAlive [4]byte
		if _, err := w.Write(KeepAlive[:]); err != nil {

			return err
		}

		return nil
	}

	body, err := encodeMessage(message)

	if err != nil {
		return err
	}

	frame := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	frame = append(frame, body...)

	n, err := w.Write(frame)

	if err != nil {
		return err
	}

	if n != len(frame) {
		return io.ErrShortWrite
	}

	return nil
}

func encodeMessage(message Message) ([]byte, error) {
	switch message := message.(type) {
	case Choke:
		return []byte{byte(ChokeID)}, nil

	case Unchoke:
		return []byte{byte(UnchokeID)}, nil

	case Interested:
		return []byte{byte(InterestedID)}, nil

	case NotInterested:
		return []byte{byte(NotInterestedID)}, nil

	case Have:
		buffer := make([]byte, 0, 5)
		buffer = append(buffer, byte(HaveID))
		buffer = binary.BigEndian.AppendUint32(buffer, message.PieceIndex)

		return buffer, nil
	case Bitfield:
		buffer := make([]byte, 0)
		buffer = append(buffer, byte(BitfieldID))
		buffer = append(buffer, message.Bits...)
		return buffer, nil
	case Request:
		// Additional cases...
		buffer := make([]byte, 0)
		buffer = append(buffer, byte(RequestID))
		bReq := encodeBlockRequest(message.Block)
		buffer = append(buffer, bReq...)
		return buffer, nil

	case CancelRequest:
		buffer := make([]byte, 0)
		buffer = append(buffer, byte(CancelID))
		buffer = append(buffer, encodeBlockRequest(message.Block)...)
		return buffer, nil

	case Piece:
		buffer := make([]byte, 0)
		buffer = append(buffer, byte(PieceID))
		buffer = binary.BigEndian.AppendUint32(buffer, message.PieceIndex)
		buffer = binary.BigEndian.AppendUint32(buffer, message.Begin)
		buffer = append(buffer, message.Data...)
		return buffer, nil
	default:
		return nil, fmt.Errorf(
			"cannot encode peer message %T",
			message,
		)
	}

}

func encodeBlockRequest(block BlockRequest) []byte {

	buffer := make([]byte, 0)

	buffer = binary.BigEndian.AppendUint32(buffer, block.PieceIndex)
	buffer = binary.BigEndian.AppendUint32(buffer, block.Begin)
	buffer = binary.BigEndian.AppendUint32(buffer, block.Length)
	return buffer
}
