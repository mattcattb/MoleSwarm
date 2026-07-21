package torrent

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

func ReadPeerHandshake(r *bufio.Reader) (PeerHandshakeMessage, error) {

	pStrLen, err := r.ReadByte()
	if err != nil {
		return PeerHandshakeMessage{}, err
	}

	peerStringLength := uint8(pStrLen)

	pStr := make([]byte, peerStringLength)

	_, err = r.Read(pStr)

	if err != nil {
		return PeerHandshakeMessage{}, err
	}

	var reserved [8]byte
	if _, err := io.ReadFull(r, reserved[:]); err != nil {
		return PeerHandshakeMessage{}, err
	}

	var infoHash InfoHash

	if _, err := io.ReadFull(r, infoHash[:]); err != nil {
		return PeerHandshakeMessage{}, err
	}

	var peerId PeerID

	if _, err := io.ReadFull(r, peerId[:]); err != nil {
		return PeerHandshakeMessage{}, err
	}

	return PeerHandshakeMessage{
		pstr:     string(pStr),
		InfoHash: infoHash,
		PeerID:   peerId,
		Reserved: reserved,
	}, nil
}

func WritePeerHandshake(w *bufio.Writer, message PeerHandshakeMessage) error {

	buffer := make([]byte, 0)
	buffer = append(buffer, byte(len(message.pstr)))
	buffer = append(buffer, message.pstr...)

	buffer = append(buffer, message.Reserved[:]...)
	buffer = append(buffer, message.InfoHash[:]...)
	buffer = append(buffer, message.PeerID[:]...)

	if _, err := w.Write(buffer); err != nil {
		return err
	}

	return nil
}

var maxPeerMessageSize = uint32(500000)

func ReadMessage(r io.Reader) (pm PeerMessage, err error) {
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

func decodeBody(body []byte) (PeerMessage, error) {
	if len(body) == 0 {
		return nil, errors.New("peer message len")
	}
	id := MessageID(body[0])
	payload := body[1:]

	switch id {
	case ChokeID:
		if len(payload) != 0 {
			return nil, invalidPayloadLength
		}

		return Choke{}, nil

	case UnchokeID:
		if len(payload) != 0 {
			return nil, invalidPayloadLength
		}

		return Unchoke{}, nil
	case HaveID:
		if len(payload) != 4 {
			return nil, invalidPayloadLength
		}
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

func decodePiece(payload []byte) (PeerMessage, error) {
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

func readBlockRequest(r *bufio.Reader) (BlockRequest, error) {
	buff := make([]byte, 4)
	_, err := r.Read(buff)
	if err != nil {
		return BlockRequest{}, err
	}
	pIndex := binary.BigEndian.Uint32(buff)

	beginBuf := make([]byte, 4)
	if _, err := r.Read(beginBuf); err != nil {
		return BlockRequest{}, err
	}
	lenBuff := make([]byte, 4)
	if _, err := r.Read(lenBuff); err != nil {
		return BlockRequest{}, err
	}

	return BlockRequest{PieceIndex: pIndex, Begin: binary.BigEndian.Uint32(beginBuf), Length: binary.BigEndian.Uint32(lenBuff)}, nil

}

func WriteMessage(w io.Writer, message PeerMessage) error {

	if _, ok := message.(KeepAlive); ok {
		var KeepAlive [4]byte
		if _, err := w.Write(KeepAlive[:]); err != nil {

			return err
		}

		return nil
	}

	buffer, err := encodeMessage(message)

	if err != nil {
		return err
	}

	n, err := w.Write(buffer)

	if err != nil {
		return err
	}

	if n != len(buffer) {
		return invalidPayloadLength
	}

	return nil
}

func encodeMessage(message PeerMessage) ([]byte, error) {
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
