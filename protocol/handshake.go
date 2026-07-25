package protocol

import "io"

const PeerProtocol = "BitTorrent protocol"

type Handshake struct {
	Protocol string
	Reserved [8]byte
	InfoHash InfoHash
	PeerID   PeerID
}

func ReadHandshake(r io.Reader) (Handshake, error) {
	var protocolLength [1]byte
	if _, err := io.ReadFull(r, protocolLength[:]); err != nil {
		return Handshake{}, err
	}

	protocolName := make([]byte, int(protocolLength[0]))
	if _, err := io.ReadFull(r, protocolName); err != nil {
		return Handshake{}, err
	}

	var handshake Handshake
	handshake.Protocol = string(protocolName)
	if _, err := io.ReadFull(r, handshake.Reserved[:]); err != nil {
		return Handshake{}, err
	}
	if _, err := io.ReadFull(r, handshake.InfoHash[:]); err != nil {
		return Handshake{}, err
	}
	if _, err := io.ReadFull(r, handshake.PeerID[:]); err != nil {
		return Handshake{}, err
	}

	return handshake, nil
}

func WriteHandshake(w io.Writer, handshake Handshake) error {
	frame := make([]byte, 0, 49+len(handshake.Protocol))
	frame = append(frame, byte(len(handshake.Protocol)))
	frame = append(frame, handshake.Protocol...)
	frame = append(frame, handshake.Reserved[:]...)
	frame = append(frame, handshake.InfoHash[:]...)
	frame = append(frame, handshake.PeerID[:]...)

	_, err := w.Write(frame)
	return err
}
