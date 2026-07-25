package protocol

import (
	"bytes"
	"testing"
)

func TestWriteMessageFramesPayload(t *testing.T) {
	var wire bytes.Buffer

	if err := WriteMessage(&wire, Have{PieceIndex: 7}); err != nil {
		t.Fatalf("write message: %v", err)
	}

	want := []byte{
		0, 0, 0, 5,
		byte(HaveID),
		0, 0, 0, 7,
	}
	if !bytes.Equal(wire.Bytes(), want) {
		t.Fatalf("wire bytes = %v, want %v", wire.Bytes(), want)
	}
}

func TestInterestedMessageRoundTrip(t *testing.T) {
	var wire bytes.Buffer

	if err := WriteMessage(&wire, Interested{}); err != nil {
		t.Fatalf("write message: %v", err)
	}

	message, err := ReadMessage(&wire)
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if _, ok := message.(Interested); !ok {
		t.Fatalf("message = %T, want torrent.Interested", message)
	}
}

func TestNotInterestedMessageRoundTrip(t *testing.T) {
	var wire bytes.Buffer

	if err := WriteMessage(&wire, NotInterested{}); err != nil {
		t.Fatalf("write message: %v", err)
	}

	message, err := ReadMessage(&wire)
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if _, ok := message.(NotInterested); !ok {
		t.Fatalf("message = %T, want torrent.NotInterested", message)
	}
}
