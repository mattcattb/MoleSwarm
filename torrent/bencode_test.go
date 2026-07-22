package torrent

import (
	"bufio"
	"strings"
	"testing"
)

func TestDecodeString(t *testing.T) {
	reader := BReader{
		r: bufio.NewReader(strings.NewReader("4:spam")),
	}

	value, err := reader.decode()
	if err != nil {
		t.Fatal(err)
	}

	got, ok := value.String()
	if !ok || got != "spam" {
		t.Fatalf("got %q, wanted spam", got)
	}
}

func TestDecodeInteger(t *testing.T) {
	reader := BReader{
		r: bufio.NewReader(strings.NewReader("i42e")),
	}

	value, err := reader.decode()
	if err != nil {
		t.Fatal(err)
	}

	got, ok := value.Int()
	if !ok || got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}
