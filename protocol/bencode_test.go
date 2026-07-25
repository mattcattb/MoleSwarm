package protocol

import (
	"strings"
	"testing"
)

func TestDecodeString(t *testing.T) {
	value, err := Decode(strings.NewReader("4:spam"))
	if err != nil {
		t.Fatal(err)
	}

	got, ok := value.String()
	if !ok || got != "spam" {
		t.Fatalf("got %q, wanted spam", got)
	}
}

func TestDecodeInteger(t *testing.T) {
	value, err := Decode(strings.NewReader("i42e"))
	if err != nil {
		t.Fatal(err)
	}

	got, ok := value.Int()
	if !ok || got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}
