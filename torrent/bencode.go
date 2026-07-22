package torrent

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type BencodingKind byte

const (
	StringKind  BencodingKind = 's'
	IntegerKind BencodingKind = 'i'
	ListKind    BencodingKind = 'l'
	DictKind    BencodingKind = 'd'
)

// integer: ixxxe
// string: 4:xxxx or 1:x or 5:xxxxx
// list: l[]e
// dictionary: keys and values

type BencodingDict map[string]Bencoding

type Bencoding struct {
	kind    BencodingKind
	array   []Bencoding
	str     string
	integer int64
	dict    BencodingDict
}

func StringBencoding(s string) Bencoding {
	return Bencoding{kind: StringKind, str: s}
}

func IntegerBencoding(n int64) Bencoding {
	return Bencoding{kind: IntegerKind, integer: n}
}

func ListBencoding(values []Bencoding) Bencoding {
	return Bencoding{kind: ListKind, array: values}
}

func DictBencoding(values map[string]Bencoding) Bencoding {
	return Bencoding{kind: DictKind, dict: values}
}

func (b *Bencoding) String() (string, bool) {
	if b.kind != StringKind {
		return "", false
	}

	return b.str, true
}

func (b *Bencoding) Dict() (BencodingDict, bool) {
	if b.kind != DictKind {
		return map[string]Bencoding{}, false
	}

	return b.dict, true
}

func (b *Bencoding) Int() (int64, bool) {
	if b.kind != IntegerKind {
		return 0, false
	}

	return b.integer, true
}

func (b *Bencoding) List() ([]Bencoding, bool) {
	if b.kind != ListKind {
		return []Bencoding{}, false
	}

	return b.array, true
}

type BReader struct {
	r *bufio.Reader
}

// using a reader vs a...

func (r *BReader) decodeBString() (Bencoding, error) {
	lengthBytes, err := r.r.ReadSlice(':')

	if err != nil {
		return Bencoding{}, err
	}

	lengthBytes = lengthBytes[:len(lengthBytes)-1]

	bStrBytes, err := strconv.ParseInt(string(lengthBytes), 10, 64)
	if err != nil {
		return Bencoding{}, err
	}

	strBuffer := make([]byte, bStrBytes)

	if _, err = io.ReadFull(r.r, strBuffer); err != nil {
		return Bencoding{}, err
	}

	return StringBencoding(string(strBuffer)), nil

}

func (r *BReader) decodeBInteger() (Bencoding, error) {
	prefix, err := r.r.ReadByte()
	if err != nil {
		return Bencoding{}, err
	}
	if BencodingKind(prefix) != IntegerKind {
		return Bencoding{}, fmt.Errorf("invalid integer prefix")
	}

	line, err := r.r.ReadString('e')

	if err != nil {
		return Bencoding{}, err
	}
	line = line[:len(line)-1]

	value, err := strconv.ParseInt(
		string(line),
		10,
		64,
	)

	if err != nil {
		return Bencoding{}, err
	}

	return IntegerBencoding(int64(value)), nil
}

func (r *BReader) decode() (Bencoding, error) {
	prefix, err := r.r.Peek(1)
	if err != nil {
		return Bencoding{}, err
	}

	switch BencodingKind(prefix[0]) {
	case DictKind:
		return r.decodeBDict()
	case IntegerKind:
		return r.decodeBInteger()
	case ListKind:
		return r.decodeBList()
	default:
		return r.decodeBString()
	}
}

func (r *BReader) decodeBDict() (Bencoding, error) {
	prefix, err := r.r.ReadByte()
	if err != nil {
		return Bencoding{}, err
	}

	if prefix != byte(DictKind) {
		return Bencoding{}, fmt.Errorf("invalid prefix for dict decoding")
	}

	bEncodingMap := map[string]Bencoding{}

	for {
		next, err := r.r.Peek(1)

		if err != nil {
			return Bencoding{}, err
		}
		if next[0] == 'e' {
			if _, err := r.r.ReadByte(); err != nil {
				return Bencoding{}, err
			}

			break
		}

		key, err := r.decodeBString()
		if err != nil {
			return Bencoding{}, err
		}
		val, err := r.decode()

		if err != nil {
			return Bencoding{}, err
		}

		bEncodingMap[key.str] = val

	}

	return Bencoding{kind: DictKind, dict: bEncodingMap}, nil
}

func (r *BReader) decodeBList() (Bencoding, error) {
	prefix, err := r.r.ReadByte()

	if err != nil {
		return Bencoding{}, err
	}

	if prefix != 'l' {
		return Bencoding{}, fmt.Errorf("invalid decode list prefix")
	}

	vals := make([]Bencoding, 0)

	for {

		next, err := r.r.Peek(1)

		if err != nil {
			return Bencoding{}, err
		}
		if next[0] == 'e' {
			if _, err := r.r.ReadByte(); err != nil {
				return Bencoding{}, err
			}

			break
		}

		val, err := r.decode()
		if err != nil {
			return Bencoding{}, err
		}
		vals = append(vals, val)

	}

	return Bencoding{kind: ListKind, array: vals}, nil
}

func (b BencodingDict) String(key string) (string, bool) {

	val, ok := b[key]

	if !ok {
		return "", false
	}

	return val.String()
}

func (b BencodingDict) Int(key string) (int64, bool) {
	val, ok := b[key]

	if !ok {
		return 0, false
	}
	return val.Int()
}

func (b BencodingDict) Dict(key string) (BencodingDict, bool) {
	val, ok := b[key]

	if !ok {
		return BencodingDict{}, false
	}

	return val.Dict()
}

func (b BencodingDict) List(key string) ([]Bencoding, bool) {
	val, ok := b["key"]

	if !ok {
		return []Bencoding{}, false
	}

	return val.List()
}
