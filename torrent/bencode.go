package torrent

import (
	"bufio"
	"fmt"
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

type Bencoding struct {
	kind    BencodingKind
	array   []Bencoding
	str     string
	integer int64
	dict    map[string]Bencoding
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

func (b *Bencoding) Dict() (map[string]Bencoding, bool) {
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
	strLenByte, err := r.r.ReadByte()

	if err != nil {
		return Bencoding{}, err
	}

	bStrBytes, err := strconv.Atoi(string(strLenByte))
	if err != nil {
		return Bencoding{}, err
	}

	strBuffer := make([]byte, bStrBytes)

	if _, err = r.r.Read(strBuffer); err != nil {
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

	line, err := r.r.ReadSlice('e')

	if err != nil {
		return Bencoding{}, err
	}

	val, err := strconv.Atoi(string(line))

	if err != nil {
		return Bencoding{}, err
	}

	return IntegerBencoding(int64(val)), nil
}

func (r *BReader) decode() (Bencoding, error) {
	prefix, err := r.r.Peek(1)
	if err != nil {
		return Bencoding{}, err
	}

	switch BencodingKind(prefix[1]) {
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
		key, err := r.decodeBString()
		if err != nil {
			return Bencoding{}, err
		}
		val, err := r.decode()

		if err != nil {
			return Bencoding{}, err
		}

		bEncodingMap[key.str] = val

		if n, err := r.r.Peek(1); err == nil && n[0] == 'e' {
			break
		}

	}
	if _, err := r.r.ReadByte(); err != nil {
		return Bencoding{}, err
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
		val, err := r.decode()
		if err != nil {
			return Bencoding{}, err
		}
		vals = append(vals, val)

		if p, err := r.r.Peek(1); err == nil && p[0] == 'e' {
			break
		}
	}

	return Bencoding{kind: ListKind, array: vals}, nil
}
