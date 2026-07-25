package torrent

import (
	"bufio"
	"fmt"
	"io"
	"sort"
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
	r     *bufio.Reader
	depth int
}

const maxBencodedStringLength = 16 << 20
const maxBencodingDepth = 100

func (r *BReader) decodeBString() (Bencoding, error) {
	lengthBytes, err := r.r.ReadSlice(':')

	if err != nil {
		return Bencoding{}, err
	}

	lengthBytes = lengthBytes[:len(lengthBytes)-1]
	if len(lengthBytes) == 0 {
		return Bencoding{}, fmt.Errorf("bencoded string length is empty")
	}
	if len(lengthBytes) > 1 && lengthBytes[0] == '0' {
		return Bencoding{}, fmt.Errorf("bencoded string length has a leading zero")
	}
	for _, digit := range lengthBytes {
		if digit < '0' || digit > '9' {
			return Bencoding{}, fmt.Errorf("bencoded string length contains a non-digit")
		}
	}

	bStrBytes, err := strconv.ParseUint(string(lengthBytes), 10, 64)
	if err != nil {
		return Bencoding{}, err
	}
	if bStrBytes > maxBencodedStringLength {
		return Bencoding{}, fmt.Errorf(
			"bencoded string length %d exceeds limit %d",
			bStrBytes,
			maxBencodedStringLength,
		)
	}

	strBuffer := make([]byte, int(bStrBytes))

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

	line, err := r.r.ReadSlice('e')

	if err != nil {
		return Bencoding{}, err
	}
	line = line[:len(line)-1]
	if len(line) == 0 {
		return Bencoding{}, fmt.Errorf("bencoded integer is empty")
	}

	digits := line
	if line[0] == '-' {
		digits = line[1:]
		if len(digits) == 0 {
			return Bencoding{}, fmt.Errorf("bencoded integer has no digits")
		}
	}
	if len(digits) > 1 && digits[0] == '0' {
		return Bencoding{}, fmt.Errorf("bencoded integer has a leading zero")
	}
	if line[0] == '-' && digits[0] == '0' {
		return Bencoding{}, fmt.Errorf("bencoded integer cannot be negative zero")
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return Bencoding{}, fmt.Errorf("bencoded integer contains a non-digit")
		}
	}

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
	if r.depth >= maxBencodingDepth {
		return Bencoding{}, fmt.Errorf("bencoding nesting exceeds limit %d", maxBencodingDepth)
	}
	r.depth++
	defer func() {
		r.depth--
	}()

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
	var previousKey string
	hasPreviousKey := false

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
		if hasPreviousKey && key.str <= previousKey {
			return Bencoding{}, fmt.Errorf("dictionary keys must be unique and sorted")
		}
		previousKey = key.str
		hasPreviousKey = true

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
	val, ok := b[key]

	if !ok {
		return []Bencoding{}, false
	}

	return val.List()
}

func writeBencoding(w io.Writer, value Bencoding) error {
	switch value.kind {
	case StringKind:
		if err := writeAll(w, []byte(strconv.Itoa(len(value.str))+":")); err != nil {
			return err
		}
		return writeAll(w, []byte(value.str))

	case IntegerKind:
		return writeAll(w, []byte("i"+strconv.FormatInt(value.integer, 10)+"e"))

	case ListKind:
		if err := writeAll(w, []byte{'l'}); err != nil {
			return err
		}
		for _, item := range value.array {
			if err := writeBencoding(w, item); err != nil {
				return err
			}
		}
		return writeAll(w, []byte{'e'})

	case DictKind:
		if err := writeAll(w, []byte{'d'}); err != nil {
			return err
		}

		keys := make([]string, 0, len(value.dict))
		for key := range value.dict {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			if err := writeBencoding(w, StringBencoding(key)); err != nil {
				return err
			}
			if err := writeBencoding(w, value.dict[key]); err != nil {
				return err
			}
		}
		return writeAll(w, []byte{'e'})

	default:
		return fmt.Errorf("unknown bencoding kind %q", value.kind)
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
