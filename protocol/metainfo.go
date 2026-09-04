package protocol

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
)

type PeerID [20]byte

type InfoHash [20]byte

type PieceHash [20]byte

type Info struct {
	Name        string
	Length      int64       // file length in bytes
	PieceLength int64       // length of each piece
	PieceHashes []PieceHash // concatination of all 20 byte sha1 hash values
}

// string whose length is a multiple of 20. It is to be subdivided into strings of length 20, each of which is the SHA1 hash of the piece at the corresponding index

var (
	ErrInvalidMetainfo = errors.New("invalid metainfo")
)

type MetaInfo struct {
	Announce string // url of tracker
	Info     Info
	InfoHash InfoHash
}

func NewMetaInfo(announce string, info Info) (MetaInfo, error) {

	if announce == "" {
		return MetaInfo{}, ErrInvalidMetainfo
	}

	infoHash, err := hashInfo(info)

	if err != nil {
		return MetaInfo{}, err
	}

	return MetaInfo{
		Announce: announce,
		Info:     info,
		InfoHash: infoHash,
	}, nil
}

func ValidateAnnounceUrl(announce string) error {

	if announce == "" {
		return ErrInvalidMetainfo
	}

	return nil
}

const maxMetaInfoSize = 16 << 20

func ReadMetaInfo(r io.Reader) (MetaInfo, error) {

	value, err := Decode(io.LimitReader(r, maxMetaInfoSize+1))

	if err != nil {
		return MetaInfo{}, err
	}

	return parseMetaInfo(value)

}
func WriteMetainfo(w io.Writer, m MetaInfo) error {

	mBencoding, err := metaInfoBencoding(m)

	if err != nil {
		return err
	}

	return Encode(w, mBencoding)
}
func mashalPieceHashes(pieces []PieceHash) []byte {

	bytes := make([]byte, 0, len(pieces)*len(PieceHash{}))

	for _, ph := range pieces {
		bytes = append(bytes, ph[:]...)
	}

	return bytes
}

func infoBencoding(info Info) (Bencoding, error) {

	piecesBytes := mashalPieceHashes(info.PieceHashes)

	bencoding := DictBencoding(BencodingDict{
		"length":       IntegerBencoding(info.Length),
		"name":         StringBencoding(info.Name),
		"piece length": IntegerBencoding(info.PieceLength),
		"pieces":       StringBencoding(string(piecesBytes)),
	})
	return bencoding, nil

}

func metaInfoBencoding(m MetaInfo) (Bencoding, error) {

	infoValue, err := infoBencoding(m.Info)

	if err != nil {
		return Bencoding{}, err
	}

	return DictBencoding(BencodingDict{
		"announce": StringBencoding(m.Announce),
		"info":     infoValue,
	}), nil

}

func hashInfo(info Info) (InfoHash, error) {

	value, err := infoBencoding(info)
	if err != nil {
		return InfoHash{}, err
	}

	hasher := sha1.New()

	if err := Encode(hasher, value); err != nil {
		return InfoHash{}, fmt.Errorf(
			"encode info dictionary: %w",
			err,
		)
	}

	var infoHash InfoHash
	copy(infoHash[:], hasher.Sum(nil))

	return infoHash, nil

}

// EncodeMetaInfo creates canonical single-file v1 metainfo from validated
// content information. The returned bytes are suitable for distribution as a
// .torrent file.
func EncodeMetaInfo(m MetaInfo) ([]byte, error) {

	metainfoValue, err := metaInfoBencoding(m)

	if err != nil {
		return nil, err
	}

	var encoded bytes.Buffer
	if err := Encode(&encoded, metainfoValue); err != nil {
		return nil, err
	}
	return encoded.Bytes(), nil
}

var invalidTorrentFile = errors.New("invalid metainfo file")

func parseMetaInfo(value Bencoding) (MetaInfo, error) {

	mDict, ok := value.Dict()

	if !ok {
		return MetaInfo{}, ErrInvalidMetainfo
	}

	announce, ok := mDict.String("announce")

	if !ok || announce == "" {
		return MetaInfo{}, ErrInvalidMetainfo
	}

	infoValue := mDict["info"]

	info, err := ParseInfo(infoValue)

	if err != nil {
		return MetaInfo{}, err
	}

	//	infoHash, ok := mDict.String("infohash")

	if !ok {
		return MetaInfo{}, ErrInvalidMetainfo
	}

	var encodedInfo bytes.Buffer
	if err := writeBencoding(&encodedInfo, infoValue); err != nil {
		return MetaInfo{}, fmt.Errorf("encode info dictionary: %w", err)
	}

	return MetaInfo{
		Announce: announce,
		Info:     info,
		InfoHash: InfoHash(sha1.Sum(encodedInfo.Bytes())),
	}, nil

}

func ParseInfo(info Bencoding) (Info, error) {
	dict, ok := info.Dict()

	if !ok {
		return Info{}, fmt.Errorf("info must be a bencoding dictionary")
	}

	if _, multiFile := dict["files"]; multiFile {
		return Info{}, fmt.Errorf("%w: multi-file torrents are not supported", invalidTorrentFile)
	}

	pieceLength, ok := dict.Int("piece length")
	if !ok || pieceLength <= 0 {
		return Info{}, fmt.Errorf("%w: piece length must be a positive integer", invalidTorrentFile)
	}

	piecesConcat, ok := dict.String("pieces")
	if !ok || len(piecesConcat)%len(PieceHash{}) != 0 {
		return Info{}, fmt.Errorf("%w: pieces must be a string containing 20-byte hashes", invalidTorrentFile)
	}

	pieces := make([]PieceHash, 0)

	for i := 0; i < len(piecesConcat); i += len(PieceHash{}) {
		var hash PieceHash
		iPiece := piecesConcat[i : i+len(hash)]
		copy(hash[:], iPiece[:])
		pieces = append(pieces, hash)
	}

	length, ok := dict.Int("length")
	if !ok || length < 0 {
		return Info{}, fmt.Errorf("%w: length must be a non-negative integer", invalidTorrentFile)
	}

	name, ok := dict.String("name")
	if !ok || name == "" {
		return Info{}, fmt.Errorf("%w: name must be a non-empty string", invalidTorrentFile)
	}

	expectedPieces := int64(0)
	if length > 0 {
		expectedPieces = (length-1)/pieceLength + 1
	}
	if int64(len(pieces)) != expectedPieces {
		return Info{}, fmt.Errorf(
			"%w: found %d piece hashes, expected %d",
			invalidTorrentFile,
			len(pieces),
			expectedPieces,
		)
	}

	return Info{
		PieceLength: pieceLength,
		Name:        name,
		PieceHashes: pieces,
		Length:      length,
	}, nil
}

func HashPiece(data []byte) PieceHash {

	return PieceHash(sha1.Sum(data))
}

func VerifyPiece(data []byte, expected PieceHash) bool {
	return HashPiece(data) == expected
}
