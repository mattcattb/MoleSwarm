package protocol

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
)

type Info struct {
	Name        string
	Length      int64       // file length in bytes
	PieceLength int64       // length of each piece
	PieceHashes []PieceHash // concatination of all 20 byte sha1 hash values
}

// string whose length is a multiple of 20. It is to be subdivided into strings of length 20, each of which is the SHA1 hash of the piece at the corresponding index

type MetaInfo struct {
	Announce string // url of tracker
	Info     Info
	InfoHash InfoHash
}

const maxMetaInfoSize = 16 << 20

func ReadMetaInfo(r io.Reader) (MetaInfo, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxMetaInfoSize+1))
	if err != nil {
		return MetaInfo{}, fmt.Errorf("read metainfo: %w", err)
	}
	if len(data) > maxMetaInfoSize {
		return MetaInfo{}, fmt.Errorf("%w: metainfo exceeds %d bytes", invalidTorrentFile, maxMetaInfoSize)
	}

	bCoding, err := Decode(bytes.NewReader(data))
	if err != nil {
		return MetaInfo{}, err
	}

	metaDict, ok := bCoding.Dict()

	if !ok {
		return MetaInfo{}, fmt.Errorf("invalid encoding")
	}

	announce, ok := metaDict.String("announce")
	if !ok || announce == "" {
		return MetaInfo{}, fmt.Errorf("%w: announce must be a non-empty string", invalidTorrentFile)
	}

	infoValue, ok := metaDict["info"]
	if !ok {
		return MetaInfo{}, fmt.Errorf("%w: info dictionary is missing", invalidTorrentFile)
	}

	mInfo, err := ParseInfo(infoValue)

	if err != nil {
		return MetaInfo{}, err
	}

	var encodedInfo bytes.Buffer
	if err := writeBencoding(&encodedInfo, infoValue); err != nil {
		return MetaInfo{}, fmt.Errorf("encode info dictionary: %w", err)
	}

	return MetaInfo{
		Announce: announce,
		Info:     mInfo,
		InfoHash: InfoHash(sha1.Sum(encodedInfo.Bytes())),
	}, nil
}

var invalidTorrentFile = errors.New("invalid metainfo file")

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
