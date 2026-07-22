package torrent

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

type PieceHash [20]byte

type Info struct {
	Name        string
	Length      int64       // file length in bytes
	PieceLength int64       // length of each piece
	PieceHashes []PieceHash // concatination of all 20 byte sha1 hash values
}
type InfoHash [20]byte

type PeerID [20]byte

// string whose length is a multiple of 20. It is to be subdivided into strings of length 20, each of which is the SHA1 hash of the piece at the corresponding index

type MetaInfo struct {
	Announce string // url of tracker
	Info     Info
	InfoHash InfoHash
}

func ReadMetaInfo(f *os.File) (MetaInfo, error) {

	bReader := BReader{r: bufio.NewReader(f)}

	bCoding, err := bReader.decodeBDict()

	if err != nil {
		return MetaInfo{}, err
	}

	metaDict, ok := bCoding.Dict()

	if !ok {
		return MetaInfo{}, fmt.Errorf("invalid encoding")
	}

	announce := metaDict["announce"]

	iMap := metaDict["info"]

	mInfo, err := ParseInfo(iMap)

	if err != nil {
		return MetaInfo{}, err
	}
	/*
		if pLen, ok := infoDict["piece length"]; ok {
			pLenInt, ok := pLen.Int()
			if ok {
				mInfoDict.PieceLength = pLenInt
			}
		}

		if pieces, ok := infoDict["pieces"]; ok {
			piecesStr, ok := pieces.String()
			if ok {
				for len(piecesStr) >= len(PieceHash{}) {
					var hash PieceHash
					copy(hash[:], piecesStr[:len(hash)])
					mInfoDict.PieceHashes = append(mInfoDict.PieceHashes, hash)
					piecesStr = piecesStr[len(hash):]
				}
			}
		}

		if fLen, ok := infoDict["length"]; ok {
			mInfoDict.Length, _ = fLen.Int()
		}

		if name, ok := infoDict["name"]; ok {
			mInfoDict.Name, _ = name.String()
		}*/

	return MetaInfo{
		Announce: announce.str,
		Info:     mInfo,
	}, nil
}

var invalidTorrentFile = errors.New("invalid metainfo file")

func ParseInfo(info Bencoding) (Info, error) {
	dict, ok := info.Dict()

	if !ok {
		return Info{}, fmt.Errorf("info must be a bencoding dictionary")
	}

	pLen, ok := dict["piece length"]

	if !ok {
		return Info{}, invalidTorrentFile
	}

	piecesConcat := dict["peices"].str

	pieces := make([]PieceHash, 0)

	for i := 0; i < len(pieces); i += 20 {
		var hash PieceHash
		iPiece := piecesConcat[i : i+20]
		copy(hash[:], iPiece[:])
		pieces = append(pieces, hash)
	}

	len := dict["length"].integer

	return Info{
		PieceLength: pLen.integer,
		Name:        dict["name"].str,
		PieceHashes: pieces,
		Length:      len,
	}, nil
}
