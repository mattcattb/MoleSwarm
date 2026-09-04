package torrent

import (
	"bytes"
	"crypto/sha1"
	"testing"
)

func TestReadMetaInfoParsesSingleFileAndComputesInfoHash(t *testing.T) {
	pieceData := []byte("hello")
	pieceHash := sha1.Sum(pieceData)

	infoBytes := []byte("d6:lengthi5e4:name5:hello12:piece lengthi5e6:pieces20:")
	infoBytes = append(infoBytes, pieceHash[:]...)
	infoBytes = append(infoBytes, 'e')

	metaBytes := []byte("d8:announce28:http://tracker.test/announce4:info")
	metaBytes = append(metaBytes, infoBytes...)
	metaBytes = append(metaBytes, 'e')

	meta, err := readMetaInfoForTest(t, metaBytes)
	if err != nil {
		t.Fatalf("read metainfo: %v", err)
	}

	if got, want := meta.Announce, "http://tracker.test/announce"; got != want {
		t.Fatalf("announce = %q, want %q", got, want)
	}
	if got, want := meta.Info.Name, "hello"; got != want {
		t.Fatalf("name = %q, want %q", got, want)
	}
	if got, want := meta.Info.Length, int64(len(pieceData)); got != want {
		t.Fatalf("length = %d, want %d", got, want)
	}
	if got, want := meta.Info.PieceLength, int64(len(pieceData)); got != want {
		t.Fatalf("piece length = %d, want %d", got, want)
	}
	if got, want := len(meta.Info.PieceHashes), 1; got != want {
		t.Fatalf("piece hash count = %d, want %d", got, want)
	}
	if got, want := meta.Info.PieceHashes[0], PieceHash(pieceHash); got != want {
		t.Fatalf("piece hash = %x, want %x", got, want)
	}
	if got, want := meta.InfoHash, InfoHash(sha1.Sum(infoBytes)); got != want {
		t.Fatalf("info hash = %x, want %x", got, want)
	}
}

func TestReadMetaInfoRejectsUnsortedInfoDictionary(t *testing.T) {
	pieceHash := sha1.Sum([]byte("hello"))

	infoBytes := []byte("d4:name5:hello6:lengthi5e12:piece lengthi5e6:pieces20:")
	infoBytes = append(infoBytes, pieceHash[:]...)
	infoBytes = append(infoBytes, 'e')

	metaBytes := []byte("d8:announce28:http://tracker.test/announce4:info")
	metaBytes = append(metaBytes, infoBytes...)
	metaBytes = append(metaBytes, 'e')

	if _, err := readMetaInfoForTest(t, metaBytes); err == nil {
		t.Fatal("read metainfo succeeded with an unsorted info dictionary")
	}
}

func TestReadMetaInfoRejectsTrailingData(t *testing.T) {
	pieceHash := sha1.Sum([]byte("hello"))

	infoBytes := []byte("d6:lengthi5e4:name5:hello12:piece lengthi5e6:pieces20:")
	infoBytes = append(infoBytes, pieceHash[:]...)
	infoBytes = append(infoBytes, 'e')

	metaBytes := []byte("d8:announce28:http://tracker.test/announce4:info")
	metaBytes = append(metaBytes, infoBytes...)
	metaBytes = append(metaBytes, []byte("ejunk")...)

	if _, err := readMetaInfoForTest(t, metaBytes); err == nil {
		t.Fatal("read metainfo succeeded with trailing data")
	}
}

func TestReadMetaInfoRejectsNegativeStringLength(t *testing.T) {
	if _, err := readMetaInfoForTest(t, []byte("d8:announce-1:e")); err == nil {
		t.Fatal("read metainfo succeeded with a negative string length")
	}
}

func TestReadMetaInfoRejectsNonCanonicalInteger(t *testing.T) {
	pieceHash := sha1.Sum([]byte("hello"))

	infoBytes := []byte("d6:lengthi05e4:name5:hello12:piece lengthi5e6:pieces20:")
	infoBytes = append(infoBytes, pieceHash[:]...)
	infoBytes = append(infoBytes, 'e')

	metaBytes := []byte("d8:announce28:http://tracker.test/announce4:info")
	metaBytes = append(metaBytes, infoBytes...)
	metaBytes = append(metaBytes, 'e')

	if _, err := readMetaInfoForTest(t, metaBytes); err == nil {
		t.Fatal("read metainfo succeeded with a non-canonical integer")
	}
}

func TestReadMetaInfoRejectsMultiFileTorrent(t *testing.T) {
	metaBytes := []byte(
		"d8:announce28:http://tracker.test/announce4:info" +
			"d5:filesle4:name5:hello12:piece lengthi5e6:pieces0:ee",
	)

	if _, err := readMetaInfoForTest(t, metaBytes); err == nil {
		t.Fatal("read metainfo succeeded for an unsupported multi-file torrent")
	}
}

func readMetaInfoForTest(t *testing.T, data []byte) (MetaInfo, error) {
	t.Helper()
	return ReadMetaInfo(bytes.NewReader(data))
}

func FuzzReadMetaInfoNeverPanics(f *testing.F) {
	f.Add([]byte("d8:announce-1:e"))
	f.Add([]byte("de"))
	f.Add([]byte("not bencode"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadMetaInfo(bytes.NewReader(data))
	})
}
