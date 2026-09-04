package torrentd

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mattcattb/MoleSwarm/protocol"
)

// tracking of metadata files and locations

type MetainfoRecord struct {
	Meta protocol.MetaInfo
	path string
	ID   string
}

type MetainfoStore struct {
	root       string
	mu         sync.RWMutex
	byId       map[string]MetainfoRecord
	byInfoHash map[protocol.InfoHash]string
}

/*
<root>/

	torrents/
			<hash>/
					<fname>.torrent
			<hash_b>/
					<fname>.torrent
*/
var (
	ErrInvalidMetadataRecord = errors.New("invalid metadata record")
)

func OpenMetainfoStore(root string) (*MetainfoStore, error) {

	metainfoDirPath := filepath.Join(root, "metainfo")

	entires, err := os.ReadDir(metainfoDirPath)
	if err != nil {
		return nil, err
	}

	byId := make(map[string]MetainfoRecord)
	byInfoHash := make(map[protocol.InfoHash]string)

	for i, entry := range entires {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(metainfoDirPath, entry.Name())

		file, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}

		metainfo, err := protocol.ReadMetaInfo(file)
		if err != nil {
			return nil, err
		}

		fSplit := strings.Split(entry.Name(), ".")

		if len(fSplit) == 0 {
			return nil, ErrInvalidMetadataRecord
		}
		id := strings.Join(fSplit[:len(fSplit)])

		record := MetainfoRecord{path: filePath, Meta: metainfo, ID: id}

		byId[id] = record
		byInfoHash[record.Meta.InfoHash] = id
	}

	return &MetainfoStore{
		root:       root,
		mu:         sync.RWMutex{},
		byId:       byId,
		byInfoHash: byInfoHash,
	}, nil

}

func (ms *MetainfoStore) Import(id string, r io.Reader) (MetainfoRecord, error) {
	// import new metainfo record, adding to array and writing to list

	metainfo, err := protocol.ReadMetaInfo(r)

	if err != nil {
		return MetainfoRecord{}, err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	//! check if exists

	metainfoFileName := id + ".torrent"

	path := filepath.Join(ms.root, "metainfo", metainfoFileName)

	record := MetainfoRecord{
		path: path,
		Meta: metainfo,
		ID:   id,
	}

	file, err := os.Create(path)

	if err != nil {
		return MetainfoRecord{}, err
	}

	if err := protocol.WriteMetainfo(file, record.Meta); err != nil {
		return MetainfoRecord{}, err
	}
	ms.byId[id] = record
	ms.byInfoHash[metainfo.InfoHash] = string(record.Meta.InfoHash[:])

	return record, nil

}

func (ms *MetainfoStore) Get(id string) (MetainfoRecord, bool) {
	return ms.byId[id]
}

func (ms *MetainfoStore) Remove(id string) (bool, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()
	metainfo, ok := ms.Get(id)

	if !ok {
		return false, nil
	}

	// find the location
	path := metainfo.path

	if err := os.Remove(path); err != nil {
		return false, err
	}

	delete(ms.byId[id])
	delete(ms.byInfoHash[metainfo.Meta.InfoHash])

	return true, nil
}

func (ms *MetainfoStore) List() []MetainfoRecord {

}
func (d *Daemon) loadTorrentMetadata() ([]MetainfoRecord, error) {

	metadataRootLocation := d.root + "/torrents"

	list, err := os.ReadDir(metadataRootLocation)

	if err != nil {
		return nil, err
	}

	records := make([]MetainfoRecord, 0)

	for _, entry := range list {
		if entry.IsDir() {
			continue
		}

		fileLocation := metadataRootLocation + `/` + entry.Name()

		file, err := os.Open(fileLocation)

		if err != nil {
			return nil, err
		}

		metadata, err := protocol.ReadMetaInfo(file)

		if err != nil {
			return nil, err
		}
		records = append(records, MetainfoRecord{metainfo: metadata, root: fileLocation, name: metadata.Info.Name})
	}
	return records, nil
}

// manged torrents should ahve different file locations
