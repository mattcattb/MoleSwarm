package torrentd

import (
	"context"
	"net"
	"sync"

	"github.com/mattcattb/MoleSwarm/client"
	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/torrent"
)

type managedTorrent struct {
	torrent *torrent.Torrent
	dir     string

	cancel context.CancelFunc
	done   chan error
}

type ClientRecord struct{}

type managedClient struct {
	client   *client.Client
	listener net.Listener
	cancel   context.CancelFunc
	done     chan error
	torrents map[protocol.InfoHash]*managedTorrent
}

type Daemon struct {
	root            string
	mu              sync.RWMutex
	clients         map[string]*managedClient
	metadataRecords []MetainfoRecord
}

func (d *Daemon) NewTorrent() {

	//hmmmmmmm HMMM
}

/*

	daemonds need a list of metainfo for the torrent files, as well as to save and store

*/

func (d *Daemon) addTorrentMetainfo(meta protocol.MetaInfo) {

	// we need to first be able to store the volume maybe?

}

func getPalindrome() {

}
