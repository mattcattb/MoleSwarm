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

type managedClient struct {
	client   *client.Client
	listener net.Listener
	cancel   context.CancelFunc
	done     chan error
	torrents map[protocol.InfoHash]*managedTorrent
}

type Daemon struct {
	root    string
	mu      sync.RWMutex
	clients map[string]*managedClient
}
