package client

import (
	"context"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/torrent"
)

type torrentStatusResult struct {
	InfoHash protocol.InfoHash
	Status   torrent.Status
	Err      error
}

type torrentStatusTarget struct {
	infohash protocol.InfoHash
	t        *torrent.Torrent
}

func (c *Client) TorrentStatuses(ctx context.Context) []torrentStatusResult {
	c.mu.Lock()

	targets := make([]torrentStatusTarget, 0, len(c.torrents))

	for iHash, activeTorrent := range c.torrents {
		targets = append(targets, torrentStatusTarget{infohash: iHash, t: activeTorrent})
	}
	c.mu.Unlock()

	statusResults := make(chan torrentStatusResult, len(targets))

	for _, t := range targets {
		go func(target torrentStatusTarget) {
			status, err := target.t.ReqStatus(ctx)
			statusResults <- torrentStatusResult{InfoHash: target.infohash, Status: status, Err: err}
		}(t)
	}

	response := make([]torrentStatusResult, 0, len(targets))

	for range targets {

		select {
		case res := <-statusResults:

			response = append(response, res)

		case <-ctx.Done():
			return response
		}
	}

	return response

}

type TorrentOperationResult struct {
	InfoHash protocol.InfoHash
	Err      error
}

func (c *Client) PauseAllDownloads(ctx context.Context) ([]TorrentOperationResult, error) {

	c.mu.Lock()

	targets := make([]torrentStatusTarget, 0)

	for infohash, target := range c.torrents {
		targets = append(targets, torrentStatusTarget{infohash: infohash, t: target})
	}

	c.mu.Unlock()

	results := make([]TorrentOperationResult, 0)

	responseChannel := make(chan TorrentOperationResult, len(targets))

	for _, target := range targets {

		go func() {
			err := target.t.PauseDownload(ctx)
			responseChannel <- TorrentOperationResult{
				InfoHash: target.infohash,
				Err:      err,
			}
		}()
	}

	for range targets {
		select {
		case <-ctx.Done():
			return results, ctx.Err()

		case targetRes := <-responseChannel:
			results = append(results, targetRes)

		}
	}

	return results, nil
}
