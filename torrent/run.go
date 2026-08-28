package torrent

import (
	"context"
	"time"

	"github.com/mattcattb/MoleSwarm/protocol"
	"github.com/mattcattb/MoleSwarm/tracker"
)

type RunConfig struct {
	PeerID protocol.PeerID
	Port   uint16
}

func (t *Torrent) beginRun(cancel context.CancelFunc) error {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()

	if t.running {
		return ErrAlreadyRunning
	}

	t.running = true
	t.cancel = cancel
	t.done = make(chan struct{})
	t.closeErr = nil
	return nil
}

func (t *Torrent) finishRun(runErr error) error {
	t.closePeers()
	fileErr := t.closeFile()

	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()

	t.running = false
	t.cancel = nil
	t.closeErr = fileErr

	close(t.done)

	if runErr == nil && fileErr != nil {
		return fileErr
	}

	return runErr
}

func (t *Torrent) RunDownload(ctx context.Context, config RunConfig) (err error) {
	runCtx, cancel := context.WithCancel(ctx)

	if err := t.beginRun(cancel); err != nil {
		cancel()
		return err
	}

	defer func() {
		cancel()
		err = t.finishRun(err)
	}()

	resp, err := t.announce(runCtx, config, tracker.StartedEvent)

	if err != nil {
		return err
	}

	t.connectTrackerPeers(runCtx, config, resp.Peers)

	interval := normalizeTrackerInterval(resp.Interval)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	reannounceC := timer.C

	for {
		select {

		case event := <-t.peerEvents:
			t.handlePeerEvent(runCtx, event)

		case request := <-t.statusRequests:
			request.reply <- t.buildStatus()
		case downloadModeRequest := <-t.downloadModeRequests:
			downloadModeRequest.reply <- t.changeDownloadMode(downloadModeRequest.mode)

			/*
				if setPause.paused {
					timer.Stop()
					reannounceC = nil
					_, announceErr := t.announce(runCtx, config, tracker.StoppedEvent)
					setPause.reply <- pauseResult{changed: true, err: announceErr}
					continue
				}

				resp, announceErr := t.announce(runCtx, config, tracker.StartedEvent)
				if announceErr == nil {
					interval = normalizeTrackerInterval(resp.Interval)
					t.connectTrackerPeers(runCtx, config, resp.Peers)
					timer.Reset(interval)
					reannounceC = timer.C
				}
				setPause.reply <- pauseResult{changed: true, err: announceErr}*/

		case <-reannounceC:
			resp, announceErr := t.announce(runCtx, config, "")

			if announceErr == nil {
				interval = normalizeTrackerInterval(resp.Interval)
				t.connectTrackerPeers(runCtx, config, resp.Peers)
			}

			timer.Reset(interval)
			reannounceC = timer.C

		case <-runCtx.Done():
			return runCtx.Err()
		}
	}
}

func (t *Torrent) Close() error {
	t.lifecycleMu.Lock()
	if !t.running {
		err := t.closeFile()
		t.lifecycleMu.Unlock()
		return err
	}

	cancel := t.cancel
	done := t.done
	t.lifecycleMu.Unlock()

	cancel()
	<-done // waits until all operations are done!

	t.lifecycleMu.Lock()
	err := t.closeErr
	t.lifecycleMu.Unlock()
	return err
}

func (t *Torrent) connectTrackerPeers(ctx context.Context, config RunConfig, candidates []tracker.Peer) {

	for _, candidate := range candidates {
		// if err := t.conn

		if len(t.peers) >= maxPeers {
			return
		}

		peer, err := t.connectPeer(ctx, config, candidate)

		if err != nil {
			continue
		}

		if err := t.activatePeer(ctx, peer); err != nil {
			t.closePeer(peer)
		}

	}
}
