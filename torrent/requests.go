package torrent

import "context"

type downloadModeRequest struct {
	mode  downloadMode
	reply chan error
}

type statusRequest struct {
	reply chan Status
}

/// type setDownloadModeRequest

func (t *Torrent) lifecycleRunningLock() (chan struct{}, error) {
	t.lifecycleMu.Lock()
	running := t.running
	done := t.done

	t.lifecycleMu.Unlock()

	if !running {
		return nil, ErrNotRunning
	}

	return done, nil

}

func (t *Torrent) ReqStatus(ctx context.Context) (Status, error) {

	done, err := t.lifecycleRunningLock()

	if err != nil {
		return Status{}, err
	}

	replyChan := make(chan Status, 1)

	request := statusRequest{reply: replyChan}

	select {

	case t.statusRequests <- request:
	case <-done:
		return Status{}, ErrNotRunning

	case <-ctx.Done():
		return Status{}, ctx.Err()
	}

	select {
	case resp := <-replyChan:
		return resp, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()

	case <-done:
		return Status{}, ErrNotRunning
	}

}

// external calls to request pausing or resuming downloads
func (t *Torrent) PauseDownload(ctx context.Context) error {
	return t.requestDownloadMode(ctx, downloadModePaused)
}

func (t *Torrent) ResumeDownload(ctx context.Context) error {
	return t.requestDownloadMode(ctx, downloadModeProgress)
}

// actually makes the command request
func (t *Torrent) requestDownloadMode(ctx context.Context, mode downloadMode) error {

	done, err := t.lifecycleRunningLock()

	if err != nil {
		return err
	}

	reply := make(chan error, 1)
	req := downloadModeRequest{
		mode:  mode,
		reply: reply,
	}

	select {
	case t.downloadModeRequests <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrStopped

	}

	select {
	case errReply := <-reply:
		return errReply

	case <-ctx.Done():

		return ctx.Err()

	case <-done:
		return ErrStopped
	}
}
