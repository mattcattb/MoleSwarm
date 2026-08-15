package tracker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/mattcattb/MoleSwarm/protocol"
)

const maxResponseSize = 4 << 20

func Announce(ctx context.Context, announceURL string, request AnnounceRequest) (AnnounceResponse, error) {
	formattedURL, err := encodeAnnounceRequest(announceURL, request)
	if err != nil {
		return AnnounceResponse{}, err
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, formattedURL.String(), nil)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("build announce HTTP request: %w", err)
	}
	response, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("announce HTTP request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return AnnounceResponse{}, fmt.Errorf("announce HTTP status: %s", response.Status)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseSize+1))
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("read tracker response: %w", err)
	}
	if len(body) > maxResponseSize {
		return AnnounceResponse{}, fmt.Errorf("tracker response exceeds %d bytes", maxResponseSize)
	}

	value, err := protocol.Decode(bytes.NewReader(body))
	if err != nil {
		return AnnounceResponse{}, fmt.Errorf("decode tracker response: %w", err)
	}
	return decodeTrackerResponse(value)
}
