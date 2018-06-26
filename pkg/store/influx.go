package store

import (
	"net/http"
	"net/url"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/prometheus/tsdb/labels"
)

// InfluxStore implements the store node API on top of the InfluxDB's HTTP API.
type InfluxStore struct {
	logger         log.Logger
	base           *url.URL
	client         *http.Client
	buffers        sync.Pool
	externalLabels labels.Labels
	timestamps     func() (mint int64, maxt int64)
}

// NewInfluxStore returns a new InfluxStore that uses the given HTTP client
// to talk to InfluxDB.
func NewInfluxStore(
	logger log.Logger,
	client *http.Client,
	baseURL *url.URL,
	externalLabels labels.Labels,
	timestamps func() (mint int64, maxt int64),
) (*InfluxStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if client == nil {
		client = &http.Client{
			Transport: tracing.HTTPTripperware(logger, http.DefaultTransport),
		}
	}
	p := &InfluxStore{
		logger:         logger,
		base:           baseURL,
		client:         client,
		externalLabels: externalLabels,
		timestamps:     timestamps,
	}
	return p, nil
}
