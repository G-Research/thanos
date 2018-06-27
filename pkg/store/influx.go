package store

import (
	"context"
	"net/http"
	"net/url"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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

// Info returns store information about the InfluxDB instance.
func (store *InfluxStore) Info(ctx context.Context, req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	lset := store.externalLabels
	mint, maxt := store.timestamps()

	res := &storepb.InfoResponse{
		MinTime: mint,
		MaxTime: maxt,
		Labels:  make([]storepb.Label, 0, len(lset)),
	}
	for _, l := range lset {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res, nil
}

func (store *InfluxStore) Series(req *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	return status.Error(codes.Unimplemented, "not implemented")
}
func (store *InfluxStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (store *InfluxStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
	return nil, nil
}
