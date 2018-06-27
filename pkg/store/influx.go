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
	"path"
	errors "github.com/pkg/errors"
	"encoding/json"
	"math"
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
	// todo: should have this logic in the method commented here
	//mint, maxt := store.timestamps()

	allMetrics, err := allMetrics(store.base)
	if err != nil {
		return nil, err
	}
	allTimes, err := minMaxTimestampByMetric(store.base, allMetrics)
	if err != nil {
		return nil, err
	}

	var minTime, maxTime int64
	minTime = math.MaxInt64
	for _, v := range allTimes {
		minTime = min(minTime, v[0])
		maxTime = max(maxTime, v[1])
	}

	res := &storepb.InfoResponse{
		MinTime: minTime,
		MaxTime: maxTime,
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
	d, err := influxQuery(store.base, "show tag keys;")
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)

	series := d.Results[0].Series
	for _, metricResult := range series {
		for v:=1; v<len(metricResult.Values); v++ {
			label := metricResult.Values[v][0].(string)
			names[label] = label
		}
	}

	keys := make([]string, len(names))

	i := 0
	for k := range names {
		keys[i] = k
		i++
	}


	res := &storepb.LabelNamesResponse{
		Names: keys,
		Warnings: make([]string, 0),
	}
	return res, nil
}
func (store *InfluxStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {

	_, err := influxQuery(store.base, "show tag values with key = \"host\";")
	if err != nil {
		return nil, err
	}

	// todo: unpack response

	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func allMetrics(base *url.URL) (*[]string, error) {
	d, err := influxQuery(base, "show measurements;")
	if err != nil {
		return nil, err
	}

	var ret = make([]string, len(d.Results[0].Series[0].Values))
	results := d.Results[0].Series[0].Values
	for i, metricArray := range results {
		ret[i] = metricArray[0].(string)
	}

	return &ret, nil
}


func minMaxTimestampByMetric(base *url.URL, metrics *[]string) (map[string][]int64, error) {

	q := "select first(*) from"
	sep := " "
	for _, metric := range *metrics {
		q += sep + "\"" + metric + "\""
		sep = ", "
	}
	q += ";"


	d, err := influxQuery(base, q)
	if err != nil {
		return nil, err
	}

	ret := make(map[string][]int64)

	series := d.Results[0].Series
	for _, metricResult := range series {
		metric := metricResult.Name
		var minTime, maxTime int64
		minTime = math.MaxInt64
		for v:=1; v<len(metricResult.Values); v++ {
			minTime = min(minTime, metricResult.Values[v][0].(int64))
			maxTime = max(maxTime, metricResult.Values[v][0].(int64))
		}
		ret[metric] = []int64{minTime, maxTime}
	}

	return ret, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}


type influxQueryResult struct {
	Results []struct {
		StatementId string
		Series []struct {
			Name string
			Columns []string
			Values [][]interface{}
		}
	} `json:"data"`
}

func influxQuery(base *url.URL, query string) (*influxQueryResult, error) {
	// http://localhost:9041/query?pretty=true&db=opentsdb&epoch=ms&q=
	u := *base
	u.Path = path.Join(u.Path, "/query")

	q := u.Query()
	q.Add("pretty", "false")
	q.Add("db", "opentsdb")
	q.Add("epoch", "ms")
	q.Add("q", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "request against %s", u.String())
	}
	defer resp.Body.Close()


	var d *influxQueryResult
	if err := json.NewDecoder(resp.Body).Decode(d); err != nil {
		return nil, errors.Wrap(err, "decode response")
	}

	return d, nil

}
