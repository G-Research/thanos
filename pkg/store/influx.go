package store

import (
	"context"
	"net/http"
	"net/url"
	"sync"

	"encoding/json"
	"math"
	"path"

	"bytes"

	"fmt"

	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func matchString(matcher storepb.LabelMatcher, quoteChar string) string {
	ret := matcher.Name
	isRe := false
	switch matcher.Type {
	case storepb.LabelMatcher_EQ:
		ret += " = " + quoteChar
		break
	case storepb.LabelMatcher_NEQ:
		ret += " != " + quoteChar
		break
	case storepb.LabelMatcher_RE:
		isRe = true
		ret += " =~ /"
		break
	case storepb.LabelMatcher_NRE:
		isRe = true
		ret += " !~ /"
		break
	}
	ret += matcher.Value
	if isRe {
		ret += "/"
	} else {
		ret += quoteChar
	}
	return ret
}

func (store *InfluxStore) Series(req *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	fmt.Printf("Received request for period %s -> %s\n", req.MinTime, req.MaxTime)
	fmt.Printf("          max resolution of %s\n", req.MaxResolutionWindow)
	fmt.Printf("   Requires aggregates:\n")
	for _, agg := range req.Aggregates {
		fmt.Printf("      -> %s\n", agg)
	}
	fmt.Printf("   Label matchers:\n")
	for _, matcher := range req.Matchers {
		fmt.Printf("      -> %s %s %s\n", matcher.Name, matcher.Type, matcher.Value)
	}
	fmt.Println()

	// matcher slice keyed by name
	matchers := make(map[string][]storepb.LabelMatcher)
	for _, matcher := range req.Matchers {
		if _, exists := matchers[matcher.Name]; !exists {
			matchers[matcher.Name] = make([]storepb.LabelMatcher, 0)
		}
		matchers[matcher.Name] = append(matchers[matcher.Name], matcher)
	}

	// find our metric names if we weren't given them!
	var haveAnyMetricNames bool
	if _, haveAnyMetricNames = matchers["__name__"]; !haveAnyMetricNames {
		q := "SHOW MEASUREMENTS WHERE"
		sep := " "
		for _, matcher := range req.Matchers {
			q += sep + matchString(matcher, "\"")
			sep = " AND "
		}

		d, err := influxQuery(store.base, q)
		if err != nil {
			return err
		}

		metrics := make([]storepb.LabelMatcher, 0)
		if d.Results[0].Series != nil {
			for _, values := range d.Results[0].Series[0].Values {
				metric := values[0].(string)
				fmt.Printf("   Auto-deduced metric name: %s\n", metric)
				metrics = append(metrics, storepb.LabelMatcher{
					Type:  storepb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: metric,
				})
			}
			matchers["__name__"] = metrics
			haveAnyMetricNames = true
		}
	}

	// righto, now we can construct our query
	if haveAnyMetricNames {
		// SELECT * FROM "host_unix_epoch" WHERE host='localhost' AND time >= 1529938383s AND time <= 1529938422s;
		q := "SELECT * FROM "
		sep := ""
		for _, matcher := range matchers["__name__"] {
			q += sep + "\"" + matcher.Value + "\""
			sep = ", "
		}
		q += " WHERE time >= " + strconv.FormatInt(req.MinTime, 10) + "ms AND time <= " + strconv.FormatInt(req.MaxTime, 10) + "ms"
		sep = " AND "
		for k, v := range matchers {
			if k == "__name__" {
				continue
			}
			for _, matcher := range v {
				q += sep + matchString(matcher, "'")
			}
		}
		q += ";"
		fmt.Printf("   Derived query: %s\n", q)

		d, err := influxQuery(store.base, q)
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}

		// now we got some data, feck knows what we do with it!

		if d.Results[0].Series != nil {
			for _, series := range d.Results[0].Series {
				metric := series.Name
				timeColumn := -1
				valueColumn := -2
				otherColumns := make(map[string]int)
				for c, name := range series.Columns {
					if name == "time" {
						timeColumn = c
					} else if name == "value" {
						valueColumn = c
					} else {
						otherColumns[name] = c
					}
				}
				// now we know where our data is
				samples := make([]prompb.Sample, 0)
				unpackedTags := false
				tags := make(map[string]string)
				for _, row := range series.Values {
					if !unpackedTags {
						for k, v := range otherColumns {
							tags[k] = row[v].(string)
						}
						unpackedTags = true
					}

					samples = append(samples, prompb.Sample{Timestamp: int64(row[timeColumn].(float64)), Value: row[valueColumn].(float64)})
				}

				seriesLabels := make([]storepb.Label, len(tags))
				i := 0
				for k, v := range tags {
					seriesLabels[i] = storepb.Label{Name: k, Value: v}
					i++
				}
				seriesLabels = append(seriesLabels, storepb.Label{Name: "__name__", Value: metric})

				fmt.Printf("Returning some data:   %v samples", len(samples))
				fmt.Printf("Returning some labels: %v", seriesLabels)
				enc, cb, err := encodeChunk(samples)
				if err != nil {
					fmt.Printf("Error encoding chunk: %s", err.Error())
					return status.Error(codes.Unknown, err.Error())
				}
				resp := storepb.NewSeriesResponse(&storepb.Series{
					Labels: seriesLabels,
					Chunks: []storepb.AggrChunk{{
						MinTime: int64(samples[0].Timestamp),
						MaxTime: int64(samples[len(samples)-1].Timestamp),
						Raw:     &storepb.Chunk{Type: enc, Data: cb},
					}},
				})
				if err := server.Send(resp); err != nil {
					fmt.Printf("Error sending response: %s", err.Error())
					return err
				}
			}
		} else {
			fmt.Printf("Found no series?")
		}

	}

	return nil
}

// encodeChunk translates the sample pairs into a chunk.
func encodeChunk(ss []prompb.Sample) (storepb.Chunk_Encoding, []byte, error) {
	c := chunkenc.NewXORChunk()

	a, err := c.Appender()
	if err != nil {
		return 0, nil, err
	}
	for _, s := range ss {
		a.Append(int64(s.Timestamp), float64(s.Value))
	}
	return storepb.Chunk_XOR, c.Bytes(), nil
}

func (store *InfluxStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	d, err := influxQuery(store.base, "show tag keys;")
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)

	series := d.Results[0].Series
	for _, metricResult := range series {
		for v := 1; v < len(metricResult.Values); v++ {
			tagKey := metricResult.Values[v][0].(string)
			names[tagKey] = tagKey
		}
	}

	keys := make([]string, len(names))

	i := 0
	for k := range names {
		keys[i] = k
		i++
	}

	res := &storepb.LabelNamesResponse{
		Names:    keys,
		Warnings: make([]string, 0),
	}
	return res, nil
}
func (store *InfluxStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {

	d, err := influxQuery(store.base, "show tag values with key = \""+req.Label+"\";")
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)

	series := d.Results[0].Series
	for _, metricResult := range series {
		for v := 1; v < len(metricResult.Values); v++ {
			// index 0: key, index 1: value
			tagValue := metricResult.Values[v][1].(string)
			names[tagValue] = tagValue
		}
	}

	values := make([]string, len(names))

	i := 0
	for k := range names {
		values[i] = k
		i++
	}

	res := &storepb.LabelValuesResponse{
		Values:   values,
		Warnings: make([]string, 0),
	}

	return res, nil
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
		for v := 1; v < len(metricResult.Values); v++ {
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
		StatementId int
		Series      []struct {
			Name    string
			Columns []string
			Values  [][]interface{}
		}
	}
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

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	bodyContent := buf.String()

	//println("body: " + bodyContent)

	var d influxQueryResult

	if err := json.Unmarshal([]byte(bodyContent), &d); err != nil {
		//fmt.Printf("err: %s\n", err.Error())
		//fmt.Printf("d: %+v\n", d)
		return nil, errors.Wrap(err, "decode response")
	}
	//fmt.Printf("d: %+v\n", d)

	return &d, nil

}
