package store

import (
	"context"
	"net/url"
	"sync"

	"fmt"

	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/improbable-eng/thanos/pkg/influx"
)

// InfluxStore implements the store node API on top of the InfluxDB's HTTP API.
type InfluxStore struct {
	logger         log.Logger
	database       string
	client         *influx.Client
	buffers        sync.Pool
	externalLabels labels.Labels
	timestamps     func() (mint int64, maxt int64)
}

// NewInfluxStore returns a new InfluxStore that uses the given HTTP client
// to talk to InfluxDB.
func NewInfluxStore(
	logger log.Logger,
	database string,
	baseURL *url.URL,
	externalLabels labels.Labels,
	timestamps func() (mint int64, maxt int64),
) (*InfluxStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	p := &InfluxStore{
		logger:         logger,
		database:       database,
		client: 		influx.NewClient(baseURL),
		externalLabels: externalLabels,
		timestamps:     timestamps,
	}
	return p, nil
}

// Info returns store information about the InfluxDB instance.
func (store *InfluxStore) Info(ctx context.Context, req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	fmt.Printf("Received request for info\n")
	labelSet := store.externalLabels

	minTime, maxTime := store.timestamps()

	res := &storepb.InfoResponse{
		MinTime: minTime,
		MaxTime: maxTime,
		Labels:  make([]storepb.Label, 0, len(labelSet)),
	}
	for _, l := range labelSet {
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
	fmt.Printf("Received request for period %d -> %d\n", req.MinTime, req.MaxTime)
	fmt.Printf("          max resolution of %d\n", req.MaxResolutionWindow)
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

		d, err := store.client.Query(nil, store.database, q)
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

		d, err := store.client.Query(nil, store.database, q)
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
	fmt.Printf("Received request for label names\n")
	d, err := store.client.Query(ctx, store.database, "show tag keys;")
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

	keys := make([]string, len(names)+1)

	i := 0
	for k := range names {
		keys[i] = k
		i++
	}
	keys[i] = "__name__"

	fmt.Printf("Returned %d label names\n", len(keys))

	res := &storepb.LabelNamesResponse{
		Names:    keys,
		Warnings: make([]string, 0),
	}
	return res, nil
}

func (store *InfluxStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	fmt.Printf("Received request for label values for label \"%s\"\n", req.Label)

	var values []string
	if req.Label == "__name__" {
		var err error
		valuesPtr, err := store.client.AllMetrics(ctx, store.database)
		if err != nil {
			return nil, err
		}
		values = *valuesPtr
	} else {
		d, err := store.client.Query(ctx, store.database, "show tag values with key = \""+req.Label+"\";")
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

		values = make([]string, len(names))

		i := 0
		for k := range names {
			values[i] = k
			i++
		}
	}

	fmt.Printf("Returned %d label values for name \"%s\"\n", len(values), req.Label)

	res := &storepb.LabelValuesResponse{
		Values:   values,
		Warnings: make([]string, 0),
	}

	return res, nil
}
