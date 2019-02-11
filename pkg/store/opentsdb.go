package store

import (
	"context"
	"fmt"
	"math"
	"regexp"

	//these two will be merged
	"github.com/bluebreezecf/opentsdb-goclient/client"
	opentsdb "github.com/bluebreezecf/opentsdb-goclient/client"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/opentsdbclient"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
)

// TODO: Either move opentsdb to an other package or use an existing one
// like this : https://github.com/bluebreezecf/opentsdb-goclient/blob/master/client/uid.go
type OpenTSDBStore struct {
	openTSDBClient opentsdbclient.OpenTSDBClient
	logger         log.Logger
	timestamps     func() (int64, int64)
	externalLabels labels.Labels
}

func NewOpenTSDBStore(url string,
	logger log.Logger,
	timestamps func() (int64, int64),
	externalLabels labels.Labels) (*OpenTSDBStore, error) {
	level.Debug(logger).Log("msg", "new opentsdb store has been created")
	return &OpenTSDBStore{
		openTSDBClient: opentsdbclient.NewOpenTSDBClient(url),
		logger:         log.With(logger, "component", "opentsdb"),
		timestamps:     timestamps,
		externalLabels: externalLabels,
	}, nil
}

func (tsdb *OpenTSDBStore) Info(
	ctx context.Context,
	req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	level.Debug(tsdb.logger).Log("msg", "call opentsdb info")
	mint, maxt := tsdb.timestamps()
	res := storepb.InfoResponse{
		MinTime: mint,
		MaxTime: maxt,
		Labels:  make([]storepb.Label, 0, len(tsdb.externalLabels)),
	}
	for _, l := range tsdb.externalLabels {
		level.Debug(tsdb.logger).Log("lables.name", l.Name, "laels.value", l.Value)
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return &res, nil
}

func (store *OpenTSDBStore) Series(
	req *storepb.SeriesRequest,
	server storepb.Store_SeriesServer) error {
	level.Debug(store.logger).Log("msg", "opentsdb series")
	// matcher slice keyed by name // from the influx implementation
	matchers := make(map[string][]storepb.LabelMatcher)
	for _, matcher := range req.Matchers {
		level.Debug(store.logger).Log("matcher", matcher.Value)
		if _, exists := matchers[matcher.Name]; !exists {
			matchers[matcher.Name] = make([]storepb.LabelMatcher, 0)
		}
		matchers[matcher.Name] = append(matchers[matcher.Name], matcher)
	}

	query := opentsdb.QueryParam{
		Start:   req.MinTime,
		End:     req.MaxTime,
		Queries: nil,
	}

	if m, ok := matchers["__name__"]; ok {
		query.Queries = []opentsdb.SubQuery{
			opentsdb.SubQuery{
				Aggregator: "sum",
				Metric:     m[0].Value,
			},
		}
	}
	resp, err := store.openTSDBClient.Query(query)
	if err != nil {
		level.Error(store.logger).Log("err", err.Error())
		return err
	}

	samples := make([]prompb.Sample, 0)
	var tags map[string]string
	for _, respI := range resp.QueryRespCnts {
		for _, dp := range respI.GetDataPoints() {
			samples = append(samples, prompb.Sample{Timestamp: dp.Timestamp * 1000, Value: dp.Value.(float64)})
		}
		tags = respI.Tags
	}
	seriesLabels := make([]storepb.Label, len(tags))
	i := 0
	for k, v := range tags {
		seriesLabels[i] = storepb.Label{Name: k, Value: v}
		i++
	}
	seriesLabels = append(seriesLabels, storepb.Label{Name: "__name__", Value: matchers["__name__"][0].Value})
	enc, cb, err := encodeChunk(samples)
	if err != nil {
	}
	res := storepb.NewSeriesResponse(&storepb.Series{
		Labels: seriesLabels,
		Chunks: []storepb.AggrChunk{{
			MinTime: samples[0].Timestamp,
			MaxTime: samples[len(samples)-1].Timestamp,
			Raw:     &storepb.Chunk{Type: enc, Data: cb},
		}},
	})
	if err := server.Send(res); err != nil {
		return err
	}
	return nil
}

func (store *OpenTSDBStore) LabelNames(
	ctx context.Context,
	req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	resp, err := store.openTSDBClient.Client.Suggest(client.SuggestParam{Type: client.TypeTagk, Q: "", MaxResultNum: math.MaxInt32})
	if err != nil {
		return nil, err
	}
	return &storepb.LabelNamesResponse{
		// using the suggest api is probably not the best way to do this, but
		// it was the easiest to implement
		Names: resp.ResultInfo,
	}, nil
}

func (store *OpenTSDBStore) LabelValues(
	ctx context.Context,
	req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	uidMetaRes := store.openTSDBClient.UIDMetaData(fmt.Sprintf("name:%%22%s%%22", req.Label))
	if len(uidMetaRes) != 1 {
		return nil, fmt.Errorf("there should be only one result")
	}
	labelUID := uidMetaRes[0].UID
	tsMetaRes := store.openTSDBClient.TSMetaData(
		fmt.Sprintf("tsuid:/((.)%%7B6%%7D)%%2B((.)%%7B12%%7D)*%s.*/", labelUID))
	valuesSet := make(map[string]bool)
	// looking for lalbeUID in tsuids
	rgx, err := regexp.Compile(labelUID)
	if err != nil {
		level.Error(store.logger).Log("msg", err)
	}
	for _, ts := range tsMetaRes {
		matches := rgx.FindAllStringIndex(ts.TSUID, 3)
		if len(matches) > 3 {
			level.Error(store.logger).Log("msg", "an uid at most 3 times can appear (tagk, tagv, metric)")
		}
		var valueID string
		for _, m := range matches {
			// we care only about the tagk
			if m[0]%6 == 0 && m[0]%12 != 0 {
				valueID = ts.TSUID[m[1] : m[1]+6]
				// it can appear only once in a tsuid
				break
			}
		}
		valuesSet[valueID] = true
	}
	resp := storepb.LabelValuesResponse{
		Values: []string{},
	}
	for v, _ := range valuesSet {
		qresp, err := store.openTSDBClient.Client.QueryUIDMetaData(map[string]string{"uid": v, "type": client.TypeTagv})
		if err != nil {
			return nil, err
		}
		resp.Values = append(resp.Values, qresp.Name)
	}

	return &resp, nil
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
