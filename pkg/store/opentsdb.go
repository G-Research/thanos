package store

import (
	"context"
	"fmt"
	"math"
	"regexp"

	//these two will be merged
	opentsdb "github.com/bluebreezecf/opentsdb-goclient/client"
	log "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/opentsdbclient"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
)

// TODO: Either move opentsdb to an other package or use an existing one
// like this : https://github.com/bluebreezecf/opentsdb-goclient/blob/master/client/uid.go
type OpenTSDBStore struct {
	openTSDBClient opentsdbclient.OpenTSDBClient
	logger         log.Logger
}

func NewOpenTSDBStore(logger log.Logger) (*OpenTSDBStore, error) {
	level.Debug(logger).Log("msg", "new opentsdb store has been created")
	return &OpenTSDBStore{
		openTSDBClient: opentsdbclient.NewOpenTSDBClient("ec2-3-88-64-48.compute-1.amazonaws.com:4242"),
		logger:         log.With(logger, "component", "opentsdb"),
	}, nil
}

func (tsdb *OpenTSDBStore) Info(
	ctx context.Context,
	req *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	level.Debug(tsdb.logger).Log("msg", "call opentsdb info")
	info := storepb.InfoResponse{
		MinTime: 0,
		MaxTime: math.MaxInt64,
		Labels:  []storepb.Label{storepb.Label{Name: "infokey", Value: "infoValue"}},
	}
	return &info, nil
}

func (store *OpenTSDBStore) Series(
	req *storepb.SeriesRequest,
	server storepb.Store_SeriesServer) error {
	level.Debug(store.logger).Log("msg", "opentsdb series")
	// matcher slice keyed by name // from the influx implementation
	matchers := make(map[string][]storepb.LabelMatcher)
	for _, matcher := range req.Matchers {
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
				Aggregator: "avg",
				Metric:     m[0].Value,
			},
		}
		level.Debug(store.logger).Log("msg", query.Queries[0])
	}
	resp, err := store.openTSDBClient.Query(query)
	if err != nil {
		level.Debug(store.logger).Log("msg", err.Error())
	}
	level.Debug(store.logger).Log("msg", resp)
	res := storepb.NewSeriesResponse(&storepb.Series{
		Labels: nil,
		Chunks: []storepb.AggrChunk{{
			MinTime: 1548324852,
			MaxTime: 1548324952,
			Raw:     nil,
		}},
	})
	server.Send(res)
	return nil
}

func (store *OpenTSDBStore) LabelNames(
	ctx context.Context,
	req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	level.Debug(store.logger).Log("msg", "opentsdb label names ")
	return &storepb.LabelNamesResponse{
		// using the suggest api is probably not the best way to do this, but
		// it was the easiest to implement
		Names: store.openTSDBClient.Suggest("tagk", "", math.MaxInt32),
	}, nil
}

func (store *OpenTSDBStore) LabelValues(
	ctx context.Context,
	req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	level.Debug(store.logger).Log("msg", "opentsdb label values ")
	uidMetaRes := store.openTSDBClient.UIDMetaData(fmt.Sprintf("name:%%22%s%%22", req.Label))
	if len(uidMetaRes) != 1 {
		return nil, fmt.Errorf("there should be only one result")
	}
	labelUID := uidMetaRes[0].UID
	tsMetaRes := store.openTSDBClient.TSMetaData(fmt.Sprintf("tsuid:/((.)%%7B6%%7D)%%2B((.)%%7B12%%7D)*%s.*/", labelUID))
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
		lableVMeta := store.openTSDBClient.UIDMetaLookup("TAGV", v)
		resp.Values = append(resp.Values, lableVMeta.Name)
	}

	return &resp, nil
}
