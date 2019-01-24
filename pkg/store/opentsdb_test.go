package store

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
)

func TestOpenTSDBLabelValues(t *testing.T) {
	tsdb, _ := NewOpenTSDBStore()
	resp, err := tsdb.LabelValues(nil, &storepb.LabelValuesRequest{
		Label: "test4-K1.t1c",
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(resp.Values) != 5 {
		t.Errorf("number of values should be 4 %s %d", resp.Values, len(resp.Values))
	}
}

func TestOpenTSDBLabelNames(t *testing.T) {
	tsdb, _ := NewOpenTSDBStore()
	resp, err := tsdb.LabelNames(nil, nil)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(resp.Names) != 56 {
		t.Errorf("number of values should be 56 it s %d", len(resp.Names))
	}
}
