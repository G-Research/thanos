// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/runutil"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxSamplesPerChunk is approximately the max number of samples that we may have in any given chunk. This is needed
	// for precalculating the number of samples that we may have to retrieve and decode for any given query
	// without downloading them. Please take a look at https://github.com/prometheus/tsdb/pull/397 to know
	// where this number comes from. Long story short: TSDB is made in such a way, and it is made in such a way
	// because you barely get any improvements in compression when the number of samples is beyond this.
	// Take a look at Figure 6 in this whitepaper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
	maxSamplesPerChunk = 120

	maxChunkSize = 16000

	maxSeriesSize = 64 * 1024

	// CompatibilityTypeLabelName is an artificial label that Store Gateway can optionally advertise. This is required for compatibility
	// with pre v0.8.0 Querier. Previous Queriers was strict about duplicated external labels of all StoreAPIs that had any labels.
	// Now with newer Store Gateway advertising all the external labels it has access to, there was simple case where
	// Querier was blocking Store Gateway as duplicate with sidecar.
	//
	// Newer Queriers are not strict, no duplicated external labels check is there anymore.
	// Additionally newer Queriers removes/ignore this exact labels from UI and querying.
	//
	// This label name is intentionally against Prometheus label style.
	// TODO(bwplotka): Remove it at some point.
	CompatibilityTypeLabelName = "@thanos_compatibility_store_type"

	partitionerMaxGapSize = 512 * 1024
)

type bucketStoreMetrics struct {
	blocksLoaded          prometheus.Gauge
	blockLoads            prometheus.Counter
	blockLoadFailures     prometheus.Counter
	blockDrops            prometheus.Counter
	blockDropFailures     prometheus.Counter
	seriesDataTouched     *prometheus.SummaryVec
	seriesDataFetched     *prometheus.SummaryVec
	seriesDataSizeTouched *prometheus.SummaryVec
	seriesDataSizeFetched *prometheus.SummaryVec
	seriesBlocksQueried   prometheus.Summary
	seriesGetAllDuration  prometheus.Histogram
	seriesMergeDuration   prometheus.Histogram
	resultSeriesCount     prometheus.Summary
	chunkSizeBytes        prometheus.Histogram
	queriesDropped        prometheus.Counter
	queriesLimit          prometheus.Gauge
	seriesRefetches       prometheus.Counter
}

func newBucketStoreMetrics(reg prometheus.Registerer) *bucketStoreMetrics {
	var m bucketStoreMetrics

	m.blockLoads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_loads_total",
		Help: "Total number of remote block loading attempts.",
	})
	m.blockLoadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_load_failures_total",
		Help: "Total number of failed remote block loading attempts.",
	})
	m.blockDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drops_total",
		Help: "Total number of local blocks that were dropped.",
	})
	m.blockDropFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drop_failures_total",
		Help: "Total number of local blocks that failed to be dropped.",
	})
	m.blocksLoaded = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	})

	m.seriesDataTouched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_touched",
		Help: "How many items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_fetched",
		Help: "How many items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesDataSizeTouched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_touched_bytes",
		Help: "Size of all items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataSizeFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_fetched_bytes",
		Help: "Size of all items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesBlocksQueried = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_blocks_queried",
		Help: "Number of blocks in a bucket store that were touched to satisfy a query.",
	})
	m.seriesGetAllDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_get_all_duration_seconds",
		Help:    "Time it takes until all per-block prepares and preloads for a query are finished.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.seriesMergeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_merge_duration_seconds",
		Help:    "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.resultSeriesCount = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_result_series",
		Help: "Number of series observed in the final result of a query.",
	})

	m.chunkSizeBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.queriesDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the sample limit.",
	})
	m.queriesLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_queries_concurrent_max",
		Help: "Number of maximum concurrent queries.",
	})
	m.seriesRefetches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_series_refetches_total",
		Help: fmt.Sprintf("Total number of cases where %v bytes was not enough was to fetch series from index, resulting in refetch.", maxSeriesSize),
	})

	if reg != nil {
		reg.MustRegister(
			m.blockLoads,
			m.blockLoadFailures,
			m.blockDrops,
			m.blockDropFailures,
			m.blocksLoaded,
			m.seriesDataTouched,
			m.seriesDataFetched,
			m.seriesDataSizeTouched,
			m.seriesDataSizeFetched,
			m.seriesBlocksQueried,
			m.seriesGetAllDuration,
			m.seriesMergeDuration,
			m.resultSeriesCount,
			m.chunkSizeBytes,
			m.queriesDropped,
			m.queriesLimit,
			m.seriesRefetches,
		)
	}
	return &m
}

// FilterConfig is a configuration, which Store uses for filtering metrics based on time.
type FilterConfig struct {
	MinTime, MaxTime model.TimeOrDurationValue
}

// BucketStore implements the store API backed by a bucket. It loads all index
// files to local disk.
type BucketStore struct {
	logger     log.Logger
	metrics    *bucketStoreMetrics
	bkt        objstore.BucketReader
	fetcher    block.MetadataFetcher
	dir        string
	indexCache storecache.IndexCache
	chunkPool  pool.BytesPool

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	mtx       sync.RWMutex
	blocks    map[ulid.ULID]*bucketBlock
	blockSets map[uint64]*bucketBlockSet

	// Verbose enabled additional logging.
	debugLogging bool
	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gater

	// samplesLimiter limits the number of samples per each Series() call.
	samplesLimiter SampleLimiter
	partitioner    partitioner

	filterConfig             *FilterConfig
	advLabelSets             []storepb.LabelSet
	enableCompatibilityLabel bool
	enableIndexHeader        bool
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	fetcher block.MetadataFetcher,
	dir string,
	indexCache storecache.IndexCache,
	maxChunkPoolBytes uint64,
	maxSampleCount uint64,
	maxConcurrent int,
	debugLogging bool,
	blockSyncConcurrency int,
	filterConfig *FilterConfig,
	enableCompatibilityLabel bool,
	enableIndexHeader bool,
) (*BucketStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	if maxConcurrent < 0 {
		return nil, errors.Errorf("max concurrency value cannot be lower than 0 (got %v)", maxConcurrent)
	}

	chunkPool, err := pool.NewBucketedBytesPool(maxChunkSize, 50e6, 2, maxChunkPoolBytes)
	if err != nil {
		return nil, errors.Wrap(err, "create chunk pool")
	}

	metrics := newBucketStoreMetrics(reg)
	s := &BucketStore{
		logger:               logger,
		bkt:                  bucket,
		fetcher:              fetcher,
		dir:                  dir,
		indexCache:           indexCache,
		chunkPool:            chunkPool,
		blocks:               map[ulid.ULID]*bucketBlock{},
		blockSets:            map[uint64]*bucketBlockSet{},
		debugLogging:         debugLogging,
		blockSyncConcurrency: blockSyncConcurrency,
		filterConfig:         filterConfig,
		queryGate: gate.NewGate(
			maxConcurrent,
			extprom.WrapRegistererWithPrefix("thanos_bucket_store_series_", reg),
		),
		samplesLimiter:           NewLimiter(maxSampleCount, metrics.queriesDropped),
		partitioner:              gapBasedPartitioner{maxGapSize: partitionerMaxGapSize},
		enableCompatibilityLabel: enableCompatibilityLabel,
		enableIndexHeader:        enableIndexHeader,
	}
	s.metrics = metrics

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

	s.metrics.queriesLimit.Set(float64(maxConcurrent))

	return s, nil
}

// Close the store.
func (s *BucketStore) Close() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, b := range s.blocks {
		runutil.CloseWithErrCapture(&err, b, "closing Bucket Block")
	}
	return err
}

// SyncBlocks synchronizes the stores state with the Bucket bucket.
// It will reuse disk space as persistent cache based on s.dir param.
func (s *BucketStore) SyncBlocks(ctx context.Context) error {
	metas, _, metaFetchErr := s.fetcher.Fetch(ctx)
	// For partial view allow adding new blocks at least.
	if metaFetchErr != nil && metas == nil {
		return metaFetchErr
	}

	var wg sync.WaitGroup
	blockc := make(chan *metadata.Meta)

	for i := 0; i < s.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			for meta := range blockc {
				if err := s.addBlock(ctx, meta); err != nil {
					continue
				}
			}
			wg.Done()
		}()
	}

	for id, meta := range metas {
		if b := s.getBlock(id); b != nil {
			continue
		}
		select {
		case <-ctx.Done():
		case blockc <- meta:
		}
	}

	close(blockc)
	wg.Wait()

	if metaFetchErr != nil {
		return metaFetchErr
	}

	// Drop all blocks that are no longer present in the bucket.
	for id := range s.blocks {
		if _, ok := metas[id]; ok {
			continue
		}
		if err := s.removeBlock(id); err != nil {
			level.Warn(s.logger).Log("msg", "drop of outdated block failed", "block", id, "err", err)
			s.metrics.blockDropFailures.Inc()
		}
		level.Info(s.logger).Log("msg", "dropped outdated block", "block", id)
		s.metrics.blockDrops.Inc()
	}

	// Sync advertise labels.
	var storeLabels []storepb.Label
	s.mtx.Lock()
	s.advLabelSets = s.advLabelSets[:0]
	for _, bs := range s.blockSets {
		storeLabels := storeLabels[:0]
		for _, l := range bs.labels {
			storeLabels = append(storeLabels, storepb.Label{Name: l.Name, Value: l.Value})
		}
		s.advLabelSets = append(s.advLabelSets, storepb.LabelSet{Labels: storeLabels})
	}
	sort.Slice(s.advLabelSets, func(i, j int) bool {
		return strings.Compare(s.advLabelSets[i].String(), s.advLabelSets[j].String()) < 0
	})
	s.mtx.Unlock()

	return nil
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (s *BucketStore) InitialSync(ctx context.Context) error {
	if err := s.SyncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}

	names, err := fileutil.ReadDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	for _, n := range names {
		id, ok := block.IsBlockDir(n)
		if !ok {
			continue
		}
		if b := s.getBlock(id); b != nil {
			continue
		}

		// No such block loaded, remove the local dir.
		if err := os.RemoveAll(path.Join(s.dir, id.String())); err != nil {
			level.Warn(s.logger).Log("msg", "failed to remove block which is not needed", "err", err)
		}
	}

	return nil
}

func (s *BucketStore) getBlock(id ulid.ULID) *bucketBlock {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.blocks[id]
}

func (s *BucketStore) addBlock(ctx context.Context, meta *metadata.Meta) (err error) {
	dir := filepath.Join(s.dir, meta.ULID.String())
	start := time.Now()

	level.Debug(s.logger).Log("msg", "loading new block", "id", meta.ULID)
	defer func() {
		if err != nil {
			s.metrics.blockLoadFailures.Inc()
			if err2 := os.RemoveAll(dir); err2 != nil {
				level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
			}
			level.Warn(s.logger).Log("msg", "loading block failed", "elapsed", time.Since(start), "id", meta.ULID, "err", err)
		} else {
			level.Info(s.logger).Log("msg", "loaded new block", "elapsed", time.Since(start), "id", meta.ULID)
		}
	}()
	s.metrics.blockLoads.Inc()

	lset := labels.FromMap(meta.Thanos.Labels)
	h := lset.Hash()

	var indexHeaderReader indexheader.Reader
	if s.enableIndexHeader {
		indexHeaderReader, err = indexheader.NewBinaryReader(ctx, s.logger, s.bkt, s.dir, meta.ULID)
		if err != nil {
			return errors.Wrap(err, "create index header reader")
		}
	} else {
		indexHeaderReader, err = indexheader.NewJSONReader(ctx, s.logger, s.bkt, s.dir, meta.ULID)
		if err != nil {
			return errors.Wrap(err, "create index cache reader")
		}
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, indexHeaderReader, "index-header")
		}
	}()

	b, err := newBucketBlock(
		ctx,
		log.With(s.logger, "block", meta.ULID),
		meta,
		s.bkt,
		dir,
		s.indexCache,
		s.chunkPool,
		indexHeaderReader,
		s.partitioner,
		s.metrics.seriesRefetches,
	)
	if err != nil {
		return errors.Wrap(err, "new bucket block")
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, b, "index-header")
		}
	}()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	sort.Sort(lset)

	set, ok := s.blockSets[h]
	if !ok {
		set = newBucketBlockSet(lset)
		s.blockSets[h] = set
	}

	if err = set.add(b); err != nil {
		return errors.Wrap(err, "add block to set")
	}
	s.blocks[b.meta.ULID] = b

	s.metrics.blocksLoaded.Inc()

	return nil
}

func (s *BucketStore) removeBlock(id ulid.ULID) error {
	s.mtx.Lock()
	b, ok := s.blocks[id]
	if ok {
		lset := labels.FromMap(b.meta.Thanos.Labels)
		s.blockSets[lset.Hash()].remove(id)
		delete(s.blocks, id)
	}
	s.mtx.Unlock()

	if !ok {
		return nil
	}

	s.metrics.blocksLoaded.Dec()
	if err := b.Close(); err != nil {
		return errors.Wrap(err, "close block")
	}
	return os.RemoveAll(b.dir)
}

// TimeRange returns the minimum and maximum timestamp of data available in the store.
func (s *BucketStore) TimeRange() (mint, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	mint = math.MaxInt64
	maxt = math.MinInt64

	for _, b := range s.blocks {
		if b.meta.MinTime < mint {
			mint = b.meta.MinTime
		}
		if b.meta.MaxTime > maxt {
			maxt = b.meta.MaxTime
		}
	}

	mint = s.limitMinTime(mint)
	maxt = s.limitMaxTime(maxt)

	return mint, maxt
}

// Info implements the storepb.StoreServer interface.
func (s *BucketStore) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	mint, maxt := s.TimeRange()
	res := &storepb.InfoResponse{
		StoreType: component.Store.ToProto(),
		MinTime:   mint,
		MaxTime:   maxt,
	}

	s.mtx.RLock()
	// Should we clone?
	res.LabelSets = s.advLabelSets
	s.mtx.RUnlock()

	if s.enableCompatibilityLabel && len(res.LabelSets) > 0 {
		// This is for compatibility with Querier v0.7.0.
		// See query.StoreCompatibilityTypeLabelName comment for details.
		res.LabelSets = append(res.LabelSets, storepb.LabelSet{Labels: []storepb.Label{{Name: CompatibilityTypeLabelName, Value: "store"}}})
	}
	return res, nil
}

func (s *BucketStore) limitMinTime(mint int64) int64 {
	if s.filterConfig == nil {
		return mint
	}

	filterMinTime := s.filterConfig.MinTime.PrometheusTimestamp()

	if mint < filterMinTime {
		return filterMinTime
	}

	return mint
}

func (s *BucketStore) limitMaxTime(maxt int64) int64 {
	if s.filterConfig == nil {
		return maxt
	}

	filterMaxTime := s.filterConfig.MaxTime.PrometheusTimestamp()

	if maxt > filterMaxTime {
		maxt = filterMaxTime
	}

	return maxt
}

type seriesEntry struct {
	lset []storepb.Label
	refs []uint64
	chks []storepb.AggrChunk
}

type bucketSeriesSet struct {
	set []seriesEntry
	i   int
	err error
}

func newBucketSeriesSet(set []seriesEntry) *bucketSeriesSet {
	return &bucketSeriesSet{
		set: set,
		i:   -1,
	}
}

func (s *bucketSeriesSet) Next() bool {
	if s.i >= len(s.set)-1 {
		return false
	}
	s.i++
	return true
}

func (s *bucketSeriesSet) At() ([]storepb.Label, []storepb.AggrChunk) {
	return s.set[s.i].lset, s.set[s.i].chks
}

func (s *bucketSeriesSet) Err() error {
	return s.err
}

func blockSeries(
	extLset map[string]string,
	indexr *bucketIndexReader,
	chunkr *bucketChunkReader,
	matchers []*labels.Matcher,
	req *storepb.SeriesRequest,
	samplesLimiter SampleLimiter,
) (storepb.SeriesSet, *queryStats, error) {
	ps, err := indexr.ExpandedPostings(matchers)
	if err != nil {
		return nil, nil, errors.Wrap(err, "expanded matching posting")
	}

	if len(ps) == 0 {
		return storepb.EmptySeriesSet(), indexr.stats, nil
	}

	// Preload all series index data.
	// TODO(bwplotka): Consider not keeping all series in memory all the time.
	// TODO(bwplotka): Do lazy loading in one step as `ExpandingPostings` method.
	if err := indexr.PreloadSeries(ps); err != nil {
		return nil, nil, errors.Wrap(err, "preload series")
	}

	// Transform all series into the response types and mark their relevant chunks
	// for preloading.
	var (
		res  []seriesEntry
		lset labels.Labels
		chks []chunks.Meta
	)
	for _, id := range ps {
		if err := indexr.LoadedSeries(id, &lset, &chks); err != nil {
			return nil, nil, errors.Wrap(err, "read series")
		}
		s := seriesEntry{
			lset: make([]storepb.Label, 0, len(lset)+len(extLset)),
			refs: make([]uint64, 0, len(chks)),
			chks: make([]storepb.AggrChunk, 0, len(chks)),
		}
		for _, l := range lset {
			// Skip if the external labels of the block overrule the series' label.
			// NOTE(fabxc): maybe move it to a prefixed version to still ensure uniqueness of series?
			if extLset[l.Name] != "" {
				continue
			}
			s.lset = append(s.lset, storepb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		for ln, lv := range extLset {
			s.lset = append(s.lset, storepb.Label{
				Name:  ln,
				Value: lv,
			})
		}
		sort.Slice(s.lset, func(i, j int) bool {
			return s.lset[i].Name < s.lset[j].Name
		})

		for _, meta := range chks {
			if meta.MaxTime < req.MinTime {
				continue
			}
			if meta.MinTime > req.MaxTime {
				break
			}

			if err := chunkr.addPreload(meta.Ref); err != nil {
				return nil, nil, errors.Wrap(err, "add chunk preload")
			}
			s.chks = append(s.chks, storepb.AggrChunk{
				MinTime: meta.MinTime,
				MaxTime: meta.MaxTime,
			})
			s.refs = append(s.refs, meta.Ref)
		}
		if len(s.chks) > 0 {
			res = append(res, s)
		}
	}

	// Preload all chunks that were marked in the previous stage.
	if err := chunkr.preload(samplesLimiter); err != nil {
		return nil, nil, errors.Wrap(err, "preload chunks")
	}

	// Transform all chunks into the response format.
	for _, s := range res {
		for i, ref := range s.refs {
			chk, err := chunkr.Chunk(ref)
			if err != nil {
				return nil, nil, errors.Wrap(err, "get chunk")
			}
			if err := populateChunk(&s.chks[i], chk, req.Aggregates); err != nil {
				return nil, nil, errors.Wrap(err, "populate chunk")
			}
		}
	}

	return newBucketSeriesSet(res), indexr.stats.merge(chunkr.stats), nil
}

func populateChunk(out *storepb.AggrChunk, in chunkenc.Chunk, aggrs []storepb.Aggr) error {
	if in.Encoding() == chunkenc.EncXOR {
		out.Raw = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: in.Bytes()}
		return nil
	}
	if in.Encoding() != downsample.ChunkEncAggr {
		return errors.Errorf("unsupported chunk encoding %d", in.Encoding())
	}

	ac := downsample.AggrChunk(in.Bytes())

	for _, at := range aggrs {
		switch at {
		case storepb.Aggr_COUNT:
			x, err := ac.Get(downsample.AggrCount)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrCount)
			}
			out.Count = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_SUM:
			x, err := ac.Get(downsample.AggrSum)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrSum)
			}
			out.Sum = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_MIN:
			x, err := ac.Get(downsample.AggrMin)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrMin)
			}
			out.Min = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_MAX:
			x, err := ac.Get(downsample.AggrMax)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrMax)
			}
			out.Max = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_COUNTER:
			x, err := ac.Get(downsample.AggrCounter)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrCounter)
			}
			out.Counter = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		}
	}
	return nil
}

// debugFoundBlockSetOverview logs on debug level what exactly blocks we used for query in terms of
// labels and resolution. This is important because we allow mixed resolution results, so it is quite crucial
// to be aware what exactly resolution we see on query.
// TODO(bplotka): Consider adding resolution label to all results to propagate that info to UI and Query API.
func debugFoundBlockSetOverview(logger log.Logger, mint, maxt, maxResolutionMillis int64, lset labels.Labels, bs []*bucketBlock) {
	if len(bs) == 0 {
		level.Debug(logger).Log("msg", "No block found", "mint", mint, "maxt", maxt, "lset", lset.String())
		return
	}

	var (
		parts            []string
		currRes          = int64(-1)
		currMin, currMax int64
	)
	for _, b := range bs {
		if currRes == b.meta.Thanos.Downsample.Resolution {
			currMax = b.meta.MaxTime
			continue
		}

		if currRes != -1 {
			parts = append(parts, fmt.Sprintf("Range: %d-%d Resolution: %d", currMin, currMax, currRes))
		}

		currRes = b.meta.Thanos.Downsample.Resolution
		currMin = b.meta.MinTime
		currMax = b.meta.MaxTime
	}

	parts = append(parts, fmt.Sprintf("Range: %d-%d Resolution: %d", currMin, currMax, currRes))

	level.Debug(logger).Log("msg", "Blocks source resolutions", "blocks", len(bs), "Maximum Resolution", maxResolutionMillis, "mint", mint, "maxt", maxt, "lset", lset.String(), "spans", strings.Join(parts, "\n"))
}

// Series implements the storepb.StoreServer interface.
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	tracing.DoInSpan(srv.Context(), "store_query_gate_ismyturn", func(ctx context.Context) {
		err = s.queryGate.IsMyTurn(srv.Context())
	})
	if err != nil {
		return errors.Wrapf(err, "failed to wait for turn")
	}

	defer s.queryGate.Done()

	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	req.MinTime = s.limitMinTime(req.MinTime)
	req.MaxTime = s.limitMaxTime(req.MaxTime)

	var (
		ctx     = srv.Context()
		stats   = &queryStats{}
		res     []storepb.SeriesSet
		mtx     sync.Mutex
		g, gctx = errgroup.WithContext(ctx)
	)

	s.mtx.RLock()

	for _, bs := range s.blockSets {
		blockMatchers, ok := bs.labelMatchers(matchers...)
		if !ok {
			continue
		}

		blocks := bs.getFor(req.MinTime, req.MaxTime, req.MaxResolutionWindow)

		mtx.Lock()
		stats.blocksQueried += len(blocks)
		mtx.Unlock()

		if s.debugLogging {
			debugFoundBlockSetOverview(s.logger, req.MinTime, req.MaxTime, req.MaxResolutionWindow, bs.labels, blocks)
		}

		for _, b := range blocks {
			b := b

			// We must keep the readers open until all their data has been sent.
			indexr := b.indexReader(gctx)
			chunkr := b.chunkReader(gctx)

			// Defer all closes to the end of Series method.
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "series block")
			defer runutil.CloseWithLogOnErr(s.logger, chunkr, "series block")

			g.Go(func() error {
				part, pstats, err := blockSeries(
					b.meta.Thanos.Labels,
					indexr,
					chunkr,
					blockMatchers,
					req,
					s.samplesLimiter,
				)
				if err != nil {
					return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
				}

				mtx.Lock()
				res = append(res, part)
				stats = stats.merge(pstats)
				mtx.Unlock()

				return nil
			})
		}
	}

	s.mtx.RUnlock()

	defer func() {
		s.metrics.seriesDataTouched.WithLabelValues("postings").Observe(float64(stats.postingsTouched))
		s.metrics.seriesDataFetched.WithLabelValues("postings").Observe(float64(stats.postingsFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("postings").Observe(float64(stats.postingsTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("postings").Observe(float64(stats.postingsFetchedSizeSum))
		s.metrics.seriesDataTouched.WithLabelValues("series").Observe(float64(stats.seriesTouched))
		s.metrics.seriesDataFetched.WithLabelValues("series").Observe(float64(stats.seriesFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("series").Observe(float64(stats.seriesTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("series").Observe(float64(stats.seriesFetchedSizeSum))
		s.metrics.seriesDataTouched.WithLabelValues("chunks").Observe(float64(stats.chunksTouched))
		s.metrics.seriesDataFetched.WithLabelValues("chunks").Observe(float64(stats.chunksFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks").Observe(float64(stats.chunksTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("chunks").Observe(float64(stats.chunksFetchedSizeSum))
		s.metrics.resultSeriesCount.Observe(float64(stats.mergedSeriesCount))

		level.Debug(s.logger).Log("msg", "stats query processed",
			"stats", fmt.Sprintf("%+v", stats), "err", err)
	}()

	// Concurrently get data from all blocks.
	{
		begin := time.Now()
		tracing.DoInSpan(ctx, "bucket_store_preload_all", func(_ context.Context) {
			err = g.Wait()
		})
		if err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
		stats.getAllDuration = time.Since(begin)
		s.metrics.seriesGetAllDuration.Observe(stats.getAllDuration.Seconds())
		s.metrics.seriesBlocksQueried.Observe(float64(stats.blocksQueried))
	}
	// Merge the sub-results from each selected block.
	tracing.DoInSpan(ctx, "bucket_store_merge_all", func(ctx context.Context) {
		begin := time.Now()

		// Merge series set into an union of all block sets. This exposes all blocks are single seriesSet.
		// Chunks of returned series might be out of order w.r.t to their time range.
		// This must be accounted for later by clients.
		set := storepb.MergeSeriesSets(res...)
		for set.Next() {
			var series storepb.Series

			stats.mergedSeriesCount++

			if req.SkipChunks {
				series.Labels, _ = set.At()
			} else {
				series.Labels, series.Chunks = set.At()

				stats.mergedChunksCount += len(series.Chunks)
				s.metrics.chunkSizeBytes.Observe(float64(chunksSize(series.Chunks)))
			}

			if err = srv.Send(storepb.NewSeriesResponse(&series)); err != nil {
				err = status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
				return
			}
		}
		if set.Err() != nil {
			err = status.Error(codes.Unknown, errors.Wrap(set.Err(), "expand series set").Error())
			return
		}
		stats.mergeDuration = time.Since(begin)
		s.metrics.seriesMergeDuration.Observe(stats.mergeDuration.Seconds())

		err = nil
	})
	return err
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

// LabelNames implements the storepb.StoreServer interface.
func (s *BucketStore) LabelNames(ctx context.Context, _ *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string

	for _, b := range s.blocks {
		indexr := b.indexReader(gctx)
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")

			// Do it via index reader to have pending reader registered correctly.
			res := indexr.block.indexHeaderReader.LabelNames()
			sort.Strings(res)

			mtx.Lock()
			sets = append(sets, res)
			mtx.Unlock()

			return nil
		})
	}

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelNamesResponse{
		Names: strutil.MergeSlices(sets...),
	}, nil
}

// LabelValues implements the storepb.StoreServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string

	for _, b := range s.blocks {
		indexr := b.indexReader(gctx)
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label values")

			// Do it via index reader to have pending reader registered correctly.
			res, err := indexr.block.indexHeaderReader.LabelValues(req.Label)
			if err != nil {
				return errors.Wrap(err, "index header label values")
			}

			mtx.Lock()
			sets = append(sets, res)
			mtx.Unlock()

			return nil
		})
	}

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &storepb.LabelValuesResponse{
		Values: strutil.MergeSlices(sets...),
	}, nil
}

// bucketBlockSet holds all blocks of an equal label set. It internally splits
// them up by downsampling resolution and allows querying.
type bucketBlockSet struct {
	labels      labels.Labels
	mtx         sync.RWMutex
	resolutions []int64          // Available resolution, high to low (in milliseconds).
	blocks      [][]*bucketBlock // Ordered buckets for the existing resolutions.
}

// newBucketBlockSet initializes a new set with the known downsampling windows hard-configured.
// The set currently does not support arbitrary ranges.
func newBucketBlockSet(lset labels.Labels) *bucketBlockSet {
	return &bucketBlockSet{
		labels:      lset,
		resolutions: []int64{downsample.ResLevel2, downsample.ResLevel1, downsample.ResLevel0},
		blocks:      make([][]*bucketBlock, 3),
	}
}

func (s *bucketBlockSet) add(b *bucketBlock) error {
	if !labels.Equal(s.labels, labels.FromMap(b.meta.Thanos.Labels)) {
		return errors.New("block's label set does not match set")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	i := int64index(s.resolutions, b.meta.Thanos.Downsample.Resolution)
	if i < 0 {
		return errors.Errorf("unsupported downsampling resolution %d", b.meta.Thanos.Downsample.Resolution)
	}
	bs := append(s.blocks[i], b)
	s.blocks[i] = bs

	// Always sort blocks by min time, then max time.
	sort.Slice(bs, func(j, k int) bool {
		if bs[j].meta.MinTime == bs[k].meta.MinTime {
			return bs[j].meta.MaxTime < bs[k].meta.MaxTime
		}
		return bs[j].meta.MinTime < bs[k].meta.MinTime
	})
	return nil
}

func (s *bucketBlockSet) remove(id ulid.ULID) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i, bs := range s.blocks {
		for j, b := range bs {
			if b.meta.ULID != id {
				continue
			}
			s.blocks[i] = append(bs[:j], bs[j+1:]...)
			return
		}
	}
}

func int64index(s []int64, x int64) int {
	for i, v := range s {
		if v == x {
			return i
		}
	}
	return -1
}

// getFor returns a time-ordered list of blocks that cover date between mint and maxt.
// Blocks with the biggest resolution possible but not bigger than the given max resolution are returned.
// It supports overlapping blocks.
//
// NOTE: s.blocks are expected to be sorted in minTime order.
func (s *bucketBlockSet) getFor(mint, maxt, maxResolutionMillis int64) (bs []*bucketBlock) {
	if mint > maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Find first matching resolution.
	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > maxResolutionMillis; i++ {
	}

	// Fill the given interval with the blocks for the current resolution.
	// Our current resolution might not cover all data, so recursively fill the gaps with higher resolution blocks
	// if there is any.
	start := mint
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		// NOTE: Block intervals are half-open: [b.MinTime, b.MaxTime).
		if b.meta.MinTime > maxt {
			break
		}

		if i+1 < len(s.resolutions) {
			bs = append(bs, s.getFor(start, b.meta.MinTime-1, s.resolutions[i+1])...)
		}
		bs = append(bs, b)

		start = b.meta.MaxTime
	}

	if i+1 < len(s.resolutions) {
		bs = append(bs, s.getFor(start, maxt, s.resolutions[i+1])...)
	}
	return bs
}

// labelMatchers verifies whether the block set matches the given matchers and returns a new
// set of matchers that is equivalent when querying data within the block.
func (s *bucketBlockSet) labelMatchers(matchers ...*labels.Matcher) ([]*labels.Matcher, bool) {
	res := make([]*labels.Matcher, 0, len(matchers))

	for _, m := range matchers {
		v := s.labels.Get(m.Name)
		if v == "" {
			res = append(res, m)
			continue
		}
		if !m.Matches(v) {
			return nil, false
		}
	}
	return res, true
}

// bucketBlock represents a block that is located in a bucket. It holds intermediate
// state for the block on local disk.
type bucketBlock struct {
	logger     log.Logger
	bkt        objstore.BucketReader
	meta       *metadata.Meta
	dir        string
	indexCache storecache.IndexCache
	chunkPool  pool.BytesPool

	indexHeaderReader indexheader.Reader

	chunkObjs []string

	pendingReaders sync.WaitGroup

	partitioner partitioner

	seriesRefetches prometheus.Counter
}

func newBucketBlock(
	ctx context.Context,
	logger log.Logger,
	meta *metadata.Meta,
	bkt objstore.BucketReader,
	dir string,
	indexCache storecache.IndexCache,
	chunkPool pool.BytesPool,
	indexHeadReader indexheader.Reader,
	p partitioner,
	seriesRefetches prometheus.Counter,
) (b *bucketBlock, err error) {
	b = &bucketBlock{
		logger:            logger,
		bkt:               bkt,
		indexCache:        indexCache,
		chunkPool:         chunkPool,
		dir:               dir,
		partitioner:       p,
		meta:              meta,
		indexHeaderReader: indexHeadReader,
		seriesRefetches:   seriesRefetches,
	}

	// Get object handles for all chunk files.
	if err = bkt.Iter(ctx, path.Join(meta.ULID.String(), block.ChunksDirname), func(n string) error {
		b.chunkObjs = append(b.chunkObjs, n)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "list chunk files")
	}
	return b, nil
}

func (b *bucketBlock) indexFilename() string {
	return path.Join(b.meta.ULID.String(), block.IndexFilename)
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.bkt.GetRange(ctx, b.indexFilename(), off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readIndexRange close range reader")

	c, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return c, nil
}

func (b *bucketBlock) readChunkRange(ctx context.Context, seq int, off, length int64) (*[]byte, error) {
	c, err := b.chunkPool.Get(int(length))
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}
	buf := bytes.NewBuffer(*c)

	r, err := b.bkt.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		b.chunkPool.Put(c)
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readChunkRange close range reader")

	if _, err = io.Copy(buf, r); err != nil {
		b.chunkPool.Put(c)
		return nil, errors.Wrap(err, "read range")
	}
	internalBuf := buf.Bytes()
	return &internalBuf, nil
}

func (b *bucketBlock) indexReader(ctx context.Context) *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(ctx, b)
}

func (b *bucketBlock) chunkReader(ctx context.Context) *bucketChunkReader {
	b.pendingReaders.Add(1)
	return newBucketChunkReader(ctx, b)
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *bucketBlock) Close() error {
	b.pendingReaders.Wait()
	return b.indexHeaderReader.Close()
}

// bucketIndexReader is a custom index reader (not conforming index.Reader interface) that reads index that is stored in
// object storage without having to fully download it.
type bucketIndexReader struct {
	ctx   context.Context
	block *bucketBlock
	dec   *index.Decoder
	stats *queryStats

	mtx          sync.Mutex
	loadedSeries map[uint64][]byte
}

func newBucketIndexReader(ctx context.Context, block *bucketBlock) *bucketIndexReader {
	r := &bucketIndexReader{
		ctx:   ctx,
		block: block,
		dec: &index.Decoder{
			LookupSymbol: block.indexHeaderReader.LookupSymbol,
		},
		stats:        &queryStats{},
		loadedSeries: map[uint64][]byte{},
	}
	return r
}

// ExpandedPostings returns postings in expanded list instead of index.Postings.
// This is because we need to have them buffered anyway to perform efficient lookup
// on object storage.
// Found posting IDs (ps) are not strictly required to point to a valid Series, e.g. during
// background garbage collections.
//
// Reminder: A posting is a reference (represented as a uint64) to a series reference, which in turn points to the first
// chunk where the series contains the matching label-value pair for a given block of data. Postings can be fetched by
// single label name=value.
func (r *bucketIndexReader) ExpandedPostings(ms []*labels.Matcher) ([]uint64, error) {
	var postingGroups []*postingGroup

	// NOTE: Derived from tsdb.PostingsForMatchers.
	for _, m := range ms {
		// Each group is separate to tell later what postings are intersecting with what.
		pg, err := toPostingGroup(r.block.indexHeaderReader.LabelValues, m)
		if err != nil {
			return nil, errors.Wrap(err, "toPostingGroup")
		}

		postingGroups = append(postingGroups, pg)
	}

	if len(postingGroups) == 0 {
		return nil, nil
	}

	if err := r.fetchPostings(postingGroups); err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	var postings []index.Postings
	for _, g := range postingGroups {
		postings = append(postings, g.Postings())
	}

	ps, err := index.ExpandPostings(index.Intersect(postings...))
	if err != nil {
		return nil, errors.Wrap(err, "expand")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	if r.block.indexHeaderReader.IndexVersion() >= 2 {
		for i, id := range ps {
			ps[i] = id * 16
		}
	}

	return ps, nil
}

type postingGroup struct {
	keys     labels.Labels
	postings []index.Postings

	aggregate func(postings []index.Postings) index.Postings
}

func newPostingGroup(keys labels.Labels, aggr func(postings []index.Postings) index.Postings) *postingGroup {
	return &postingGroup{
		keys:      keys,
		postings:  make([]index.Postings, len(keys)),
		aggregate: aggr,
	}
}

func (p *postingGroup) Fill(i int, posting index.Postings) {
	p.postings[i] = posting
}

func (p *postingGroup) Postings() index.Postings {
	if len(p.keys) == 0 {
		return index.EmptyPostings()
	}

	for i, posting := range p.postings {
		if posting == nil {
			// This should not happen. Debug for https://github.com/thanos-io/thanos/issues/874.
			return index.ErrPostings(errors.Errorf("at least one of %d postings is nil for %s. It was never fetched.", i, p.keys[i]))
		}
	}

	return p.aggregate(p.postings)
}

func merge(p []index.Postings) index.Postings {
	return index.Merge(p...)
}

func allWithout(p []index.Postings) index.Postings {
	return index.Without(p[0], index.Merge(p[1:]...))
}

// NOTE: Derived from tsdb.postingsForMatcher. index.Merge is equivalent to map duplication.
func toPostingGroup(lvalsFn func(name string) ([]string, error), m *labels.Matcher) (*postingGroup, error) {
	var matchingLabels labels.Labels

	// If the matcher selects an empty value, it selects all the series which don't
	// have the label name set too. See: https://github.com/prometheus/prometheus/issues/3575
	// and https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555.
	if m.Matches("") {
		allName, allValue := index.AllPostingsKey()

		matchingLabels = append(matchingLabels, labels.Label{Name: allName, Value: allValue})
		vals, err := lvalsFn(m.Name)
		if err != nil {
			return nil, err
		}
		for _, val := range vals {
			if !m.Matches(val) {
				matchingLabels = append(matchingLabels, labels.Label{Name: m.Name, Value: val})
			}
		}

		if len(matchingLabels) == 1 {
			// This is known hack to return all series.
			// Ask for x != <not existing value>. Allow for that as Prometheus does,
			// even though it is expensive.
			return newPostingGroup(matchingLabels, merge), nil
		}

		return newPostingGroup(matchingLabels, allWithout), nil
	}

	// Fast-path for equal matching.
	if m.Type == labels.MatchEqual {
		return newPostingGroup(labels.Labels{{Name: m.Name, Value: m.Value}}, merge), nil
	}

	vals, err := lvalsFn(m.Name)
	if err != nil {
		return nil, err
	}

	for _, val := range vals {
		if m.Matches(val) {
			matchingLabels = append(matchingLabels, labels.Label{Name: m.Name, Value: val})
		}
	}

	return newPostingGroup(matchingLabels, merge), nil
}

type postingPtr struct {
	groupID int
	keyID   int
	ptr     index.Range
}

// fetchPostings fill postings requested by posting groups.
func (r *bucketIndexReader) fetchPostings(groups []*postingGroup) error {
	var ptrs []postingPtr

	// Fetch postings from the cache with a single call.
	keys := make([]labels.Label, 0)
	for _, g := range groups {
		keys = append(keys, g.keys...)
	}

	fromCache, _ := r.block.indexCache.FetchMultiPostings(r.ctx, r.block.meta.ULID, keys)

	// Iterate over all groups and fetch posting from cache.
	// If we have a miss, mark key to be fetched in `ptrs` slice.
	// Overlaps are well handled by partitioner, so we don't need to deduplicate keys.
	for i, g := range groups {
		for j, key := range g.keys {
			// Get postings for the given key from cache first.
			if b, ok := fromCache[key]; ok {
				r.stats.postingsTouched++
				r.stats.postingsTouchedSizeSum += len(b)

				_, l, err := r.dec.Postings(b)
				if err != nil {
					return errors.Wrap(err, "decode postings")
				}

				g.Fill(j, l)
				continue
			}

			// Cache miss; save pointer for actual posting in index stored in object store.
			ptr, err := r.block.indexHeaderReader.PostingsOffset(key.Name, key.Value)
			if err == indexheader.NotFoundRangeErr {
				// This block does not have any posting for given key.
				g.Fill(j, index.EmptyPostings())
				continue
			}

			if err != nil {
				return errors.Wrap(err, "index header PostingsOffset")
			}

			r.stats.postingsToFetch++
			ptrs = append(ptrs, postingPtr{ptr: ptr, groupID: i, keyID: j})
		}
	}

	sort.Slice(ptrs, func(i, j int) bool {
		return ptrs[i].ptr.Start < ptrs[j].ptr.Start
	})

	// TODO(bwplotka): Asses how large in worst case scenario this can be. (e.g fetch for AllPostingsKeys)
	// Consider sub split if too big.
	parts := r.block.partitioner.Partition(len(ptrs), func(i int) (start, end uint64) {
		return uint64(ptrs[i].ptr.Start), uint64(ptrs[i].ptr.End)
	})

	g, ctx := errgroup.WithContext(r.ctx)
	for _, part := range parts {
		i, j := part.elemRng[0], part.elemRng[1]

		start := int64(part.start)
		// We assume index does not have any ptrs that has 0 length.
		length := int64(part.end) - start

		// Fetch from object storage concurrently and update stats and posting list.
		g.Go(func() error {
			begin := time.Now()

			b, err := r.block.readIndexRange(ctx, start, length)
			if err != nil {
				return errors.Wrap(err, "read postings range")
			}
			fetchTime := time.Since(begin)

			r.mtx.Lock()
			r.stats.postingsFetchCount++
			r.stats.postingsFetched += j - i
			r.stats.postingsFetchDurationSum += fetchTime
			r.stats.postingsFetchedSizeSum += int(length)
			r.mtx.Unlock()

			for _, p := range ptrs[i:j] {
				// index-header can estimate endings, which means we need to resize the endings.
				pBytes, err := resizePostings(b[p.ptr.Start-start : p.ptr.End-start])
				if err != nil {
					return err
				}

				r.mtx.Lock()
				// Return postings and fill LRU cache.
				// Truncate first 4 bytes which are length of posting.
				groups[p.groupID].Fill(p.keyID, newBigEndianPostings(pBytes[4:]))
				r.block.indexCache.StorePostings(r.ctx, r.block.meta.ULID, groups[p.groupID].keys[p.keyID], pBytes)

				// If we just fetched it we still have to update the stats for touched postings.
				r.stats.postingsTouched++
				r.stats.postingsTouchedSizeSum += len(pBytes)
				r.mtx.Unlock()
			}
			return nil
		})
	}

	return g.Wait()
}

func resizePostings(b []byte) ([]byte, error) {
	d := encoding.Decbuf{B: b}
	n := d.Be32int()
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read postings list")
	}

	// 4 for postings number of entries, then 4, foreach each big endian posting.
	size := 4 + n*4
	if len(b) < size {
		return nil, encoding.ErrInvalidSize
	}
	return b[:size], nil
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	list []byte
	cur  uint32
}

// TODO(bwplotka): Expose those inside Prometheus.
func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() uint64 {
	return uint64(it.cur)
}

func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = binary.BigEndian.Uint32(it.list)
		it.list = it.list[4:]
		return true
	}
	return false
}

func (it *bigEndianPostings) Seek(x uint64) bool {
	if uint64(it.cur) >= x {
		return true
	}

	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= uint32(x)
	})
	if i < num {
		j := i * 4
		it.cur = binary.BigEndian.Uint32(it.list[j:])
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

func (r *bucketIndexReader) PreloadSeries(ids []uint64) error {
	// Load series from cache, overwriting the list of ids to preload
	// with the missing ones.
	fromCache, ids := r.block.indexCache.FetchMultiSeries(r.ctx, r.block.meta.ULID, ids)
	for id, b := range fromCache {
		r.loadedSeries[id] = b
	}

	parts := r.block.partitioner.Partition(len(ids), func(i int) (start, end uint64) {
		return ids[i], ids[i] + maxSeriesSize
	})
	g, ctx := errgroup.WithContext(r.ctx)
	for _, p := range parts {
		s, e := p.start, p.end
		i, j := p.elemRng[0], p.elemRng[1]

		g.Go(func() error {
			return r.loadSeries(ctx, ids[i:j], false, s, e)
		})
	}
	return g.Wait()
}

func (r *bucketIndexReader) loadSeries(ctx context.Context, ids []uint64, refetch bool, start, end uint64) error {
	begin := time.Now()

	b, err := r.block.readIndexRange(ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "read series range")
	}

	r.mtx.Lock()
	r.stats.seriesFetchCount++
	r.stats.seriesFetched += len(ids)
	r.stats.seriesFetchDurationSum += time.Since(begin)
	r.stats.seriesFetchedSizeSum += int(end - start)
	r.mtx.Unlock()

	for i, id := range ids {
		c := b[id-start:]

		l, n := binary.Uvarint(c)
		if n < 1 {
			return errors.New("reading series length failed")
		}
		if len(c) < n+int(l) {
			if i == 0 && refetch {
				return errors.Errorf("invalid remaining size, even after refetch, remaining: %d, expected %d", len(c), n+int(l))
			}

			// Inefficient, but should be rare.
			r.block.seriesRefetches.Inc()
			level.Warn(r.block.logger).Log("msg", "series size exceeded expected size; refetching", "id", id, "series length", n+int(l), "maxSeriesSize", maxSeriesSize)

			// Fetch plus to get the size of next one if exists.
			return r.loadSeries(ctx, ids[i:], true, id, id+uint64(n+int(l)+1))
		}
		c = c[n : n+int(l)]
		r.mtx.Lock()
		r.loadedSeries[id] = c
		r.block.indexCache.StoreSeries(r.ctx, r.block.meta.ULID, id, c)
		r.mtx.Unlock()
	}
	return nil
}

type part struct {
	start uint64
	end   uint64

	elemRng [2]int
}

type partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be ted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []part
}

type gapBasedPartitioner struct {
	maxGapSize uint64
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := part{}
		p.start, p.end = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.end+g.maxGapSize < s {
				break
			}

			if p.end <= e {
				p.end = e
			}
		}
		p.elemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}

// LoadedSeries populates the given labels and chunk metas for the series identified
// by the reference.
// Returns ErrNotFound if the ref does not resolve to a known series.
func (r *bucketIndexReader) LoadedSeries(ref uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	b, ok := r.loadedSeries[ref]
	if !ok {
		return errors.Errorf("series %d not found", ref)
	}

	r.stats.seriesTouched++
	r.stats.seriesTouchedSizeSum += len(b)

	return r.dec.Series(b, lset, chks)
}

// Close released the underlying resources of the reader.
func (r *bucketIndexReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

type bucketChunkReader struct {
	ctx   context.Context
	block *bucketBlock
	stats *queryStats

	preloads [][]uint32
	mtx      sync.Mutex
	chunks   map[uint64]chunkenc.Chunk

	// Byte slice to return to the chunk pool on close.
	chunkBytes []*[]byte
}

func newBucketChunkReader(ctx context.Context, block *bucketBlock) *bucketChunkReader {
	return &bucketChunkReader{
		ctx:      ctx,
		block:    block,
		stats:    &queryStats{},
		preloads: make([][]uint32, len(block.chunkObjs)),
		chunks:   map[uint64]chunkenc.Chunk{},
	}
}

// addPreload adds the chunk with id to the data set that will be fetched on calling preload.
func (r *bucketChunkReader) addPreload(id uint64) error {
	var (
		seq = int(id >> 32)
		off = uint32(id)
	)
	if seq >= len(r.preloads) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.preloads[seq] = append(r.preloads[seq], off)
	return nil
}

// preload all added chunk IDs. Must be called before the first call to Chunk is made.
func (r *bucketChunkReader) preload(samplesLimiter SampleLimiter) error {
	g, ctx := errgroup.WithContext(r.ctx)

	numChunks := uint64(0)
	for _, offsets := range r.preloads {
		for range offsets {
			numChunks++
		}
	}
	if err := samplesLimiter.Check(numChunks * maxSamplesPerChunk); err != nil {
		return errors.Wrap(err, "exceeded samples limit")
	}

	for seq, offsets := range r.preloads {
		sort.Slice(offsets, func(i, j int) bool {
			return offsets[i] < offsets[j]
		})
		parts := r.block.partitioner.Partition(len(offsets), func(i int) (start, end uint64) {
			return uint64(offsets[i]), uint64(offsets[i]) + maxChunkSize
		})

		seq := seq
		offsets := offsets

		for _, p := range parts {
			s, e := uint32(p.start), uint32(p.end)
			m, n := p.elemRng[0], p.elemRng[1]

			g.Go(func() error {
				return r.loadChunks(ctx, offsets[m:n], seq, s, e)
			})
		}
	}
	return g.Wait()
}

func (r *bucketChunkReader) loadChunks(ctx context.Context, offs []uint32, seq int, start, end uint32) error {
	begin := time.Now()

	b, err := r.block.readChunkRange(ctx, seq, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrapf(err, "read range for %d", seq)
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.chunkBytes = append(r.chunkBytes, b)
	r.stats.chunksFetchCount++
	r.stats.chunksFetched += len(offs)
	r.stats.chunksFetchDurationSum += time.Since(begin)
	r.stats.chunksFetchedSizeSum += int(end - start)

	for _, o := range offs {
		cb := (*b)[o-start:]

		l, n := binary.Uvarint(cb)
		if n < 1 {
			return errors.Errorf("reading chunk length failed")
		}
		if len(cb) < n+int(l)+1 {
			return errors.Errorf("preloaded chunk too small, expecting %d", n+int(l)+1)
		}
		cid := uint64(seq<<32) | uint64(o)
		r.chunks[cid] = rawChunk(cb[n : n+int(l)+1])
	}
	return nil
}

func (r *bucketChunkReader) Chunk(id uint64) (chunkenc.Chunk, error) {
	c, ok := r.chunks[id]
	if !ok {
		return nil, errors.Errorf("chunk with ID %d not found", id)
	}

	r.stats.chunksTouched++
	r.stats.chunksTouchedSizeSum += len(c.Bytes())

	return c, nil
}

// rawChunk is a helper type that wraps a chunk's raw bytes and implements the chunkenc.Chunk
// interface over it.
// It is used to Store API responses which don't need to introspect and validate the chunk's contents.
type rawChunk []byte

func (b rawChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding(b[0])
}

func (b rawChunk) Bytes() []byte {
	return b[1:]
}

func (b rawChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	panic("invalid call")
}

func (b rawChunk) Appender() (chunkenc.Appender, error) {
	panic("invalid call")
}

func (b rawChunk) NumSamples() int {
	panic("invalid call")
}

func (r *bucketChunkReader) Close() error {
	r.block.pendingReaders.Done()

	for _, b := range r.chunkBytes {
		r.block.chunkPool.Put(b)
	}
	return nil
}

type queryStats struct {
	blocksQueried int

	postingsTouched          int
	postingsTouchedSizeSum   int
	postingsToFetch          int
	postingsFetched          int
	postingsFetchedSizeSum   int
	postingsFetchCount       int
	postingsFetchDurationSum time.Duration

	seriesTouched          int
	seriesTouchedSizeSum   int
	seriesFetched          int
	seriesFetchedSizeSum   int
	seriesFetchCount       int
	seriesFetchDurationSum time.Duration

	chunksTouched          int
	chunksTouchedSizeSum   int
	chunksFetched          int
	chunksFetchedSizeSum   int
	chunksFetchCount       int
	chunksFetchDurationSum time.Duration

	getAllDuration    time.Duration
	mergedSeriesCount int
	mergedChunksCount int
	mergeDuration     time.Duration
}

func (s queryStats) merge(o *queryStats) *queryStats {
	s.blocksQueried += o.blocksQueried

	s.postingsTouched += o.postingsTouched
	s.postingsTouchedSizeSum += o.postingsTouchedSizeSum
	s.postingsFetched += o.postingsFetched
	s.postingsFetchedSizeSum += o.postingsFetchedSizeSum
	s.postingsFetchCount += o.postingsFetchCount
	s.postingsFetchDurationSum += o.postingsFetchDurationSum

	s.seriesTouched += o.seriesTouched
	s.seriesTouchedSizeSum += o.seriesTouchedSizeSum
	s.seriesFetched += o.seriesFetched
	s.seriesFetchedSizeSum += o.seriesFetchedSizeSum
	s.seriesFetchCount += o.seriesFetchCount
	s.seriesFetchDurationSum += o.seriesFetchDurationSum

	s.chunksTouched += o.chunksTouched
	s.chunksTouchedSizeSum += o.chunksTouchedSizeSum
	s.chunksFetched += o.chunksFetched
	s.chunksFetchedSizeSum += o.chunksFetchedSizeSum
	s.chunksFetchCount += o.chunksFetchCount
	s.chunksFetchDurationSum += o.chunksFetchDurationSum

	s.getAllDuration += o.getAllDuration
	s.mergedSeriesCount += o.mergedSeriesCount
	s.mergedChunksCount += o.mergedChunksCount
	s.mergeDuration += o.mergeDuration

	return &s
}
