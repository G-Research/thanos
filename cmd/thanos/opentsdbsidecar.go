package main

import (
	"context"
	"math"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/reloader"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerOpenTSDBSideCar(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for opentsdb server")

	grpcBindAddr, httpBindAddr, cert, key, clientCA, newPeerFn := regCommonServerFlags(cmd)

	opentTSDBURL := cmd.Flag("opentsdb.url", "URL at which to reach OpenTSDB's API. For better performance use local network.").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "Config file watched by the reloader.").Default("").String()

	reloaderCfgOutputFile := cmd.Flag("reloader.config-envsubst-file", "Output file for environment variable substituted config file.").Default("").String()

	reloaderRuleDirs := cmd.Flag("reloader.rule-dir", "Rule directories for the reloader to refresh (repeated field).").Strings()

	objStoreConfig := regCommonObjStoreFlags(cmd, "")

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {

		rl := reloader.New(
			log.With(logger, "component", "reloader"),
			reloader.ReloadURLFromBase(*opentTSDBURL),
			*reloaderCfgFile,
			*reloaderCfgOutputFile,
			*reloaderRuleDirs,
		)
		peer, err := newPeerFn(logger, reg, false, "", false)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runOpenTSDBSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*cert,
			*key,
			*clientCA,
			*httpBindAddr,
			*opentTSDBURL,
			*dataDir,
			objStoreConfig,
			peer,
			rl,
			name,
		)
	}
}

func runOpenTSDBSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	cert string,
	key string,
	clientCA string,
	httpBindAddr string,
	openTSDBURL *url.URL,
	dataDir string,
	objStoreConfig *pathOrContent,
	peer cluster.Peer,
	reloader *reloader.Reloader,
	component string,
) error {
	var metadata = &opentTSDBMetadata{
		logger:      logger,
		openTSDBURL: openTSDBURL,
		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: 0,
		maxt: math.MaxInt64,
		//openTSDBClient: opentsdbclient.NewOpenTSDBClient(openTSDBURL.String()),
	}
	// Setup all the concurrent groups.
	{
		tsdbUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		reg.MustRegister(tsdbUp, lastHeartbeat)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				if err := metadata.UpdateLabels(ctx, logger); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					tsdbUp.Set(0)
					return err
				}

				tsdbUp.Set(1)
				lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(metadata.Labels()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured")
			}

			// New gossip cluster.
			mint, maxt := metadata.Timestamps()
			if err = peer.Join(cluster.PeerTypeSource, cluster.PeerMetadata{
				Labels:  metadata.LabelsPB(logger),
				MinTime: mint,
				MaxTime: maxt,
			}); err != nil {
				return errors.Wrap(err, "join cluster")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				if err := metadata.UpdateLabels(iterCtx, logger); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					tsdbUp.Set(0)
				} else {
					// Update gossip.
					peer.SetLabels(metadata.LabelsPB(logger))

					tsdbUp.Set(1)
					lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				}

				return nil
			})
		}, func(error) {
			cancel()
			peer.Close(2 * time.Second)
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return reloader.Watch(ctx)
		}, func(error) {
			cancel()
		})
	}
	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}
	{
		l, err := net.Listen("tcp", grpcBindAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "opentsdbsidecar")
		opentTSDBStore, err := store.NewOpenTSDBStore(metadata.openTSDBURL.String(),
			logger,
			metadata.Timestamps, metadata.Labels())
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		opts, err := defaultGRPCServerOpts(logger, reg, tracer, cert, key, clientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}
		s := grpc.NewServer(opts...)
		storepb.RegisterStoreServer(s, opentTSDBStore)

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
		})
	}
	level.Info(logger).Log("msg", "starting sidecar", "peer", peer.Name())
	return nil

}

type opentTSDBMetadata struct {
	openTSDBURL *url.URL
	logger      log.Logger
	mtx         sync.Mutex
	mint        int64
	maxt        int64
	labels      labels.Labels
}

func (s *opentTSDBMetadata) UpdateLabels(ctx context.Context, logger log.Logger) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labels = labels.Labels{
		labels.Label{
			Value: "host",
			Name:  s.openTSDBURL.String(),
		},
	}
	return nil
}

func (s *opentTSDBMetadata) UpdateTimestamps(mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.mint = mint
	s.maxt = maxt
}

func (s *opentTSDBMetadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.labels
}

func (s *opentTSDBMetadata) LabelsPB(logger log.Logger) []storepb.Label {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := make([]storepb.Label, 0, len(s.labels))
	for _, l := range s.labels {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return lset
}

func (s *opentTSDBMetadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.mint, s.maxt
}
