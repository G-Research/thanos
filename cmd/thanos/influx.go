package main

import (
	"context"
	"math"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerInfluxSidecar(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for InfluxDB server")

	grpcBindAddr, httpBindAddr, newPeerFn := regCommonServerFlags(cmd)

	externalLabels := cmd.Flag("influxdb.labels", "Labels to expose for the fronted InfluxDB instance, of the form <key=value>").Strings()

	influxURL := cmd.Flag("influxdb.url", "URL at which to reach InfluxDB's API.").
		Default("http://localhost:8086").URL()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {

		parsedLabels, err := parseLabelArgs(externalLabels)
		if err != nil {
			return err
		}

		peer, err := newPeerFn(logger, reg, false, "", false)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runInfluxSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*httpBindAddr,
			*influxURL,
			parsedLabels,
			peer,
			name,
		)
	}
}

func parseLabelArgs(externalLabels *[]string) (labels.Labels, error) {
	l := make([]labels.Label, 0, len(*externalLabels))
	for _, externalLabel := range *externalLabels {
		splitLabel := strings.Split(externalLabel, "=")
		if len(splitLabel) != 2 {
			return nil, errors.New("Label not of form key=value: " + externalLabel)
		}
		l = append(l, labels.Label{Name: splitLabel[0], Value: splitLabel[1]})
	}
	return labels.New(l...), nil
}

func runInfluxSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	httpBindAddr string,
	influxURL *url.URL,
	influxLabels labels.Labels,
	peer *cluster.Peer,
	component string,
) error {

	var metadata = &influxMetadata{
		influxURL: influxURL,

		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		labels: influxLabels,
		mint:   0,
		maxt:   math.MaxInt64,
	}

	// Setup all the concurrent groups.
	{
		promUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_influxdb_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its InfluxDB peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_influxdb_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		reg.MustRegister(promUp, lastHeartbeat)

		if len(metadata.Labels()) == 0 {
			return errors.New("no external labels configured for InfluxDB server, uniquely identifying external labels must be configured")
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				if err := metadata.Ping(ctx); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to connect. Is InfluxDB running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					return err
				}

				promUp.Set(1)
				lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			// New gossip cluster.
			mint, maxt := metadata.Timestamps()
			if err = peer.Join(cluster.PeerTypeSource, cluster.PeerMetadata{
				Labels:  metadata.LabelsPB(),
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

				if err := metadata.Ping(iterCtx); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					promUp.Set(1)
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
		context.WithCancel(context.Background())
	}
	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}
	{
		l, err := net.Listen("tcp", grpcBindAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "store")

		var client http.Client

		promStore, err := store.NewPrometheusStore(
			logger, &client, influxURL, metadata.Labels, metadata.Timestamps)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, promStore)

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}

	level.Info(logger).Log("msg", "starting sidecar", "peer", peer.Name())
	return nil
}

type influxMetadata struct {
	influxURL *url.URL

	mtx    sync.Mutex
	mint   int64
	maxt   int64
	labels labels.Labels
}

func (s *influxMetadata) UpdateTimestamps(mint int64, maxt int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.mint = mint
	s.maxt = maxt
	return nil
}

func (s *influxMetadata) Ping(ctx context.Context) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// todo: ping url not working
	return nil
}

func (s *influxMetadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *influxMetadata) LabelsPB() []storepb.Label {
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

func (s *influxMetadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.mint, s.maxt
}
