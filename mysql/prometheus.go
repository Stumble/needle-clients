package mysql

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type metricSet struct {
	ConnPool *prometheus.GaugeVec
	Request  *prometheus.CounterVec
	Latency  *prometheus.HistogramVec
}

var (
	labels        = []string{"name"}
	latencyBucket = []float64{
		4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}
	connPoolUpdateInterval = 1 * time.Second
)

func newMetricSet(appName string) *metricSet {
	return &metricSet{
		ConnPool: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: fmt.Sprintf("%s_mysql_conn_pool", appName),
				Help: "",
			}, labels),
		Request: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_mysql_request_total", appName),
				Help: "how many CRUD operations sent to DB.",
			}, labels),
		Latency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    fmt.Sprintf("%s_mysql_latency_milliseconds", appName),
				Help:    "CRUD latency in milliseconds",
				Buckets: latencyBucket,
			}, labels),
	}
}
func (m *metricSet) Register() {
	err := prometheus.Register(m.ConnPool)
	if err != nil {
		log.Err(err).Msgf("failed to register Prometheus ConnPool gauges")
	}
	err = prometheus.Register(m.Request)
	if err != nil {
		log.Err(err).Msgf("failed to register prometheus Request counters")
	}
	err = prometheus.Register(m.Latency)
	if err != nil {
		log.Err(err).Msgf("failed to register Prometheus Latency histogram")
	}
}

func (m *metricSet) Unregister() {
	prometheus.Unregister(m.ConnPool)
	prometheus.Unregister(m.Request)
	prometheus.Unregister(m.Latency)
}

func (s *metricSet) Observe(name string, startedAt time.Time) func() {
	return func() {
		if s.Request != nil {
			s.Request.WithLabelValues(name).Inc()
		}
		if s.Latency != nil {
			s.Latency.WithLabelValues(name).Observe(
				float64(time.Since(startedAt).Milliseconds()))
		}
	}
}
