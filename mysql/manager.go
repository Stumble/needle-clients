package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

// Manager wraps the underlying *sql.DB	to provide a uniformed interface of getting
// a simple or transactional SQL executor.
type Manager interface {
	Ping() error
	Close() error
	GetConn() *sql.DB

	GetExec() DBExecuter
	Transact(txFunc func(DBExecuter) (interface{}, error)) (resp interface{}, err error)
}

type manager struct {
	conn  *sql.DB
	gauge *prometheus.GaugeVec
	exec  DBExecuter

	counter      *prometheus.CounterVec
	histogram    *prometheus.HistogramVec
	statementMap *sync.Map
}

const (
	updateInterval = time.Second * 1
)

func newManagerWithMetrics(config *Config, gauge *prometheus.GaugeVec, execCounter *prometheus.CounterVec, execHistogram *prometheus.HistogramVec) (Manager, error) {
	// setup database
	dsn := fmt.Sprintf("%s:%s@tcp([%s]:%d)/%s?charset=utf8mb4&parseTime=true",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.DBName)
	conn, e := sql.Open("mysql", dsn)
	if e != nil {
		log.Fatal().Err(e).Msg("failed to connect to sql")
	}
	conn.SetMaxOpenConns(config.MaxOpenConns)
	conn.SetMaxIdleConns(config.MaxIdleConns)
	conn.SetConnMaxLifetime(config.MaxLifetime)
	statementMap := &sync.Map{}
	manager := &manager{
		conn:         conn,
		counter:      execCounter,
		histogram:    execHistogram,
		statementMap: statementMap,
		exec: &simpleDBExecuter{
			conn:         conn,
			counter:      execCounter,
			histogram:    execHistogram,
			statementMap: statementMap,
		},
	}

	if gauge != nil {
		manager.gauge = gauge
		go manager.updateMetrics()
	}

	return manager, nil
}

func (m *manager) updateMetrics() {
	for {
		stats := m.conn.Stats()
		m.gauge.WithLabelValues("MaxOpenConnections").Set(float64(stats.MaxOpenConnections))
		m.gauge.WithLabelValues("Idle").Set(float64(stats.Idle))
		m.gauge.WithLabelValues("OpenConnections").Set(float64(stats.OpenConnections))
		m.gauge.WithLabelValues("InUse").Set(float64(stats.InUse))
		m.gauge.WithLabelValues("WaitCount").Set(float64(stats.WaitCount))
		m.gauge.WithLabelValues("WaitDuration").Set(float64(stats.WaitDuration))
		m.gauge.WithLabelValues("MaxIdleClosed").Set(float64(stats.MaxIdleClosed))
		m.gauge.WithLabelValues("MaxLifetimeClosed").Set(float64(stats.MaxLifetimeClosed))
		time.Sleep(updateInterval)
	}
}

func (m *manager) GetExec() DBExecuter {
	return m.exec
}

// Transact is a wrapper that wraps around transaction
func (m *manager) Transact(txFunc func(DBExecuter) (interface{}, error)) (resp interface{}, err error) {
	tx, e := m.conn.Begin()
	if e != nil {
		return nil, e
	}

	txExec := &txDBExecuter{
		tx:           tx,
		counter:      m.counter,
		histogram:    m.histogram,
		statementMap: m.statementMap,
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			e := tx.Commit()
			if e != nil {
				err = e
			} else {
				txExec.runInvalidateFuncs()
			}
		}
	}()
	resp, err = txFunc(txExec)
	return resp, err
}

func (m *manager) Close() error {
	return m.conn.Close()
}

func (m *manager) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	return m.conn.PingContext(ctx)
}

// PrepareStmts will attempt to prepare each unprepared query on the database.
// If one fails, the function returns with an error.
func (m *manager) CheckStmts(service string, unprepared map[string]string) (map[string]*sql.Stmt, error) {
	prepared := map[string]*sql.Stmt{}
	// Only prepare statement in local to check for syntax error in statements
	if os.Getenv("GO_ENV") == "" {
		for k, v := range unprepared {
			stmt, err := m.conn.Prepare(v)
			if err != nil {
				return nil, err
			}
			prepared[k] = stmt
		}
	}
	return prepared, nil
}

func (m *manager) GetConn() *sql.DB {
	return m.conn
}
