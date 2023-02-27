package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

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
	conn *sql.DB
	exec DBExecuter

	statementMap *sync.Map
	stats        *metricSet
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func newManagerWithMetrics(config *Config) (Manager, error) {
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

	ctx, cancel := context.WithCancel(context.Background())

	exec := &simpleDBExecuter{
		conn:         conn,
		statementMap: statementMap,
	}

	manager := &manager{
		conn:         conn,
		statementMap: statementMap,
		ctx:          ctx,
		cancel:       cancel,
		exec:         exec,
	}

	if config.EnablePrometheus {
		manager.stats = newMetricSet(config.AppName)
		exec.counter = manager.stats.Request
		exec.histogram = manager.stats.Latency
		manager.stats.Register()
		manager.wg.Add(1)
		go manager.updateMetrics()
	}

	return manager, nil
}

func (m *manager) updateMetrics() {
	defer m.wg.Done()
	ticker := time.NewTicker(connPoolUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-m.ctx.Done():
			return
		}
		stats := m.conn.Stats()
		m.stats.ConnPool.WithLabelValues("MaxOpenConnections").Set(float64(stats.MaxOpenConnections))
		m.stats.ConnPool.WithLabelValues("Idle").Set(float64(stats.Idle))
		m.stats.ConnPool.WithLabelValues("OpenConnections").Set(float64(stats.OpenConnections))
		m.stats.ConnPool.WithLabelValues("InUse").Set(float64(stats.InUse))
		m.stats.ConnPool.WithLabelValues("WaitCount").Set(float64(stats.WaitCount))
		m.stats.ConnPool.WithLabelValues("WaitDuration").Set(float64(stats.WaitDuration))
		m.stats.ConnPool.WithLabelValues("MaxIdleClosed").Set(float64(stats.MaxIdleClosed))
		m.stats.ConnPool.WithLabelValues("MaxLifetimeClosed").Set(float64(stats.MaxLifetimeClosed))
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
		counter:      m.stats.Request,
		histogram:    m.stats.Latency,
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
	m.cancel()
	m.wg.Wait()
	if m.stats != nil {
		m.stats.Unregister()
	}

	err := m.conn.Close()
	if err != nil {
		return err
	}
	return nil
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
