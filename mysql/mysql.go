package mysql

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog/log"
)

const (
	HighQPSMaxOpenConns = 100
	AppNameLengthMax    = 32
)

type Config struct {
	Username         string        `default:"root"`
	Password         string        `default:"my-secret"`
	Host             string        `default:"localhost"`
	Port             int           `default:"3306"`
	DBName           string        `default:"test_db"`
	MaxOpenConns     int           `default:"10"`
	MaxIdleConns     int           `default:"10"`
	MaxLifetime      time.Duration `default:"60s"`
	AppName          string        `default:""`
	EnablePrometheus bool          `default:"true"`
}

func (c *Config) String() string {
	if c == nil {
		return "nil"
	}
	copy := *c
	copy.Password = "*hidden*"
	return fmt.Sprintf("%+v", copy)
}

func (c *Config) Valid() error {
	if len(c.AppName) == 0 || len(c.AppName) > AppNameLengthMax {
		return fmt.Errorf("invalid AppName: %s", c)
	}
	return nil
}

func ConfigFromEnv() *Config {
	config := &Config{}
	envconfig.MustProcess("mysql", config)
	return config
}

func ConfigFromEnvPrefix(prefix string) *Config {
	config := &Config{}
	envconfig.MustProcess(prefix, config)
	return config
}

func RawMysqlConn(config *Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/", config.Username, config.Password, config.Host, config.Port))
	return db, err
}

func NewMysqlManager(config *Config) (Manager, error) {
	return NewMysqlManagerWithMetrics(config)
}

func NewMysqlManagerWithMetrics(config *Config) (Manager, error) {
	if config == nil {
		config = ConfigFromEnv()
	}
	if config.MaxOpenConns < HighQPSMaxOpenConns {
		log.Warn().Msgf(
			"MysqlManager Config has MaxOpenConns = %d,"+
				"which may be too low to handle high QPS.", config.MaxOpenConns)
	}
	if err := config.Valid(); err != nil {
		return nil, err
	}
	manager, err := newManagerWithMetrics(config)
	if err != nil {
		return nil, err
	}
	err = manager.Ping()
	if err != nil {
		return nil, err
	}
	return manager, nil
}
