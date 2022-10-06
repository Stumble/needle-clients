package testsuite

import (
	"io/ioutil"
	"os"
	"strings"
	"flag"
	"context"
	"database/sql"
	"fmt"
	"time"
	"path/filepath"

	"github.com/stretchr/testify/suite"

	"github.com/stumble/needle-clients/mysql"
)

var update = flag.Bool("update", false, "update .golden files")

type MysqlTestSuite struct {
	suite.Suite
	Testdb  string
	Tables  []string
	Config  *mysql.Config
	Manager mysql.Manager
}

// NewMysqlTestSuite @p db is the name of test db and tables are table creation
// SQL statements. DB will be created, so does tables, on SetupTest.
// If you pass different @p db for suites in different packages, you can test them in parallel.
func NewMysqlTestSuite(db string, tables []string) *MysqlTestSuite {
	config := mysql.ConfigFromEnv()
	config.DBName = db
	return NewMysqlTestSuiteWithConfig(config, db, tables)
}

func NewMysqlTestSuiteWithConfig(config *mysql.Config, db string, tables []string) *MysqlTestSuite {
	return &MysqlTestSuite{
		Testdb: db,
		Tables: tables,
		Config: config,
	}
}

func (suite *MysqlTestSuite) GetConn() *sql.DB {
	return suite.Manager.GetConn()
}

func (suite *MysqlTestSuite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create DB
	conn, err := mysql.RawMysqlConn(suite.Config)
	suite.Require().Nil(err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", suite.Testdb))
	if err != nil {
		panic(err)
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s;", suite.Testdb))
	if err != nil {
		panic(err)
	}

	// create manager
	manager, err := mysql.NewMysqlManager(suite.Config)
	if err != nil {
		panic(err)
	}
	suite.Manager = manager

	// create tables
	for _, v := range suite.Tables {
		exec := suite.Manager.GetDBExecuter()
		_, err := exec.Exec(ctx, v)
		if err != nil {
			panic(err)
		}
	}
}

func (suite *MysqlTestSuite) LoadState(enableSubTest bool, importJsonPaths ...string) {
	t := suite.T()
	name := t.Name()
	parentName := name[:strings.LastIndex(name, "/")]

	var fileName string
	if enableSubTest {
		fileName = name
	} else {
		fileName = parentName
	}

	gp := filepath.Join("input", fileName+".input")
	if fileExists(gp) {
		d.LoadFromFile(suite.Ctx, gp)
	}
	for _, path := range importJsonPaths {
		gp := filepath.Join("input", "common", path+".input")
		d.LoadFromFile(suite.Ctx, gp)
	}
}

func ensureDir(dirName string) error {
	err := os.MkdirAll(dirName, 0700)
	if err == nil || os.IsExist(err) {
		return nil
	} else {
		return err
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}