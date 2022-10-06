package testsuite

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/stumble/needle-clients/mysql"
)

type Loader interface {
	Load(data []byte) error
}

type Dumper interface {
	Dump() ([]byte, error)
}

const (
	TestDataDirPath = "testdata"
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

// NewMysqlTestSuiteWithConfig connect to MysqlServer according to @p config,
// @p db is the name of test db and tables are table creation
// SQL statements. DB will be created, so does tables, on SetupTest.
// If you pass different @p db for suites in different packages, you can test them in parallel.
func NewMysqlTestSuiteWithConfig(config *mysql.Config, db string, tables []string) *MysqlTestSuite {
	return &MysqlTestSuite{
		Testdb: db,
		Tables: tables,
		Config: config,
	}
}

// returns a raw *sql.DB connection.
func (suite *MysqlTestSuite) GetConn() *sql.DB {
	return suite.Manager.GetConn()
}

// setup the database to a clean state: tables have been created according to the
// schema, empty.
func (suite *MysqlTestSuite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create DB
	conn, err := mysql.RawMysqlConn(suite.Config)
	suite.Require().NoError(err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", suite.Testdb))
	suite.Require().NoError(err)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s;", suite.Testdb))
	suite.Require().NoError(err)

	// create manager
	manager, err := mysql.NewMysqlManager(suite.Config)
	suite.Require().NoError(err)
	suite.Manager = manager

	// create tables
	for _, v := range suite.Tables {
		exec := suite.Manager.GetExec()
		_, err := exec.Exec(ctx, v)
		suite.Require().NoError(err)
	}
}

// load bytes from file
func (suite *MysqlTestSuite) loadFile(file string) []byte {
	suite.Require().FileExists(file)
	f, err := os.Open(file)
	suite.Require().NoError(err)
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	suite.Require().NoError(err)
	return data
}

// LoadState load state from the file to DB.
// For example LoadState(ctx, "sample1.input.json") will load (insert) from
// "testdata/sample1.input.json" to table
func (suite *MysqlTestSuite) LoadState(filename string, loader Loader) {
	input := testDirFilePath(filename)
	data := suite.loadFile(input)
	suite.Require().NoError(loader.Load(data))
}

// Dumpstate dump state to the file.
// For example DumpState(ctx, "sample1.golden.json") will dump (insert) bytes from
// dumper.dump() to "testdata/sample1.golden.json".
func (suite *MysqlTestSuite) DumpState(filename string, dumper Dumper) {
	outputFile := testDirFilePath(filename)
	dir, _ := filepath.Split(outputFile)
	suite.Require().NoError(ensureDir(dir))
	f, err := os.Create(outputFile)
	suite.Require().NoError(err)
	defer f.Close()
	bytes, err := dumper.Dump()
	suite.Require().NoError(err)
	_, err = f.Write(bytes)
	suite.Require().NoError(err)
	suite.Require().NoError(f.Sync())
}

func (suite *MysqlTestSuite) Golden(dbName string, dumper Dumper) {
	goldenFile := fmt.Sprintf("%s.%s.golden", suite.T().Name(), dbName)
	if *update {
		suite.DumpState(goldenFile, dumper)
		return
	}
	golden := suite.loadFile(testDirFilePath(goldenFile))
	state, err := dumper.Dump()
	suite.Require().NoError(err)
	suite.Equal(golden, state)
}

func testDirFilePath(filename string) string {
	return filepath.Join(TestDataDirPath, filename)
}

func ensureDir(dirName string) error {
	err := os.MkdirAll(dirName, 0700)
	if err == nil || os.IsExist(err) {
		return nil
	} else {
		return err
	}
}
