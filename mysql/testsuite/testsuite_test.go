package testsuite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/stumble/needle-clients/mysql"
	sqlsuite "github.com/stumble/needle-clients/mysql/testsuite"
)

type metaTestSuite struct {
	*sqlsuite.MysqlTestSuite
}

type Doc struct {
	Id          int             `json:"id"`
	Rev         float64         `json:"rev"`
	Content     string          `json:"content"`
	CreatedAt   time.Time       `json:"created_at"`
	Description json.RawMessage `json:"description"`
}

type loaderDumper struct {
	exec mysql.DBExecuter
}

func (m *loaderDumper) Dump() ([]byte, error) {
	rows, err := m.exec.Query(
		context.Background(),
		"SELECT `id`,`rev`,`content`, `created_at`, `description` FROM `docs`")
	if err != nil {
		return nil, err
	}
	results := make([]Doc, 0)
	for rows.Next() {
		row := Doc{}
		err := rows.Scan(&row.Id, &row.Rev, &row.Content, &row.CreatedAt, &row.Description)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	bytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (m *loaderDumper) Load(data []byte) error {
	docs := make([]Doc, 0)
	err := json.Unmarshal(data, &docs)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		_, err := m.exec.Exec(
			context.Background(),
			"INSERT INTO `docs` (`id`,`rev`,`content`, `created_at`, `description`) VALUES (?,?,?,?,?)",
			doc.Id, doc.Rev, doc.Content, doc.CreatedAt, doc.Description)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewMetaTestSuite() *metaTestSuite {
	return &metaTestSuite{
		MysqlTestSuite: sqlsuite.NewMysqlTestSuite("metaTestDB", []string{
			`CREATE TABLE IF NOT EXISTS docs (
             id int(6) unsigned NOT NULL,
             rev DOUBLE unsigned NOT NULL,
             content varchar(200) NOT NULL,
             created_at datetime NOT NULL,
             description JSON NOT NULL,
             PRIMARY KEY (id)
             ) DEFAULT CHARSET=utf8mb4;`,
		}),
	}
}

func TestMetaTestSuite(t *testing.T) {
	suite.Run(t, NewMetaTestSuite())
}

func (suite *metaTestSuite) SetupTest() {
	suite.MysqlTestSuite.SetupTest()
}

func (suite *metaTestSuite) TestInsertQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := suite.Manager.GetExec()
	rst, err := exec.Exec(ctx,
		"INSERT INTO `docs` (`id`,`rev`,`content`, `created_at`, `description`) VALUES (?,?,?,?,?)",
		33, 666.7777, "hello world", time.Now(), json.RawMessage("{}"))
	suite.Nil(err)
	n, err := rst.RowsAffected()
	suite.Nil(err)
	suite.Equal(int64(1), n)

	exec = suite.MysqlTestSuite.Manager.GetExec()
	rows, err := exec.Query(ctx, "SELECT `content` FROM `docs` WHERE `id` = ?", 33)
	suite.Nil(err)

	content := ""
	suite.True(rows.Next())
	err = rows.Scan(&content)
	suite.Nil(err)
	suite.Equal("hello world", content)
}

func (suite *metaTestSuite) TestInsertUseGolden() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exec := suite.Manager.GetExec()
	rst, err := exec.Exec(ctx,
		"INSERT INTO `docs` (`id`,`rev`,`content`, `created_at`, `description`) VALUES (?,?,?,?,?)",
		33, 666.7777, "hello world", time.Unix(1000, 0), json.RawMessage("{}"))
	suite.Nil(err)
	n, err := rst.RowsAffected()
	suite.Nil(err)
	suite.Equal(int64(1), n)
	dumper := &loaderDumper{exec: exec}
	suite.MysqlTestSuite.Golden("docs", dumper)
}

func (suite *metaTestSuite) TestQueryUseLoader() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exec := suite.Manager.GetExec()

	// load state to db from input
	loader := &loaderDumper{exec: exec}
	suite.MysqlTestSuite.LoadState("TestMetaTestSuite/TestQueryUseLoader.docs.json", loader)

	rows, err := exec.Query(ctx, "SELECT `content`, `rev`, `created_at`, `description` FROM `docs` WHERE `id` = ?", 33)
	suite.Nil(err)
	defer rows.Close()

	var content string
	var rev float64
	var createdAt time.Time
	var desc json.RawMessage
	suite.True(rows.Next())
	err = rows.Scan(&content, &rev, &createdAt, &desc)
	suite.Nil(err)
	suite.Equal("content read from file", content)
	suite.Equal(float64(66.66), rev)
	suite.Equal(int64(1000), createdAt.Unix())
	suite.Equal(json.RawMessage(`{"github_url": "github.com/stumble/needle-clients"}`), desc)
}
