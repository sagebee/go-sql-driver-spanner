// Copyright 2020 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spannerdriver

import (
	"cloud.google.com/go/spanner"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime/debug"
	"testing"

	// api/lib packages not imported by driver
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

var (
	project  string
	instance string
	dbname   string
	dsn      string
	projset  bool
	instset  bool
	dbset    bool
)

type Connector struct {
	ctx         context.Context
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewConnector() (*Connector, error) {

	ctx := context.Background()

	adminClient, err := adminapi.NewDatabaseAdminClient(
		ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint("0.0.0.0:9010"),
		option.WithGRPCDialOption(grpc.WithInsecure()))
	if err != nil {
		return nil, err
	}

	dataClient, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return nil, err
	}

	curs := &Connector{
		ctx:         ctx,
		client:      dataClient,
		adminClient: adminClient,
	}
	return curs, nil
}

func (c *Connector) Close() {
	c.client.Close()
	c.adminClient.Close()
}

// structs for row data
type testaRow struct {
	A string
	B string
	C string
}
type typeTestaRow struct {
	stringt string
	intt    int
	floatt  float64
	boolt   bool
}

func init() {

	// get environment variables or set to default
	if instance, instset = os.LookupEnv("SPANNER_TEST_INSTANCE"); !instset {
		instance = "test-instance"
	}
	if project, projset = os.LookupEnv("SPANNER_TEST_PROJECT"); !projset {
		project = "test-project"
	}
	if dbname, dbset = os.LookupEnv("SPANNER_TEST_DBNAME"); !dbset {
		dbname = "gotest"
	}

	// derive data source name
	dsn = "projects/" + project + "/instances/" + instance + "/databases/" + dbname
}

// Executes DDL statements
func executeDdlApi(curs *Connector, ddls []string) {

	op, err := curs.adminClient.UpdateDatabaseDdl(curs.ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: ddls,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := op.Wait(curs.ctx); err != nil {
		log.Fatal(err)
	}
}

// executes DML using the client library
func ExecuteDMLClientLib(dml []string) {

	// open client
	var db = "projects/" + project + "/instances/" + instance + "/databases/" + dbname
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Put strings into spanner.Statement structure
	var states []spanner.Statement
	for _, line := range dml {
		states = append(states, spanner.NewStatement(line))
	}

	// execute statements
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmts := states
		rowCounts, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}
		fmt.Printf("Executed %d SQL statements using Batch DML.\n", len(rowCounts))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

// Tests general query functionality
func TestQueryContext(t *testing.T) {

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	executeDdlApi(curs, []string{`CREATE TABLE Testa (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`})
	ExecuteDMLClientLib([]string{`INSERT INTO Testa (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `})

	// cases
	tests := []struct {
		input string
		want  []testaRow
	}{
		// return one row
		{input: "SELECT * FROM Testa WHERE A = \"a1\"",
			want: []testaRow{
				{A: "a1", B: "b1", C: "c1"},
			}},
		// return whole table
		{input: "SELECT * FROM Testa ORDER BY A",
			want: []testaRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			}},
	}

	// open db
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
	defer db.Close()

	// run tests
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if err != nil {
			t.Fatalf(err.Error())
		}

		got := []testaRow{}
		for rows.Next() {
			curr := testaRow{}
			if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
				t.Error(err)
			}
			got = append(got, curr)
		}
		rows.Close()

		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("expected: %v, got: %v", tc.want, got)
		}
	}

	// drop table
	executeDdlApi(curs, []string{`DROP TABLE Testa`})
}
