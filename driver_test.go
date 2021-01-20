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

	// API/lib packages not imported by driver.
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

var (
	dsn        string
)

type Connector struct {
	ctx         context.Context
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewConnector() (*Connector, error) {

	// Configure emulator if set.
	spannerHost, ok := os.LookupEnv("SPANNER_EMULATOR_HOST")

	ctx := context.Background()

	var adminClient *adminapi.DatabaseAdminClient
	var err error

	if ok {
		adminClient, err = adminapi.NewDatabaseAdminClient(
			ctx,
			option.WithoutAuthentication(),
			option.WithEndpoint(spannerHost),
			option.WithGRPCDialOption(grpc.WithInsecure()))
		if err != nil {
			return nil, err
		}
	} else {
		adminClient, err = adminapi.NewDatabaseAdminClient(ctx)
		if err != nil {
			return nil, err
		}
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

// Structs for row data.
type testQueryContextRow struct {
	A string
	B string
	C string
}

func init() {

	var projectId, instanceId, databaseId string
	var ok bool

	// Get environment variables or set to default.
	if instanceId, ok = os.LookupEnv("SPANNER_TEST_INSTANCE"); !ok {
		instanceId = "test-instance"
	}
	if projectId, ok = os.LookupEnv("SPANNER_TEST_PROJECT"); !ok {
		projectId = "test-project"
	}
	if databaseId, ok = os.LookupEnv("SPANNER_TEST_DBID"); !ok {
		databaseId = "gotest"
	}

	// Derive data source name.
	dsn = "projects/" + projectId + "/instances/" + instanceId + "/databases/" + databaseId
}

// Executes DDL statements.
func executeDdlApi(curs *Connector, ddls []string) (err error) {

	op, err := curs.adminClient.UpdateDatabaseDdl(curs.ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: ddls,
	})
	if err != nil {
		return err
	}
	if err := op.Wait(curs.ctx); err != nil {
		return err
	}
	return nil
}

// Executes DML using the client library.
func ExecuteDMLClientLib(dml []string) (err error) {

	// Open client/
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return err
	}
	defer client.Close()

	// Put strings into spanner.Statement structure.
	var stmts []spanner.Statement
	for _, line := range dml {
		stmts = append(stmts, spanner.NewStatement(line))
	}

	// Execute statements.
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmts := stmts
		rowCounts, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}
		fmt.Printf("Executed %d SQL statements using Batch DML.\n", len(rowCounts))
		return nil
	})

	return err
}

func TestQueryContext(t *testing.T) {

	// Set up test table.
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	err = executeDdlApi(curs, []string{`CREATE TABLE TestQueryContext (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`})
	if err != nil {
		t.Error(err)
	}
	err = ExecuteDMLClientLib([]string{`INSERT INTO TestQueryContext (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `})
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		input string
		want  []testQueryContextRow
	}{
		// Return one row.
		{input: "SELECT * FROM TestQueryContext WHERE A = \"a1\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
			}},
		// Return whole table.
		{input: "SELECT * FROM TestQueryContext ORDER BY A",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			}},
	}

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
	defer db.Close()

	// Run tests
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if err != nil {
			t.Fatalf(err.Error()) //  ~ err doesn't get set qhen qury fails
		}
		defer rows.Close()

		got := []testQueryContextRow{}
		for rows.Next() {
			curr := testQueryContextRow{}
			if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
				t.Error(err)
			}
			got = append(got, curr)
		}

		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("expected: %v, got: %v", tc.want, got)
		}
	}

	// Drop table.
	err = executeDdlApi(curs, []string{`DROP TABLE TestQueryContext`})
	if err != nil {
		t.Error(err)
	}
}
