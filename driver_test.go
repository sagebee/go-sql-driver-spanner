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
	"os"
	"reflect"
	"strings"
	"testing"

	// API/lib packages not imported by driver.
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

var (
	dsn string
)

type Connector struct {
	ctx         context.Context
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewConnector() (*Connector, error) {

	ctx := context.Background()

	adminClient, err := CreateAdminClient(ctx)
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

func CreateAdminClient(ctx context.Context) (*adminapi.DatabaseAdminClient, error) {

	var adminClient *adminapi.DatabaseAdminClient
	var err error

	// Configure emulator if set.
	spannerHost, ok := os.LookupEnv("SPANNER_EMULATOR_HOST")
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

	return adminClient, nil
}

func (c *Connector) Close() {
	c.client.Close()
	c.adminClient.Close()
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

// Used to check if error contains expected string
// If want is the empty string, no error is expected
func ErrorContainsStr(err error, want string) bool {
	if want == "" && err != nil {
		return false
	}
	if err == nil {
		return want == ""
	}
	return strings.Contains(err.Error(), want)
}

func ErrorExpected(err error, want bool) bool {

	if err != nil {
		return want
	}
	return !want
}

// Executes DDL statements.
func executeDdlApi(curs *Connector, ddls []string) error {

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
func ExecuteDMLClientLib(dml []string) error {

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
		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func TestQueryContext(t *testing.T) {

	// Set up test table.
	curs, err := NewConnector()
	if err != nil {
		t.Fatal(err)
	}
	defer curs.Close()

	err = executeDdlApi(curs, []string{`CREATE TABLE TestQueryContext (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`})
	if err != nil {
		t.Fatal(err)
	}
	err = ExecuteDMLClientLib([]string{`INSERT INTO TestQueryContext (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `})
	if err != nil {
		t.Fatal(err)
	}

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type testQueryContextRow struct {
		A, B, C string
	}

	tests := []struct {
		name           string
		input          string
		want           []testQueryContextRow
		wantErrorQuery bool
		wantErrorScan  bool
	}{
		{name: "empty query",
			input: "", want: []testQueryContextRow{}},
		{name: "suntax error",
			input: "SELECT SELECT * FROM TestQueryContext", want: []testQueryContextRow{}},
		{name: "return nothing",
			input: "SELECT * FROM TestQueryContext WHERE A = \"hihihi\"", want: []testQueryContextRow{}},
		{name: "select one tuple",
			input: "SELECT * FROM TestQueryContext WHERE A = \"a1\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
			}},
		{name: "select subset of tuples",
			input: "SELECT * FROM TestQueryContext WHERE A = \"a1\" OR A = \"a2\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			}},
		{name: "select subset of tuples with !=",
			input: "SELECT * FROM TestQueryContext WHERE A != \"a3\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			}},
		{name: "select entire table",
			input:         "SELECT * FROM TestQueryContext ORDER BY A",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			}},
		{name: "query non existant table",
			input: "SELECT * FROM TestQueryContexta", want: []testQueryContextRow{}},
	}

	// Run tests
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if !ErrorExpected(err, tc.wantErrorQuery) {
			if tc.wantErrorQuery {
				t.Errorf("%s: expected query error but error was %v", tc.name, err)
			} else {
				t.Errorf("%s: unexpected query error: %v", tc.name, err)
			}
		}

		got := []testQueryContextRow{}
		for rows.Next() {
			var curr testQueryContextRow
			err := rows.Scan(&curr.A, &curr.B, &curr.C)
			if !ErrorExpected(err, tc.wantErrorScan) {
				if tc.wantErrorScan {
					t.Errorf("%s: expected query error but error was %v", tc.name, err)
				} else {
					t.Errorf("%s: unexpected query error: %v", tc.name, err)
				}
			}
			got = append(got, curr)
		}
		rows.Close()

		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("Test failed: %s. expected: %v, got: %v", tc.name, tc.want, got)
		}
	}

	// Drop table.
	err = executeDdlApi(curs, []string{`DROP TABLE TestQueryContext`})
	if err != nil {
		t.Error(err)
	}
}
