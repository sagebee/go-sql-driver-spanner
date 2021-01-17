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

	"math"

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
)

// connector things
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
	bytest  []byte
	intt    int
	floatt  float64
	boolt   bool
}

func init() {

	// get environment variables
	instance = os.Getenv("SPANNER_TEST_INSTANCE")
	project = os.Getenv("SPANNER_TEST_PROJECT")
	dbname = os.Getenv("SPANNER_TEST_DBNAME")

	// set defaults if none provided
	if instance == "" {
		instance = "test-instance"
	}
	if project == "" {
		project = "test-project"
	}
	if dbname == "" {
		dbname = "gotest"
	}

	// derive data source name
	dsn = "projects/" + project + "/instances/" + instance + "/databases/" + dbname
}

// functions that use the client lib / apis ~
// ******************* //

// Executes DDL statements
func executeDdlApi(curs *Connector, ddls []string) {

	op, err := curs.adminClient.UpdateDatabaseDdl(curs.ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dsn,
		Statements: ddls,
	})
	if err != nil {
		//return nil, err
		log.Fatal(err)
	}
	if err := op.Wait(curs.ctx); err != nil {
		//return nil, err
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

// end client lib funs
// ******************* //

// helper funs for tests //
func mustExecContext(t *testing.T, ctx context.Context, db *sql.DB, query string) {
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		debug.PrintStack()
		t.Fatalf(err.Error())
	}
}

func mustQueryContext(t *testing.T, ctx context.Context, db *sql.DB, query string) (rows *sql.Rows) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return rows
}

// Tests general query functionality
func TestQueryGeneral(t *testing.T) {

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}

	executeDdlApi(curs, []string{`CREATE TABLE Testa (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`})
	ExecuteDMLClientLib([]string{`INSERT INTO Testa (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `})

	// cases
	type test struct {
		input string
		want  []testaRow
	}

	tests := []test{
		// empty query
		{input: "", want: []testaRow{}},
		// syntax error
		{input: "SELECT SELECT * FROM Testa", want: []testaRow{}},
		// retur nothing
		{input: "SELECT SELECT * FROM Testa", want: []testaRow{}},
		// return one tuple
		{input: "SELECT * FROM Testa WHERE A = \"a1\"",
			want: []testaRow{
				{A: "a1", B: "b1", C: "c1"},
			}},
		// return subset of tuples
		{input: "SELECT * FROM Testa WHERE A = \"a1\" OR A = \"a2\"",
			want: []testaRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			}},
		// subet of tuples with !=
		{input: "SELECT * FROM Testa WHERE A != \"a3\"",
			want: []testaRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			}},
		// return entire table
		{input: "SELECT * FROM Testa ORDER BY A",
			want: []testaRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			}},
		// query non existant table
		{input: "SELECT * FROM Testaa", want: []testaRow{}},
	}

	// run tests
	for _, tc := range tests {
		got := RunQueryGeneral(t, tc.input)
		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("expected: %v, got: %v", tc.want, got)
		}
	}

	// TODO attribute subset, won'w work with existing fun

	// drop table
	executeDdlApi(curs, []string{`DROP TABLE Testa`})

	// close connection
	curs.Close()
}

// runs query on Testa table, returns result in testaRow array
func RunQueryGeneral(t *testing.T, query string) []testaRow {

	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	rows := mustQueryContext(t, ctx, db, query)

	got := []testaRow{}
	for rows.Next() {
		curr := testaRow{A: "", B: "", C: ""}
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}
		got = append(got, curr)
	}
	rows.Close()

	db.Close()
	return got
}

func TestQueryAtomicTypes(t *testing.T) {

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}

	executeDdlApi(curs, []string{`CREATE TABLE TypeTesta (
		stringt	STRING(1024),
		bytest BYTES(1024),
		intt  INT64,
		floatt   FLOAT64,
		boolt BOOL
	)	 PRIMARY KEY (stringt)`})

	ExecuteDMLClientLib([]string{`INSERT INTO TypeTesta (stringt,bytest ,intt, floatt, boolt) 
		VALUES ("aa", CAST("aa" as bytes), 42, 42, TRUE), ("bb", CAST("bb" as bytes),-42, -42, FALSE),
		("x", CAST("x" as bytes), 64, CAST("nan" AS FLOAT64), TRUE), 
		("xx", CAST("xx" as bytes), 64, CAST("inf" AS FLOAT64), TRUE),
		("xxx", CAST("xxx" as bytes), 64, CAST("-inf" AS FLOAT64), TRUE)`})

	// cases
	type test struct {
		input string
		want  []typeTestaRow
	}

	tests := []test{
		// read general values, negitive & pisitive insts
		{input: "SELECT * FROM TypeTesta WHERE stringt = \"aa\" OR stringt = 'bb' ORDER BY stringt",
			want: []typeTestaRow{
				{stringt: "aa", bytest: []byte("aa"), intt: 42, floatt: 42, boolt: true},
				{stringt: "bb", bytest: []byte("bb"), intt: -42, floatt: -42, boolt: false},
			}},
		// float special values
		{input: "SELECT * FROM TypeTesta WHERE stringt = \"x\" ORDER BY stringt",
			want: []typeTestaRow{
				{stringt: "x", bytest: []byte("x"), intt: 64, floatt: math.NaN(), boolt: true},
			}},
	}

	// run tests
	for _, tc := range tests {
		got := RunQueryAtomicTypes(t, tc.input)
		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("expected: %v, got: %v", tc.want, got)
		}
	}

	// heck land

	ffff := RunQueryAtomicTypes(t, "SELECT * FROM typetesta WHERE stringt = \"xx\" ORDER BY stringt")
	for _, ii := range ffff {
		fmt.Printf("\n\nMATH INF BITS: {")
		fmt.Print(math.Float64bits(math.Inf(1)))
		fmt.Printf("}\nSPANNER INF BITS: {")
		fmt.Print(math.Float64bits(ii.floatt))
		if math.Inf(1) == ii.floatt {
			fmt.Print("	 *** EQUAL")
		}
		fmt.Printf("\n")
	}
	fff := RunQueryAtomicTypes(t, "SELECT * FROM typetesta WHERE stringt = \"x\" ORDER BY stringt")
	for _, i := range fff {
		fmt.Printf("\n\nMATH NAN BITS: {")
		fmt.Print(math.Float64bits(math.NaN()))
		fmt.Printf("}\nSPANNER NAN BITS: {")
		fmt.Print(math.Float64bits(i.floatt))
		if math.NaN() == i.floatt {
			fmt.Print("	 *** EQUAL")
		}
		fmt.Printf("\n")
	}

	// end heck land

	// drop table
	executeDdlApi(curs, []string{`DROP TABLE TypeTesta`})

	// close
	curs.Close()
}

// runs query on Testa table, returns result in testaRow array
func RunQueryAtomicTypes(t *testing.T, query string) []typeTestaRow {

	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	rows := mustQueryContext(t, ctx, db, query)

	got := []typeTestaRow{}
	for rows.Next() {
		curr := typeTestaRow{stringt: "", bytest: nil, intt: -1, floatt: -1, boolt: false}
		if err := rows.Scan(
			&curr.stringt, &curr.bytest, &curr.intt, &curr.floatt, &curr.boolt); err != nil {
			t.Error(err.Error())
		}
		got = append(got, curr)
	}
	rows.Close()

	db.Close()
	return got
}
