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
	//"math"
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

// Executes DDL statements
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
func TestQueryContext(t *testing.T) {

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	err = executeDdlApi(curs, []string{`CREATE TABLE Testa (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`})
	if err != nil{
		t.Error(err)
	}
	ExecuteDMLClientLib([]string{`INSERT INTO Testa (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `})

	// cases
	tests := []struct {
		input string
		want  []testaRow
	}{
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

		rows := mustQueryContext(t, ctx, db, tc.input)
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

	// TODO attribute subset, won'w work with existing fun

	// drop table
	err = executeDdlApi(curs, []string{`DROP TABLE Testa`})
	if err != nil{
		t.Error(err)
	}

}

func TestQueryContextAtomicTypes(t *testing.T){

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	err = executeDdlApi(curs, []string{`CREATE TABLE TypeTesta (
		stringt	STRING(1024),
		bytest BYTES(1024),
		intt  INT64,
		floatt   FLOAT64,
		boolt BOOL
	)	 PRIMARY KEY (stringt)`})
	if err != nil {
		t.Error(err)
	}

	ExecuteDMLClientLib([]string{`INSERT INTO TypeTesta (stringt,bytest ,intt, floatt, boolt) 
		VALUES ("aa", CAST("aa" as bytes), 42, 42, TRUE), ("bb", CAST("bb" as bytes),-42, -42, FALSE),
		("x", CAST("x" as bytes), 64, CAST("nan" AS FLOAT64), TRUE), 
		("xx", CAST("xx" as bytes), 64, CAST("inf" AS FLOAT64), TRUE),
		("xxx", CAST("xxx" as bytes), 64, CAST("-inf" AS FLOAT64), TRUE)`})

	// cases
	tests := []struct {
		input string
		want  []typeTestaRow
	}{
		// read general values, negitive & positive insts
		{input: "SELECT * FROM TypeTesta WHERE stringt = \"aa\" OR stringt = 'bb' ORDER BY stringt",
			want: []typeTestaRow{
				{stringt: "aa", bytest: []byte("aa"), intt: 42, floatt: 42, boolt: true},
				{stringt: "bb", bytest: []byte("bb"), intt: -42, floatt: -42, boolt: false},
			}},
		// float special values
		/*{input: "SELECT * FROM TypeTesta WHERE stringt = \"x\" ORDER BY stringt",
		want: []typeTestaRow{
			{stringt: "x", bytest: []byte("x"), intt: 64, floatt: math.NaN(), boolt: true},
		}},*/
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

		rows := mustQueryContext(t, ctx, db, tc.input)
		got := []typeTestaRow{}
		for rows.Next() {
			curr := typeTestaRow{stringt: "", bytest: nil, intt: -1, floatt: -1, boolt: false}
			if err := rows.Scan(
				&curr.stringt, &curr.bytest, &curr.intt, &curr.floatt, &curr.boolt); err != nil {
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
	err = executeDdlApi(curs, []string{`DROP TABLE TypeTesta`})
	if err != nil {
		t.Error(err)
	}
}


// special tests that don't work well in table format
func TestQueryContextOverflowTypes(t *testing.T){

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	err = executeDdlApi(curs, []string{`CREATE TABLE TypeTestb (
		stringt	STRING(1024),
		bytest BYTES(1024),
		intt  INT64,
		floatt   FLOAT64,
		boolt BOOL
	)	 PRIMARY KEY (stringt)`})
	if err != nil {
		t.Error(err)
	}

	ExecuteDMLClientLib([]string{`INSERT INTO TypeTestb (stringt,bytest ,intt, floatt, boolt) 
		VALUES ("aa", CAST("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUV" as bytes),9223372036854775807,
		 3, TRUE)`})

	// open db
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
	defer db.Close()

	// read 
	var strine string 
	var tinybytes []byte
	var tinyint int8
	var tinyfloat float32 
	var bl bool

	rows := mustQueryContext(t, ctx, db, "SELECT * FROM TypeTestb")
	for rows.Next() {
		if err := rows.Scan(
			&strine, &tinybytes, &tinyint, &tinyfloat, &bl); err != nil {
			fmt.Println(reflect.TypeOf(err))
			fmt.Printf("ERROR NOT NIL\n\n\n\n")	
			t.Error(err)
		}
	}
	rows.Close()

	fmt.Printf("XXX %x %d %f %t\n",  tinybytes, tinyint, tinyfloat, bl)

	// drop table
	err = executeDdlApi(curs, []string{`DROP TABLE TypeTestb`})
	if err != nil {
		t.Error(err)
	}

	t.Error("DHFKJDH")

}


/*
// using query context, from doc 
func TestHeck(t *testing.T){

	// run DDL on the driver

	dbb, err := sql.Open(
		"spanner",
		"projects/test-project/instances/test-instance/databases/gotest",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer dbb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	_, err = dbb.ExecContext(
		ctx,
		`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
	)
	if err != nil{
		debug.PrintStack()
		t.Error(err)
	}

	log.Print("Created tables.")
}	

*/

