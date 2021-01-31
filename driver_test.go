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
	"math"
	"os"
	"reflect"
	"runtime/debug"
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

	// Configure emulator if set.
	spannerHost, emulator := os.LookupEnv("SPANNER_EMULATOR_HOST")

	// configure production if credentials set

	ctx := context.Background()

	var adminClient *adminapi.DatabaseAdminClient
	var err error

	if emulator {
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

	//log.Fatal(dsn)
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

	type testQueryContextRow struct {
		A, B, C string
	}

	tests := []struct {
		input          string
		want           []testQueryContextRow
		wantErrorQuery string
		wantErrorScan  string
	}{
		// empty query
		{input: "", want: []testQueryContextRow{}},
		// syntax error
		{input: "SELECT SELECT * FROM TestQueryContext", want: []testQueryContextRow{}},
		// retur nothing
		{input: "SELECT SELECT * FROM TestQueryContext", want: []testQueryContextRow{}},
		// return one tuple
		{input: "SELECT * FROM TestQueryContext WHERE A = \"a1\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
			}},
		// return subset of tuples
		{input: "SELECT * FROM TestQueryContext WHERE A = \"a1\" OR A = \"a2\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			}},
		// subet of tuples with !=
		{input: "SELECT * FROM TestQueryContext WHERE A != \"a3\"",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
			}},
		// return entire table
		{input: "SELECT * FROM TestQueryContext ORDER BY A",
			want: []testQueryContextRow{
				{A: "a1", B: "b1", C: "c1"},
				{A: "a2", B: "b2", C: "c2"},
				{A: "a3", B: "b3", C: "c3"},
			}},
		// query non existant table
		{input: "SELECT * FROM TestQueryContexta", want: []testQueryContextRow{}},
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
		if !ErrorContainsStr(err, tc.wantErrorQuery) {
			t.Errorf("Unexpected error %v", err)
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

func CreateAtomicTypeTable() {

	// set up test table
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	executeDdlApi(curs, []string{`CREATE TABLE TestQueryType (
		stringt	STRING(1024),
		bytest BYTES(1024),
		intt  INT64,
		floatt   FLOAT64,
		boolt BOOL
	)	 PRIMARY KEY (stringt)`})

	ExecuteDMLClientLib([]string{`INSERT INTO TestQueryType (stringt,bytest ,intt, floatt, boolt) 
		VALUES ("aa", CAST("aa" as bytes), 42, 42, TRUE), ("bb", CAST("bb" as bytes),-42, -42, FALSE),
		("nan", CAST("nan" as bytes), 64, CAST("nan" AS FLOAT64), TRUE), 
		("pinf", CAST("pinf" as bytes), 64, CAST("inf" AS FLOAT64), TRUE),
		("ninf", CAST("ninf" as bytes), 64, CAST("-inf" AS FLOAT64), TRUE),
		("byteoverflow", CAST("abcdefghijklmnop" as bytes), 42, 42, TRUE),
		("maxint", CAST("maxint" as bytes), 9223372036854775807, 42, TRUE),
		("minint", CAST("minint" as bytes), -9223372036854775808, 42, TRUE),
		("nullbytes", null, 42, 42, TRUE),
		("nullint", CAST("nullint" as bytes), null, 42, TRUE),
		("nullfloat", CAST("nullfloat" as bytes), 42, null, TRUE),
		("nullbool", CAST("nullbool" as bytes), 42, 42, null)`})
}

func xTestQueryContextAtomicTypes(t *testing.T) {

	CreateAtomicTypeTable()

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type testQueryTypetRow struct {
		stringt string
		bytest  []byte
		intt    int
		floatt  float64
		boolt   bool
	}

	tests := []struct {
		input          string
		want           []testQueryTypetRow
		wantErrorQuery string
		wantErrorScan  string
	}{

		// Read general values, negitive & positive insts.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"aa\" OR stringt = 'bb' ORDER BY stringt",
			want: []testQueryTypetRow{
				{stringt: "aa", bytest: []byte("aa"), intt: 42, floatt: 42, boolt: true},
				{stringt: "bb", bytest: []byte("bb"), intt: -42, floatt: -42, boolt: false},
			}},
		// Read Max int.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"maxint\" ",
			want: []testQueryTypetRow{
				{stringt: "maxint", bytest: []byte("maxint"), intt: 9223372036854775807, floatt: 42, boolt: true},
			}},
		// Read Min int.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"minint\" ",
			want: []testQueryTypetRow{
				{stringt: "minint", bytest: []byte("minint"), intt: -9223372036854775808, floatt: 42, boolt: true},
			}},
		// Read null bytes.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"nullbytes\" ",
			want: []testQueryTypetRow{
				{stringt: "nullbytes", bytest: nil, intt: 42, floatt: 42, boolt: true},
			}},
		// Read null int.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"nullint\" ",
			want: []testQueryTypetRow{
				{stringt: "nullint", bytest: []byte("nullint"), intt: 0, floatt: 42, boolt: true},
			}},
		// Read null float.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"nullfloat\" ",
			want: []testQueryTypetRow{
				{stringt: "nullfloat", bytest: []byte("nullfloat"), intt: 42, floatt: 0, boolt: true},
			}},
		// Read null bool.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"nullbool\" ",
			want: []testQueryTypetRow{
				{stringt: "nullbool", bytest: []byte("nullbool"), intt: 42, floatt: 42, boolt: false},
			}},
		// Read special float value infinity.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"pinf\" ",
			want: []testQueryTypetRow{
				{stringt: "pinf", bytest: []byte("pinf"), intt: 64, floatt: math.Inf(1), boolt: true},
			}},
		// Read special float value negative infinity.
		{input: "SELECT * FROM TestQueryType WHERE stringt = \"ninf\" ",
			want: []testQueryTypetRow{
				{stringt: "ninf", bytest: []byte("ninf"), intt: 64, floatt: math.Inf(-1), boolt: true},
			}},

		// float special values
		/*{input: "SELECT * FROM TestQueryType WHERE stringt = \"nan\" ORDER BY stringt",
		want: []testQueryTypetRow{
			{stringt: "nan", bytest: []byte("nan"), intt: 64, floatt: math.NaN(), boolt: true},
		}},*/ // unexpected behavior
	}

	// Run tests.
	for _, tc := range tests {

		rows, err := db.QueryContext(ctx, tc.input)
		if !ErrorContainsStr(err, tc.wantErrorQuery) {
			t.Errorf("Unexpected error %v", err)
		}
		defer rows.Close()

		got := []testQueryTypetRow{}
		for rows.Next() {
			curr := testQueryTypetRow{}
			err := rows.Scan(&curr.stringt, &curr.bytest, &curr.intt, &curr.floatt, &curr.boolt)
			if !ErrorContainsStr(err, tc.wantErrorScan) {
				t.Errorf("Unexpected error %v", err)
			}
			got = append(got, curr)
		}

		if !reflect.DeepEqual(tc.want, got) {
			t.Errorf("expected: %v, got: %v", tc.want, got)
		}
	}

	// Drop table.
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()
	executeDdlApi(curs, []string{`DROP TABLE TestQueryType`})
}

// Tests that don't work well with table style.
func xTestByteOverflow(t *testing.T) {

	CreateAtomicTypeTable()

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type testQueryTypetRow struct {
		stringt string
		bytest  [2]byte // Small byte buffer
		intt    int
		floatt  float64
		boolt   bool
	}

	type byteOverflowTest struct {
		input, wantErrorQuery, wantErrorScan string
		want                                 testQueryTypetRow
	}
	bt := byteOverflowTest{
		input:          "SELECT * FROM TestQueryType WHERE stringt = \"byteoverflow\" ",
		wantErrorQuery: "",
		wantErrorScan:  "storing driver.Value type []uint8 into type *[2]uint8",
		want:           testQueryTypetRow{stringt: "byteoverflow"},
	}

	rows, err := db.QueryContext(ctx, bt.input)
	if !ErrorContainsStr(err, bt.wantErrorQuery) {
		t.Errorf("Unexpected error %v", err)
	}
	defer rows.Close()

	got := testQueryTypetRow{}
	rows.Next()
	err = rows.Scan(&got.stringt, &got.bytest, &got.intt, &got.floatt, &got.boolt)
	if !ErrorContainsStr(err, bt.wantErrorScan) {
		t.Errorf("Unexpected error %v", err)
	}

	if !reflect.DeepEqual(got, bt.want) {
		t.Errorf("expected: %v, got: %v", bt.want, got)
	}

	// Drop table.
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()
	executeDdlApi(curs, []string{`DROP TABLE TestQueryType`})

}

func xTestIntOverflow(t *testing.T) {

	CreateAtomicTypeTable()

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type testQueryTypetRow struct {
		stringt string
		bytest  []byte
		intt    int8 // Too small int
		floatt  float64
		boolt   bool
	}

	// Read spanner max INT64 into golang int8
	type intOverflowTest struct {
		input, wantErrorQuery, wantErrorScan string
		want                                 testQueryTypetRow
	}

	bi := intOverflowTest{
		input:          "SELECT * FROM TestQueryType WHERE stringt = \"maxint\" ",
		wantErrorQuery: "",
		wantErrorScan:  "converting driver.Value type int64 (\"9223372036854775807\") to a int8: value out of range",
		want:           testQueryTypetRow{stringt: "maxint", bytest: []byte("maxint")},
	}

	rows, err := db.QueryContext(ctx, bi.input)
	if !ErrorContainsStr(err, bi.wantErrorQuery) {
		t.Errorf("Unexpected error %v", err)
	}
	defer rows.Close()

	got := testQueryTypetRow{}
	rows.Next()
	err = rows.Scan(&got.stringt, &got.bytest, &got.intt, &got.floatt, &got.boolt)
	if !ErrorContainsStr(err, bi.wantErrorScan) {
		t.Errorf("Unexpected error %v", err)
	}

	if !reflect.DeepEqual(got, bi.want) {
		t.Errorf("expected: %v, got: %v", bi.want, got)
	}

	// Read spanner INT64 into go int8 that should fit ok
	bi = intOverflowTest{
		input:          "SELECT * FROM TestQueryType WHERE stringt = \"aa\" ",
		wantErrorQuery: "",
		wantErrorScan:  "",
		want:           testQueryTypetRow{stringt: "aa", bytest: []byte("aa"), intt: 42, floatt: 42, boolt: true},
	}

	rows, err = db.QueryContext(ctx, bi.input)
	if !ErrorContainsStr(err, bi.wantErrorQuery) {
		t.Errorf("Unexpected error %v", err)
	}
	defer rows.Close()

	got = testQueryTypetRow{}
	rows.Next()
	err = rows.Scan(&got.stringt, &got.bytest, &got.intt, &got.floatt, &got.boolt)
	if !ErrorContainsStr(err, bi.wantErrorScan) {
		t.Errorf("Unexpected error %v", err)
	}

	if !reflect.DeepEqual(got, bi.want) {
		t.Errorf("expected: %v, got: %v", bi.want, got)
	}

	// Drop table.
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()
	executeDdlApi(curs, []string{`DROP TABLE TestQueryType`})
}



// Null read inspectigation 

/*
func TestNullIntRead(t *testing.T) {

	CreateAtomicTypeTable()

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	type testQueryTypetRow struct {
		stringt string
		bytest  []byte
		intt    int
		floatt  float64
		boolt   bool
	}

		// Read spanner max INT64 into golang int8
	type intOverflowTest struct {
		input, wantErrorQuery, wantErrorScan string
		want                                 testQueryTypetRow
	}

	bi := intOverflowTest{
		input:          "SELECT * FROM TestQueryType WHERE stringt = \"nullfloat\" ",
		wantErrorQuery: "",
		wantErrorScan:  "",
		want:	testQueryTypetRow{stringt: "aa", bytest: []byte("aa"), intt: 4, floatt: 4, boolt: true},
	}

	rows, err := db.QueryContext(ctx, bi.input)
	if !ErrorContainsStr(err, bi.wantErrorQuery) {
		t.Errorf("Unexpected error %v", err)
	}
	defer rows.Close()

	got := testQueryTypetRow{}
	rows.Next()
	err = rows.Scan(&got.stringt, &got.bytest, &got.intt, &got.floatt, &got.boolt)
	if !ErrorContainsStr(err, bi.wantErrorScan) {
		t.Errorf("Unexpected error %v", err)
	}

	if !reflect.DeepEqual(got, bi.want) {
		t.Errorf("expected: %v, got: %v  \nINT: %d", bi.want, got, got.intt)
	}

}

*/


/*
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

	rows, err := db.QueryContext(ctx, tc.input)
	if err != tc.wantError {
		t.Errorf("Unexpected error, got %#v want %#v", err, tc.wantError)
	}
	defer rows.Close()


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

*/


