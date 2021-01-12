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

// todo 
// - configure to work with remote proj as well

import (
	"testing"
	"os"
	"context"
	"log"
	"database/sql"
	"runtime/debug"
	"fmt"
	"reflect"
	"cloud.google.com/go/spanner"

	// api/lib packages not imported by driver
	"google.golang.org/grpc"
	"google.golang.org/api/option"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	_ "github.com/rakyll/go-sql-driver-spanner"

)

var(
	project string
	instance string
	dbname string
	dsn string
)


// connector things 
type Connector struct {
	ctx         context.Context	
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewConnector()(*Connector, error){

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
			return nil,err
	}

	curs := &Connector{
		ctx: ctx,
		client: dataClient,
		adminClient: adminClient,

	}
	return curs,nil
}

func (c *Connector) Close() {
	c.client.Close()
	c.adminClient.Close()
}

// structures for row data 
type testaRow struct{
	A string
	B string
	C string
}
type typeTestaRow struct {
	stringt string 
	intt int 
	floatt float64
	boolt bool
}

func init(){

	// get environment variables
	instance = os.Getenv("SPANNER_TEST_INSTANCE")
	project = os.Getenv("SPANNER_TEST_PROJECT")
	dbname = os.Getenv("SPANNER_TEST_DBNAME")

	// set defaults if none provided 
	if instance == "" {instance = "test-instance" }
	if project == "" { project = "test-project" }
	if dbname == "" { dbname = "gotest" }

	// derive data source name 
	dsn = "projects/" + project + "/instances/" + instance + "/databases/" + dbname

}


// helper funs //

// functions that use the client lib / apis ~ 
// ******************* //

// Executes DDL statements 
// (CREATE, DROP, ALTER, TRUNCATE, RENAME, etc)
// Using 
// !!! adminpb is an experimenal repo
// duct tape
func executeDdlApi(curs *Connector, ddls []string){

	os.Setenv("SPANNER_EMULATOR_HOST","0.0.0.0:9010")

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

 
// duct tape
func ExecuteDMLClientLib(dml []string){

	os.Setenv("SPANNER_EMULATOR_HOST","0.0.0.0:9010")

	// open client
	var db = "projects/"+project+"/instances/"+instance+"/databases/"+dbname;
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
			log.Fatal(err)
	}
	defer client.Close()

	// Put strings into spanner.Statement structure
	var states []spanner.Statement
	for _,line := range dml {
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
	if (err != nil) { log.Fatal(err) }

}

// end client lib funs 
// ******************* //


// helper funs for tests //
func mustExecContext(t * testing.T, ctx context.Context, db *sql.DB, query string){
	_,err := db.ExecContext(ctx, query)
	if err != nil {
		debug.PrintStack()
		t.Fatalf(err.Error())
	}
}

func mustQueryContext( t *testing.T, ctx context.Context, db *sql.DB, query string) (rows *sql.Rows){
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf(err.Error())
	}

	return rows
}

//  #### tests ####  // 

// Tests general query functionality 
func TestQueryBasic(t *testing.T){

	// set up test table
	curs, err := NewConnector()
	if err != nil{
		log.Fatal(err)
	}

	executeDdlApi(curs, []string{`CREATE TABLE Testa (
		A   STRING(1024),
		B  STRING(1024),
		C   STRING(1024)
	)	 PRIMARY KEY (A)`}) // duct tape 

	ExecuteDMLClientLib([]string{`INSERT INTO Testa (A, B, C) 
		VALUES ("a1", "b1", "c1"), ("a2", "b2", "c2") , ("a3", "b3", "c3") `}) // duct tape 

	// open db 
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	// run unit tests 
	EmptyQuery(t, db, ctx)
	SyntaxErrorQuery(t, db, ctx)
	ReturnNothingrQuery(t, db, ctx)
	OneTupleQuery(t, db, ctx)
	SubsetQuery(t, db, ctx)
	WholeTableQuery(t, db, ctx)
	ColSubseteQuery(t, db, ctx)

	// clear table 
	executeDdlApi(curs, []string{`DROP TABLE Testa`})

	// close connection 
	curs.Close()
	db.Close()
}

// helper to check if two arrays of tuples are equal
func testaTupleListEquals(expected, actual []testaRow)(bool){

	if len(expected) != len(actual){
		return false
	}
	for i, tup := range expected {
		if tup.A != actual[i].A || tup.B != actual[i].B || tup.C != actual[i].C { 
			return false
		}
	}
	return true 
}

// sql unit tests //

// send empty string as query 
func EmptyQuery(t *testing.T, db *sql.DB, ctx context.Context){

	rows, err := db.QueryContext(ctx, "")
	if err != nil {
		t.Error(err.Error()) // doesn't err, just prints to stdout
	}

	numRows := 0
	for rows.Next(){
		numRows ++
	}
	rows.Close()

	if numRows != 0 {
		t.Errorf("Shouldn't return any rows")
	}

}

// seend query with sql syntax error 
func SyntaxErrorQuery(t *testing.T, db *sql.DB, ctx context.Context){

	rows, err := db.QueryContext(ctx, "SELECT SELECT * FROM Testa")

	if err != nil {
		t.Errorf(err.Error()) // doesn't err, just prints to stdout
	}

	numRows := 0
	for rows.Next(){
		numRows ++
	}
	rows.Close()

	if numRows != 0 {
		t.Errorf("Shouldn't return any rows")
	}
}


// query that shold return nothing
func ReturnNothingrQuery(t *testing.T, db *sql.DB, ctx context.Context){

	rows, err := db.QueryContext(ctx, "SELECT * FROM Testa WHERE A = \"a60170ae6d93af54ee67b953f7baa767f439dc0c\"")
	if err != nil {
		t.Errorf(err.Error())
	}

	numRows := 0
	for rows.Next(){
		numRows ++
	}
	rows.Close()

	if numRows != 0 {
		t.Errorf("Shouldn't return any rows")
	}
}

// query that should return one tuple
func OneTupleQuery(t *testing.T, db *sql.DB, ctx context.Context){
	rows, err := db.QueryContext(ctx, "SELECT * FROM Testa WHERE A = \"a1\"")
	if err != nil {
		t.Errorf(err.Error())
	}

	numRows := 0
	curr := testaRow{A:"", B:"", C:""}
	for rows.Next(){
		numRows ++
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}

	}
	rows.Close()

	if numRows != 1 {
		t.Errorf("Should have returned exactly one row but got %d", numRows)
	}

	if curr.A != "a1" || curr.B != "b1" || curr.C != "c1"{
		t.Errorf("Got wrong tuple")
	}
}

// should return two tuples
func SubsetQuery(t *testing.T, db *sql.DB, ctx context.Context){

	var expected []testaRow
	var actual []testaRow
	expected = append(expected, testaRow{A:"a1", B:"b1", C:"c1"})
	expected = append(expected, testaRow{A:"a2", B:"b2", C:"c2"})

	rows, err := db.QueryContext(ctx, "SELECT * FROM Testa WHERE A = \"a1\" OR A = \"a2\"")
	if err != nil {
		t.Errorf(err.Error())
	}

	numRows := 0
	for rows.Next(){
		curr := testaRow{A:"", B:"", C:""}
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}
		actual = append(actual, curr)
		numRows ++
	}
	rows.Close()

	if ! testaTupleListEquals(expected, actual) {
		t.Errorf("Unexpected tuples returned")
	}
}

// should return entire table
func WholeTableQuery(t *testing.T, db *sql.DB, ctx context.Context){

	var expected []testaRow
	var actual []testaRow
	expected = append(expected, testaRow{A:"a1", B:"b1", C:"c1"})
	expected = append(expected, testaRow{A:"a2", B:"b2", C:"c2"})
	expected = append(expected, testaRow{A:"a3", B:"b3", C:"c3"})

	rows, err := db.QueryContext(ctx, "SELECT * FROM Testa ORDER BY A")
	if err != nil {
		t.Errorf(err.Error())
	}

	numRows := 0

	for rows.Next(){
		curr := testaRow{A:"", B:"", C:""}
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}
		actual = append(actual, curr)
		numRows ++
	}
	rows.Close()

	if ! testaTupleListEquals(expected, actual) {
		t.Errorf("Unexpected tuples returned")
	}
}

// Should return subset of columns
func ColSubseteQuery(t *testing.T, db *sql.DB, ctx context.Context){

	var expected []testaRow
	var actual []testaRow
	expected = append(expected, testaRow{A:"a1", B:"b1", C:""})

	rows, err := db.QueryContext(ctx, "SELECT A,B FROM Testa WHERE A = \"a1\" ORDER BY A")
	if err != nil {
		t.Errorf(err.Error())
	}

	numRows := 0
	for rows.Next(){
		curr := testaRow{A:"", B:"", C:""}
		if err := rows.Scan(&curr.A, &curr.B); err != nil {
			t.Error(err.Error())
		}
		actual = append(actual, curr)
		numRows ++
	}
	rows.Close()

	if ! testaTupleListEquals(expected, actual) {
		t.Errorf("Unexpected tuples returned")
	}
}


// tests spanner types 
func TestQueryTypes( t *testing.T){

		// set up test table
		curs, err := NewConnector()
		if err != nil{
			log.Fatal(err)
		}
	
		executeDdlApi(curs, []string{`CREATE TABLE TypeTesta (
			stringt	STRING(1024),
			intt  INT64,
			floatt   FLOAT64,
			boolt BOOL
		)	 PRIMARY KEY (stringt)`}) // duct tape 
	
		ExecuteDMLClientLib([]string{`INSERT INTO TypeTesta (stringt, intt, floatt, boolt) 
			VALUES ("aa", 42, 42, TRUE), ("bb", -42, -42, FALSE), 
			("cc", -1, 42, FALSE)`}) // duct tape 
	
		// open db
		ctx := context.Background()
		db, err := sql.Open("spanner", dsn)
		if err != nil {
			debug.PrintStack()
			log.Fatal(err)
		}
	
		// run unit tests 
		AtomicTypeQuery(t, db, ctx)
	
		// drop table 
		executeDdlApi(curs, []string{`DROP TABLE TypeTesta`})
	
		// close  
		curs.Close()
		db.Close()
}

// type unit tests

// check that atomic types read in as expected 
func AtomicTypeQuery(t *testing.T, db *sql.DB, ctx context.Context){

	// general type tests
	want1 := []typeTestaRow{
		{stringt: "aa", intt:42, floatt:42, boolt:true},
		{stringt: "bb", intt:-42, floatt:-42, boolt:false},
	}

	rows, err := db.QueryContext(ctx, 
		"SELECT * FROM TypeTesta WHERE stringt = \"aa\" OR stringt = 'bb' ORDER BY stringt")
	if err != nil {
		t.Errorf(err.Error())
	}

	got1 := []typeTestaRow{}
	numRows := 0
	for rows.Next(){
		curr := typeTestaRow{stringt:"", intt:-1, floatt:-1, boolt:false}
		if err := rows.Scan(&curr.stringt, &curr.intt, &curr.floatt, &curr.boolt); err != nil {
			t.Error(err.Error())
		}
		got1 = append(got1,curr)
		numRows ++
	}
	rows.Close()

	if !reflect.DeepEqual(want1,got1){
		t.Errorf("Unexpected tuples returned \n got1: %#v\nwant: %#v", got1, want1)	
	}

	// NaN, +Inf, -Inf


}


// error: spanner: code = "InvalidArgument", desc = "Statement not supported: CreateTableStatement [at 1:1]
func TestDDLBasic(t *testing.T){

	// open db
	//ctx := context.Background()
	//db, err := sql.Open("spanner", dsn)
	//if err != nil {
	//	log.Fatal(err)
	//}

	// create table  
	//mustExec(t, db ,"CREATE TABLE test (val integer)")
	//mustExecContext(t, ctx, db ,"CREATE TABLE test (val int) PRIMARY KEY val") // spanner syntax
	//mustExecContext(t, ctx, db ,"CREATE TABLE test (val integer)")

	// using client library cause that went to heck

} 


/*
func TestDebug(t *testing.T){
	if 1 == 1 {
		t.Errorf(dsn)
	}
}
*/

// hmm
// when use client lib to make db get error: 
// 