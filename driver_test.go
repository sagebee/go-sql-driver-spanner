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

// structs for row data 
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


// functions that use the client lib / apis ~ 
// ******************* //

// Executes DDL statements 
func executeDdlApi(curs *Connector, ddls []string){

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
func ExecuteDMLClientLib(dml []string){

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

// Tests general query functionality 
func TestQueryGeneral(t *testing.T){

	// set up test table
	curs, err := NewConnector()
	if err != nil{
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
		{input: "SELECT * FROM Testa WHERE A = \"a1\"", 
		want: []testaRow{
			{A:"a1", B:"b1", C:"c1"},
		}},

		{input: "SELECT * FROM Testa ORDER BY A", 
		want: []testaRow{
			{A:"a1", B:"b1", C:"c1"},
			{A:"a2", B:"b2", C:"c2"},
			{A:"a3", B:"b3", C:"c3"},
		}},
	}

	// run tests
	for _, tc := range tests {
		got := RunQueryGeneral(t, tc.input)
        if !reflect.DeepEqual(tc.want, got) {
            t.Errorf("expected: %v, got: %v", tc.want, got)
		}
	}

	// drop table 
	executeDdlApi(curs, []string{`DROP TABLE Testa`});

	// close connection 
	curs.Close()
}

// runs query on Testa table, returns result in testaRow array 
func RunQueryGeneral(t *testing.T, query string,)([]testaRow){

	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	rows := mustQueryContext(t, ctx, db, query)

	got := []testaRow{}
	for rows.Next(){
		curr := testaRow{A:"", B:"", C:""}
		if err := rows.Scan(&curr.A, &curr.B, &curr.C); err != nil {
			t.Error(err.Error())
		}
		got = append(got, curr)
	}
	rows.Close()

	db.Close()
	return got
}