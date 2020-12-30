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

	"cloud.google.com/go/spanner"

	// not imported by driver
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var(
	project string
	instance string
	dbname string
	dsn string
)


// cursor things to connect to database 
type Cursor struct {
	ctx         context.Context	
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewCursor()(*Cursor, error){

	ctx := context.Background()

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
			return nil, err
	}

	dataClient, err := spanner.NewClient(ctx, dsn)
	if err != nil {
			return nil,err
	}

	curs := &Cursor{
		ctx: ctx,
		client: dataClient,
		adminClient: adminClient,

	}
	return curs,nil
}

func (c *Cursor) Close() {
	c.client.Close()
	c.adminClient.Close()
}



// setup //

// 
func init(){

	// get environment variables
	instance = os.Getenv("SPANNER_TEST_INSTANCE")
	project = os.Getenv("SPANNER_TEST_PROJECT")
	dbname = os.Getenv("SPANNER_TEST_DBNAME")

	// set defaults if none provided 
	if dbname == "" { dbname = "gotest" }
	if instance == "" {instance = "test-instance" }
	if project == "" { project = "test-project" }

	// derive data source name 
	dsn = "projects/" + project + "/instances/" + instance + "/databases/" + dbname


}

// run tests & tear down //
func TestMain(m *testing.M){

	// build test db
	//createEmptyDatabaseCliLib(project,instance,dbname)

	// open cursor
	curs, err := NewCursor()
	if err != nil{
		log.Fatal(err)
	}
	

	// ddl
	executeDdlApi(curs, []string{`CREATE TABLE Singerssss (
		SingerId   INT64 NOT NULL,
		FirstName  STRING(1024),
		LastName   STRING(1024),
		SingerInfo BYTES(MAX)
)	 PRIMARY KEY (SingerId)`})

	// run tests
	exitval := m.Run()

	// tear down test db & exit
	os.Exit(exitval)

}

// helper funs //


// functions that use the client lib / apis ~ 
// ******************* //

func executeDdlApi(curs *Cursor, ddls []string){

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



// error: IAM errors or malformed request 
/*
func createEmptyDatabaseCliLib(project,instance,dbname string){

	// set up client
	//ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	//defer cancel()

	ctx := context.Background();
	adminClient, err := adminapi.NewDatabaseAdminClient(ctx);
	if err != nil { log.Fatal(err); }


	// parent
	var parentstr = "projects/"+project+"/instances/"+instance;

		// debug
		fmt.Println("XXXXX " + dbname)
		fmt.Println("YYYYYY CREATE DATABASE `" + dbname + "`")
		fmt.Println("ZZZZZZ {" + parentstr +"}")
		fmt.Println("ENV "+ os.Getenv("SPANNER_EMULATOR_HOST"))

	
	// create db, read in file
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &adminapi.CreateDatabaseRequest{
		Parent:          parentstr,
		CreateStatement: "CREATE DATABASE `" + dbname + "`",
		ExtraStatements: []string{
				`CREATE TABLE Singers (
						SingerId   INT64 NOT NULL,
						FirstName  STRING(1024),
						LastName   STRING(1024),
						SingerInfo BYTES(MAX)
				) PRIMARY KEY (SingerId)`,
				`CREATE TABLE Albums (
						SingerId     INT64 NOT NULL,
						AlbumId      INT64 NOT NULL,
						AlbumTitle   STRING(MAX)
				) PRIMARY KEY (SingerId, AlbumId),
				INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
		},
	})

	if err != nil {
			debug.PrintStack();
			log.Fatal(err)
	}
	if _, err := op.Wait(ctx); err != nil {
			debug.PrintStack();
			log.Fatal(err)
	}
	fmt.Println( "Created database +", dbname)
}


*/

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

func mustExec(t * testing.T, db *sql.DB, query string){
	_,err := db.Exec(query)
	if err != nil {
		debug.PrintStack()
		t.Fatalf(err.Error())
	}
}

//func mustQueryContext(t * testing.T, ctx context.Context, db *sql.DB, query string){


//}



//  #### tests ####  // 

// basic tests  ~

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


func TestQueryBasic(t *testing.T){

	// open db
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT * FROM xx ")
	if err != nil {
		log.Fatal(err)
	}
	var (
		val   int64
	)
	for rows.Next() {
		if err := rows.Scan(&val); err != nil {
			log.Fatal(err)
		}
		fmt.Println(val)

		if val != 1{
			t.Error(val)
		}
	}
	rows.Close()

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
// 	- "transport: Error while dialing dial tcp: i/o timeout"





