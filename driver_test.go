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

	//"fmt"
	//"log"
	//database "cloud.google.com/go/spanner/admin/database/apiv1"
	//adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var(
	project string
	instance string
	dbname string
	dsn string
)

// setup //
func init(){

	// get environment variables
	instance = os.Getenv("SPANNER_TEST_INSTANCE")
	project = os.Getenv("SPANNER_TEST_PROJECT")

	// get test db name, use defaults if not provided 
	dbname = os.Getenv("SPANNER_TEST_DBNAME")
	if dbname == ""{
		dbname = "gotest"
	}

	// derive data source name 
	dsn = "projects/" + project + "/instances/" + instance + "/databases/" + dbname

}

// run tests & tear down //
func TestMain(m *testing.M){

	// build test db
	//createEmptyDatabase(project,instance,dbname)

	// run tests
	exitval := m.Run()

	// tear down test db & exit
	os.Exit(exitval)

}

// helper funs for main //
/*
func createEmptyDatabase(project,instance,dbname string){

	// set up client
	//ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	//defer cancel()

	ctx := context.Background();
	adminCli, err := database.NewDatabaseAdminClient(ctx);
	if err != nil { log.Fatal(err); }

	// parent
	var parentstr = "projects/"+project+"/instances/"+instance;

	
	// create db, read in file
	op, err := adminCli.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          parentstr,
		CreateStatement: "CREATE DATABASE `" + dbname + "`",
		ExtraStatements: nil,
	})
	if err != nil {
			log.Fatal(err)
	}
	if _, err := op.Wait(ctx); err != nil {
			log.Fatal(err)
	}
	fmt.Println( "Created database +", dbname)
	
}
*/


// helper funs for tests //


func mustExecContext(t * testing.T, ctx context.Context, db *sql.DB, query string){
	_,err := db.ExecContext(ctx, query)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func mustExec(t * testing.T, db *sql.DB, query string){
	_,err := db.Exec(query)
	if err != nil {
		t.Fatalf(err.Error())
	}
}



// tests // 

func TestDDLBasic(t *testing.T){

	// open db
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}

	// create table
	mustExec(t, db ,"CREATE TABLE test (value BOOL)")
	mustExecContext(t, ctx, db ,"CREATE TABLE test (value BOOL)")

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





