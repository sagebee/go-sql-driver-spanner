package main

// problem running DDL

import (
	"context"
	"database/sql"
	"fmt"
	"runtime/debug"
	"log"
	"time"
	//"github.com/pkg/errors"

	_ "github.com/rakyll/go-sql-driver-spanner"
)

func main() {

	AttemptOne()
	//AttemptTwo()
	// AttemptThree()
}

// use exec context (as suggested in doc)
// context with timeout 
func AttemptOne(){

	// open 
	db, err := sql.Open(
		"spanner",
		"projects/test-project/instances/test-instance/databases/gotest",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	_, err = db.ExecContext(
		ctx,
		`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
	)

	if err != nil {
		fmt.Println("Trying to traceback ~ ")
		fmt.Printf("Type of err: %T \n", err)
		fmt.Printf("Val err: %#v \n", err)

		log.Fatal("Bye")
	}

	log.Print("Created tables.")
}

// use exec context (as suggested in doc)
// context no timeout 
func AttemptTwo(){

	// open 
	db, err := sql.Open(
		"spanner",
		"projects/test-project/instances/test-instance/databases/gotest",
	)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.ExecContext(
		ctx,
		`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
	)

	debug.PrintStack()
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Created tables.")
}

// use exec 
func AttemptThree(){
	
	// open 
	db, err := sql.Open(
		"spanner",
		"projects/test-project/instances/test-instance/databases/gotest",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(
		`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
	)

	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	log.Print("Created tables.")
}
