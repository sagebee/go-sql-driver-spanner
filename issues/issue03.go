package main

// Null read problems

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	//"time"
	"cloud.google.com/go/spanner"
	"os"

	_ "github.com/rakyll/go-sql-driver-spanner"

	// API/lib packages not imported by driver.
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

var (
	dsn = "projects/test-project/instances/test-instance/databases/gotest"
)

func main() {

	// Build table.
	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}
	defer curs.Close()

	// Open db.
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	executeDdlApi(curs, []string{`CREATE TABLE TestQueryType (
		key	STRING(1024),
		stringt	STRING(1024),
		intt  INT64,
		floatt   FLOAT64,
		boolt BOOL
	)	 PRIMARY KEY (stringt)`})

	err = ExecuteDMLClientLib([]string{`INSERT INTO TestQueryType (key, stringt,intt, floatt, boolt) 
		VALUES ("null", null ,null, null, null)`})
	if err != nil {
		log.Fatal(err)
	}



	ReadIntoRegular(db,ctx)
	ReadIntoPointers(db,ctx)
	ReadIntoNullable(db, ctx)
	ReadIntoNullablePointer(db, ctx)


	// drop table
	executeDdlApi(curs, []string{`DROP TABLE TestQueryType`})
}

func ReadIntoRegular(db *sql.DB, ctx context.Context){

	type regularRow struct {
		key     string
		stringt string
		intt    int
		floatt  float64
		boolt   bool
	}

	// filled w junk to see if it overrides or leaves as is
	curr := regularRow{stringt :"hi", intt: 42, floatt: 42, boolt: true}

	rows, err := db.QueryContext(ctx, "SELECT * FROM TestQueryType where key = 'null'")
	if err != nil {
		log.Fatal(err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&curr.key, &curr.stringt, &curr.intt, &curr.floatt, &curr.boolt); err != nil {
			log.Fatal(err.Error())
		}
	}
	rows.Close()

	fmt.Println("\nRead null vals from database into regular typess>")
	fmt.Printf("string: %s int: %d float %f bool: %t\n",
		curr.stringt, curr.intt, curr.floatt, curr.boolt)
	fmt.Printf("\n\n")

}

func ReadIntoPointers(db *sql.DB, ctx context.Context){

	type pointerRow struct {
		key     string
		stringt *string
		intt    *int
		floatt  *float64
		boolt   *bool
	}

	dnum := 42
	fnum := 42.0
	tru := true
	strin := "hi"
	curr := pointerRow{stringt: &strin, intt: &dnum, floatt: &fnum, boolt: &tru}

	rows, err := db.QueryContext(ctx, "SELECT * FROM TestQueryType where key = 'null'")
	if err != nil {
		log.Fatal(err.Error())
	}
	for rows.Next() {
		if err := rows.Scan(&curr.key, &curr.stringt, &curr.intt, &curr.floatt, &curr.boolt); err != nil {
			log.Fatal(err.Error())
		}
	}
	rows.Close()

	fmt.Println("Read null vals from database into pointers>")
	fmt.Printf("string: %s int: %d float %f bool: %t\n",
		*curr.stringt, *curr.intt, *curr.floatt, *curr.boolt)
	fmt.Printf("\n\n")
}

func ReadIntoNullablePointer(db *sql.DB, ctx context.Context){

	type nullableRow struct {
		key     string
		stringt *sql.NullString
		intt    *sql.NullInt64
		floatt  *sql.NullFloat64
		boolt   *sql.NullBool
	}

		// read null into sql nullab;e vals 
		nint := sql.NullInt64{Int64: 0, Valid:false}
		nstrin := sql.NullString{String:"hi", Valid:false}
		currn := nullableRow{intt: &nint, stringt: &nstrin}


		rows, err := db.QueryContext(ctx, "SELECT * FROM TestQueryType where key = 'null'")
		if err != nil {
			log.Fatal(err.Error())
		}
		for rows.Next() {
			if err := rows.Scan(&currn.key, &currn.stringt, &currn.intt, &currn.floatt, &currn.boolt); err != nil {
				log.Fatal(err.Error())
			}
		}
		rows.Close()
	
		fmt.Println("Read null vals from database into sql nullable struct with pointers>")
		fmt.Printf("string: %v int: %v float %v bool: %v\n",
			*currn.stringt, *currn.intt, *currn.floatt, *currn.boolt)
		fmt.Printf("\n\n")

}

func ReadIntoNullable(db *sql.DB, ctx context.Context){

	type nullableRow struct {
		key     string
		stringt sql.NullString
		intt    sql.NullInt64
		floatt  sql.NullFloat64
		boolt   sql.NullBool
	}

		// read null into sql nullab;e vals 
		currn := nullableRow{intt: sql.NullInt64{Int64: 0, Valid:false}, stringt: sql.NullString{String:"hi", Valid:false}}


		rows, err := db.QueryContext(ctx, "SELECT * FROM TestQueryType where key = 'null'")
		if err != nil {
			log.Fatal(err.Error())
		}
		for rows.Next() {
			if err := rows.Scan(&currn.key, &currn.stringt, &currn.intt, &currn.floatt, &currn.boolt); err != nil {
				log.Fatal(err.Error())
			}
		}
		rows.Close()
	
		fmt.Println("Read null vals from database into sql nullable struct without pointers>")
		fmt.Printf("string: %v int: %v float %v bool: %v\n",
			currn.stringt, currn.intt, currn.floatt, currn.boolt)
		fmt.Printf("\n\n")

}


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
		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}
