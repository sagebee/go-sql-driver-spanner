package main

// Null read problems

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	//"time"
	"os"
	"cloud.google.com/go/spanner"

	_ "github.com/rakyll/go-sql-driver-spanner"

		// API/lib packages not imported by driver.
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

var(
	dsn = "projects/test-project/instances/test-instance/databases/gotest"
)

func main() {

	// Build table.  
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
		("nullbytes", null, 42, 42, TRUE),
		("nullint", CAST("nullint" as bytes), null, 42, TRUE),
		("nullfloat", CAST("nullfloat" as bytes), 42, null, TRUE),
		("nullbool", CAST("nullbool" as bytes), 42, 42, null)`})


		// null bytes




		// drop table 
		executeDdlApi(curs, []string{`DROP TABLE TestQueryType`})
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

	dsn := "projects/test-project/instances/test-instance/databass/gotest"

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