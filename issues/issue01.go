package main


// Very very strange FLOAT64 NaN equality thing

// The NaN value returned by spanner is not equal to the golangs math library NaN
// Even thoguh they have equal bits and types

import (
	"context"
	"log"
	"os"
	"database/sql"
	"runtime/debug"
	"fmt"
	"math"
	"reflect"

	"cloud.google.com/go/spanner"

	// api/lib packages not imported by driver
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"

	_ "github.com/rakyll/go-sql-driver-spanner"
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

var (
	dsn      = "projects/test-project/instances/test-instance/databases/gotest"
	project  = "test-project"
	instance = "test-instance"
	dbname   = "gotest"
)

type sfrow struct {
	id int
	notnum float64
	pinf float64
	ninf float64
}

func main() {

	curs, err := NewConnector()
	if err != nil {
		log.Fatal(err)
	}

	// make table 
	executeDdlApi(curs, []string{`CREATE TABLE SpecialFloat (
		id	INT64,
		notnum	FLOAT64,
		pinf	FLOAT64,
		ninf  FLOAT64,
	)	 PRIMARY KEY (id)`})

	ExecuteDMLClientLib([]string{`INSERT INTO SpecialFloat
		(id, notnum, pinf, ninf) 
		VALUES (0, CAST("nan" AS FLOAT64), CAST("inf" AS FLOAT64), CAST("-inf" AS FLOAT64) )`})

	
	// open db
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}

	// read
	curr := sfrow{id: -1, notnum: -1, pinf: -1, ninf: -1}

	rows, err := db.QueryContext(ctx, "SELECT * FROM SpecialFloat")
	if err != nil {
		log.Fatal(err.Error())
	}

	for rows.Next() {
		if err := rows.Scan(&curr.id, &curr.notnum, &curr.pinf, &curr.ninf); err != nil {
			log.Fatal(err.Error())
		}
	}
	rows.Close()

	inspectSpannerSpecialFloats(curr)

	executeDdlApi(curs, []string{`DROP TABLE SpecialFloat`})
	curs.Close()
	db.Close()

}

// print debigging extravaganza
func inspectSpannerSpecialFloats(curr sfrow){

	fmt.Printf("\nInspecting: %f %f %f \n\n", curr.notnum, curr.pinf, curr.ninf)

	// running equality 
	fmt.Println("\nCheckig equality to math.(nan|inf):")
	if curr.notnum != math.NaN(){fmt.Printf("- spanner NaN != math.NaN() \n")}
	if curr.pinf != math.Inf(1){ fmt.Printf("- spanner +Inf != math.Inf(1) \n")}
	if curr.ninf != math.Inf(-1){fmt.Printf("- spanner -Inf != math.Inf(-1) \n")}

	// bits 
	fmt.Println("\n~~~Checking bits>")
	// NaN bits
	fmt.Printf("\nSpanner NaN vs math.NaN() bits: \n")
	fmt.Println(math.Float64bits(curr.notnum))
	fmt.Println(math.Float64bits(math.NaN()))
	if math.Float64bits(curr.notnum) == math.Float64bits(math.NaN()){
		fmt.Println(">>Equal bits")
	}
	// +Inf bits 
	fmt.Printf("\nSpanner +Inf vs math.Inf(1) bits: \n")
	fmt.Println(math.Float64bits(curr.pinf))
	fmt.Println(math.Float64bits(math.Inf(1)))
	if math.Float64bits(curr.pinf) == math.Float64bits(math.Inf(1)){
		fmt.Println(">>Equal bits")
	}
	// -Inf bits 
	fmt.Printf("\nSpanner -Inf vs math.Inf(1) bits: \n")
	fmt.Println(math.Float64bits(curr.ninf))
	fmt.Println(math.Float64bits(math.Inf(-1)))
	if math.Float64bits(curr.ninf) == math.Float64bits(math.Inf(-1)){
		fmt.Println(">>Equal bits")
	}

	// types 
	fmt.Printf("\n ~~~Checking types>\n")
	// NaN
	fmt.Printf("\nspanner NaN: %s\n", reflect.TypeOf(curr.notnum))
	fmt.Printf("math.NaN(): %s\n", reflect.TypeOf(math.NaN()))
	if reflect.TypeOf(curr.notnum) == reflect.TypeOf(math.NaN()){
		fmt.Println(">> Equal types")
	}

	// +Inf
	fmt.Printf("\nspanner +Inf: %s\n", reflect.TypeOf(curr.pinf))
	fmt.Printf("math.Inf(1): %s\n", reflect.TypeOf(math.Inf(1)))
	if reflect.TypeOf(curr.pinf) == reflect.TypeOf(math.Inf(1)){
		fmt.Println(">> Equal types")
	}

	// -Inf
	fmt.Printf("\nspanner -Inf: %s\n", reflect.TypeOf(curr.ninf))
	fmt.Printf("math.Inf(-1): %s\n", reflect.TypeOf(math.Inf(-1)))
	if reflect.TypeOf(curr.pinf) == reflect.TypeOf(math.Inf(-1)){
		fmt.Println(">> Equal types")
	}



	fmt.Printf("\n\n")
}


func executeDdlApi(curs *Connector, ddls []string) {

	os.Setenv("SPANNER_EMULATOR_HOST", "0.0.0.0:9010")

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
func ExecuteDMLClientLib(dml []string) {

	os.Setenv("SPANNER_EMULATOR_HOST", "0.0.0.0:9010")

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
