
package main 

import(
	"os"
	"context"
	"log"
	"database/sql"
	"runtime/debug"
	"fmt"

	"cloud.google.com/go/spanner"

	// api/lib packages not imported by driver
	"google.golang.org/grpc"
	"google.golang.org/api/option"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	_ "github.com/rakyll/go-sql-driver-spanner"
)



// conn things 
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


var(
	dsn = "projects/test-project/instances/test-instance/databases/gotest"
	project = "test-project"
	instance = "test-instance"
	dbname = "gotest"
)

func main(){


	curs, err := NewConnector()
	if err != nil{
		log.Fatal(err)
	}

	// tiny
	executeDdlApi(curs, []string{`CREATE TABLE Tiny (
		t	STRING(1024),
	)	 PRIMARY KEY (t)`}) 

	ExecuteDMLClientLib([]string{`INSERT INTO Tiny (t) 
		VALUES ("smol"), ("Bean") `}) 

	// typetesta
	executeDdlApi(curs, []string{`CREATE TABLE TypeTesta (
		stringt	STRING(1024),
		bytest BYTES(1024),
		intt  INT64,
		floatt   FLOAT64,
		boolt BOOL
	)	 PRIMARY KEY (stringt)`}) 

	ExecuteDMLClientLib([]string{`INSERT INTO TypeTesta (stringt, bytest, intt, floatt, boolt) 
		VALUES ("hello", CAST("hello" as bytes), 42, 42, TRUE), ("there", CAST("there" as bytes), 42, 42, TRUE) `})
	

	// open db
	ctx := context.Background()
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		debug.PrintStack()
		log.Fatal(err)
	}


	// go through typetesta 
	rowss, err := db.QueryContext(ctx, "SELECT * FROM Typetesta")
	if err != nil {
		log.Fatal(err.Error())
	}
	numRows := 0
	for rowss.Next(){
		numRows ++
	}
	rowss.Close()
	fmt.Printf("Rows in typetesta: %d \n", numRows)


	// go through tiny
	rows, err := db.QueryContext(ctx, "SELECT * FROM Tiny")
	if err != nil {
		log.Fatalf(err.Error())
	}
	numRows = 0
	for rows.Next(){
		numRows ++
	}
	rows.Close()
	fmt.Printf("Rows in tiny: %d \n", numRows)


	executeDdlApi(curs, []string{`DROP TABLE Tiny`})
	executeDdlApi(curs, []string{`DROP TABLE TypeTesta`})
	curs.Close()
	db.Close()

}



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