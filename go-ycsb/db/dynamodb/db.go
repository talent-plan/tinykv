package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"log"
	"strings"
	"time"
)

type dynamodbWrapper struct {
	client             *dynamodb.Client
	tablename          *string
	primarykey         string
	primarykeyPtr      *string
	readCapacityUnits  int64
	writeCapacityUnits int64
	consistentRead     bool
	deleteAfterRun     bool
	command            string
}

func (r *dynamodbWrapper) Close() error {
	var err error = nil
	if strings.Compare("run", r.command) == 0 {
		log.Printf("Ensuring that the table is deleted after the run stage...\n")
		if r.deleteAfterRun {
			err = r.deleteTable()
			if err != nil {
				log.Printf("Couldn't delete table after run. Here's why: %v\n", err)
			}
		}
	}
	return err
}

func (r *dynamodbWrapper) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *dynamodbWrapper) CleanupThread(_ context.Context) {
}

func (r *dynamodbWrapper) Read(ctx context.Context, table string, key string, fields []string) (data map[string][]byte, err error) {
	data = make(map[string][]byte, len(fields))

	response, err := r.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key:            r.GetKey(key),
		TableName:      r.tablename,
		ConsistentRead: aws.Bool(r.consistentRead),
	})
	if err != nil {
		log.Printf("Couldn't get info about %v. Here's why: %v\n", key, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &data)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return

}

// GetKey returns the composite primary key of the document in a format that can be
// sent to DynamoDB.
func (r *dynamodbWrapper) GetKey(key string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		r.primarykey: &types.AttributeValueMemberB{Value: []byte(key)},
	}
}

func (r *dynamodbWrapper) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *dynamodbWrapper) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	var upd = expression.UpdateBuilder{}
	for name, value := range values {
		upd = upd.Set(expression.Name(name), expression.Value(&types.AttributeValueMemberB{Value: value}))
	}
	expr, err := expression.NewBuilder().WithUpdate(upd).Build()

	_, err = r.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                       r.GetKey(key),
		TableName:                 r.tablename,
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		log.Printf("Couldn't update item to table. Here's why: %v\nUpdateExpression:%s\nExpressionAttributeNames:%s\n", err, *expr.Update(), expr.Names())
	}
	return
}

func (r *dynamodbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	values[r.primarykey] = []byte(key)
	item, err := attributevalue.MarshalMap(values)
	if err != nil {
		panic(err)
	}
	_, err = r.client.PutItem(context.TODO(),
		&dynamodb.PutItemInput{
			TableName: r.tablename, Item: item,
		})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
	}
	return
}

func (r *dynamodbWrapper) Delete(ctx context.Context, table string, key string) error {
	_, err := r.client.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: r.tablename,
		Key:       r.GetKey(key),
	})
	return err
}

type dynamoDbCreator struct{}

// TableExists determines whether a DynamoDB table exists.
func (r *dynamodbWrapper) tableExists() (bool, error) {
	exists := true
	_, err := r.client.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: r.tablename},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", *r.tablename)
			err = nil
		} else {
			log.Printf("Couldn't determine existence of table %v. Here's why: %v\n", *r.tablename, err)
		}
		exists = false
	}
	return exists, err
}

// This function uses NewTableExistsWaiter to wait for the table to be created by
// DynamoDB before it returns.
func (r *dynamodbWrapper) createTable() (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	table, err := r.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: r.primarykeyPtr,
			AttributeType: types.ScalarAttributeTypeB,
		}},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: r.primarykeyPtr,
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName: r.tablename,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(r.readCapacityUnits),
			WriteCapacityUnits: aws.Int64(r.writeCapacityUnits),
		},
	})
	if err != nil {
		log.Printf("Couldn't create table %v. Here's why: %v\n", *r.tablename, err)
	} else {
		log.Printf("Waiting for table to be available.\n")
		waiter := dynamodb.NewTableExistsWaiter(r.client)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: r.tablename}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
		tableDesc = table.TableDescription
	}
	return tableDesc, err
}

func (r dynamoDbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rds := &dynamodbWrapper{}

	rds.tablename = aws.String(p.GetString(tablename, tablenameDefault))
	// other than the primary key, you do not need to define
	// any extra attributes or data types when you create a table.
	rds.primarykey = p.GetString(primaryKeyFieldName, primaryKeyFieldNameDefault)
	rds.primarykeyPtr = aws.String(rds.primarykey)
	rds.readCapacityUnits = p.GetInt64(readCapacityUnitsFieldName, readCapacityUnitsFieldNameDefault)
	rds.writeCapacityUnits = p.GetInt64(writeCapacityUnitsFieldName, writeCapacityUnitsFieldNameDefault)
	rds.consistentRead = p.GetBool(consistentReadFieldName, consistentReadFieldNameDefault)
	rds.deleteAfterRun = p.GetBool(deleteTableAfterRunFieldName, deleteTableAfterRunFieldNameDefault)
	endpoint := p.GetString(endpointField, endpointFieldDefault)
	region := p.GetString(regionField, regionFieldDefault)
	rds.command, _ = p.Get(prop.Command)
	var err error = nil
	var cfg aws.Config
	if strings.Contains(endpoint, "localhost") && strings.Compare(region, "localhost") != 0 {
		log.Printf("given you're using dynamodb local endpoint you need to specify -p %s='localhost'. Ignoring %s and enforcing -p %s='localhost'\n", regionField, region, regionField)
		region = "localhost"
	}
	if strings.Compare(endpoint, endpointFieldDefault) == 0 {
		if strings.Compare(region, regionFieldDefault) != 0 {
			// if endpoint is default but we have region
			cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
		} else {
			// if both endpoint and region are default
			cfg, err = config.LoadDefaultConfig(context.TODO())
		}
	} else {
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
			config.WithEndpointResolver(aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					return aws.Endpoint{URL: endpoint, SigningRegion: region}, nil
				})),
		)
	}
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	// Create DynamoDB client
	rds.client = dynamodb.NewFromConfig(cfg)
	exists, err := rds.tableExists()

	if strings.Compare("load", rds.command) == 0 {
		if !exists {
			_, err = rds.createTable()
		} else {
			ensureCleanTable := p.GetBool(ensureCleanTableFieldName, ensureCleanTableFieldNameDefault)
			if ensureCleanTable {
				log.Printf("dynamo table named %s already existed. Deleting it...\n", *rds.tablename)
				_ = rds.deleteTable()
				_, err = rds.createTable()
			} else {
				log.Printf("dynamo table named %s already existed. Skipping table creation.\n", *rds.tablename)
			}
		}
	} else {
		if !exists {
			log.Fatalf("dynamo table named %s does not exist. You need to run the load stage previous than '%s'...\n", *rds.tablename, "run")
		}
	}
	return rds, err
}

func (rds *dynamodbWrapper) deleteTable() error {
	_, err := rds.client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: rds.tablename,
	})
	if err != nil {
		log.Fatalf("Unable to delete table, %v", err)
	}
	waiter := dynamodb.NewTableNotExistsWaiter(rds.client)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: rds.tablename}, 5*time.Minute)
	if err != nil {
		log.Fatalf("Wait for table deletion failed. Here's why: %v", err)
	}
	return err
}

const (
	tablename                          = "dynamodb.tablename"
	tablenameDefault                   = "ycsb"
	primaryKeyFieldName                = "dynamodb.primarykey"
	primaryKeyFieldNameDefault         = "_key"
	readCapacityUnitsFieldName         = "dynamodb.rc.units"
	readCapacityUnitsFieldNameDefault  = 10
	writeCapacityUnitsFieldName        = "dynamodb.wc.units"
	writeCapacityUnitsFieldNameDefault = 10
	ensureCleanTableFieldName          = "dynamodb.ensure.clean.table"
	ensureCleanTableFieldNameDefault   = true
	endpointField                      = "dynamodb.endpoint"
	endpointFieldDefault               = ""
	regionField                        = "dynamodb.region"
	regionFieldDefault                 = ""
	// GetItem provides an eventually consistent read by default.
	// If your application requires a strongly consistent read, set ConsistentRead to true.
	// Although a strongly consistent read might take more time than an eventually consistent read, it always returns the last updated value.
	consistentReadFieldName             = "dynamodb.consistent.reads"
	consistentReadFieldNameDefault      = false
	deleteTableAfterRunFieldName        = "dynamodb.delete.after.run.stage"
	deleteTableAfterRunFieldNameDefault = false
)

func init() {
	ycsb.RegisterDBCreator("dynamodb", dynamoDbCreator{})
}
