# Overview

### What Carpenter is

Carpenter is a set of tools designed to help you play with Dynamo tables on your local machine. 

### What Carpenter is NOT

Carpenter is NOT designed to be used to manage your infrastructure against the real AWS DynamoDB service. For that use infrastructure as code tools like terraform.

## Carpenter functions

### createTable
Carpenter creates a table. If a name is not provided through the *tableName* parameter Carpenter will generate a uuid name. Is it also possible to create secondary indexes with the table via *gsiNumber* and *lsiNumber* parameter. For each global secondary index a couple of additional parameters (GSI<_number_>PK and GSI<_number_>SK) will be created. For local secondary indexes only one additional parameter will be created (LSI<_number_>SK);

### deleteTable
Carpenter deletes a table, a table name must be provided.

### listTables
Carpenter provides a list of the tables available in Dynamo. 

### dynamoUp
Carpenter starts *instructure/dynamo-local-admin* Dynamo docker image which is based on amazon/dynamodb-local image and provides a nice GUI accessible at http://localhost:<_dynamoPort_>. A *dynamoPort* parameter can also be provided. 

### dynamoDown
Carpenter stops Dynamo.

### scanTable
Carpenter scan a table and provide a count of the items found in the table. The scan is recursive, a table name must be provided. If you also want to return a list of all the documents provide *--returnDocuments* set to true. 

### truncate
Carpenter truncates a table. A table name must be provided.

## Usage
Install carpenter:

```
npm i -g @pikselpalette/carpenter
```

Use carpenter:

```
carpenter --help
```

Example of a create table:

```
carpenter --function createTable --tableName identity_local --gsiNumber 15 --lsiNumber 5
```