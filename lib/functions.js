const AWS = require('aws-sdk');
const _ = require('lodash');

const awsConfig = {
  region: 'local',
  endpoint: 'http://localhost:8000',
  accessKeyId: 'unittest',
  secretAccessKey: 'letmein',
  maxRetries: 5
};

const dynamoOptions = { apiVersion: '2012-08-10' };

const getDynamoClient = (dynamoPort) => {
  if (dynamoPort) {
    awsConfig.endpoint = `http://localhost:${dynamoPort}`;
  }
  AWS.config.update(awsConfig);
  AWS.config.setPromisesDependency();
  return new AWS.DynamoDB(dynamoOptions);
}

const generateIndexNames = (indexType, indexNumber) => {
  const indexNames = [];
  for (let i = 0; i < indexNumber; i++) {
    indexNames.push(`${indexType}${i + 1}`);
  }
  return indexNames;
}

const createTable = async (tableName, gsiNumber, lsiNumber, partitionKeyName, sortKeyName, dynamoPort = null) => {
  const dynamoClient = getDynamoClient(dynamoPort);

  const gsiNames = generateIndexNames('GSI', gsiNumber);
  const lsiNames = generateIndexNames('LSI', lsiNumber);

  const creationParams = {
    KeySchema: [
      { AttributeName: partitionKeyName, KeyType: 'HASH' },
      { AttributeName: sortKeyName, KeyType: 'RANGE' }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5, // TODO make it configurable
      WriteCapacityUnits: 5 // TODO make it configurable
    },
    AttributeDefinitions: [
      { AttributeName: partitionKeyName, AttributeType: 'S' }, // TODO make it configurable
      { AttributeName: sortKeyName, AttributeType: 'S' } // TODO make it configurable
    ],
    TableName: tableName
  };
  if (gsiNames.length > 0) {
    creationParams.GlobalSecondaryIndexes = gsiNames.map(name => ({
      IndexName: name,
      KeySchema: [
        {
          AttributeName: `${name}PK`, // TODO index names should be configurable
          KeyType: 'HASH'
        },
        {
          AttributeName: `${name}SK`,
          KeyType: 'RANGE'
        }
      ],
      Projection: {
        ProjectionType: 'ALL'
      },
      ProvisionedThroughput: {
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5
      }
    }));
    gsiNames.forEach((name) => {
      creationParams.AttributeDefinitions.push({
        AttributeName: `${name}PK`, AttributeType: 'S'
      });
      creationParams.AttributeDefinitions.push({
        AttributeName: `${name}SK`, AttributeType: 'S'
      });
    });
  }
  if (lsiNames.length) {
    creationParams.LocalSecondaryIndexes = lsiNames.map(name => ({
      IndexName: name,
      KeySchema: [
        {
          AttributeName: partitionKeyName,
          KeyType: 'HASH'
        },
        {
          AttributeName: `${name}SK`,
          KeyType: 'RANGE'
        }
      ],
      Projection: {
        ProjectionType: 'ALL'
      }
    }));
    lsiNames.forEach((name) => {
      creationParams.AttributeDefinitions.push({
        AttributeName: `${name}SK`, AttributeType: 'S'
      });
    });
  }
  await dynamoClient.createTable(creationParams).promise();
};

const listTables = async (dynamoPort = null) => {
  const dynamoClient = getDynamoClient(dynamoPort);
  const { TableNames } = await dynamoClient.listTables({}).promise();
  return TableNames;
};

const deleteTable = async (tableName, dynamoPort) => {
  const dynamoClient = getDynamoClient(dynamoPort);
  const params = {
    TableName: tableName
  };
  await dynamoClient.deleteTable(params).promise();
};

const truncateTable = async (tableName, partitionKeyName, sortKeyName, dynamoPort) => {
  const dynamoClient = getDynamoClient(dynamoPort);
  const deleteItem = (item) => {
    const params = { Key: item, TableName: tableName, ReturnValues: 'ALL_OLD' };
    // TODO could do batch delete instead of 1 by 1
    return dynamoClient.deleteItem(params).promise();
  };
  const { Items } = await scanTable(tableName, partitionKeyName, sortKeyName, dynamoPort);
  const deletePromises = Items.map(i => deleteItem(i));
  await Promise.all(deletePromises);
};

const scanTable = async (tableName, partitionKeyName, sortKeyName, dynamoPort) => {
  const dynamoClient = getDynamoClient(dynamoPort);
  const params = {
    TableName: tableName,
    AttributesToGet: [partitionKeyName, sortKeyName],
    Select: 'SPECIFIC_ATTRIBUTES'
  };
  const itemsRes = [];
  let res = await dynamoClient.scan(params).promise();
  itemsRes.push(...res.Items);
  while (res.LastEvaluatedKey) {
    params.ExclusiveStartKey = res.LastEvaluatedKey;
    res = await dynamoClient.scan(params).promise();
    itemsRes.push(...res.Items);
  }
  return {
    Items: itemsRes
  };
}


module.exports = {
  createTable: createTable,
  deleteTable: deleteTable,
  listTables: listTables,
  scanTable: scanTable,
  truncateTable: truncateTable
};
