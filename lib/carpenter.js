const AWS = require('aws-sdk');
const _ = require('lodash');
const UUID = require('uuid');

const generateIndexNames = (indexType, indexNumber) => {
  const indexNames = [];
  for (let i = 0; i < indexNumber; i++) {
    indexNames.push(`${indexType}${i + 1}`);
  }
  return indexNames;
}

class Carpenter {
  constructor(tableName = null, dynamoPort = null) {
    const awsConfig = {
      region: 'local',
      endpoint: 'http://localhost:8000',
      accessKeyId: 'unittest',
      secretAccessKey: 'letmein',
      maxRetries: 5
    };
    if (dynamoPort) {
      awsConfig.endpoint = `http://localhost:${dynamoPort}`;
    }
    AWS.config.update(awsConfig);
    AWS.config.setPromisesDependency();
    this.tableName = tableName || UUID.v4();
    this.dynamoClient = new AWS.DynamoDB({ apiVersion: '2012-08-10' });
  }

  async createTable(partitionKeyName, sortKeyName, gsiNumber = 0, lsiNumber = 0) {
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
      TableName: this.tableName
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
    await this.dynamoClient.createTable(creationParams).promise();
  }

  async listTables() {
    const { TableNames } = await this.dynamoClient.listTables({}).promise();
    return TableNames;
  }

  async deleteTable () {
    const params = {
      TableName: this.tableName
    };
    await this.dynamoClient.deleteTable(params).promise();
  }

  async truncateTable(partitionKeyName, sortKeyName) {
    const deleteItem = (item) => {
      const params = { Key: item, TableName: this.tableName, ReturnValues: 'ALL_OLD' };
      // TODO could do batch delete instead of 1 by 1
      return this.dynamoClient.deleteItem(params).promise();
    };
    const { Items } = await this.scanTable(partitionKeyName, sortKeyName);
    const deletePromises = Items.map(i => deleteItem(i));
    await Promise.all(deletePromises);
  }

  async scanTable(partitionKeyName, sortKeyName) {
    const params = {
      TableName: this.tableName,
      AttributesToGet: [partitionKeyName, sortKeyName],
      Select: 'SPECIFIC_ATTRIBUTES'
    };
    const itemsRes = [];
    let res = await this.dynamoClient.scan(params).promise();
    itemsRes.push(...res.Items);
    while (res.LastEvaluatedKey) {
      params.ExclusiveStartKey = res.LastEvaluatedKey;
      res = await this.dynamoClient.scan(params).promise();
      itemsRes.push(...res.Items);
    }
    return {
      Items: itemsRes
    };
  }
}

module.exports = Carpenter;
