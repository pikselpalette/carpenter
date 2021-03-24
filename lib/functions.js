const AWS = require('aws-sdk');
const {
  Constants: { DYNAMO_OPTIONS, PARTITION_KEY_NAME, SORT_KEY_NAME },
  DynamoManagement
} = require('@sequoia/lib-engine-common-dynamo');
const _ = require('lodash');

const awsConfig = {
  region: 'local',
  endpoint: 'http://localhost:8000',
  accessKeyId: 'unittest',
  secretAccessKey: 'letmein',
  maxRetries: 5
};

const getDynamoClient = (dynamoPort) => {
  if (dynamoPort) {
    awsConfig.endpoint = `http://localhost:${dynamoPort}`;
  }
  AWS.config.update(awsConfig);
  AWS.config.setPromisesDependency();
  return new AWS.DynamoDB(DYNAMO_OPTIONS);
}

const createTable = async (tableName, gsiNumber, lsiNumber, dynamoPort = null) => {
  if (dynamoPort) {
    awsConfig.endpoint = `http://localhost:${dynamoPort}`;
  }

  const gsiNames = [];
  const lsiNames = [];

  for (let i = 0; i < gsiNumber; i++) {
    gsiNames.push(`GSI${i + 1}`);
  }

  for (let j = 0; j < lsiNumber; j++) {
    lsiNames.push(`LSI${j + 1}`);
  }

  const dynamoManagement = new DynamoManagement(awsConfig);

  const tableConfig = {
    tableName: tableName
  };

  if (!_.isEmpty(gsiNames)) {
    tableConfig.gsiNames = gsiNames;
  }

  if (!_.isEmpty(lsiNames)) {
    tableConfig.lsiNames = lsiNames;
  }

  await dynamoManagement.createTable(tableConfig);
};

const listTables = async (dynamoPort = null) => {
  const dynamoClient = getDynamoClient(dynamoPort);
  const { TableNames } = await dynamoClient.listTables({}).promise();
  return TableNames;
};

const deleteTable = async (tableName, dynamoPort = null) => {
  if (dynamoPort) {
    awsConfig.endpoint = `http://localhost:${dynamoPort}`;
  }
  const dynamoManagement = new DynamoManagement(awsConfig);
  await dynamoManagement.deleteTable(tableName);
};

const truncateTable = async (tableName, dynamoPort = null) => {
  if (dynamoPort) {
    awsConfig.endpoint = `http://localhost:${dynamoPort}`;
  }
  const dynamoManagement = new DynamoManagement(awsConfig);
  await dynamoManagement.clearDownSeeds(tableName);
};

const scanTable = async (tableName, lastEvaluatedKey = null, dynamoPort = null, dynamoClient = null) => {
  if (!dynamoClient) {
    dynamoClient = getDynamoClient(dynamoPort);
  }
  const params = {
    TableName: tableName,
    AttributesToGet: [PARTITION_KEY_NAME, SORT_KEY_NAME],
    Select: 'SPECIFIC_ATTRIBUTES'
  };
  if (lastEvaluatedKey) {
    params.ExclusiveStartKey = lastEvaluatedKey;
  }
  const { Items: items, LastEvaluatedKey } = await dynamoClient.scan(params).promise();
  if (LastEvaluatedKey) {
    const { Items: newItems } = await scanTable(tableName, LastEvaluatedKey, dynamoPort, dynamoClient);
    return {
      Items: items.concat(newItems)
    };
  }
  return {
    Items: items
  };
}


module.exports = {
  createTable: createTable,
  deleteTable: deleteTable,
  listTables: listTables,
  scanTable: scanTable,
  truncateTable: truncateTable
};
