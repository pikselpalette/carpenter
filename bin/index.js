#!/usr/bin/env node
const yargs = require('yargs');
const chalk = require('chalk');
const UUID = require('uuid');
const { exec } = require('child_process');

const {
  createTable,
  deleteTable,
  listTables,
  scanTable,
  truncateTable
} = require('../lib/functions');

const options = yargs
  .usage('Usage: carpenter works agains local docker image. ')
  .option('function', {
    describe: 'The function name that you want to use',
    choices: [
      'createTable',
      'deleteTable',
      'dynamoDown',
      'dynamoUp',
      'listTables',
      'scanTable',
      'truncateTable'
    ],
    demandOption: true
  })
  .option('tableName', {
    describe: 'The name of the table to create'
  })
  .option('gsiNumber', {
    describe: 'The number of GSIs to create with the table'
  })
  .option('lsiNumber', {
    describe: 'The number of LSIs to create with the table'
  })
  .option('dynamoPort', {
    describe: 'The dynamo port of the local image',
    default: 8000
  })
  .option('gui', {
    describe: 'Uses instructure/dynamo-local-admin'
  })
  .option('returnDocuments', {
    describe: 'Return all documents when scanning the table',
    default: false
  })
  .argv;

const dynamoPort = yargs.argv.dynamoPort;

if (options.function === 'createTable') {
  const tableName = yargs.argv.tableName || UUID.v4();
  const gsiNumber = yargs.argv.gsiNumber || 0;
  const lsiNumber = yargs.argv.lsiNumber || 0;

  createTable(tableName, gsiNumber, lsiNumber, dynamoPort).then(() => {
    console.log(chalk.green(`Table ${tableName} created`));
  }).catch((err) => {
    console.log(chalk.red(`Error creating ${tableName} - ${err.name} - ${err.message}`));
  });
}
if (options.function === 'listTables') {
  listTables(dynamoPort).then((tablesNames) => {
    console.log(chalk.green('Table list'));
    console.log(tablesNames);
  }).catch((err) => {
    console.log(chalk.red(`Error listing tables - ${err.name} - ${err.message}`));
  });
}
if (options.function === 'deleteTable') {
  const tableName = yargs.argv.tableName;
  deleteTable(tableName).then(() => {
    console.log(chalk.green(`Table ${tableName} deleted`));
  }).catch((err) => {
    console.log(chalk.red(`Error deleting table - ${err.name} - ${err.message}`));
  });
}
if (options.function === 'dynamoUp') {
  exec(`docker run -p ${dynamoPort}:${dynamoPort} -it --rm -d --name dynamoCarpenter instructure/dynamo-local-admin`);
}
if (options.function === 'dynamoDown') {
  exec('docker stop dynamoCarpenter')
}
if (options.function === 'scanTable') {
  const tableName = yargs.argv.tableName;
  scanTable(tableName).then((data) => {
    console.log(chalk.green(`Number of document found: ${data.Items.length}`))
    if (yargs.argv.returnDocuments) {
      console.log(data.Items);
    }
  }).catch((err) => {
    console.log(chalk.red(`Error scanning table - ${err.name} - ${err.message}`));
  });
}
if (options.function === 'truncateTable') {
  const tableName = yargs.argv.tableName;
  truncateTable(tableName).then(() => {
    console.log(chalk.green(`Table ${tableName} truncated`));
  }).catch((err) => {
    console.log(chalk.red(`Error truncating table - ${err.name} - ${err.message}`));
  });
}
