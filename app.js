// Copyright 2019 Google LLC. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// [START run_pubsub_server_setup]
const express = require('express');
const axios = require('axios');
const bodyParser = require('body-parser');
const app = express();

const axiosInstance = axios.create({
  baseURL: 'http://metadata.google.internal/',
  timeout: 1000,
  headers: {'Metadata-Flavor': 'Google'}
});

const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

// Set some application specific parameters we will need later
// TODO: Make these environment variables on the Cloud Run application
const bq_table = 'ga_sessions'
const bq_dataset = 'user_data'
const bq_user_column = 'fullVisitorId' // We don't have userID in our test data set, so use this for now
const ga_property_id = 'PROPERTY_ID'

app.use(bodyParser.json());
// [END run_pubsub_server_setup]

// [START run_pubsub_handler]
app.post('/', async (req, res) => {
    try {
        if (!req.body) {
            const msg = 'no Pub/Sub message received';
            console.error(`error: ${msg}`);
            res.status(400).send(`Bad Request: ${msg}`);
            return;
        }
        if (!req.body.message) {
            const msg = 'invalid Pub/Sub message format';
            console.error(`error: ${msg}`);
            res.status(400).send(`Bad Request: ${msg}`);
            return;
        }

        const pubSubMessage = req.body.message;
        const subscription = req.body.subscription;
        // Extract the userID from the message data
        const userID = Buffer.from(pubSubMessage.data, 'base64').toString().trim();

        // Fetch the project ID from the metadata server
        let path = 'computeMetadata/v1/project/project-id'
        var response = await axiosInstance.get(path)
        var projectId = response.data
        console.log('Project ID: ', projectId)

        if (subscription.includes('bigquery')) {
            console.log('Processing message from bigquery subscription...')

            // Build the full table name using the project ID
            var fullTableName = `${projectId}.${bq_dataset}.${bq_table}`

            // Build DML query to delete user data, being careful on enclosing strings
            const query = `DELETE \`${fullTableName}\` WHERE ${bq_user_column} = '${userID}'`
            console.log('Full DELETE query: ', query)

            const options = {
                query: query,
                location: 'US'
            }

            const [job] = await bigquery.createQueryJob(options)
            console.log(`JOB ${job.id} started.`)

            // Get query results once finished
            const [rows] = await job.getQueryResults()
            console.log('Rows: ')
            rows.forEach(row => console.log(row))

        } else if (subscription.includes('g-analytics')) {
            console.log('Processing message from g-analytics subscription...')
            
        }

        // Send a successful response back
        res.status(204).send();
    } catch (error) {
        console.error(error)
    }
});
// [END run_pubsub_handler]

module.exports = app;
