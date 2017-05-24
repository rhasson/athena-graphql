let Express = require('express')
let bodyParser = require('body-parser')
let { graphqlExpress, graphiqlExpress } = require('graphql-server-express')
let server = Express()
let API = require('./api/api')

/* load the GraphQL Schema to be used to querying the dataset */
let SCHEMA = require('./schema/athena_stats.js')

// **** USE JDBC DRIVER  ****
/*
let JDBC = require('./jdbc/jdbc')
// Instanciate the JDBC wrapper.  Defaults to using ECS temp credentials from the assigned ECS Task role
new JDBC({
//    aws_profile: 'athena',  // if you want to use a credentials file and point to a profile name
//    cred_filename: './src/credentials'  // provide the path and filename for your credentials file
    s3_staging_dir: 's3://royon-spark/athena_temp/'
}).then((jdbc) => { 
    jdbc.createConnection()
    .then(() => {
        server.use('/graphql', bodyParser.json(), graphqlExpress({ schema: SCHEMA, context: {jdbc} }))
        server.use('/graphiql', graphiqlExpress({ endpointURL: '/graphql' }))
        server.use('/health', healthCheckHandler)

        server.listen(8080)
    })
}).catch((err) => {
    console.log('FATAL ERROR: ', err)
    process.exit()
})
*/

// **** USE ATHENA API ****
let client = new API({
    Region: 'us-east-1',
    OutputLocation: 's3://royon-spark/athena_temp/',
    /*EncryptionConfiguration: {
        EncryptionOption: 'SSE_S3', // | 'SSE_KMS' | 'CSE_KMS', // required 
        KmsKey: 'STRING_VALUE'
    },*/
    Database: 'default'
})

server.use('/graphql', bodyParser.json(), graphqlExpress({ schema: SCHEMA, context: {client} }))
server.use('/graphiql', graphiqlExpress({ endpointURL: '/graphql' }))
server.use('/health', healthCheckHandler)

server.listen(8080)

/* Only needed when running the ECS task behind an Application Load Balancer */
function healthCheckHandler(req, res) {
  res.status(200).end()
}