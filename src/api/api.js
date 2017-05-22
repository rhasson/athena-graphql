let AWS = require('aws-sdk')

class API {
    constructor(params) {
        this.region = ('Region' in params) ? params.Region : 'us-east-1'
        this.database = ('Database' in params) ? params.Database : 'default'
        this.output_location = ('OutputLocation' in params) ? params.OutputLocation : ''
        this.encryption = ('EncryptionConfiguration' in params) ? params.EncryptionConfiguration : {}
        
        this.client = new AWS.Athena({
            apiVersion: '2017-05-18',
            region: this.region,
            convertResponseTypes: false,
        })
    }

    query(params) {
        let sql = undefined
        if (params instanceof String) { sql = params; params = {} }
        let default_params = {
            QueryString: sql, /* required */
            ResultConfiguration: { /* required */
                OutputLocation: this.output_location, /* required */
                EncryptionConfiguration: this.encryption
            },
            QueryExecutionContext: {
                Database: this.database
            }
        }

        let self = this
        let obj = Object.assign(default_params, params)
        return new Promise((resolve, reject) => {
            this.client.StartQueryExecution(obj, (err, result) => {
                if (err) return reject(err)
                
            })
        })
    }
}