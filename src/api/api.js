let AWS = require('aws-sdk')

class API {
    constructor(params) {
        this.region = ('Region' in params) ? params.Region : 'us-east-1'
        this.database = ('Database' in params) ? params.Database : 'default'
        this.output_location = ('OutputLocation' in params) ? params.OutputLocation : ''
        this.encryption = ('EncryptionConfiguration' in params) ? params.EncryptionConfiguration : undefined
        
        this.client = new AWS.Athena({
            apiVersion: '2017-05-18',
            region: this.region,
            convertResponseTypes: false,
        })
    }

    query(params) {
        let sql = undefined
        if (typeof(params) === 'string') { sql = params; params = {} }
        let default_params = {
            QueryString: sql, /* required */
            ResultConfiguration: { /* required */
                OutputLocation: this.output_location /* required */
            },
            QueryExecutionContext: {
                Database: this.database
            }
        }

        if (this.encryption) default_params.ResultConfiguration.EncryptionConfiguration = this.encryption

        let self = this
        let obj = Object.assign(params, default_params)
        return new Promise((resolve, reject) => {
            this.client.startQueryExecution(obj, (err, result) => {
                if (err) return reject(err)
                return resolve(result.QueryExecutionId)
            })
        })
    }

    results(query_id, max, page) {
        let max_num_results = max ? max : 100
        let page_token = page ? page : undefined
        let self = this
        return Promise((resolve, reject) => {
            let params = {
                QueryExecutionId: query_id,
                MaxResults: max_num_results,
                NextToken: page_token
            }
            self.client.getQueryResults(params, (err, data) => {
                if (err) return reject(err)
                return resolve(data)
            })
        })
    }
}

module.exports = API
