let AWS = require('aws-sdk')
let Queue = require('async/queue')
let _ = require('lodash')

class API {
    constructor(params) {
        this.POLL_INTERVAL = 1000
        this.region = ('Region' in params) ? params.Region : 'us-east-1'
        this.database = ('Database' in params) ? params.Database : 'default'
        this.output_location = ('OutputLocation' in params) ? params.OutputLocation : ''
        this.encryption = ('EncryptionConfiguration' in params) ? params.EncryptionConfiguration : undefined
        
        this.q = Queue((id, cb) => {
            this.startPolling(id)
            .then((data) => { return cb(null, data) })
            .catch((err) => { console.log('Failed to poll query: ', err); return cb(err) })
        }, 5);

        let creds = new AWS.SharedIniFileCredentials({filename:'/app/src/api/credentials', profile: 'royon'});
        AWS.config.credentials = creds;
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
                self.q.push(result.QueryExecutionId, (err, qid) => {
                    if (err) return reject(err)
                    return self.results(qid)
                        .then((data) => { return resolve(data) })
                        .catch((err) => { return reject(err) })
                })
            })
        })
    }

    startPolling(id) {
        let self = this
        return new Promise((resolve, reject) => {
            function poll(id) {
                self.client.getQueryExecution({QueryExecutionId: id}, (err, data) => {
                    if (err) return reject(err)
                    if (data.QueryExecution.Status.State === 'SUCCEEDED') return resolve(id)
                    else { setTimeout(poll, self.POLL_INTERVAL, id) }
                })
            }
            poll(id)
        })
    }

    results(query_id, max, page) {
        let max_num_results = max ? max : 100
        let page_token = page ? page : undefined
        let self = this
        return new Promise((resolve, reject) => {
            let params = {
                QueryExecutionId: query_id,
                MaxResults: max_num_results,
                NextToken: page_token
            }
            self.client.getQueryResults(params, (err, data) => {
                if (err) return reject(err)
                let header = _.head(data.ResultSet.ResultRows).Data
                var list = []
                _.drop(data.ResultSet.ResultRows).forEach((item) => {
                    list.push(_.zipObject(header, item.Data))
                })
                return resolve(list)
            })
        })
    }

    stats(id) {
        let self = this
        id = _.trim(id, '"')
        return new Promise((resolve, reject) => {
            self.client.getQueryExecution({QueryExecutionId: id}, (err, data) => {
                if (err) return reject(err)
                return resolve(data.QueryExecution)  //if (data.QueryExecution.Status.State === 'SUCCEEDED') 
            })
        })
    }
}

module.exports = API
