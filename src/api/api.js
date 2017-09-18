let AWS = require('aws-sdk')
let Queue = require('async/queue')
let _ = require('lodash')

const PROFILE_NAME = 'roy'
const CRED_PATH = '/app/src/api/credentials'

class API {
    constructor(params) {
        this.RESULT_SIZE = 1000
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

        let creds = new AWS.SharedIniFileCredentials({filename: CRED_PATH, profile: PROFILE_NAME});
        AWS.config.credentials = creds;
        this.client = new AWS.Athena({
            correctClockSkew: true,
            apiVersion: '2017-05-18',
            region: this.region,
            convertResponseTypes: false,
        })
    }

    query(params) {
        let sql = undefined
        if (typeof(params) === 'string') { sql = params; params = {} }
        console.log(sql)
        let default_params = {
            QueryString: sql,
            ResultConfiguration: { OutputLocation: this.output_location },
            QueryExecutionContext: { Database: this.database }
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
                    else if (['FAILED', 'CANCELLED'].includes(data.QueryExecution.Status.State)) return reject(new Error(`Query ${data.QueryExecution.Status.State}`))
                    else { setTimeout(poll, self.POLL_INTERVAL, id) }
                })
            }
            poll(id)
        })
    }

    results(query_id, max, page) {
        let max_num_results = max ? max : this.RESULT_SIZE
        let page_token = page ? page : undefined
        let self = this
        return new Promise((resolve, reject) => {
            let params = {
                QueryExecutionId: query_id,
                MaxResults: max_num_results,
                NextToken: page_token
            }

            let dataBlob = []
            go(params)

            function go(param) {
                getResults(param)
                .then((res) => {
                    dataBlob = _.concat(dataBlob, res.list)
                    if (res.next) {
                        param.NextToken = res.next
                        return go(param)
                    } else return resolve(dataBlob)
                }).catch((err) => { return reject(err) })
            }

            function getResults() {
                return new Promise((resolve, reject) => {
                    self.client.getQueryResults(params, (err, data) => {
                        if (err) return reject(err)
                        var list = []
                        let header = self.buildHeader(data.ResultSet.ColumnInfos)
                        let resultSet = (_.difference(header, _.head(data.ResultSet.ResultRows).Data).length > 0) ?
                            data.ResultSet.ResultRows :
                            _.drop(data.ResultSet.ResultRows)
                            
                        resultSet.forEach((item) => {
                            list.push(_.zipObject(header, item.Data))
                        })
                        return resolve({next: ('NextToken' in data) ? data.NextToken : undefined, list: list})
                    })
                })
            }

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

    batchStats(ids) {
        let self = this
        let eIds = _.map(ids, (id) => { return _.trim(id, '"') })
        let pList = []
        while(eIds.length > 0) {
            let arr = _.take(eIds, 50)
            let p = new Promise((resolve, reject) => {
                self.client.batchGetQueryExecution({QueryExecutionIds: arr}, (err, data) => {
                    if (err) return reject(err)
                    let results = _.filter(data.QueryExecutions, (e) => { return e.Statistics.DataScannedInBytes > 0 })
                    return resolve(results)
                })
            })
            pList.push(p)
            eIds = _.slice(eIds, 50)
        }
        return Promise.all(pList).then((data) => {
            return _.flatMap(data)
        })
    }

    buildHeader(columns) {
        return _.map(columns, (i) => { return i.Name })
    }
}

module.exports = API
