let _ = require('lodash')
let { GraphQLObjectType, GraphQLString, GraphQLInt, GraphQLList, GraphQLSchema, GraphQLFloat } = require('graphql')

let AthenaResultEncryptionConfiguration = new GraphQLObjectType({
    name: 'AthenaResultEncryptionConfiguration',
    fields: () => ({
        encryptionOption: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.EncryptionOption }
        },
        kmsKey: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.KmsKey }
        }
    })
})

let AthenaResultConfiguration = new GraphQLObjectType({
    name: 'AthenaResultConfiguration',
    fields: () => ({
        outputLocation: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.OutputLocation }
        },
        encryptionConfiguration: {
            type: AthenaResultEncryptionConfiguration,
            resolve: (obj) => { return obj.EncryptionConfiguration }
        }
    })
})

let AthenaQueryExecutionContext = new GraphQLObjectType({
    name: 'AthenaQueryExecutionContext',
    fields: () => ({
        database: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.Database }
        }
    })
})

let AthenaQueryExecutionStatus = new GraphQLObjectType({
    name: 'AthenaQueryExecutionStatus',
    fields: () => ({
        state: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.State }
        },
        stateChangeReason: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.StateChangeReason }
        },
        submissionDateTime: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.SubmissionDateTime }
        },
        completionDateTime: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.CompletionDateTime }
        }
    })
})

let AthenaQueryStatistics = new GraphQLObjectType({
    name: 'AthenaQueryStatistics',
    fields: () => ({
        engineExecutionTimeInMillis: { 
            type: GraphQLInt,
            resolve: (obj) => { return parseInt(obj.EngineExecutionTimeInMillis).toFixed(1) }
         },
        dataScannedInBytes: { 
            type: GraphQLInt,
            resolve: (obj) => { return parseInt(obj.DataScannedInBytes).toFixed(1) }
        },
        dataScannedFormatted: {
            type: GraphQLString,
            resolve: (obj) => { return bytesToSize(parseInt(obj.DataScannedInBytes)) }
        },
        queryCost: {
            type: GraphQLString,
            resolve: (obj) => { return toCost(toTB(parseInt(obj.DataScannedInBytes)))}
        }
    })
})

let AthenaQueryStats = new GraphQLObjectType({
	name: 'AthenaQueryStats',
	fields: () => ({
		queryExecutionId: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.QueryExecutionId }
        },
		queryString: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.Query }
        },
		resultConfiguration: { 
            type: AthenaResultConfiguration,
            resolve: (obj) => { return obj.ResultConfiguration }
        },
		queryExecutionContext: { 
            type: AthenaQueryExecutionContext,
            resolve: (obj) => { return obj.QueryExecutionContext }
        },
		queryStatus: { 
            type: AthenaQueryExecutionStatus,
            resolve: (obj) => { return obj.Status }
        },
		queryStats: { 
            type: AthenaQueryStatistics,
            resolve: (obj) => { return obj.Statistics }
        }
	})
})

let AthenaFailedQueries = new GraphQLObjectType({
	name: 'AthenaFailedQueries',
	fields: () => ({
        ts: { type: GraphQLString },
        date: { type: GraphQLString },
		identity_username: { type: GraphQLString },
		errorcode: { type: GraphQLString },
        errormessage: { type: GraphQLString },
		queryexecutionid: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.queryexecutionid == undefined ? '' : _.trim(obj.queryexecutionid, '\"') }
        },
        sqlquery: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.sqlquery == undefined ? '' : _.trim(obj.sqlquery, '\"') }
        }/*,
        athenaStats: {
            type: AthenaQueryStats,
            resolve: (obj, args, context) => {
                let client = context.client
                return (obj.queryexecutionid != undefined && obj.queryexecutionid != '') ? client.stats(obj.queryexecutionid) : undefined
            }
        }*/
	})
})

let AthenaUsageRollup = new GraphQLObjectType({
    name: 'AthenaUsageRollup',
    fields: () => ({
        totalCost: { type: GraphQLString },
        dataScanned: { type: GraphQLString },
        totalMinutes: { type: GraphQLString }
    })
})

let QueryRoot = new GraphQLObjectType({
	name: 'RootQuery',
	fields: {
		failedQueries: {
			type: new GraphQLList(AthenaFailedQueries),
			args: {
				limit: { type: GraphQLInt },
                user: { type: GraphQLString },
                interval: { type: GraphQLInt },
                unit: { type: GraphQLString }
			},
			resolve: (parent, args, context) => {
                const UNIT_ARRAY = ['hour', 'day', 'month']
				let client = context.client
                let limit = ('limit' in args && args.limit > 0) ? ` LIMIT ${args.limit}` : ''
                let dateSearch = ''
                let userSearch = ('user' in args && args.user !== '') ? `identity_username like '%${args.user}%'` : ''
                if (('interval' in args && args.interval > 0) && ('unit' in args && _.indexOf(UNIT_ARRAY, args.unit))) { 
                    dateSearch = `
                      ${userSearch == '' ? '' : 'and'} ts between (current_timestamp - interval '${args.interval}' ${args.unit}) and current_timestamp
                      ${args.interval == 'hour' ? ' and date("date") = current_date' : ''}
                    `
                }
                
                return client.query(`WITH logs AS (
                    SELECT
                        ts,
                        "date",
                        identity_username,
                        errorcode,
                        errormessage,
                        json_extract(responseelements, '$.queryExecutionId') as queryexecutionid,
                        json_extract(requestparameters, '$.queryString') as sqlquery
                    FROM cloudtrail.ct_cloudtrail
                    WHERE identity_username <> '' and errorcode <> '' and eventname = 'StartQueryExecution'
                    )
                    SELECT * FROM logs
                        where
                        ${userSearch}
                        ${dateSearch}
                        order by ts desc
                        ${limit};`
                )
			}
		},
        stats: {
            type: AthenaQueryStats,
            args: {
                id: { type: GraphQLString }
            },
            resolve: (parent, args, context) => {
                let client = context.client
                return client.stats(args.id)
            }
        },
        usageRollup: {
            type: AthenaUsageRollup,
            args: {
                month: { type: GraphQLInt },
                user: { type: GraphQLString }
            },
            resolve: (parent, args, context) => {
                return new Promise((resolve, reject) => {
                    let client = context.client
                    let dateSearch = ''
                    let userSearch = ('user' in args && args.user !== '') ? `identity_username like '%${args.user}%'` : ''
                    if ('month' in args && (args.month > 0 && args.month < 13)) { dateSearch = ` and "month" = ${args.month}` }
                    client.query(`WITH logs AS (
                        SELECT
                            ts,
                            month(date("date")) as "month",
                            identity_username,
                            json_extract(responseelements, '$.queryExecutionId') as queryexecutionid
                        FROM cloudtrail.ct_cloudtrail
                        WHERE identity_username <> '' and eventname = 'StartQueryExecution'
                        )
                        SELECT * FROM logs
                        WHERE ${userSearch}
                        ${dateSearch};`
                    ).then((data) => {
                        let ids = _.filter(
                            _.map(data, (item) => { return _.trim(item.queryexecutionid, '"') }),
                            (item) => { return item != '' })
                            client.batchStats(_.uniq(ids))
                            .then((data) => {
                                let bytes = _.sumBy(data, 'Statistics.DataScannedInBytes')
                                let milis = _.sumBy(data, 'Statistics.EngineExecutionTimeInMillis')
                                return resolve({
                                    dataScanned: bytesToSize(bytes), 
                                    totalCost: toCost(toTB(bytes)),
                                    totalMinutes: toMinutes(milis)
                                })
                            })
                    }).catch((err) => { return reject(err) })
                })
            }
        }
	}
})

module.exports = new GraphQLSchema({
	descrption: 'Athena CloudTrail Logs',
	query: QueryRoot
})

function bytesToSize(bytes) {
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
  if (bytes === 0) return 'n/a'
  const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)), 10)
  if (i === 0) return `${bytes} ${sizes[i]})`
  return `${(bytes / (1024 ** i)).toFixed(2)} ${sizes[i]}`
}

function toTB(bytes) {
    const tb = (1024 * 1024 * 1024 * 1024)
    return b = bytes > 0 ? (bytes / tb) : 0
}

function toCost(tb) {
    const dollarPerTb = 5.00
    let b = tb * dollarPerTb
    return `$ ${b > 1 ? b.toFixed(2) : b.toFixed(8)}`
}

function toMinutes(ms) {
    ms = 1000*Math.round(ms/1000); // round to nearest second
    var d = new Date(ms);
    return d.getUTCMinutes()
}