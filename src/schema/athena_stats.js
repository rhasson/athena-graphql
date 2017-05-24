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

let AthenaCloudTrail = new GraphQLObjectType({
	name: 'AthenaCloudTrail',
	fields: () => ({
		eventTime: { type: GraphQLString },
		eventSource: { type: GraphQLString },
		eventName: { type: GraphQLString },
		eventCode: { type: GraphQLString },
		awsRegion: { type: GraphQLString },
		userType: { type: GraphQLString },
		userArn: { type: GraphQLString },
		userPrincipleId: { type: GraphQLString },
        userAccountId: { type: GraphQLString },
		queryExecutionId: { 
            type: GraphQLString,
            resolve: (obj) => { return obj.queryExecutionId == undefined ? '' : _.trim(obj.queryExecutionId, '\"') }
        },
        athenaStats: {
            type: AthenaQueryStats,
            resolve: (obj, args, context) => {
                let client = context.client
                return (obj.queryExecutionId != undefined && obj.queryExecutionId != '') ? client.stats(obj.queryExecutionId) : undefined
            }
        }
	})
})

let QueryRoot = new GraphQLObjectType({
	name: 'RootQuery',
	fields: {
		cloudtrail: {
			type: new GraphQLList(AthenaCloudTrail),
			args: {
				limit: { type: GraphQLInt },
                user: { type: GraphQLString },
                gtMonth: { type: GraphQLInt },
                gtDay: { type: GraphQLInt },
                eqMonth: { type: GraphQLInt },
                eqDay: { type: GraphQLInt }
			},
			resolve: (parent, args, context) => {
				let client = context.client
                let dateSearch = ''
                let userSearch = ('user' in args && args.user !== '') ? `and userArn like '%${args.user}'` : ''
                if ('gtMonth' in args && (args.gtMonth > 0 && args.gtMonth < 13)) { dateSearch = `and month(ts) > ${args.gtMonth}` }
                else if ('gtDay' in args && (args.gtDay > 0 && args.gtDay < 32)) { dateSearch = `and day(ts) > ${args.gtDay}` }
                else if ('eqMonth' in args && (args.eqMonth > 0 && args.eqMonth < 13)) { dateSearch = `and month(ts) = ${args.eqMonth}` }
                else if ('eqDay' in args && (args.eqDay > 0 && args.eqDay < 32)) { dateSearch = `and day(ts) = ${args.eqDay}` }
				
                return client.query(`WITH logs AS (
                    SELECT
                        event.eventTime as eventTime,
                        from_iso8601_timestamp(event.eventtime) as ts,
                        event.eventSource as eventSource,
                        event.eventName as eventName,
                        event.awsRegion as awsRegion,
                        event.errorCode as errorCode,
                        event.userIdentity.type as userType,
                        event.userIdentity.arn as userArn,
                        event.userIdentity.principalId as userPrincipalId,
                        event.userIdentity.accountId as userAccountId,
                        json_extract(event.responseElements, '$.queryexecutionid') as queryExecutionId
                    FROM cloudtrail_logs
                    CROSS JOIN UNNEST (Records) AS r (event)
                    )
                    SELECT * FROM logs 
                        where eventsource like 'athena%' 
                        and eventname like 'StartQueryExecution' 
                        ${userSearch}
                        ${dateSearch}
                        LIMIT ${args.limit};`
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
  return `${(bytes / (1024 ** i)).toFixed(1)} ${sizes[i]}`
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