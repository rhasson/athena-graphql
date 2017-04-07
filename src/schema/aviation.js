let { GraphQLObjectType, GraphQLString, GraphQLInt, GraphQLList, GraphQLSchema } = require('graphql')

let Flight = new GraphQLObjectType({
	name: 'Flight',
	fields: {
		airport_code: {
			type : GraphQLString
		},
		broad_phase_of_flight: {
			type : GraphQLString
		},
		total_fatal_injuries: {
			type : GraphQLInt
		},
		total_serious_injuries: {
			type : GraphQLInt
		},
		event_date: {
			type : GraphQLString
		}
	}
})

let QueryRoot = new GraphQLObjectType({
	name: 'RootQuery',
	fields: {
		flights: {
			type: new GraphQLList(Flight),
			args: {
				limit: { type: GraphQLInt }
			},
			resolve: (parent, args, context) => {
				let jdbc = context.jdbc
				return jdbc.isConnected() ? jdbc.query(`select * from default.aviation_csv limit ${args.limit};`) : new Error('No JDBC connection available')
			}
		}
	}
})

module.exports = new GraphQLSchema({
	descrption: 'flight failure dataset',
	query: QueryRoot
})