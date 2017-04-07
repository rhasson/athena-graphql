let { GraphQLObjectType, GraphQLString, GraphQLInt, GraphQLList, GraphQLSchema, GraphQLFloat } = require('graphql')

let Movie = new GraphQLObjectType({
	name: 'Movie',
	fields: () => ({
		color: { type: GraphQLString },
		director_name: { type: GraphQLString },
		num_critic_for_reviews: { type: GraphQLInt },
		duration: { type: GraphQLInt },
		director_facebook_likes: { type: GraphQLInt },
		actor_3_facebook_likes: { type: GraphQLInt },
		actor_2_name: { type: GraphQLString },
		actor_1_facebook_likes: { type: GraphQLInt },
		gross: { type: GraphQLInt },
		genres: {
			type: new GraphQLList(GraphQLString),
			resolve: (obj) => { return obj.genres.split('|') }
		},
		actor_1_name: { type: GraphQLString },
		movie_title: { 
			type: GraphQLString,
			resolve: (obj) => { return obj.movie_title.trim() }
		},
		num_voted_users: { type: GraphQLInt },
		cast_total_facebook_likes: { type: GraphQLInt },
		actor_3_name: { type: GraphQLString },
		facenumber_in_poster: { type: GraphQLInt },
		plot_keywords: {
			type: new GraphQLList(GraphQLString),
			resolve: (obj) => { return obj.plot_keywords.split('|') }
		},
		movie_imdb_link: { type: GraphQLString },
		num_user_for_reviews: { type: GraphQLInt },
		language: { type: GraphQLString },
		country: { type: GraphQLString },
		content_rating: { type: GraphQLString },
		budget: { type: GraphQLInt },
		title_year: { type: GraphQLInt },
		actor_2_facebook_likes: { type: GraphQLInt },
		imdb_score: { 
			type: GraphQLFloat,
			resolve: (obj) => { return parseFloat(obj.imdb_score).toFixed(1) }
		},
		aspect_ratio: { type: GraphQLFloat },
		movie_facebook_likes: { type: GraphQLInt }
	})
})

let QueryRoot = new GraphQLObjectType({
	name: 'RootQuery',
	fields: {
		movies: {
			type: new GraphQLList(Movie),
			args: {
				limit: { type: GraphQLInt }
			},
			resolve: (parent, args, context) => {
				let jdbc = context.jdbc
				return jdbc.isConnected() ? jdbc.query(`select * from default.movies limit ${args.limit};`) : new Error('No JDBC connection available')
			}
		},
		popularMovies: {
			type: new GraphQLList(Movie),
			args: {
				limit: { type: GraphQLInt },
				over: { type: GraphQLFloat }
			},
			resolve: (parent, args, context) => {
				let jdbc = context.jdbc
				args.over = ('over' in args && args.over > 0) ? args.over : 7.0
				return jdbc.isConnected() ? jdbc.query(`select * from default.movies where imdb_score >= ${parseFloat(args.over)} limit ${args.limit};`) : new Error('No JDBC connection available')
			}
		},
		moviesByGenres: {
			type: new GraphQLList(Movie),
			args: {
				limit: { type: GraphQLInt },
				genres: { type: GraphQLString }
			},
			resolve: (parent, args, context) => {
				let jdbc = context.jdbc
				args.genres = ('genres' in args && args.genres !== '') ? args.genres : 'Action'
				return jdbc.isConnected() ? jdbc.query(`select * from default.movies where genres like '%${args.genres}%' limit ${args.limit};`) : new Error('No JDBC connection available')
			}
		}
	}
})

module.exports = new GraphQLSchema({
	descrption: 'IMDB Movie Reviews',
	query: QueryRoot
})