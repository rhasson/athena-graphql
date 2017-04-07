CREATE EXTERNAL TABLE IF NOT EXISTS movies (
	`color` String,
	`director_name` String,
	`num_critic_for_reviews` Int,
	`duration` Int,
	`director_facebook_likes` Int,
	`actor_3_facebook_likes` Int,
	`actor_2_name` String,
	`actor_1_facebook_likes` Int,
	`gross` Int,
	`genres` String,
	`actor_1_name` String,
	`movie_title` String,
	`num_voted_users` Int,
	`cast_total_facebook_likes` Int,
	`actor_3_name` String,
	`facenumber_in_poster` Int,
	`plot_keywords` String,
	`movie_imdb_link` String,
	`num_user_for_reviews` Int,
	`language` String,
	`country` String,
	`content_rating` String,
	`budget` Int,
	`title_year` Int,
	`actor_2_facebook_likes` Int,
	`imdb_score` Float,
	`aspect_ratio` Float,
	`movie_facebook_likes` Int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
)
STORED AS TEXTFILE
LOCATION 's3://royon-demo/imdb/';
