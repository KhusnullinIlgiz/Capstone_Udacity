CREATE TABLE public.stage_movies (
	movie_key INT NOT NULL PRIMARY KEY DISTKEY,
	title VARCHAR,
	overview VARCHAR(10000),
  	release_date VARCHAR,
	language VARCHAR,
    runtime INT,
    vote_average FLOAT,
    production_companies VARCHAR(15000),
    vote_count FLOAT,
    revenue FLOAT,
    budget FLOAT,
  	keywords VARCHAR(15000)
);

CREATE TABLE public.fact_movies (
	fact_movie_key INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
	movie_key INT,
	user_key INT,
	date_key TIMESTAMP,
	movie_staff_key INT,
    movie_crew_key INT,
    revenue FLOAT,
    budget FLOAT
);

CREATE TABLE public.dim_movies (
	movie_key INT NOT NULL PRIMARY KEY DISTKEY,
	title VARCHAR,
	overview VARCHAR(10000),
	language VARCHAR,
	release_date VARCHAR,
    runtime INT,
    vote_average FLOAT,
    production_companies VARCHAR(15000),
    vote_count FLOAT,
    keywords VARCHAR(15000)
);

CREATE TABLE public.stage_ratings (
	user_key INT NOT NULL,
	movieId INT,
	rating FLOAT,
    timestamp TIMESTAMP

);

CREATE TABLE public.dim_users (
	user_key INT NOT NULL PRIMARY KEY DISTKEY,
	movieId INT,
	rating FLOAT
);

CREATE TABLE public.dim_date (
	date_key TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
	hour INT,
	day INT,
	week INT,
	month INT,
    year INT,
    weekday INT
);

CREATE TABLE public.dim_movie_staff (
	movie_staff_key INT IDENTITY(0,1) PRIMARY KEY DISTKEY,
	movie_id INT,
    character VARCHAR,
	actor_name VARCHAR,
    gender VARCHAR
);

CREATE TABLE public.dim_movie_crew (
	movie_crew_key INT IDENTITY(0,1) PRIMARY KEY DISTKEY,
	movie_id INT,
    job VARCHAR,
	department VARCHAR,
    name VARCHAR,
    gender VARCHAR
);





