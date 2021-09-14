class SqlQueries:
    dim_movies_insert = (""" (movie_key, title, overview, language, release_date, runtime, vote_average, production_companies, vote_count, keywords)
        SELECT stage_movies.movie_key, stage_movies.title, stage_movies.overview, stage_movies.language, stage_movies.release_date, stage_movies.runtime, stage_movies.vote_average,
        stage_movies.production_companies, stage_movies.vote_count, stage_movies.keywords
        FROM stage_movies              
    """)

    fact_movies_insert = (""" (movie_key, user_key, date_key, movie_staff_key, movie_crew_key, revenue, budget)
        SELECT stage_movies.movie_key, stage_ratings.userId, stage_ratings.timestamp, dim_movie_staff.movie_staff_key, dim_movie_crew.movie_crew_key,
        stage_movies.revenue, stage_movies.budget
        FROM stage_movies, stage_ratings, dim_movie_staff, dim_movie_crew
        WHERE (stage_movies.movie_key = stage_ratings.movieId AND stage_movies.movie_key = dim_movie_staff.movie_id AND stage_movies.movie_key = dim_movie_crew.movie_id)
        

    """)
    
    dim_users_insert = (""" (user_key, movieId, rating)
        SELECT DISTINCT stage_ratings.userId, stage_ratings.movieId, stage_ratings.rating
        FROM stage_ratings
        WHERE (stage_ratings.userId IS NOT NULL AND stage_ratings.movieId IS NOT NULL AND stage_ratings.rating IS NOT NULL)
    """)

    dim_date_insert = (""" (date_key, hour, day, week, month, year, weekday)
        SELECT DISTINCT stage_ratings.timestamp,
        EXTRACT(hour FROM  stage_ratings.timestamp),
        EXTRACT(day FROM  stage_ratings.timestamp),
        EXTRACT(week FROM  stage_ratings.timestamp),
        EXTRACT(month FROM  stage_ratings.timestamp),
        EXTRACT(year FROM  stage_ratings.timestamp),
        EXTRACT(weekday FROM  stage_ratings.timestamp)
        FROM stage_ratings  
        WHERE stage_ratings.timestamp IS NOT NULL
    """)

    dim_movie_staff_insert = (""" (movie_id, character, actor_name, gender)
        SELECT stage_movie_staff.movie_id, stage_movie_staff.character, stage_movie_staff.actor_name, stage_movie_staff.gender
        FROM stage_movie_staff              
    """)
    
    dim_movie_crew_insert = (""" (movie_id, job, department, name, gender)
        SELECT stage_movie_crew.movie_id, stage_movie_crew.job, department, name, gender
        FROM stage_movie_crew              
    """)
        
        