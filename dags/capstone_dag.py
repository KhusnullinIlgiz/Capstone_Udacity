from datetime import datetime, timedelta
import os
import glob
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.copy_redshift import CopyToRedshiftOperator
from operators.load import LoadOperator

from operators.data_quality import DataQualityOperator                        
from helpers.sql_queries import SqlQueries


#S3 bucket addresses for files

s3_bucket = "s3://tempbucket1168"

#Default arguments for DAG
default_args = {
    'owner': 'Ilgiz',
    'start_date': datetime(2021, 1, 12),
    'depends_on_past':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'email_on_retry':False,
    
}
#Setting DAG
dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )
#Initial task for defining job's start
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Copy stage_ratings.parquet from S3 buckets to redshift stage_ratings table
copy_stage_ratings_to_redshift = CopyToRedshiftOperator(
    task_id='Copy_stage_ratings',
    dag=dag,
    provide_context = True,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "stage_ratings",
    file_format = "PARQUET",
    s3_bucket = os.path.join(s3_bucket,"stage_ratings.parquet")   
)


#Copy stage_movies.parquet from S3 buckets to redshift stage_movies table
copy_stage_movies_to_redshift = CopyToRedshiftOperator(
    task_id='Copy_stage_movies',
    dag=dag,
    provide_context = True,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "stage_movies",
    file_format = "PARQUET",
    s3_bucket = os.path.join(s3_bucket,"stage_movies.parquet")   
)

#Copy dim_movie_staff.parquet from S3 buckets to redshift stage_movie_staff table
copy_stage_movie_staff_to_redshift = CopyToRedshiftOperator(
    task_id='Copy_stage_movie_staff',
    dag=dag,
    provide_context = True,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "stage_movie_staff",
    file_format = "PARQUET",
    s3_bucket = os.path.join(s3_bucket,"dim_movie_staff.parquet")   
)

#Copy dim_movie_crew.parquet from S3 buckets to redshift stage_movie_crew table
copy_stage_movie_crew_to_redshift = CopyToRedshiftOperator(
    task_id='Copy_stage_movie_crew',
    dag=dag,
    provide_context = True,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "stage_movie_crew",
    file_format = "PARQUET",
    s3_bucket = os.path.join(s3_bucket,"dim_movie_crew.parquet")   
)

#Insert Data from stage_movie_staff tables to dim_movie_staff table in redshift
load_dim_movie_staff = LoadOperator(
    task_id='Load_dim_movie_staff_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "dim_movie_staff",
    query = SqlQueries.dim_movie_staff_insert

)

#Insert Data from stage_movie_staff tables to dim_movie_staff table in redshift
load_dim_movie_crew = LoadOperator(
    task_id='Load_dim_movie_crew_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "dim_movie_crew",
    query = SqlQueries.dim_movie_crew_insert

)


#Insert Data from stage_ratings tables to dim_users table in redshift
load_dim_users = LoadOperator(
    task_id='Load_dim_users_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "dim_users",
    query = SqlQueries.dim_users_insert

)
#Insert Data from stage_ratings tables to dim_date table in redshift
load_dim_date = LoadOperator(
    task_id='Load_dim_date_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "dim_date",
    query = SqlQueries.dim_date_insert

)

#Insert Data from stage_movies tables to dim_movies table in redshift
load_dim_movies = LoadOperator(
    task_id='Load_dim_movies_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "dim_movies",
    query = SqlQueries.dim_movies_insert

)

#Insert Data from stage_movies,stage_ratings,dim_movie_staff,dim_movie_crew tables to fact_movies table in redshift
load_fact_movies = LoadOperator(
    task_id='Load_fact_movies_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "fact_movies",
    query = SqlQueries.fact_movies_insert

)



# Data quality check for all tables in Redshift
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables_qry = [{"table":"stage_ratings","expected_result":"26024289"},{"table":"stage_movies","expected_result":"42105"},{"table":"dim_movie_staff","expected_result":"59555"},{"table":"dim_movie_crew","expected_result":"6230"},{"table":"dim_users","expected_result":"26024289"},{"table":"dim_date","expected_result":"26024289"},{"table":"dim_movies","expected_result":"42105"}, {"table":"fact_movies","expected_result":"292172"},{"table":"stage_movie_crew","expected_result":"6230"},{"table":"stage_movie_staff","expected_result":"59555"}]
)

# Last task in pipeline for defining job's end
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Creating consequent execution steps for Airflow
start_operator >> copy_stage_ratings_to_redshift
start_operator >> copy_stage_movies_to_redshift
start_operator >> copy_stage_movie_staff_to_redshift
start_operator >> copy_stage_movie_crew_to_redshift

copy_stage_ratings_to_redshift     >> load_dim_users
copy_stage_ratings_to_redshift     >> load_dim_date
copy_stage_movies_to_redshift      >> load_dim_movies
copy_stage_movie_staff_to_redshift >> load_dim_movie_staff
copy_stage_movie_crew_to_redshift  >>  load_dim_movie_crew

load_dim_movies    >> load_fact_movies



copy_stage_ratings_to_redshift     >> run_quality_checks
copy_stage_movies_to_redshift      >> run_quality_checks
copy_stage_movie_staff_to_redshift >> run_quality_checks
copy_stage_movie_crew_to_redshift  >> run_quality_checks
load_dim_movies                    >> run_quality_checks
load_fact_movies                   >> run_quality_checks
load_dim_users                     >> run_quality_checks
load_dim_date                      >> run_quality_checks
load_dim_movie_staff               >> run_quality_checks
load_dim_movie_crew                >> run_quality_checks

run_quality_checks >> end_operator

