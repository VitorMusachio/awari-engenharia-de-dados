import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from imdb_download_from_source_operator import ImdbDownloadFromSourceOperator
from ds_convert_to_parquet_operator import ConvertToParquetOperator
from bi_pg_operator import BIPgOperator

URLS_IMDB = {
   'name_basics': 'https://datasets.imdbws.com/name.basics.tsv.gz',
   'title_akas': 'https://datasets.imdbws.com/title.akas.tsv.gz',
   'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
   'title_crew': 'https://datasets.imdbws.com/title.crew.tsv.gz',
   'title_episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
   'title_principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
   'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

FILES_IMDB = {
   'name_basics': 'name.basics.csv',
   'title_akas': 'title.akas.csv',
   'title_basics': 'title.basics.csv',
   'title_crew': 'title.crew.csv',
   'title_episode': 'title.episode.csv',
   'title_principals': 'title.principals.csv',
   'title_ratings': 'title.ratings.csv'
}

# DAG 1
dag1 =  DAG(dag_id=f"name_basics_dag",start_date=datetime(2021,1,1),schedule_interval='@daily', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['name_basics'])}", url=URLS_IMDB['name_basics'],dag=dag1
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['name_basics']}", url=FILES_IMDB['name_basics'],dag=dag1
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['name_basics']}", url=FILES_IMDB['name_basics'], tablename='name_basics', dag=dag1
)

# TASKS
download_task >> parquet_task
download_task >> bi_task


# DAG 2
dag2 =  DAG(dag_id=f"title_akas_dag",start_date=datetime(2021,1,1),schedule_interval='0 0 * * *', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_akas'])}", url=URLS_IMDB['title_akas'],dag=dag2
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['title_akas']}", url=FILES_IMDB['title_akas'],dag=dag2
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['title_akas']}", url=FILES_IMDB['title_akas'], tablename='title_akas', dag=dag2
)

# TASKS
download_task >> parquet_task
download_task >> bi_task


# DAG 3
dag3 =  DAG(dag_id=f"title_basics_dag",start_date=datetime(2021,1,1),schedule_interval='@daily', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_basics'])}", url=URLS_IMDB['title_basics'],dag=dag3
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['title_basics']}", url=FILES_IMDB['title_basics'],dag=dag3
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['title_basics']}", url=FILES_IMDB['title_basics'], tablename='title_basics', dag=dag3
)

# TASKS
download_task >> parquet_task
download_task >> bi_task


# DAG 4
dag4 =  DAG(dag_id=f"title_crew_dag",start_date=datetime(2021,1,1),schedule_interval='@daily', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_crew'])}", url=URLS_IMDB['title_crew'],dag=dag4
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['title_crew']}", url=FILES_IMDB['title_crew'],dag=dag4
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['title_crew']}", url=FILES_IMDB['title_crew'], tablename='title_crew', dag=dag4
)

# TASKS
download_task >> parquet_task
download_task >> bi_task


# DAG 5
dag5 =  DAG(dag_id=f"title_episode_dag",start_date=datetime(2021,1,1),schedule_interval='@daily', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_episode'])}", url=URLS_IMDB['title_episode'],dag=dag5
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['title_episode']}", url=FILES_IMDB['title_episode'],dag=dag5
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['title_episode']}", url=FILES_IMDB['title_episode'], tablename='title_episode', dag=dag5
)

# TASKS
download_task >> parquet_task
download_task >> bi_task


# DAG 6
dag6 =  DAG(dag_id=f"title_principals_dag",start_date=datetime(2021,1,1),schedule_interval='@daily', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_principals'])}", url=URLS_IMDB['title_principals'],dag=dag6
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['title_principals']}", url=FILES_IMDB['title_principals'],dag=dag6
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['title_principals']}", url=FILES_IMDB['title_principals'], tablename='title_principals', dag=dag6
)

# TASKS
download_task >> parquet_task
download_task >> bi_task


# DAG 7
dag7 =  DAG(dag_id=f"title_ratings_dag",start_date=datetime(2021,1,1),schedule_interval='@daily', catchup=False)
      
download_task = ImdbDownloadFromSourceOperator(
   task_id=f"download_{os.path.basename(URLS_IMDB['title_ratings'])}", url=URLS_IMDB['title_ratings'],dag=dag7
)

parquet_task = ConvertToParquetOperator(
   task_id=f"parquet_{FILES_IMDB['title_ratings']}", url=FILES_IMDB['title_ratings'],dag=dag7
)

bi_task = BIPgOperator(
   task_id=f"bi_{FILES_IMDB['title_ratings']}", url=FILES_IMDB['title_ratings'], tablename='title_ratings', dag=dag7
)

# TASKS
download_task >> parquet_task
download_task >> bi_task
