import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def CSV_to_Postgres():
    import psycopg2
    from sqlalchemy import create_engine
    import pandas as pd
    df=pd.read_csv('/opt/airflow/data/covid19.csv')
    host="postgres"
    database="airflow"
    user="airflow"
    password="airflow"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    df.to_sql('covid19', engine,if_exists='replace',index=False)

    
def Pull_CSV_from_Postgres_Convert_to_Json():
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    host="postgres"
    database="airflow"
    user="airflow"
    password="airflow"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    df2=pd.read_sql("SELECT * FROM covid19" , engine)
    df2.to_json('/opt/airflow/data/covid19.json',orient='records')
    


    
def push_json_to_mongo():
    import pymongo
    from pymongo import MongoClient
    import json
    client = MongoClient('mongo:27017', username='root',password='example')
    db = client['airflow']
    collection_covid19 = db['covid19']
    with open('/opt/airflow/data/covid19.json') as f:
        file_data = json.load(f)
        collection_covid19.insert_many(file_data)
    client.close()
    
 
 
default_args = {
 
    'owner': 'I.Shahbaz',
 
    'start_date': dt.datetime(2020, 5, 16),
 
    'retries': 1,
 
    'retry_delay': dt.timedelta(minutes=1),
}
 
with DAG('CSV_to_JSON',
 
         default_args=default_args,
         schedule_interval=timedelta(minutes=10),  
        catchup=False,     
         ) as dag:
 
    #Download_CSV = BashOperator(task_id='downloading_csv',bash_command='cd /opt/airflow/data & wget https://bit.ly/33s7SBI -O covid19.csv -q')
    
    CSV_to_Postgres = PythonOperator(task_id='PushCSVtoPostgres',
 
                             python_callable=CSV_to_Postgres)
    
    CSV_from_Postgres_to_JSON = PythonOperator(task_id='Pull_CSV_from_Postgres_and_Convert_to_JSON',
 
                             python_callable=Pull_CSV_from_Postgres_Convert_to_Json)
    
    Install_dependecies = BashOperator(task_id='installing_PyMongo',bash_command='pip install pymongo dnspython')
 
    JSON_to_Mongo = PythonOperator(task_id='json_to_mongo',
 
                             python_callable=push_json_to_mongo)
 
 

CSV_to_Postgres >> CSV_from_Postgres_to_JSON >> Install_dependecies >> JSON_to_Mongo