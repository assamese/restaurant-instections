import os
import json
from time import time
from datetime import datetime, timedelta
from schema_creator import SchemaCreator
from csv_reader import CSV_Reader
from data_sanitizer import DataSanitizer
from config_framework import ConfigFramework
from dim_table_readers import DimTableReaders
from table_updaters import TableUpdaters

# Apache Airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'robnewman',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 3),
    'email': ['sanjay@sanjaydas.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context':True
}

# DAG
dag = DAG(
    'daily_run_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1))

# Operators
def create_schema(**kwargs):
    SchemaCreator.create_tables()

def readCSV(**kwargs):
    return CSV_Reader.readCSV()()

def task2_method(**kwargs):
    pass

def task3_method(**kwargs):
    pass

def task4_method(**kwargs):
    pass

def task5_method(**kwargs):
    pass

def appendToFactTable(**kwargs):
    TableUpdaters.appendToFactTable('xx','yy')

# Tasks
task0 = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    dag=dag
)
task1 = PythonOperator(
    task_id='readCSV',
    python_callable=readCSV,
    dag=dag
)
task2 = PythonOperator(
    task_id='task2_method',
    python_callable=task2_method,
    dag=dag
)
task3 = PythonOperator(
    task_id='task3_method',
    python_callable=task3_method,
    dag=dag
)
task4 = PythonOperator(
    task_id='task4_method',
    python_callable=task4_method,
    dag=dag
)
task5 = PythonOperator(
    task_id='task5_method',
    python_callable=task5_method,
    dag=dag
)
task6 = PythonOperator(
    task_id='appendToFactTable',
    python_callable=appendToFactTable,
    dag=dag
)
# Set dependencies
task0 >> task1 >> task2 >> task3 >> task4 >> task5 >> task6