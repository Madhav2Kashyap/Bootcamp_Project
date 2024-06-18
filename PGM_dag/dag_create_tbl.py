from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from PGM_dag.sql.create_tbl import create_tables_sql


default_args = {
    'owner': 'madhav',
    'depends_on_past': False,
    'email': 'userworkhere@gmail.com',
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 10),
    'email_on_retry': False,
    'retries': 0
}

with DAG('create_table_dag',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    create_tables = PostgresOperator(
        task_id='create_tables',
        sql=create_tables_sql,
        postgres_conn_id='ec2-postgres',  # Connection ID configured in Airflow UI
        autocommit=True,  # Ensure autocommit is set to True
        dag=dag
    )
    
    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

    start >> create_tables >> end
