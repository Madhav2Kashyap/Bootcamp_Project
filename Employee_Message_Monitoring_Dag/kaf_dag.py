from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

import json

tbl1 ="employee_messages_dim"
tbl2 ="employee_ts_table_dim"

jdbc_url = "jdbc:postgresql://54.211.19.46:5432/advance"
connection_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver",
    "batchsize": "50000"  # Set the batch size to 50000 records
}

connection_properties = json.dumps(connection_properties)

checkStrikeStep = [
    {
        'Name': 'checkStrike Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/athena_results/readMsgCheckStrike.py',  # yha pr script ki location dal dena
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name1',tbl1,
                '--table_name2',tbl2 # yha pr table ka naam
            ],
        },
    }
]

CLUSTER_ID = 'j-195HNP97DFY7E'

bucket_name_bronze = 'ttn-de-bootcamp-2024-bronze-us-east-1'

from kaf.sql_strike_upsert import strike_upsert
from kaf.sql_inactive_marking import inactive_marking
from kaf.sql_SCD_upserd import SCD_upserd
from kaf.sql_SCD_table_update import SCD_data_updation

default_args = {
    'owner': 'priyanshu',
    'depends_on_past': False,
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_retry': False,
    'retries': 0
}

# Define the DAG
with DAG('kafka_dag',
         default_args=default_args,
         schedule_interval='30 7 * * *'
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Add the step to the EMR cluster
    readStrike = EmrAddStepsOperator(
        task_id='readStrike',
        job_flow_id=CLUSTER_ID,
        steps=checkStrikeStep,
        dag=dag,
    )

    # Wait for the step to complete
    task_step_checker = EmrStepSensor(
        task_id='task_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='readStrike', key='return_value')[0] }}",
        dag=dag,
    )

    strike_upsert_query = PostgresOperator(
         task_id = 'strike_upsert_query',
         postgres_conn_id='ec2-postgres',
         sql = strike_upsert
    )

    SCD_upserd_Query = PostgresOperator(
        task_id='SCD_upserd_Query',
        postgres_conn_id='ec2-postgres',
        sql=SCD_upserd
    )

    SCD_dataupdate_Query = PostgresOperator(
        task_id='SCD_dataupdate_Query',
        postgres_conn_id='ec2-postgres',
        sql=SCD_data_updation
    )

    inactive_marking_query = PostgresOperator(
        task_id='inactive_marking_query',
        postgres_conn_id='ec2-postgres',
        sql=inactive_marking
    )

    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

    start >> readStrike >> task_step_checker >> strike_upsert_query >> inactive_marking_query >> SCD_upserd_Query >> SCD_dataupdate_Query >> end
