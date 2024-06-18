from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import ShortCircuitOperator

from PGM_dag.sql.create_tbl import create_tables_sql
from PGM_dag.sql.sql_task_2 import compare_staging_employee_ts_table_query
from PGM_dag.python_file.task1_move_file import task1_move_file


from PGM_dag.python_file.S3_silver_push import Save_to_S3
from PGM_dag.python_file.readS3 import get_latest_s3_file_path


from PGM_dag.steps.steps import task2_step
from PGM_dag.steps.steps import CLUSTER_ID
from PGM_dag.steps.steps import bucket_name_bronze
from PGM_dag.steps.steps import bucket_name_silver
from PGM_dag.steps.steps import table_task_2
from PGM_dag.steps.steps import ELTF

ELTF_save = 'madhav.kashyap/emp-time-data-save/'

def check_xcom_value(task_id,key,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_id, key=key)
    if xcom_value == "None":  # Define your condition here
        return False
    else:
        return True



default_args = {
    'owner': 'priyanshu',
    'depends_on_past': False,
    'email': 'userworkhere@gmail.com',
    'email_on_failure': True,
    'start_date': datetime.combine(datetime.today() , datetime.min.time()),
    'email_on_retry': False,
    'retries': 0
}

# Define the DAG
with DAG('dag_task_2',
         default_args=default_args,
         schedule_interval='0 7 * * *',
         catchup=True
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    task2_get_oldest_s3_file = PythonOperator(
        task_id='task2_get_oldest_s3_file',
        provide_context=True,
        python_callable=get_latest_s3_file_path,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix": ELTF},
        dag=dag,
    )
    short_circuit_op = ShortCircuitOperator(
        task_id='short_circuit_op',
        python_callable=check_xcom_value,
        op_kwargs={'task_id':'task2_get_latest_s3_file','key':'s3_path'},
        provide_context=True,
        dag=dag,
    )
    # Add the step to the EMR cluster
    task2_add_step = EmrAddStepsOperator(
        task_id='task2_add_step',
        job_flow_id=CLUSTER_ID,
        steps=task2_step,
       #aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task2_step_checker = EmrStepSensor(
        task_id='task2_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='task2_add_step', key='return_value')[0] }}",
       #aws_conn_id='aws_default',
        dag=dag,
    )

    task_2_sql = PostgresOperator(
         task_id = 'task_2_sql',
         postgres_conn_id='ec2-postgres',
         sql = compare_staging_employee_ts_table_query
    )

    ELTF_Save_to_S3_task = PythonOperator(
            task_id='ELTF_Save_to_S3_task',
            python_callable=Save_to_S3,
            op_kwargs={'bucket_name': bucket_name_silver, 'prefix': ELTF, 'table_name': table_task_2,
                       'file_format': 'csv'},
            provide_context=True,
            dag=dag,
        )

    task1_move_file = PythonOperator(
        task_id='task1_move_file',
        provide_context=True,
        python_callable=task1_move_file,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix": ELTF, "destination_prefix": ELTF_save},
        dag=dag,
    )

    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

    start >> task2_get_oldest_s3_file >> short_circuit_op >>task2_add_step >> task2_step_checker >> task_2_sql  >> ELTF_Save_to_S3_task >> task1_move_file >>end
