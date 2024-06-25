from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from PGM_dag.sql.sql_task_1 import compare_stg_tbl_task1
from PGM_dag.python_file.task1_move_file import task1_move_file
from PGM_dag.python_file.readS3 import get_latest_s3_file_path
from PGM_dag.steps.steps import task1_step
from PGM_dag.steps.steps import CLUSTER_ID
from airflow.operators.python import ShortCircuitOperator

def check_xcom_value(task_id,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_id,key = 's3_path')
    if xcom_value == "None":  # Define your condition here
        return False
    else:
        return True


bucket_name_bronze = 'ttn-de-bootcamp-2024-bronze-us-east-1'

ED = 'madhav.kashyap/emp-data/'                         #(Silver)
ED_save ='madhav.kashyap/emp-data-save/'

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
with DAG('dag_task_1',
         default_args=default_args,
         schedule_interval= '0 7 * * *',
         catchup=True
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    task1_get_oldest_s3_file = PythonOperator(
        task_id='task1_get_oldest_s3_file',
        provide_context=True,
        python_callable=get_latest_s3_file_path,
        op_kwargs={"bucket_name": bucket_name_bronze,"prefix":ED},
        dag=dag,
    )

    short_circuit_op = ShortCircuitOperator(
        task_id='short_circuit_op',
        python_callable=check_xcom_value,
        op_kwargs={'task_id':'task1_get_oldest_s3_file'},
        provide_context=True,
    )

    # Add the step to the EMR cluster
    task1_add_step = EmrAddStepsOperator(
        task_id='task1_add_step',
        job_flow_id=CLUSTER_ID,
        steps=task1_step,
       #aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task1_step_checker = EmrStepSensor(
        task_id='task1_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='task1_add_step', key='return_value')[0] }}",
        #aws_conn_id='aws_default',
        dag=dag,
    )

    task1_compare_stg_tbl = PostgresOperator(
        task_id='task1_compare_stg_tbl',
        sql=compare_stg_tbl_task1,
        postgres_conn_id='ec2-postgres',  # Connection ID configured in Airflow UI
        autocommit=True,  # Ensure autocommit is set to True
        dag=dag
    )
    
    task1_move_file = PythonOperator(
        task_id='task1_move_file',
        provide_context=True,
        python_callable=task1_move_file,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix": ED ,"destination_prefix":ED_save,"task_id":'task1_get_oldest_s3_file'},
        dag=dag,
    )


    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

    start >> task1_get_oldest_s3_file >> short_circuit_op >> task1_add_step >> task1_step_checker >> task1_compare_stg_tbl >> task1_move_file >> end
