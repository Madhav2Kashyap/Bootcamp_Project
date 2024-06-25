from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from PGM_dag.sql.create_tbl import create_tables_sql


from PGM_dag.python_file.S3_silver_push import Save_to_S3_1
from PGM_dag.python_file.readS3_2_prefix import get_latest_s3_file_path_with_2_prefix

from PGM_dag.steps.steps import task4_3_step
from PGM_dag.steps.steps import CLUSTER_ID
from PGM_dag.steps.steps import table_task_4_3
from PGM_dag.steps.steps import bucket_name_bronze
from PGM_dag.steps.steps import bucket_name_silver
from PGM_dag.steps.steps import bucket_name_gold
from PGM_dag.steps.steps import ELD
from PGM_dag.steps.steps import ELQD
from PGM_dag.steps.steps import ELSD





def check_xcom_value2(task_id,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_id, key='s3_path')
    xcom_value2 = task_instance.xcom_pull(task_ids=task_id, key='s3_path2')
    if xcom_value == "None" or xcom_value2 == "None":  # Define your condition here
        return False
    else:
        return True

file_format = 'text'
default_args = {
    'owner': 'priyanshu',
    'depends_on_past': False,
    'email': 'userworkhere@gmail.com',
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime.combine(datetime.today() , datetime.min.time()),
    'email_on_retry': False,
    'retries': 0
}


# Define the DAG
with DAG('dag_task_4_3',
         default_args=default_args,
         schedule_interval='0 0 1 * *',
         catchup=True
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    task4_3_get_oldest_s3_file = PythonOperator(
            task_id='task4_3_get_oldest_s3_file',
            provide_context=True,
            python_callable=get_latest_s3_file_path_with_2_prefix,
            op_kwargs={"bucket_name": bucket_name_silver, "prefix": ELQD, "prefix2": ELD},
            dag=dag,
        )
    short_circuit_op = ShortCircuitOperator(
        task_id='short_circuit_op',
        python_callable=check_xcom_value2,
        op_kwargs={'task_id':'task4_3_get_latest_s3_file'},
        provide_context=True,
    )

    # Add the step to the EMR cluster
    task4_3_add_step = EmrAddStepsOperator(
            task_id='task4_3_add_step',
            job_flow_id=CLUSTER_ID,
            steps=task4_3_step,
           #aws_conn_id='aws_default',
            dag=dag,
        )

    # Wait for the step to complete
    task4_3_step_checker = EmrStepSensor(
            task_id='task4_3_step_checker',
            job_flow_id=CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='task4_3_add_step', key='return_value')[0] }}",
           #aws_conn_id='aws_default',
            dag=dag,
        )

    task_4_3_Save_to_S3 = PythonOperator(
            task_id='task_4_3_Save_to_S3',
            python_callable=Save_to_S3_1,
            op_kwargs={'bucket_name': bucket_name_gold, 'prefix': ELSD, 'table_name': table_task_4_3,
                       'file_format': 'text'},
            provide_context=True,
            dag=dag,
        )

    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

start >> task4_3_get_oldest_s3_file >> short_circuit_op >> task4_3_add_step >> task4_3_step_checker >> task_4_3_Save_to_S3 >> end
