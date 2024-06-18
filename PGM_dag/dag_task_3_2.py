from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import ShortCircuitOperator
from PGM_dag.python_file.task1_move_file import task1_move_file

from PGM_dag.sql.create_tbl import create_tables_sql
from PGM_dag.sql.task_3_2 import employee_leave_data_transformation_sql

from PGM_dag.python_file.S3_silver_push import Save_to_S3
from PGM_dag.python_file.readS3 import get_latest_s3_file_path

from PGM_dag.steps.steps import task3_2_step
from PGM_dag.steps.steps import task_db_step

from PGM_dag.steps.steps import CLUSTER_ID
from PGM_dag.steps.steps import bucket_name_bronze
from PGM_dag.steps.steps import bucket_name_silver
from PGM_dag.steps.steps import bucket_name_gold
from PGM_dag.steps.steps import ELD

ELD_save = 'madhav.kashyap/emp-leave-data-save/'

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
def check_xcom_value(task_id,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_id,key = 's3_path')
    if xcom_value == "None":  # Define your condition here
        return False
    else:
        return True



# Define the DAG
with DAG('dag_task_3_2',
         default_args=default_args,
         schedule_interval='0 7 * * *',
         catchup=True
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Read file from S3
    task3_2_get_oldest_s3_file = PythonOperator(
        task_id='task3_2_get_oldest_s3_file',
        python_callable=get_latest_s3_file_path,
        op_kwargs={'bucket_name': bucket_name_bronze, 'prefix': ELD},
        provide_context=True,
    )
    short_circuit_op = ShortCircuitOperator(
        task_id='short_circuit_op',
        python_callable=check_xcom_value,
        op_kwargs={'task_id':'task3_2_get_latest_s3_file'},
        provide_context=True,
    )

    # ADD STEP HERE TO ADD TO STAGING TABLE
    task3_2_add_step = EmrAddStepsOperator(
            task_id='task3_2_add_step',
            job_flow_id=CLUSTER_ID,
            steps=task3_2_step,
           #aws_conn_id='aws_default',
            dag=dag,
        )

    task3_2_step_checker = EmrStepSensor(
        task_id='task3_2_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='task3_2_add_step', key='return_value')[0] }}",
       #aws_conn_id='aws_default',
        dag=dag,
    )


    # ADD Data from Staging to Final
    daily_leave_tracking_task_3_2 = PostgresOperator(
        task_id="daily_leave_tracking_task_3_2",
        postgres_conn_id='ec2-postgres',
        sql=employee_leave_data_transformation_sql
    )

    task_db_to_ec2 = EmrAddStepsOperator(
            task_id='task_db_to_ec2',
            job_flow_id=CLUSTER_ID,
            steps=task_db_step,
           #aws_conn_id='aws_default',
            dag=dag,
    )

    task_db_step_checker = EmrStepSensor(
        task_id='task_db_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='task_db_to_ec2', key='return_value')[0] }}",
       #aws_conn_id='aws_default',
        dag=dag,
    )

    task1_move_file = PythonOperator(
        task_id='task1_move_file',
        provide_context=True,
        python_callable=task1_move_file,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix": ELD, "destination_prefix": ELD_save},
        dag=dag,
    )

    end = DummyOperator(
            task_id='end',
            dag=dag,
    )

start >> task3_2_get_oldest_s3_file >> short_circuit_op >> task3_2_add_step >> task3_2_step_checker >>daily_leave_tracking_task_3_2 >> task_db_to_ec2 >> task_db_step_checker >> task1_move_file >> end
