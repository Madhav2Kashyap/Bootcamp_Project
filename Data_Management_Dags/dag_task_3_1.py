from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from PGM_dag.python_file.task1_move_file import task1_move_file


from PGM_dag.python_file.S3_silver_push import Save_to_S3
from PGM_dag.python_file.readS3 import get_latest_s3_file_path
from PGM_dag.sql.create_tbl import create_tables_sql

from PGM_dag.steps.steps import task_3_1_1_step
from PGM_dag.steps.steps import task_3_1_2_step
from PGM_dag.steps.steps import CLUSTER_ID
from PGM_dag.steps.steps import table_task_3_1_1
from PGM_dag.steps.steps import table_task_3_1_2
from PGM_dag.steps.steps import bucket_name_bronze
from PGM_dag.steps.steps import bucket_name_silver
from PGM_dag.steps.steps import ELCD
from PGM_dag.steps.steps import ELQD

ELQD_save ='madhav.kashyap/emp-leave-quota-data-save/'
ELCD_save = 'madhav.kashyap/emp-leave-calender-data-save/'


def check_xcom_value(task_id,key,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_id, key=key)
    if xcom_value == "None":  # Define your condition here
        return False
    else:
        return True


default_args = {
    'owner': 'PGM',
    'depends_on_past': False,
    'email': 'userworkhere@gmail.com',
    'email_on_failure': True,
    'start_date': datetime.combine(datetime.today() , datetime.min.time()),
    'email_on_retry': False,
    'retries' : 0
}

with DAG('dag_task_3_1',
         default_args=default_args,
         schedule_interval='0 0 1 1 *',
         start_date=datetime(2023, 1, 1),
         catchup=True ) as dag:

    start = DummyOperator(
        task_id='Start',
        dag=dag,
    )

    task3_1_1_get_oldest_s3_file = PythonOperator(
        task_id='task3_1_1_get_oldest_s3_file',
        python_callable=get_latest_s3_file_path,
        op_kwargs={'bucket_name': bucket_name_bronze, 'prefix': ELQD},
        provide_context=True
    )

    short_circuit_op = ShortCircuitOperator(
        task_id='short_circuit_op',
        python_callable=check_xcom_value,
        op_kwargs={'task_id': 'task3_1_1_get_latest_s3_file', 'key': 's3_path'},
        provide_context=True,
    )

    task_3_1_1_add_step = EmrAddStepsOperator(
        task_id='task_3_1_1_add_step',
        job_flow_id=CLUSTER_ID,
        steps=task_3_1_1_step,
       #aws_conn_id='aws_default',
        dag=dag,
    )

    task_3_1_1_step_checker = EmrStepSensor(
        task_id='task_3_1_1_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='task_3_1_1_add_step', key='return_value')[0] }}",
       #aws_conn_id='aws_default',
        dag=dag,
    )

    ELQD_Save_to_S3_task = PythonOperator(
        task_id='ELQD_Save_to_S3_task',
        python_callable=Save_to_S3,
        op_kwargs={'bucket_name': bucket_name_silver, 'prefix': ELQD, 'table_name': table_task_3_1_1,
                   'file_format': 'csv'},
        provide_context=True,
        dag=dag,
    )
    
    task1_move_file_1 = PythonOperator(
        task_id='task1_move_file_1',
        provide_context=True,
        python_callable=task1_move_file,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix":ELQD, "destination_prefix": ELQD_save},
        dag=dag,
    )

    task3_1_2_get_oldest_s3_file = PythonOperator(
        task_id='task3_1_2_get_oldest_s3_file',
        python_callable=get_latest_s3_file_path,
        op_kwargs={'bucket_name': bucket_name_bronze, 'prefix': ELCD},
        provide_context=True,
    )

    short_circuit_op_2 = ShortCircuitOperator(
        task_id='short_circuit_op_2',
        python_callable=check_xcom_value,
        op_kwargs={'task_id':'task3_1_2_get_latest_s3_file','key':'s3_path'},
        provide_context=True,
    )

    task_3_1_2_add_step = EmrAddStepsOperator(
        task_id='task_3_1_2_add_step',
        job_flow_id=CLUSTER_ID,
        steps=task_3_1_2_step,
       #aws_conn_id='aws_default',
        dag=dag,
    )

    task_3_1_2_step_checker = EmrStepSensor(
        task_id='task_3_1_2_step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='task_3_1_2_add_step', key='return_value')[0] }}",
       #aws_conn_id='aws_default',
        dag=dag,
    )

    ELCD_Save_to_S3_task = PythonOperator(
        task_id='ELCD_Save_to_S3_task',
        python_callable=Save_to_S3,
        op_kwargs={'bucket_name': bucket_name_silver, 'prefix': ELCD, 'table_name': table_task_3_1_2,
                   'file_format': 'csv'},
        provide_context=True,
        dag=dag,
    )

    task1_move_file_2 = PythonOperator(
        task_id='task1_move_file_2',
        provide_context=True,
        python_callable=task1_move_file,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix": ELCD, "destination_prefix": ELCD_save},
        dag=dag,
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

start >> task3_1_1_get_oldest_s3_file >> short_circuit_op >>  task_3_1_1_add_step >> task_3_1_1_step_checker >> ELQD_Save_to_S3_task>>task1_move_file_1 >> end
start >> task3_1_2_get_oldest_s3_file >> short_circuit_op_2 >> task_3_1_2_add_step >> task_3_1_2_step_checker >> ELCD_Save_to_S3_task >>task1_move_file_2 >> end
