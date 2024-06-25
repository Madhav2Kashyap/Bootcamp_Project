from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from PGM_dag.sql.sql_task_4_1 import compare_staging_desig_count
from PGM_dag.sql.sql_task_4_2 import compare_staging_upcomming_leaves
from PGM_dag.sql.create_tbl import create_tables_sql

from PGM_dag.python_file.readS3 import get_latest_s3_file_path
from PGM_dag.python_file.S3_silver_push import Save_to_S3
from PGM_dag.python_file.readS3_2_prefix import get_latest_s3_file_path_with_2_prefix

from PGM_dag.steps.steps import task4_1_step
from PGM_dag.steps.steps import task4_2_step
from PGM_dag.steps.steps import table_task_4_1
from PGM_dag.steps.steps import table_task_4_2
from PGM_dag.steps.steps import bucket_name_bronze
from PGM_dag.steps.steps import bucket_name_silver
from PGM_dag.steps.steps import bucket_name_gold
from PGM_dag.steps.steps import ELD
from PGM_dag.steps.steps import ELCD
from PGM_dag.steps.steps import ELTF
from PGM_dag.steps.steps import EADC
from PGM_dag.steps.steps import EULC


from PGM_dag.steps.steps import CLUSTER_ID
from airflow.operators.python import ShortCircuitOperator

def check_xcom_value(task_id,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_id, key ='s3_path')
    if xcom_value == "None":  # Define your condition here
        return False
    else:
        return True



def check_xcom_value2(task_ids,**context):
    task_instance = context['ti']
    xcom_value = task_instance.xcom_pull(task_ids=task_ids,key='s3_path')
    xcom_value2 = task_instance.xcom_pull(task_ids=task_ids,key='s3_path2')
    if xcom_value ==  "None" or xcom_value2 =="None":  # Define your condition here
        return False
    else:
        return True


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
with DAG('dag_task_4_1_2',
         default_args=default_args,
         schedule_interval='15 7 * * *',
         catchup=True
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    task4_1_get_oldest_s3_file = PythonOperator(
            task_id='task4_1_get_oldest_s3_file',
            provide_context=True,
            python_callable=get_latest_s3_file_path,
            op_kwargs={"bucket_name": bucket_name_silver, "prefix": ELTF},
            dag=dag,
        )
    short_circuit_op = ShortCircuitOperator(
        task_id='short_circuit_op',
        python_callable=check_xcom_value,
        op_kwargs={'task_id':'task4_1_get_latest_s3_file'},
        provide_context=True,
    )

    # Add the step to the EMR cluster
    task4_1_add_step = EmrAddStepsOperator(
            task_id='task4_1_add_step',
            job_flow_id=CLUSTER_ID,
            steps=task4_1_step,
           #aws_conn_id='aws_default',
            dag=dag,
        )

    # Wait for the step to complete
    task4_1_step_checker = EmrStepSensor(
            task_id='task4_1_step_checker',
            job_flow_id=CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='task4_1_add_step', key='return_value')[0] }}",
           #aws_conn_id='aws_default',
            dag=dag,
        )

    compare_staging_desig_count = PostgresOperator(
            task_id="compare_staging_desig_count",
            postgres_conn_id='ec2-postgres',
            sql=compare_staging_desig_count
        )

    EADC_Save_to_S3_task = PythonOperator(
        task_id='EADC_Save_to_S3_task',
        python_callable=Save_to_S3,
        op_kwargs={'bucket_name': bucket_name_gold, 'prefix': EADC, 'table_name': table_task_4_1,
                   'file_format': 'csv'},
        provide_context=True,
        dag=dag,
    )

    task4_2_get_oldest_s3_file = PythonOperator(
            task_id='task4_2_get_oldest_s3_file',
            provide_context=True,
            python_callable=get_latest_s3_file_path_with_2_prefix,
            op_kwargs={"bucket_name": bucket_name_silver, "prefix": ELCD, "prefix2": ELD},
            dag=dag,
        )

    short_circuit_op2 = ShortCircuitOperator(
        task_id='short_circuit_op2',
        python_callable=check_xcom_value2,
        op_kwargs={'task_ids': 'task4_2_get_latest_s3_file'},
        provide_context=True,
    )

    # Add the step to the EMR cluster
    task4_2_add_step = EmrAddStepsOperator(
            task_id='task4_2_add_step',
            job_flow_id=CLUSTER_ID,
            steps=task4_2_step,
           #aws_conn_id='aws_default',
            dag=dag,
        )

    # Wait for the step to complete
    task4_2_step_checker = EmrStepSensor(
            task_id='task4_2_step_checker',
            job_flow_id=CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='task4_2_add_step', key='return_value')[0] }}",
           #aws_conn_id='aws_default',
            dag=dag,
        )

    task_4_2_compare_upcomming_leaves = PostgresOperator(
            task_id="task_4_2_compare_upcomming_leaves",
            postgres_conn_id='ec2-postgres',
            sql=compare_staging_upcomming_leaves
        )

    EULC_Save_to_S3_task = PythonOperator(
        task_id='EULC_Save_to_S3_task',
        python_callable=Save_to_S3,
        op_kwargs={'bucket_name': bucket_name_gold, 'prefix': EULC, 'table_name': table_task_4_2,
                   'file_format': 'csv'},
        provide_context=True,
        dag=dag,
    )


    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

start >> task4_1_get_oldest_s3_file >> short_circuit_op >>task4_1_add_step >> task4_1_step_checker >> compare_staging_desig_count >> EADC_Save_to_S3_task  >> end
start >> task4_2_get_oldest_s3_file >> short_circuit_op2 >>task4_2_add_step >> task4_2_step_checker >> task_4_2_compare_upcomming_leaves >> EULC_Save_to_S3_task  >> end

