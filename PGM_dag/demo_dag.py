# from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
import boto3
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
# from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
# from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
# from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
# from airflow.providers.amazon.aws.operators.emr import EmrStepSensor
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta,timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'new_emr_dag',
    default_args=default_args,
    description='A DAG to create EMR cluster and post data to Postgres',
    schedule_interval='@once',
)

def get_latest_s3_file_path(**kwargs):
    bucket_name = 'ttn-de-bootcamp-2024-bronze-us-east-1'
    prefix = 'priyanshu.rana/emp-data'

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Get the most recent file based on LastModified timestamp
    latest_file = None
    latest_timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone
    for obj in response.get('Contents', []):
        obj_last_modified = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file is None or obj_last_modified > latest_timestamp):
            latest_file = obj['Key']
            latest_timestamp = obj_last_modified

    if latest_file:
        s3_path = f's3a://{bucket_name}/{latest_file}'
        # Push the s3_path to XCom
        kwargs['ti'].xcom_push(key='s3_path', value=s3_path)
    else:
        raise ValueError("No files found in the specified S3 bucket/prefix.")

get_latest_s3_file = PythonOperator(
    task_id='get_latest_s3_file',
    provide_context=True,
    python_callable=get_latest_s3_file_path,
    dag=dag,
)

submit_spark_job = EmrAddStepsOperator(
    task_id='submit_spark_job',
    job_flow_id="j-7A78GZ1A23EY",
    steps=[
        {
            'Name': 'Spark Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                         '--deploy-mode',
                         'cluster',
                         '--jars',
                         '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                         's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script_2.py',
                         '{{ ti.xcom_pull(task_ids="get_latest_s3_file", key="s3_path") }}'],
            },
        },
    ],
    aws_conn_id='aws_default',
    dag=dag,
)

wait_for_spark_job_completion = EmrStepSensor(
    task_id='wait_for_spark_job_completion',
    job_flow_id="j-7A78GZ1A23EY",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_job', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)



get_latest_s3_file >> submit_spark_job >> wait_for_spark_job_completion
