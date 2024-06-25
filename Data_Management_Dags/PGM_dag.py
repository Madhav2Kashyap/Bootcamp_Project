from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from datetime import datetime, timedelta
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

jdbc_url = "jdbc:postgresql://35.170.206.20:5432/advance"
jdbc_user = "priyanshu"
jdbc_pass = "1234"
driver_class = "org.postgresql.Driver"


# # Convert the connection_properties dictionary to a string
# connection_properties = json.dumps(connection_properties)

dag = DAG(
    'emr_postgres_dag',
    default_args=default_args,
    description='A DAG to create EMR cluster and post data to Postgres',
    schedule_interval='@once',
)



# Read path of file from S3 using boto3
# pass it to the pyspark job using spark submit

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
                         '--jars','/usr/lib/spark/jars/postgresql-42.6.2.jar',
                         's3://ttn-de-bootcamp-2024-bronze-us-east-1/gagan.thakur/spark_scripts/script.py',
                         's3://ttn-de-bootcamp-2024-bronze-us-east-1/gagan.thakur/upload_from_local/games_data.csv',
                         '--jdbc_url',jdbc_url,
                         '--jdbc_user',jdbc_user,
                         '--jdbc_pass',jdbc_pass,
                         '--driver_class',driver_class]
            }
        }
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



submit_spark_job >> wait_for_spark_job_completion
