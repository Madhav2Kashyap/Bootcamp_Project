from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
# from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


consumerStep = [
    {
        'Name': 'Consumer Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'client',
                '--packages',
                'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/consumer.py'
            ],
        },
    }
]

CLUSTER_ID = 'j-280U2BGXTGVV1'

default_args = {
    'owner': 'madhav',
    'depends_on_past': False,
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_retry': False,
    'retries': 0
}

# Define the DAG
with DAG('consumer_dag',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Add the step to the EMR cluster
    consumer_start = EmrAddStepsOperator(
        task_id='consumer_start',
        job_flow_id=CLUSTER_ID,
        steps=consumerStep,
        dag=dag,
    )


    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

    start >> consumer_start >> end
