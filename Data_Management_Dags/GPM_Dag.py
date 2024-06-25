from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


from PGM_dag.sql.create_tbl import create_tables_sql
from PGM_dag.sql.create_tbl1 import create_tables_sql1
from PGM_dag.sql.sql_task_2 import compare_staging_employee_ts_table_query
from PGM_dag.sql.task_3_2 import employee_leave_data_transformation_sql
from PGM_dag.sql.sql_task_1 import compare_stg_tbl_task1
from PGM_dag.sql.sql_task_4_1 import compare_staging_desig_count
from PGM_dag.sql.sql_task_4_2 import compare_staging_upcomming_leaves


from PGM_dag.python_file.S3_silver_push import Save_to_S3
from PGM_dag.python_file.readS3 import get_latest_s3_file_path
from PGM_dag.python_file.readS3_2_prefix import get_latest_s3_file_path_with_2_prefix
from PGM_dag.python_file.readS3_3_prefix import get_latest_s3_file_path_with_3_prefix


from PGM_dag.steps.steps import task1_step
from PGM_dag.steps.steps import task2_step
from PGM_dag.steps.steps import task4_1_step
from PGM_dag.steps.steps import task4_2_step
from PGM_dag.steps.steps import task4_3_step
from PGM_dag.steps.steps import task3_2_step
from PGM_dag.steps.steps import CLUSTER_ID


# Short Circuit condition
def check_xcom_value(task_id,key,failure,success,**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids=task_id, key=key)
    if xcom_value == "None":  # Define your condition here
        return failure
    else:
        return success

# Short Circuit condition
def check_xcom_value_2_prefix(task_id,key1,key2,failure,success,**kwargs):
    ti = kwargs['ti']
    xcom_value1 = ti.xcom_pull(task_ids=task_id, key=key1)
    xcom_value2 = ti.xcom_pull(task_ids=task_id, key=key2)
    if xcom_value1 == "None" or xcom_value2 == "None":  # Define your condition here
        return failure
    else:
        return success

# Short Circuit condition
def check_xcom_value_3_prefix(task_id,key1,key2,key3,failure,success,**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids=task_id, key=key)
    xcom_value2 = ti.xcom_pull(task_ids=task_id, key=key2)
    xcom_value3 = ti.xcom_pull(task_ids=task_id, key=key3)
    if xcom_value == "None" or xcom_value2 == "None" or xcom_value3 == "None":  # Define your condition here
        return failure
    else:
        return success


bucket_name_bronze = 'ttn-de-bootcamp-2024-bronze-us-east-1'
bucket_name_silver = 'ttn-de-bootcamp-2024-silver-us-east-1'
bucket_name_gold = 'ttn-de-bootcamp-2024-gold-us-east-1'

ED = 'priyanshu.rana/emp-data/'                         #(Silver)
ELD = 'priyanshu.rana/emp-leave-data/'                  #(Silver)
ELQD = 'priyanshu.rana/emp-leave-quota-data/'           #(Silver)
ELCD = 'priyanshu.rana/employee-leave-calender-data/'   #(Silver)
ELTF = 'priyanshu.rana/emp-time-data/'                  #(Silver)
ELSD = 'priyanshu.rana/employee-leaves-spend-data/'    #Yet to write(Gold)
EADC = 'priyanshu.rana/emp-active-desig-data/'         #Yet to write(Gold)
EULC = 'priyanshu.rana/emp-upcoming-leaves-data/'      #Yet to write(Gold)


default_args = {
    'owner': 'priyanshu',
    'depends_on_past': False,
    'email_on_failure': False,
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_retry': False,
    'retries': 0
}

# Define the DAG
with DAG('GPM_Dag',
         default_args=default_args,
         schedule_interval='0 7 * * *'
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    create_tables = PostgresOperator(
        task_id='create_tables',
        sql=create_tables_sql1,
        postgres_conn_id='ec2-postgres',  # Connection ID configured in Airflow UI
        autocommit=True,  # Ensure autocommit is set to True
        dag=dag
    )

    task1_get_latest_s3_file = PythonOperator(
        task_id='task1_get_latest_s3_file',
        provide_context=True,
        python_callable=get_latest_s3_file_path,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix":ED},
        dag=dag,
    )
    branching_1 = BranchPythonOperator(
        task_id='branching_1',
        python_callable=check_xcom_value,
        provide_context=True,
        op_kwargs={"task_id": 'task1_get_latest_s3_file', "key": 's3_path', "failure": "task2_get_latest_s3_file",
                   "success": "task_group_1.task1_add_step"},
        dag=dag,
    )
    with TaskGroup("task_group_1", dag=dag) as tg1:
        # Add the step to the EMR cluster
        task1_add_step = EmrAddStepsOperator(
            task_id='task1_add_step',
            job_flow_id=CLUSTER_ID,
            steps=task1_step,
            aws_conn_id='aws_default',
            dag=dag,
        )

        # Wait for the step to complete
        task1_step_checker = EmrStepSensor(
            task_id='task1_step_checker',
            job_flow_id=CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='task_group_1.task1_add_step', key='return_value')[0] }}",
            aws_conn_id='aws_default',
            dag=dag,
        )

        task1_compare_stg_tbl = PostgresOperator(
            task_id='task1_compare_stg_tbl',
            sql=compare_stg_tbl_task1,
            postgres_conn_id='ec2-postgres',  # Connection ID configured in Airflow UI
            autocommit=True,  # Ensure autocommit is set to True
            dag=dag
        )
        task1_add_step >> task1_step_checker >> task1_compare_stg_tbl

    task2_get_latest_s3_file = PythonOperator(
        task_id='task2_get_latest_s3_file',
        provide_context=True,
        python_callable=get_latest_s3_file_path,
        op_kwargs={"bucket_name": bucket_name_bronze, "prefix": ELTF},
        dag=dag,
    )
    branching_2 = BranchPythonOperator(
        task_id='branching_2',
        python_callable=check_xcom_value,
        provide_context=True,
        op_kwargs={"task_id": 'task2_get_latest_s3_file', "key": 's3_path', "failure": "task3_2_get_latest_s3_file",
                   "success": "tg2.task2_add_step"},
        dag=dag,
    )

    with TaskGroup("task_group_2", dag=dag) as tg2:

        # Add the step to the EMR cluster
        task2_add_step = EmrAddStepsOperator(
            task_id='task2_add_step',
            job_flow_id=CLUSTER_ID,
            steps=task2_step,
            aws_conn_id='aws_default',
            dag=dag,
        )

        # Wait for the step to complete
        task2_step_checker = EmrStepSensor(
            task_id='task2_step_checker',
            job_flow_id=CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='task2_add_step', key='return_value')[0] }}",
            aws_conn_id='aws_default',
            dag=dag,
        )

        compare_ts_stage_final_data = PostgresOperator(
            task_id="compare_ts_stage_final_data",
            postgres_conn_id='ec2-postgres',
            sql=compare_staging_employee_ts_table_query
        )

        ELTF_Save_to_S3_task = PythonOperator(
                task_id='ELTF_Save_to_S3_task',
                python_callable=Save_to_S3,
                op_kwargs={'bucket_name': bucket_name_silver, 'prefix': ELTF, 'table_name': "employee_ts_table",
                           'file_format': 'csv'},
                provide_context=True,
                dag=dag,
            )

        task2_add_step >> task2_step_checker >> compare_ts_stage_final_data >> ELTF_Save_to_S3_task

    # TASK 3_1 HERE......
     #     This is yearly


    # Read file from S3
    task3_2_get_latest_s3_file = PythonOperator(
        task_id='task3_2_get_latest_s3_file',
        python_callable=get_latest_s3_file_path,
        op_kwargs={'bucket_name': bucket_name_bronze, 'prefix': ELD},
        provide_context=True,
    )

    branching_3_2 = BranchPythonOperator(
        task_id='branching_3_2',
        python_callable=check_xcom_value,
        provide_context=True,
        op_kwargs={"task_id": 'task3_2_get_latest_s3_file', "key": 's3_path', "failure": "task4_1_get_latest_s3_file",
                   "success": "tg3.task3_2_add_step"},
        dag=dag,
    )

    with TaskGroup("task_group_3", dag=dag) as tg3_2:

        # ADD STEP HERE TO ADD TO STAGING TABLE
        task3_2_add_step = EmrAddStepsOperator(
                task_id='task3_2_add_step',
                job_flow_id=CLUSTER_ID,
                steps=task3_2_step,
                aws_conn_id='aws_default',
                dag=dag,
            )

        task3_2_step_checker = EmrStepSensor(
            task_id='task3_2_step_checker',
            job_flow_id=CLUSTER_ID,
            step_id="{{ task_instance.xcom_pull(task_ids='task3_2_add_step', key='return_value')[0] }}",
            aws_conn_id='aws_default',
            dag=dag,
        )


        # ADD Data from Staging to Final
        daily_leave_tracking_task_3_2 = PostgresOperator(
            task_id="daily_leave_tracking_task_3_2",
            postgres_conn_id='ec2-postgres',
            sql=employee_leave_data_transformation_sql
        )

        # Save from Final table to S3 Silver
        ELD_Save_to_S3_task = PythonOperator(
                task_id='ELD_Save_to_S3_task',
                python_callable=Save_to_S3,
                op_kwargs={'bucket_name': bucket_name_silver, 'prefix': ELD, 'table_name': "employee_leave_data",
                           'file_format': 'csv'},
                provide_context=True,
                dag=dag,
        )

    task4_1_get_latest_s3_file = PythonOperator(
            task_id='task4_1_get_latest_s3_file',
            provide_context=True,
            python_callable=get_latest_s3_file_path,
            op_kwargs={"bucket_name": bucket_name_silver, "prefix": ELTF},
            dag=dag,
        )
    branching_4_1 = BranchPythonOperator(
        task_id='branching_4_1',
        python_callable=check_xcom_value,
        provide_context=True,
        op_kwargs={"task_id": 'task4_1_get_latest_s3_file', "key": 's3_path', "failure": "task4_2_get_latest_s3_file",
                   "success": "tg4_1.task4_1_add_step"},
        dag=dag,
    )

    with TaskGroup("task_group_4_1", dag=dag) as tg4_1:
        # Add the step to the EMR cluster
        task4_1_add_step = EmrAddStepsOperator(
                task_id='task4_1_add_step',
                job_flow_id=CLUSTER_ID,
                steps=task4_1_step,
                aws_conn_id='aws_default',
                dag=dag,
            )

        # Wait for the step to complete
        task4_1_step_checker = EmrStepSensor(
                task_id='task4_1_step_checker',
                job_flow_id=CLUSTER_ID,
                step_id="{{ task_instance.xcom_pull(task_ids='task4_1_add_step', key='return_value')[0] }}",
                aws_conn_id='aws_default',
                dag=dag,
            )

        compare_staging_desig_count = PostgresOperator(
                task_id="compare_staging_desig_count",
                postgres_conn_id='ec2-postgres',
                sql=compare_staging_desig_count
            )
        task4_1_add_step >> task4_1_step_checker >> compare_staging_desig_count

    task4_2_get_latest_s3_file = PythonOperator(
            task_id='task4_2_get_latest_s3_file',
            provide_context=True,
            python_callable=get_latest_s3_file_path_with_2_prefix,
            op_kwargs={"bucket_name": bucket_name_silver, "prefix": ELCD, "prefix2": ELD},
            dag=dag,
        )

    branching_4_2 = BranchPythonOperator(
        task_id='branching_4_2',
        python_callable=check_xcom_value_2_prefix,
        provide_context=True,
        op_kwargs={"task_id": 'task4_2_get_latest_s3_file', "key1": 's3_path', "key2": 's3_path2',
                   "failure": "task4_3_get_latest_s3_file",
                   "success": "tg4_2.task4_2_add_step"},
        dag=dag,
    )

    with TaskGroup("task_group_4_2", dag=dag) as tg4_2:
        # Add the step to the EMR cluster
        task4_2_add_step = EmrAddStepsOperator(
                task_id='task4_2_add_step',
                job_flow_id=CLUSTER_ID,
                steps=task4_2_step,
                aws_conn_id='aws_default',
                dag=dag,
            )

        # Wait for the step to complete
        task4_2_step_checker = EmrStepSensor(
                task_id='task4_2_step_checker',
                job_flow_id=CLUSTER_ID,
                step_id="{{ task_instance.xcom_pull(task_ids='task4_2_add_step', key='return_value')[0] }}",
                aws_conn_id='aws_default',
                dag=dag,
            )

        task_4_2_compare_upcomming_leaves = PostgresOperator(
                task_id="task_4_2_compare_upcomming_leaves",
                postgres_conn_id='ec2-postgres',
                sql=compare_staging_upcomming_leaves
            )

        task4_2_add_step >> task4_2_step_checker >> task_4_2_compare_upcomming_leaves

    task4_3_get_latest_s3_file = PythonOperator(
            task_id='task4_3_get_latest_s3_file',
            provide_context=True,
            python_callable=get_latest_s3_file_path_with_2_prefix,
            op_kwargs={"bucket_name": bucket_name_silver, "prefix": ELQD, "prefix2": ELD},
            dag=dag,
        )

    branching_4_3 = BranchPythonOperator(
        task_id='branching_4_3',
        python_callable=check_xcom_value_2_prefix,
        provide_context=True,
        op_kwargs={"task_id": 'task4_3_get_latest_s3_file', "key1": 's3_path', "key2": 's3_path2',
                   "failure": "end",
                   "success": "tg4_3.task4_3_add_step"},
        dag=dag,
    )

    with TaskGroup("task_group_4_3", dag=dag) as tg4_3:

        # Add the step to the EMR cluster
        task4_3_add_step = EmrAddStepsOperator(
                task_id='task4_3_add_step',
                job_flow_id=CLUSTER_ID,
                steps=task4_3_step,
                aws_conn_id='aws_default',
                dag=dag,
            )

        # Wait for the step to complete
        task4_3_step_checker = EmrStepSensor(
                task_id='task4_3_step_checker',
                job_flow_id=CLUSTER_ID,
                step_id="{{ task_instance.xcom_pull(task_ids='task4_3_add_step', key='return_value')[0] }}",
                aws_conn_id='aws_default',
                dag=dag,
            )

        task_4_3_Save_to_S3 = PythonOperator(
                task_id='task_4_3_Save_to_S3',
                python_callable=Save_to_S3,
                op_kwargs={'bucket_name': bucket_name_gold, 'prefix': ELSD, 'table_name': "employee_leaves_spend_table",
                           'file_format': 'text'},
                provide_context=True,
                dag=dag,
            )
        task4_3_add_step >> task4_3_step_checker >> task_4_3_Save_to_S3

    end = DummyOperator(
            task_id='end',
            dag=dag,
        )

# start >> create_tables >> task1_get_latest_s3_file >> task1_add_step >> task1_step_checker >> task1_compare_stg_tbl >> task2_get_latest_s3_file >> task2_add_step >> task2_step_checker >> ELTF_Save_to_S3_task  >> ELD_Save_to_S3_task >> daily_leave_tracking_task_3_2 >> task4_1_get_latest_s3_file >> task4_1_add_step >> task4_1_step_checker >> task4_2_get_latest_s3_file >> task4_2_add_step >> task4_2_step_checker >> task_4_2_compare_upcomming_leaves >> task4_3_get_latest_s3_file >> task4_3_add_step >> task4_3_step_checker >> task_4_3_Save_to_S3 >> end
start >> create_tables >> task1_get_latest_s3_file >> branching_1
branching_1 >> task2_get_latest_s3_file >> branching_2
branching_1 >> tg1 >> task2_get_latest_s3_file >> branching_2


branching_2 >> task3_2_get_latest_s3_file >> branching_3_2
branching_2 >> tg2 >> task3_2_get_latest_s3_file >> branching_3_2

branching_3_2 >> task4_1_get_latest_s3_file >> branching_4_1
branching_3_2 >> tg3_2 >> task4_1_get_latest_s3_file >> branching_4_1


branching_4_1 >>  task4_2_get_latest_s3_file >> branching_4_2
branching_4_1 >> tg4_1 >> task4_2_get_latest_s3_file >> branching_4_2

branching_4_2 >> task4_3_get_latest_s3_file >> branching_4_3
branching_4_2 >> tg4_2 >> task4_3_get_latest_s3_file >> branching_4_3

branching_4_3 >> end
branching_4_3 >> tg4_3 >> end
