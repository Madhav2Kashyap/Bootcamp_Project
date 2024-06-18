import json

# Define global variables
jdbc_url = "jdbc:postgresql://34.229.143.113/advance"
connection_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver",
    "batchsize": "50000"  # Set the batch size to 50000 records
}

# Convert the connection_properties dictionary to a string
connection_properties = json.dumps(connection_properties)


bucket_name_bronze = 'ttn-de-bootcamp-2024-bronze-us-east-1'
bucket_name_silver = 'ttn-de-bootcamp-2024-silver-us-east-1'
bucket_name_gold = 'ttn-de-bootcamp-2024-gold-us-east-1'
ELSD = 'madhav.kashyap/emp-leaves-spend-data/'
ED = 'madhav.kashyap/emp-data/'
ELD = 'madhav.kashyap/emp-leave-data/'
ELQD = 'madhav.kashyap/emp-leave-quota-data/'
ELCD = 'madhav.kashyap/emp-leave-calender-data/'
ELTF = 'madhav.kashyap/emp-time-data/'
ELSD = 'madhav.kashyap/emp-leaves-spend-data/'    #Yet to write(Gold)
EADC = 'madhav.kashyap/emp-active-desig-count/'
EULC = 'madhav.kashyap/emp-upcoming-leaves-data/'


table_task_1 = 'employee_data_stg'
table_task_2 = 'employee_ts_table_dim'
table_task_3_1_1 = 'employee_leave_quota_data_dim'
table_task_3_1_2 = 'employee_leave_calender_data_dim'
table_task_3_2 = 'staging_employee_leave_data'
table_task_4_1 = 'employee_desig_count_table_dim'
table_task_4_2 = 'employee_upcoming_leaves_table_dim'
table_task_4_3 = 'employee_leaves_spend_table_dim'
table_task_db = 'employee_leave_data_dim'
table_step_4_2 = 'stg_employee_upcoming_leaves'
table_step_4_1 = 'staging_desig_count'


CLUSTER_ID = 'j-280U2BGXTGVV1'

file_format = 'text'



# Define the EMR step
# jdbc_url, connection_properties, bucket_name, prefix, table_name
task1_step = [
    {
        'Name': '1 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task1.py',
                '{{ ti.xcom_pull(task_ids="task1_get_latest_s3_file", key="s3_path") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_task_1
            ],
        },
    }
]

# connection_properties, jdbc_url, bucket_name, prefix
task2_step = [
    {
        'Name': '2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task2.py',
                '{{ ti.xcom_pull(task_ids="task2_get_latest_s3_file", key="s3_path") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_task_2
            ],
        },
    }
]
task_3_1_1_step = [
    {
        'Name': '3_1_1 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task_3_1.py',
                '{{ ti.xcom_pull(task_ids="task3_1_1_get_latest_s3_file", key="s3_path") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_task_3_1_1

            ],
        },
    }
]

task_3_1_2_step  = [
    {
        'Name': '3_1_2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task_3_1.py',
                '{{ ti.xcom_pull(task_ids="task3_1_2_get_latest_s3_file", key="s3_path") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_task_3_1_2
            ],
        },
    }
]

# connection_properties, jdbc_url, bucket_name, prefix
task3_2_step = [
    {
        'Name': '3_2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task3_2.py',
                '{{ ti.xcom_pull(task_ids="task3_2_get_latest_s3_file", key="s3_path") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_task_3_2
            ],
        },
    }
]

task_db_step = [
    {
        'Name': 'db Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/db_to_ec2.py',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_task_db ,
                '--bucket_name',bucket_name_silver,
                '--prefix',ELD
            ],
        },
    }
]


# connection_properties, jdbc_url, bucket_name, prefix
task4_1_step = [
    {
        'Name': '4_1 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task4_1.py',
                '{{ ti.xcom_pull(task_ids="task4_1_get_latest_s3_file", key="s3_paths") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_step_4_1
            ],
        },
    }
]

# jdbc_url, bucket_name, prefix, prefix2, connection_properties
task4_2_step = [
    {
        'Name': '4_2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task4_2.py',
                '{{ ti.xcom_pull(task_ids="task4_2_get_latest_s3_file", key="s3_path") }}',
                '{{ ti.xcom_pull(task_ids="task4_2_get_latest_s3_file", key="s3_path2") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties,
                '--table_name',table_step_4_2
            ],
        },
    }
]

# jdbc_url, connection_properties, bucket_name, prefix, prefix2, prefix3
task4_3_step = [
    {
        'Name': '4_3 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', '/usr/lib/spark/jars/postgresql-42.6.2.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/madhav.kashyap/athena_results/task4_3.py',
                '{{ ti.xcom_pull(task_ids="task4_3_get_latest_s3_file", key="s3_path") }}',
                '{{ ti.xcom_pull(task_ids="task4_3_get_latest_s3_file", key="s3_path2") }}',
                '--jdbc_url',jdbc_url,
                '--cp',connection_properties
            ],
        },
    }
]
