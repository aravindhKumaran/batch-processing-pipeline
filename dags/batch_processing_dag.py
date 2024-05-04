import airflow
import json
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator
)


SPARK_STEPS = [
    {
        'Name': 'data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'client', 
                '--num-executors', '2',
                '--driver-memory', '512m',
                '--executor-memory', '3g',
                '--executor-cores', '2',
                's3://batch-processing-artifact/pyscript.py', 
                '{{ ds }}', 
                "{{ task_instance.xcom_pull('parse_request', key='sales') }}"
                "{{ task_instance.xcom_pull('parse_request', key='calendar') }}"
                "{{ task_instance.xcom_pull('parse_request', key='inventory') }}"
                "{{ task_instance.xcom_pull('parse_request', key='store') }}"
                "{{ task_instance.xcom_pull('parse_request', key='product') }}"
            ]
        }
    } 

]

JOB_FLOW_OVERRIDES = {
    "Name": "batch-processing-cluster",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [
        {
            "Name": "Spark"
        },         
        {
            "Name": "Hadoop"
        }
    ],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ]
        }
    ],
    "Instances": {
        "Ec2SubnetId": "subnet-066d5b253527eab0a",
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 1",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,                
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "batch-processing-ec2-role",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://batch_processing-logs/emr-logs/"
}


DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def retrieve_s3_files(**kwargs):
    print("*****returened value*****" , kwargs['params'])
    params = kwargs["params"]
    for key, value in params.items():
        print("Key:", key)
        print("Value:", value)       
        kwargs['ti'].xcom_push(key = key, value = value)


dag = DAG(
    'batch_processing_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

parse_request = PythonOperator(
    task_id = 'parse_request',
    provide_context = True,
    python_callable = retrieve_s3_files,
    dag = dag
 ) 


create_emr_cluster = EmrCreateJobFlowOperator(
    task_id = "create_emr_cluster",
    aws_conn_id = "aws_default", 
    job_flow_overrides = JOB_FLOW_OVERRIDES,
    dag = dag  
)


add_steps = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)


last_step = len(SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" 
    + str(last_step)
    + "] }}",
    aws_conn_id = "aws_default", 
    dag = dag
)


terminate_cluster = EmrTerminateJobFlowOperator(
    task_id = "terminate_cluster",
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id = "aws_default",
    dag = dag
)


create_emr_cluster.set_upstream(parse_request)  
add_steps.set_upstream(create_emr_cluster)
step_checker.set_upstream(add_steps)
terminate_cluster.set_upstream(step_checker)
