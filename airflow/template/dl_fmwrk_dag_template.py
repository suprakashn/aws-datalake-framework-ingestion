from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

def initializer(**kwargs):
    src_sys_id = "src_sys_id_placeholder"
    ast_id = "ast_id_placeholder"
    instance_id = datetime.now().strftime("%Y%m%d%H%M%S")
    exec_id = f"{src_sys_id}_{ast_id}_{instance_id}"
    src_path = f"s3://dl-fmwrk-{src_sys_id}-us-east-2/{ast_id}/init/{instance_id}"
    job_name_args = "dl-fmwrk-data-standardization"

    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key="src_sys_id", value=src_sys_id)
    task_instance.xcom_push(key="ast_id", value=ast_id)
    task_instance.xcom_push(key="exec_id", value=exec_id)
    task_instance.xcom_push(key="src_path", value=src_path)
    task_instance.xcom_push(key="job_name_args", value=job_name_args)

schedule = "schedule_placeholder"
yesterday = datetime.combine(datetime.today(), datetime.min.time())
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date' : yesterday
}

dag =  DAG(
    dag_id = "dag_id_placeholder",
    default_args = default_args,
    schedule_interval=schedule,    
    catchup=False,
    is_paused_upon_creation=False
)

t1 = PythonOperator(
    task_id='start',
    python_callable=initializer,
    provide_context=True,
    dag = dag
)

t2 = AwsGlueJobOperator(
    task_id = "data_ingestion",
    job_name = "dl-fmwrk-data-ingestion",
    region_name = "us-east-2",
    script_location = "s3://dl-fmwrk-code-us-east-2/aws-datalake-framework-ingestion/ingestion/",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}",
        "--source_path" : "{{ task_instance.xcom_pull(task_ids='start', key='src_path')}}"
        },
    dag = dag
    )

t3 = AwsGlueJobOperator(
    task_id = "quality_check",
    job_name = "dl-fmwrk-data-quality-checks",
    region_name = "us-east-2",
    script_location = "s3://dl-fmwrk-code-us-east-2/aws-datalake-framework/src/",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}",
        "--source_path" : "{{ task_instance.xcom_pull(task_ids='start', key='src_path')}}"
        },
    dag = dag
    )

t4 = AwsGlueJobOperator(
    task_id = "data_masking",
    job_name = "dl-fmwrk-data-masking",
    region_name = "us-east-2",
    script_location = "s3://dl-fmwrk-code-us-east-2/aws-datalake-framework/src/",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}",
        "--source_path" : "{{ task_instance.xcom_pull(task_ids='start', key='src_path')}}"
        },
    dag = dag
    )

t5 = AwsGlueJobOperator(
    task_id = "data_standardization",
    job_name = "dl-fmwrk-data-standardization",
    region_name = "us-east-2",
    script_location = "s3://dl-fmwrk-code-us-east-2/aws-datalake-framework/src/",
    num_of_dpus = 1,
    script_args = {
        "--source_id" : "{{ task_instance.xcom_pull(task_ids='start', key='src_sys_id')}}",
        "--asset_id" : "{{ task_instance.xcom_pull(task_ids='start', key='ast_id')}}",
        "--exec_id" : "{{ task_instance.xcom_pull(task_ids='start', key='exec_id')}}",
        "--source_path" : "{{ task_instance.xcom_pull(task_ids='start', key='src_path')}}",
        "--JOB_NAME" : "{{ task_instance.xcom_pull(task_ids='start', key='job_name_args')}}"
        },
    dag = dag
    )


t6 = DummyOperator(
    task_id='end',
    dag = dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
