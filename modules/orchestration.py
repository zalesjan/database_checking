from airflow.sensors.filesystem import FileSensor
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from modules.database_utils import load_data_with_copy_command
from modules.dataframe_actions import determine_configs, df_from_detected_file, find_extra_columns, etl_process_df
from modules.validate_files_module import validate_file, plausibility_test
import logging
import os
import glob

# Configure logging
logging.basicConfig(
    filename='orchestration.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logging.debug("Script started...")


class CustomFileSensor(FileSensor):
    def poke(self, context):
        """ Custom FileSensor that detects all matching files and pushes them to XCom. """
        matching_files = glob.glob(self.filepath)  # Get all matching files

        if matching_files:
            self.log.info(f"Files found: {matching_files}")
            context["ti"].xcom_push(key="file_paths", value=matching_files)  # Push all files
            return True  # Sensor succeeds
        else:
            self.log.info(f"No matching files found for pattern: {self.filepath}")
            return False  # Keep checking until timeout


# Define the Airflow DAG
with DAG(
    dag_id="euforia_db_dag",
    default_args={
        'owner': 'zalesjan',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='A file processing pipeline',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Detect new file and push path to XCom
    file_sensor = CustomFileSensor(
        task_id='detect_new_file',
        filepath=Variable.get("file_sensor_filepath", default_var="/mnt/c/Users/zalesak/Downloads/FVA/*.txt"),
        fs_conn_id="local_file_system",
        poke_interval=150,
        timeout=600,
        mode="poke",
    )

    # Task 2a: Create DataFrame from the detected file
    create_df_from_detected_file = PythonOperator(
        task_id='create_df',
        python_callable=df_from_detected_file,
        provide_context=True,
        op_kwargs={'file_path': '{{ ti.xcom_pull(task_ids="detect_new_file", key="file_path") }}'}
    )

    # Task 2b: etl_process_df
    etl_process_df_task = PythonOperator(
        task_id='etl_process_df',
        python_callable=etl_process_df,
        provide_context=True,
        op_kwargs={
            'df': '{{ ti.xcom_pull(task_ids="create_df") }}',
            'uploaded_file_name': '{{ ti.xcom_pull(task_ids="detect_new_file") }}', # question is if this can take the filepath there
            'df_columns': '{{ ti.xcom_pull(task_ids=""create_df"") }}' # question is if this can take the df_columns there
        },
    )
    # Task 3: Validate the file structure
    validate_file_task = PythonOperator(
        task_id='validate_file',
        python_callable=validate_file,
        provide_context=True,
        op_kwargs={
            'df': '{{ ti.xcom_pull(task_ids="create_df") }}',
            'config': '{{ ti.xcom_pull(task_ids="etl_process_df") }}',
            'file_name': '{{ ti.xcom_pull(task_ids="detect_new_file") }}'
        },
    )

    # Task 4: Perform plausibility checks
    plausibility_test_task = PythonOperator(
        task_id='plausibility_test',
        python_callable=plausibility_test,
        provide_context=True,
        op_kwargs={
            'df': '{{ ti.xcom_pull(task_ids="create_df") }}'
        },
    )

    


    # Define task dependencies ADD move to tree table, create roles and pwd, calculate basic query
    file_sensor >> create_df_from_detected_file >> etl_process_df_task >> validate_file_task >> plausibility_test_task
    # ADD: move to tree table, create roles and pwd, calculate basic query


    
    # Task 3: Validate file structure for each detected file
    validate_file_task = PythonOperator.partial(
        task_id="validate_file",
        python_callable=validate_file,
    ).expand(op_kwargs=[{
        "df": "{{ ti.xcom_pull(task_ids='etl_process_df') }}",
        "config": "{{ ti.xcom_pull(task_ids='etl_process_df') }}",
        "file_name": file
    } for file in "{{ ti.xcom_pull(task_ids='detect_new_file', key='file_paths') }}"])

    # Task 4: Perform plausibility checks for each detected file
    plausibility_test_task = PythonOperator.partial(
        task_id="plausibility_test",
        python_callable=plausibility_test,
    ).expand(op_kwargs=[{
        "df": "{{ ti.xcom_pull(task_ids='etl_process_df') }}"
    } for file in "{{ ti.xcom_pull(task_ids='detect_new_file', key='file_paths') }}"])
