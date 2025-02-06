from airflow.sensors.filesystem import FileSensor
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from modules.database_utils import load_data_with_copy_command
from modules.dataframe_actions import determine_configs, df_from_detected_file, find_extra_columns
from modules.logs import write_and_log
import logging

logging.basicConfig(
    filename='orchestration.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logging.debug("Script started...")

def process_file(**kwargs):
    try:
        # Get the file path detected by FileSensor
        file_path = kwargs['ti'].xcom_pull(task_ids='detect_new_file')
        if not file_path:
            raise ValueError("No file path received from FileSensor.")
        
        logging.info(f"Processing file: {file_path}")

        # Load the detected file into a DataFrame
        df = df_from_detected_file(file_path)

        # Get configs and columns based on file name
        table_name, ordered_core_attributes, core_columns_string, config, core_and_alternative_columns = determine_configs(
            file_path, df.columns
        )
        extra_columns_list = find_extra_columns(df, core_and_alternative_columns, ordered_core_attributes)
        
        # Define columns to ignore
        ignored_columns = kwargs.get('ignored_columns', [])

        # Log details
        logging.info(f"Table Name: {table_name}")
        logging.info(f"Core Attributes: {ordered_core_attributes}")
        logging.info(f"Extra Columns: {extra_columns_list}")
        logging.info(f"Ignored Columns: {ignored_columns}")

        # Copy data to the database
        logging.info(f"Starting data upload for file: {file_path}")
        load_data_with_copy_command(df, file_path, table_name, ordered_core_attributes, extra_columns_list, ignored_columns)
        logging.info(f"Data upload completed for file: {file_path}")

        print("Data copy to the database is at its end.")

    except Exception as e:
        # Log any errors that occur
        logging.error(f"An error occurred while processing the file: {e}", exc_info=True)
        raise

with DAG(
    'file_sensor',
    default_args={
        'owner': 'zalesjan',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple file sensor DAG',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    file_sensor = FileSensor(
    task_id='detect_new_file',
    filepath=r'C:\Users\zalesak\Downloads\FVA\*.txt',
    fs_conn_id='local_file_system',
    poke_interval=15,
    timeout=600
    )

    process_file_task = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
        op_kwargs={
            'ignored_columns': [],  # Add ignored columns here
        },
    )
    print("Script started...")
    file_sensor >> process_file_task