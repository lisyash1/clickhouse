from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime, timedelta
import uuid

CLICKHOUSE_HOST = "clickhouse1"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "data_engineer"
CLICKHOUSE_PASSWORD = "njvfc135"
CLICKHOUSE_DATABASE = "crypto"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_s3_backup',
    default_args=default_args,
    description='Daily ClickHouse backup to S3 disk',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 24),
    catchup=False,
)


def backup_clickhouse_to_s3(ti):
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        unique_id = str(uuid.uuid4())[:8]
        backup_file_name = f"backup_{CLICKHOUSE_DATABASE}_{timestamp}_{unique_id}.zip"
        full_backup_name = f"Disk('s3_disk', '{backup_file_name}')"

        clickhouse_client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        backup_query = f"""
        BACKUP DATABASE {CLICKHOUSE_DATABASE} TO {full_backup_name} SYNC
        """
        clickhouse_client.execute(backup_query)
        print(f"Backup completed: {full_backup_name}")

        ti.xcom_push(key='backup_name', value=full_backup_name)
    except Exception as e:
        print(f"Error during backup: {e}")
        raise


def check_backup_status(ti):
    try:
        full_backup_name = ti.xcom_pull(task_ids='backup_clickhouse_to_s3', key='backup_name')
        if not full_backup_name:
            raise ValueError("Backup name not found in XCom")

        clickhouse_client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        check_backup_query = f"""
        SELECT id, name, status FROM system.backups WHERE name = '{full_backup_name}'
        """
        result = clickhouse_client.execute(check_backup_query)
        if not result:
            raise ValueError(f"No backup found with name '{full_backup_name}'")
        print("Backup status:", result)
    except Exception as e:
        print(f"Error checking backup status: {e}")
        raise


# Задачи в DAG
backup_task = PythonOperator(
    task_id='backup_clickhouse_to_s3',
    python_callable=backup_clickhouse_to_s3,
    dag=dag,
)

check_backup_task = PythonOperator(
    task_id='check_backup_status',
    python_callable=check_backup_status,
    dag=dag,
)

backup_task >> check_backup_task