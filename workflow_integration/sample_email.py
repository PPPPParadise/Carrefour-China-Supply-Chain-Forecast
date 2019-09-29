import datetime
import warnings
from datetime import timedelta

warnings.filterwarnings('ignore')
from impala.dbapi import connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 1, 8),
    'end_date': datetime.datetime(2049, 1, 1),
    'depends_on_past': True,
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15)
}


sample_email = DAG('sample_email',
          schedule_interval='10 0 * * *',
          max_active_runs = 1,
          default_args=default_args, catchup=False)

send_email = EmailOperator (
    task_id="send_email",
    to=["test@gmail.com"],
    subject="Test email",
    html_content='<h3>Email content </h3>',
    files=["testfile.text"],
    dag = sample_email)