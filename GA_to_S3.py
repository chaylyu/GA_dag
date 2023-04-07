from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging 
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'GA',
    default_args=default_args,
    description='Retrieves Data from Google Analytics, processes it into a pandas dataframe, saves it into an S3 Bucket',
    schedule_interval='@daily'
)


def retrieve_data(**kwargs):
    start_date = datetime.utcnow() - timedelta(days=1)
    end_date = datetime.utcnow()
    google_analytics_conn = BaseHook.get_connection('google_analytics_default')
    key_path = google_analytics_conn.extra_dejson.get('key_path')
    view_id = google_analytics_conn.extra_dejson.get('view_id')
    scopes = 'https://www.googleapis.com/auth/analytics.readonly'

    credentials = service_account.Credentials.from_service_account_file(key_path, scopes)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    response = analytics.reports().batchGet(
      body={
           'reportRequests': [
                {
        'viewId': view_id,
        'source' : 'ga',
        'report_type' :'video_events',
        'metrics':[
            {"expression": "ga:totalEvents"},
            {"expression": "ga:uniqueEvents"},
            {"expression": "ga:eventValue"},
            ],
        'dimensions': [
            {"name": "ga:dateHourMinute"},
            {"name": "ga:eventLabel"},
            {"name": "ga:eventAction"},
            {"name": "ga:dimension1"},
            {"name": "ga:date"},
            {"name": "ga:hour"},
            {"name": "ga:minute"},
            ],
        }]
      }
).execute()

 
    rows = response['reports'][0]['data']['rows']
    header = [dim['name'] for dim in response['reports'][0]['columnHeader']['dimensions']]
    header.extend(metric['expression'] for metric in response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries'])
    data = [list(row['dimensions']) + row['metrics'][0]['values'] for row in rows]
    df = pd.DataFrame(data, columns=header)

 
    ti = kwargs['ti']
    ti.xcom_push(key='google_analytics_report', value=df.to_csv(index=False))

    logging.info('Data retrieved successfully.')


def save_to_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    task_instance = kwargs['ti']
    current_time = datetime.utcnow()
    year = current_time.strftime('%Y')
    month = current_time.strftime('%m')
    file_name = f"{'source'}/{'report_name'}/{year}/{month}/{current_time.strftime('%d_%H%M%S')}.csv"
    data = task_instance.xcom_pull(task_ids='retrieve_data', key='google_analytics_report')
    s3_hook.load_string(
        data=data.to_csv(index=False), 
        key=file_name, 
        bucket_name='mygoogleanalytics', 
        replace=True
    )
    logging.info('Data saved to S3 successfully.')


retrieve_data_task = PythonOperator(
    task_id='retrieve_data',
    provide_context=True, 
    python_callable=retrieve_data,
    dag=dag,
)

save_to_s3_task = PythonOperator(
    task_id='save_to_s3',
    python_callable=save_to_s3,
    provide_context=True,
    dag=dag,
)

retrieve_data_task >> save_to_s3_task

