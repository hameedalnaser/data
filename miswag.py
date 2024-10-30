from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import csv
import io
import json
import requests

# Variables defined in Airflow UI
FB_ACCESS_TOKEN = Variable.get("FB_ACCESS_TOKEN")
FACEBOOK_BASE_API_URL = Variable.get("FACEBOOK_BASE_API_URL")
FB_CATALOG_ID = Variable.get("FB_CATALOG_ID")
FB_BUSSINES_ID = Variable.get("FB_BUSSINES_ID")

def alert_on_failure(context):
    # Custom alert logic here
    pass


def testmydag(response_text):
    """Test if response contains necessary data."""
    return "id" in response_text

def fetch_data_from_github(**kwargs):
    """Fetch data from Github and push to XCom."""
    http_hook = HttpHook(http_conn_id="githubcon", method="GET")
    response = http_hook.run(endpoint="hameedalnaser/data/refs/heads/main/products.csv")

    csv_file = io.StringIO(response.text)
    data = list(csv.DictReader(csv_file))
    
    kwargs['ti'].xcom_push(key='products_data', value=data)
    return bool(data)  # Returns False if data is empty

def batch_data(data, batch_size=4999):
    """Yield data in batches."""
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def transform_data(**kwargs):
    """Transform data and prepare for Facebook batch API."""
    input_data = kwargs['ti'].xcom_pull(key='products_data', task_ids='fetch_products_data')
    transformed_data = [
        {
            "method": "UPDATE",
            "data": {
                "id": str(item["id"]),
                "title": item["title"],
                "description": item["description"],
                "availability": item["availability"],
                "price": item["price"],
                "link": item["link"],
                "image_link": item["image_link"],
                "brand": item["brand"],
                "condition": item["condition"],
                "google_product_category": item["product_type"]
            }
        }
        for item in input_data
    ]
    print(transformed_data)
    kwargs['ti'].xcom_push(key='transformed_batches', value=list(batch_data(transformed_data)))

def send_to_facebook_catalog(**kwargs):
    """Send each batch to Facebook's catalog API."""
    batches = kwargs['ti'].xcom_pull(key='transformed_batches', task_ids='transform_data')
    handles = []
    
    for batch in batches:
        url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/items_batch?item_type=PRODUCT_ITEM"
        headers = {
            "Authorization": f"Bearer {FB_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json={"requests": batch})
        response_data = response.json()

        if response.status_code == 200 and 'handles' in response_data:
            handles.extend(response_data['handles'])  # Use extend to add all handles from the list
        else:
            raise Exception(f"Failed to send batch: {response.status_code} - {response.text}")

    kwargs['ti'].xcom_push(key='batch_handles', value=handles)

def check_batch_status(**kwargs):
    """Check the status of each batch in Facebook's API."""
    handles = kwargs['ti'].xcom_pull(key='batch_handles', task_ids='send_to_facebook_catalog')
    
    for handle in handles:
        url = f"{FACEBOOK_BASE_API_URL}/{FB_CATALOG_ID}/check_batch_request_status?handle={handle}"
        headers = {"Authorization": f"Bearer {FB_ACCESS_TOKEN}"}
        response = requests.get(url, headers=headers)
        response_data = response.json()
        
        if response.status_code == 200 and "data" in response_data:
            batch_data = response_data["data"][0]
            if batch_data["status"] == "finished":
                errors = batch_data.get("errors", [])
                warnings = batch_data.get("warnings", [])
                
                if errors:
                    print("Errors found:", errors)
                if warnings:
                    print("Warnings found:", warnings)
                
                if not errors:
                    print("Batch processed successfully with no errors.")
            else:
                print(f"Batch process status: {batch_data['status']}")
        else:
            raise Exception(f"Failed to check batch status: {response.status_code} - {response.text}")

with DAG(
    'Miswag_Products_sync_with_FB',
    description='Sync Miswag products with Facebook catalog with fault tolerance and batching',
    start_date=datetime(2024, 10, 30),
    schedule_interval='@hourly',
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': lambda context: send_email(
            to="res.eng.hameed@hotmail.com",
            subject="DAG Miswag_Products_sync_with_FB failed",
            html_content=f"Task {context['task_instance_key_str']} failed. Check the logs for details."
        ),
        'retry_exponential_backoff': True,
    }
) as dag:
    
    extract_products = HttpSensor(
        task_id="check_data_fetching_api_endpoint",
        http_conn_id="githubcon",
        endpoint="hameedalnaser/data/refs/heads/main/products.csv",
        response_check=lambda response: testmydag(response.text),
        poke_interval=5,
        timeout=20,
        on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),
    )
    
    fetch_products_data = ShortCircuitOperator(
        task_id='fetch_products_data',
        python_callable=fetch_data_from_github,
        provide_context=True,
        on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),
    )

    send_to_facebook_task = PythonOperator(
        task_id='send_to_facebook_catalog',
        python_callable=send_to_facebook_catalog,
        provide_context=True,
        on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),
    )

    check_batch_status_task = PythonOperator(
        task_id='check_batch_status',
        python_callable=check_batch_status,
        provide_context=True,
        on_failure_callback=alert_on_failure,
        sla=timedelta(hours=1),

    )

    extract_products >> fetch_products_data >> transform_task >> send_to_facebook_task >> check_batch_status_task
