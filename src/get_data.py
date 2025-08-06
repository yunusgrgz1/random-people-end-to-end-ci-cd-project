import json
import logging
import requests
import boto3
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

NUM_OF_RESULTS = 100
url = f"https://randomuser.me/api/?results={NUM_OF_RESULTS}"


def fetch_api_data(url):
    logger.info(f"Fetching data from API: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("API data fetched successfully.")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        return None

def upload_to_s3(data):
    
    data_json_str = json.dumps(data)
    logger.info("Uploading data to S3 bucket...")
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        s3.put_object(
            Bucket=os.getenv("S3_BUCKET_NAME"),
            Key=f"raw_data/people_data{datetime.now()}.json",
            Body=data_json_str,
            ContentType='application/json'
        )
        logger.info("Data uploaded to S3 successfully.")
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {e}")
        raise



def get_and_upload_to_s3():
    logger.info("Starting main execution...")

    data = fetch_api_data(url)
    if data is None:
        logger.error("No data retrieved. Exiting.")
        return

    # Convert raw data to JSON string to upload to S3
    try:
        upload_to_s3(data)
    except Exception:
        logger.error("Uploading to S3 failed. Exiting.")
        return

    logger.info("Main execution finished successfully.")

if __name__ == "__main__":
    get_and_upload_to_s3()
