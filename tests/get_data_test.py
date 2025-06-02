import pytest
from unittest.mock import MagicMock
import os
import json
import requests 

# Adjust the path to import the module from the src directory
import sys
sys.path.insert(0, '/opt/airflow/src')

# Import functions from your script
from get_data import fetch_api_data, upload_to_s3, get_and_upload_to_s3, NUM_OF_RESULTS, url

# --- Fixtures for Setup ---

@pytest.fixture(autouse=True)
def set_env_vars():
    """Sets and unsets environment variables for testing AWS/S3 interactions."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test_secret_key"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["S3_BUCKET_NAME"] = "test-bucket"
    yield
    del os.environ["AWS_ACCESS_KEY_ID"]
    del os.environ["AWS_SECRET_ACCESS_KEY"]
    del os.environ["AWS_REGION"]
    del os.environ["S3_BUCKET_NAME"]

# --- Test Functions ---

def test_fetch_api_data_success(mocker):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"results": [{"id": 1, "name": "Test User"}]}
    mock_get = mocker.patch('get_data.requests.get', return_value=mock_response)

    data = fetch_api_data(url)
    assert data is not None
    assert data["results"][0]["name"] == "Test User"
    mock_get.assert_called_once_with(url, timeout=10)

def test_fetch_api_data_failure_http_error(mocker):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Not Found")
    mock_get = mocker.patch('get_data.requests.get', return_value=mock_response)

    data = fetch_api_data(url)
    assert data is None
    mock_get.assert_called_once_with(url, timeout=10)

def test_fetch_api_data_failure_connection_error(mocker):
    mock_get = mocker.patch('get_data.requests.get', side_effect=requests.exceptions.ConnectionError("No network"))

    data = fetch_api_data(url)
    assert data is None
    mock_get.assert_called_once_with(url, timeout=10)

def test_upload_to_s3_success(mocker):
    mock_s3_client = MagicMock()
    mocker.patch('get_data.boto3.client', return_value=mock_s3_client)

    test_data = {"results": [{"test_key": "test_value"}]}
    upload_to_s3(test_data)

    mock_s3_client.put_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="temp/people_data.json",
        Body=json.dumps(test_data),
        ContentType='application/json'
    )

def test_upload_to_s3_failure(mocker):
    mock_s3_client = MagicMock()
    mock_s3_client.put_object.side_effect = Exception("S3 Upload Error")
    mocker.patch('get_data.boto3.client', return_value=mock_s3_client)

    test_data = {"results": [{"test_key": "test_value"}]}
    with pytest.raises(Exception, match="S3 Upload Error"):
        upload_to_s3(test_data)

def test_get_and_upload_to_s3_successful_flow(mocker):
    mock_fetch = mocker.patch('get_data.fetch_api_data', return_value={"results": [{"id": 1, "name": "Success"}]})
    mock_upload = mocker.patch('get_data.upload_to_s3')

    get_and_upload_to_s3()

    mock_fetch.assert_called_once_with(url)
    mock_upload.assert_called_once()

def test_get_and_upload_to_s3_api_fetch_fails(mocker):
    mock_fetch = mocker.patch('get_data.fetch_api_data', return_value=None)
    mock_upload = mocker.patch('get_data.upload_to_s3')

    get_and_upload_to_s3()

    mock_fetch.assert_called_once_with(url)
    mock_upload.assert_not_called()

def test_get_and_upload_to_s3_upload_fails(mocker):
    mock_fetch = mocker.patch('get_data.fetch_api_data', return_value={"results": [{"id": 1, "name": "Data"}]})
    mock_upload = mocker.patch('get_data.upload_to_s3', side_effect=Exception("Upload Failed"))

    get_and_upload_to_s3()

    mock_fetch.assert_called_once_with(url)
    mock_upload.assert_called_once()
    
