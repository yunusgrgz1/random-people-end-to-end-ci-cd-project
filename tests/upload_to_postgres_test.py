import pytest
from unittest.mock import MagicMock, patch
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

# imort the functions from upload_to_postgres.py
import upload_to_postgres as etl


@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate()
    yield spark_session
    spark_session.stop()


def test_define_schema():
    schema = etl.define_schema()
    assert isinstance(schema, StructType)
    assert any(field.name == "gender" for field in schema.fields)


@patch("upload_to_postgres.boto3.client")
def test_get_data_from_s3(mock_boto_client):
    # Mock S3 response
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_response = {
        "Body": MagicMock(read=MagicMock(return_value=b'{"results":[{"gender":"male"}]}'))
    }
    mock_s3.get_object.return_value = mock_response

    data = etl.get_data_from_s3()
    assert "results" in data
    assert data["results"][0]["gender"] == "male"


@patch("upload_to_postgres.psycopg2.connect")
def test_connect_postgres_success(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    conn = etl.connect_postgres()
    assert conn == mock_conn
    mock_connect.assert_called_once()


def test_create_flat_dataframe(spark):
    # Basic test data
    sample_data = {
        "results": [{
            "gender": "female",
            "name": {"title": "Ms", "first": "Jane", "last": "Doe"},
            "location": {
                "street": {"number": 123, "name": "Main St"},
                "city": "TestCity",
                "state": "TS",
                "country": "Testland",
                "postcode": "12345",
                "coordinates": {"latitude": "0.0", "longitude": "0.0"},
                "timezone": {"offset": "+0:00", "description": "UTC"}
            },
            "email": "jane.doe@test.com",
            "login": {
                "uuid": "uuid123",
                "username": "janedoe",
                "password": "pass",
                "salt": "salt",
                "md5": "md5",
                "sha1": "sha1",
                "sha256": "sha256"
            },
            "dob": {"date": "2000-01-01T00:00:00Z", "age": 25},
            "registered": {"date": "2010-01-01T00:00:00Z", "age": 15},
            "phone": "123-456",
            "cell": "789-101",
            "id": {"name": "IDName", "value": "IDValue"},
            "picture": {"large": "", "medium": "", "thumbnail": ""},
            "nat": "TL"
        }]
    }
    schema = etl.define_schema()
    df = etl.create_flat_dataframe(spark, sample_data, schema)
    assert df.count() == 1
    cols = df.columns
    assert "gender" in cols
    assert "name_first" in cols
    assert df.collect()[0]["name_first"] == "Jane"


@patch("upload_to_postgres.psycopg2.connect")
def test_insert_data_to_postgres(mock_connect, spark):
    # Mock Postgres connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Basic df example
    sample_data = {
        "results": [{
            "gender": "female",
            "name": {"title": "Ms", "first": "Jane", "last": "Doe"},
            "location": {
                "street": {"number": 123, "name": "Main St"},
                "city": "TestCity",
                "state": "TS",
                "country": "Testland",
                "postcode": "12345",
                "coordinates": {"latitude": "0.0", "longitude": "0.0"},
                "timezone": {"offset": "+0:00", "description": "UTC"}
            },
            "email": "jane.doe@test.com",
            "login": {
                "uuid": "uuid123",
                "username": "janedoe",
                "password": "pass",
                "salt": "salt",
                "md5": "md5",
                "sha1": "sha1",
                "sha256": "sha256"
            },
            "dob": {"date": "2000-01-01T00:00:00Z", "age": 25},
            "registered": {"date": "2010-01-01T00:00:00Z", "age": 15},
            "phone": "123-456",
            "cell": "789-101",
            "id": {"name": "IDName", "value": "IDValue"},
            "picture": {"large": "", "medium": "", "thumbnail": ""},
            "nat": "TL"
        }]
    }
    schema = etl.define_schema()
    spark_df = etl.create_flat_dataframe(spark, sample_data, schema)

    etl.insert_data_to_postgres(mock_conn, mock_cursor, spark_df)

    assert mock_cursor.execute.call_count > 0
    mock_conn.commit.assert_called_once()


@patch("upload_to_postgres.get_data_from_s3")
@patch("upload_to_postgres.create_spark_session")
@patch("upload_to_postgres.connect_postgres")
def test_load_to_postgres(mock_connect_postgres, mock_create_spark, mock_get_s3, spark):
    mock_get_s3.return_value = {
        "results": [{
            "gender": "female",
            "name": {"title": "Ms", "first": "Jane", "last": "Doe"},
            "location": {
                "street": {"number": 123, "name": "Main St"},
                "city": "TestCity",
                "state": "TS",
                "country": "Testland",
                "postcode": "12345",
                "coordinates": {"latitude": "0.0", "longitude": "0.0"},
                "timezone": {"offset": "+0:00", "description": "UTC"}
            },
            "email": "jane.doe@test.com",
            "login": {
                "uuid": "uuid123",
                "username": "janedoe",
                "password": "pass",
                "salt": "salt",
                "md5": "md5",
                "sha1": "sha1",
                "sha256": "sha256"
            },
            "dob": {"date": "2000-01-01T00:00:00Z", "age": 25},
            "registered": {"date": "2010-01-01T00:00:00Z", "age": 15},
            "phone": "123-456",
            "cell": "789-101",
            "id": {"name": "IDName", "value": "IDValue"},
            "picture": {"large": "", "medium": "", "thumbnail": ""},
            "nat": "TL"
        }]
    }
    mock_create_spark.return_value = spark

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect_postgres.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    etl.load_to_postgres()

    # Check: Were the connection and Spark session functions called?
    mock_create_spark.assert_called_once()
    mock_get_s3.assert_called_once()
    mock_connect_postgres.assert_called_once()
    mock_cursor.execute.assert_called()

