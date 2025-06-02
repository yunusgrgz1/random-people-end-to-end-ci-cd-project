import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
from pyspark.sql.functions import col
import boto3
import os
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    logger.info("Initializing Spark session...")
    try:
        spark = SparkSession.builder.appName("ApiToSparkDF").getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise


def connect_postgres():
    logger.info("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DBNAME", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        logger.info("PostgreSQL connection established.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def get_data_from_s3():
    logger.info("Getting data from S3...")
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        bucket_name = os.getenv("S3_BUCKET_NAME")
        key = "raw_data/people_data.json"

        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        logger.info("Data retrieved from S3 successfully.")
        return data
    except Exception as e:
        logger.error(f"Error retrieving data from S3: {e}")
        raise


def define_schema():
    logger.info("Defining Spark DataFrame schema...")
    return StructType([
        StructField("gender", StringType(), True),
        StructField("name", StructType([
            StructField("title", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
        ]), True),
        StructField("location", StructType([
            StructField("street", StructType([
                StructField("number", IntegerType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("coordinates", StructType([
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True),
            ]), True),
            StructField("timezone", StructType([
                StructField("offset", StringType(), True),
                StructField("description", StringType(), True),
            ]), True),
        ]), True),
        StructField("email", StringType(), True),
        StructField("login", StructType([
            StructField("uuid", StringType(), True),
            StructField("username", StringType(), True),
            StructField("password", StringType(), True),
            StructField("salt", StringType(), True),
            StructField("md5", StringType(), True),
            StructField("sha1", StringType(), True),
            StructField("sha256", StringType(), True),
        ]), True),
        StructField("dob", StructType([
            StructField("date", StringType(), True),
            StructField("age", IntegerType(), True),
        ]), True),
        StructField("registered", StructType([
            StructField("date", StringType(), True),
            StructField("age", IntegerType(), True),
        ]), True),
        StructField("phone", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("id", StructType([
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
        ]), True),
        StructField("picture", StructType([
            StructField("large", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("thumbnail", StringType(), True),
        ]), True),
        StructField("nat", StringType(), True),
    ])


def create_flat_dataframe(spark, data, schema):
    logger.info("Creating Spark DataFrame from data...")
    try:
        df = spark.createDataFrame(data["results"], schema=schema)
    except Exception as e:
        logger.error(f"Error creating DataFrame: {e}")
        raise

    logger.info("Flattening nested fields in the DataFrame...")
    flat_df = df.select(
        col("gender"),
        col("name.title").alias("name_title"),
        col("name.first").alias("name_first"),
        col("name.last").alias("name_last"),
        col("location.street.number").alias("street_number"),
        col("location.street.name").alias("street_name"),
        col("location.city").alias("city"),
        col("location.state").alias("state"),
        col("location.country").alias("country"),
        col("location.postcode").alias("postcode"),
        col("location.coordinates.latitude").alias("latitude"),
        col("location.coordinates.longitude").alias("longitude"),
        col("location.timezone.offset").alias("timezone_offset"),
        col("location.timezone.description").alias("timezone_timezone_description"),
        col("email"),
        col("login.uuid").alias("login_uuid"),
        col("login.username").alias("login_username"),
        col("login.password").alias("login_password"),
        col("login.salt").alias("login_salt"),
        col("login.md5").alias("login_md5"),
        col("login.sha1").alias("login_sha1"),
        col("login.sha256").alias("login_sha256"),
        col("dob.date").alias("dob_date"),
        col("dob.age").alias("dob_age"),
        col("registered.date").alias("registered_date"),
        col("registered.age").alias("registered_age"),
        col("phone"),
        col("cell"),
        col("id.name").alias("id_name"),
        col("id.value").alias("id_value"),
        col("picture.large").alias("picture_large"),
        col("picture.medium").alias("picture_medium"),
        col("picture.thumbnail").alias("picture_thumbnail"),
        col("nat")
    )

    logger.info("DataFrame creation and flattening completed successfully.")
    return flat_df


def insert_data_to_postgres(conn, cursor, spark_df):
    logger.info("Inserting data into PostgreSQL...")

    try:
        # For 'users' table
        users_df = spark_df.select(
            "gender",
            "name_first",
            "name_last",
            "name_title",
            "email",
            "login_username"
        ).toPandas()

        users_insert_query = """
            INSERT INTO users (gender, first_name, last_name, title, email, username)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for _, row in users_df.iterrows():
            cursor.execute(users_insert_query, (
                row['gender'],
                row['name_first'],
                row['name_last'],
                row['name_title'],
                row['email'],
                row['login_username']
            ))

        # For 'addresses' table
        addresses_df = spark_df.select(
            "street_name",
            "street_number",
            "city",
            "state",
            "country",
            "postcode"
        ).toPandas()

        addresses_insert_query = """
            INSERT INTO addresses (street_name, street_number, city, state, country, postcode)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for _, row in addresses_df.iterrows():
            cursor.execute(addresses_insert_query, (
                row['street_name'],
                str(row['street_number']) if row['street_number'] is not None else None,
                row['city'],
                row['state'],
                row['country'],
                str(row['postcode']) if row['postcode'] is not None else None
            ))

        # For 'logins' table
        logins_df = spark_df.select("login_username").toPandas()
        logins_insert_query = """
            INSERT INTO logins (username)
            VALUES (%s)
        """
        for _, row in logins_df.iterrows():
            cursor.execute(logins_insert_query, (row['login_username'],))

        # For 'contacts' table
        contacts_df = spark_df.select("phone", "cell").toPandas()
        contacts_insert_query = """
            INSERT INTO contacts (phone, cell)
            VALUES (%s, %s)
        """
        for _, row in contacts_df.iterrows():
            cursor.execute(contacts_insert_query, (
                row['phone'],
                row['cell']
            ))

        conn.commit()
        logger.info(f"{len(users_df)} records inserted into users table.")
        logger.info(f"{len(addresses_df)} records inserted into addresses table.")
        logger.info(f"{len(logins_df)} records inserted into logins table.")
        logger.info(f"{len(contacts_df)} records inserted into contacts table.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error during insert: {e}")
        raise


def load_to_postgres():
    logger.info("Starting full ETL process...")

    # 1. Create Spark session
    spark = create_spark_session()

    # 2. Get data from S3
    data = get_data_from_s3()

    # 3. Define schema
    schema = define_schema()

    # 4. Create and flatten DataFrame
    spark_df = create_flat_dataframe(spark, data, schema)

    # 5. Connect to PostgreSQL and load data (using 'with' for proper resource management)
    with connect_postgres() as conn:
        with conn.cursor() as cursor:
            insert_data_to_postgres(conn, cursor, spark_df)

    logger.info("ETL process completed successfully.")


if __name__ == "__main__":
    load_to_postgres()