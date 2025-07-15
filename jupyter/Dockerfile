FROM jupyter/base-notebook:python-3.9

USER root

# Java 17 
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"


USER jovyan


RUN pip install --no-cache-dir pyspark boto3 psycopg2-binary
