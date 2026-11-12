# OpenLake Engine — Spark + Iceberg runtime image.
# One image is used for the Spark master, the worker and the pipeline driver so
# that Python and JVM dependencies are identical on every node.
FROM apache/spark:4.0.1-python3

USER root

ARG ICEBERG_VERSION=1.10.0
ARG HADOOP_AWS_VERSION=3.4.1
# The AWS SDK v2 shipped inside iceberg-aws-bundle 1.10.0 is 2.33.0; hadoop-aws
# additionally needs the S3 transfer manager from the same SDK release.
ARG AWS_SDK_VERSION=2.33.0
ARG MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/software/amazon/awssdk/s3-transfer-manager/${AWS_SDK_VERSION}/s3-transfer-manager-${AWS_SDK_VERSION}.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && python3 -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('iceberg', 'httpfs', 'avro')]" \
    && mkdir -p /home/spark/.duckdb && cp -r /root/.duckdb/* /home/spark/.duckdb/ \
    && mkdir -p /opt/spark/work && chown -R spark:spark /home/spark /opt/spark/work

WORKDIR /opt/openlake
COPY pyproject.toml README.md ./
COPY config ./config
COPY openlake ./openlake
COPY tests ./tests
RUN mkdir -p data reports benchmarks/results checkpoints && chown -R spark:spark /opt/openlake

ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    OPENLAKE_HOME=/opt/openlake \
    HOME=/home/spark \
    PYTHONPATH=/opt/openlake:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

USER spark
