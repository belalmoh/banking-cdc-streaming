FROM bitnamilegacy/spark:3.5.0-debian-12-r17

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

# Install Python packages
RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    pandas==2.0.3 \
    faker==19.3.0 \
    boto3==1.28.25 \
    pyarrow==12.0.1 \
    python-dotenv==1.0.0 \
    loguru==0.7.0

# Copy Spark configuration
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001