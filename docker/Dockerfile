FROM bitnami/spark:latest

# Install pip and Python dependencies
USER root

RUN apt-get update && \
    apt-get install -y curl && \
    curl -sSLo miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh && \
    /opt/conda/bin/pip install --upgrade pip

ENV PATH="/opt/conda/bin:$PATH"

# Use system Python for PySpark (more reliable in distributed mode)
ENV PYSPARK_PYTHON=python3.12
ENV PYSPARK_DRIVER_PYTHON=python3.12

WORKDIR /opt/spark-app
COPY ./spark/app /opt/spark-app

# Install Python dependencies in both conda and system Python
RUN pip install --no-cache-dir -r requirements.txt
RUN python3.12 -m pip install --no-cache-dir -r requirements.txt

# Install Python dependencies first
RUN pip install --no-cache-dir -r requirements.txt

# Create the output directories and set permissions (Windows volume mount compatible)

RUN mkdir -p /app/shared_volume && chmod -R 777 /app/shared_volume
RUN mkdir -p /app/shared_volume/parquet_output && \
    chmod -R 777 /app/shared_volume

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

#RUN mkdir -p /opt/spark-app/data/parquet/watch_events && \
#    mkdir -p /opt/spark-app/data/parquet/checkpoints/watch_events && \
#    chmod -R 777 /opt/spark-app && \
#    chmod -R 777 /tmp

# Stay as root user for Windows compatibility
USER root

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]

#CMD ["bash", "-c", "sleep 50 && /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 /opt/spark-app/stream_watch_events.py"]