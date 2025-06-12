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

WORKDIR /opt/spark-app
COPY ./spark/app /opt/spark-app

# Install Python dependencies first
RUN pip install --no-cache-dir -r requirements.txt

# Create the output directories and set permissions (Windows volume mount compatible)
RUN mkdir -p /opt/spark-app/data/parquet/watch_events && \
    mkdir -p /opt/spark-app/data/parquet/checkpoints/watch_events && \
    chmod -R 777 /opt/spark-app && \
    chmod -R 777 /tmp

# Stay as root user for Windows compatibility
USER root

RUN pip install --no-cache-dir -r requirements.txt

CMD ["bash", "-c", "sleep 50 && /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 /opt/spark-app/stream_watch_events.py"]