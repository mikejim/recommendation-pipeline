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

USER 1001

#COPY spark/app/stream_watch_events.py /opt/spark-app/
WORKDIR /opt/spark-app
COPY ./spark/app /opt/spark-app

# Ensure the output directory exists and has correct permissions
#RUN mkdir -p /opt/spark-app/data/parquet && \
#    chmod -R 777 /opt/spark-app/data/parquet

RUN pip install --no-cache-dir -r requirements.txt

USER 1001

#CMD ["bash", "-c", "sleep 20 && /opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1", "/opt/spark-app/stream_watch_events.py"]
CMD ["bash", "-c", "sleep 50 && /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 /opt/spark-app/stream_watch_events.py"]