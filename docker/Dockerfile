FROM bitnami/spark:latest

COPY spark/app/stream_watch_events.py /opt/spark-app/
WORKDIR /opt/spark-app

CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1", "/opt/spark-app/stream_watch_events.py"]
