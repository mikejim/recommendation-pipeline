
4. Create Kafka Topic
Once Docker is running:

docker exec -it <kafka-container-id> bash

# Inside container:
kafka-topics --create --topic watch_events \
 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1


# Good links:
https://www.sparkcodehub.com/pyspark/dataframe/create-dataframe-from-kafka-stream


Spark Streaming:

[Structured Streaming](https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html)

[Stream Processing](https://www.macrometa.com/event-stream-processing/spark-structured-streaming)