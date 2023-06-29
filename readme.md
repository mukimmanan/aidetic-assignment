# Approach Used

## Kafka

1. kafka setup using docker.
2. Created a topic named aidetic.

## Spark Strucutred Streaming

1. For retrieving the data from kafka.
2. Created a schema for the incoming data from kafka.
3. Aggregated the data using group by.
4. Then use spark write Stream to write data into elasticsearch.

5. Not handled committing of kafka offset till where the data is read. 