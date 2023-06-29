from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_sparksession():
    """
    This function creates a spark session.

    :return:
        SparkSession: A SparkSession Object
    """

    return SparkSession.builder \
        .master("local[2]") \
        .config("spark.ui.port", "6066") \
        .appName("Aidetic Assignment") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1,"
                                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,"
                                       "commons-httpclient:commons-httpclient:3.1,"
                ) \
        .config("group.id", "elasticsearch") \
        .getOrCreate()


def connect_kafka(spark: SparkSession, broker_url: str, topic: str):
    """
    This functions connects you to the kafka datasource and returns a dataframe object.

    :param spark: SparkSession
    :param broker_url: The Broker URL for kafka
    :param topic: Topic name from which the messages are to be read.
    :return: dataFrame object
    """

    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker_url) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()


def get_schema():
    """
    This function returns the schema for the data coming from the datasource.
    :return: Schema
    """
    return StructType([
        StructField("key", StringType(), True),
        StructField("click_data", StructType([
            StructField("user_id", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("url", StringType())
        ]), True),
        StructField("geo_data", StructType([
            StructField("country", StringType()),
            StructField("city", StringType())
        ]), True),
        StructField("user_agent_data", StructType([
            StructField("browser", StringType()),
            StructField("os", StringType()),
            StructField("device", StringType())
        ]), True),
    ])


def convert_to_columns(data: DataFrame):
    """
    Convert the dataframe into structured data.
    :param data: Pyspark dataframe, with the kafka messages data.
    :return: DataFrame
    """
    return data \
        .select(from_json(col("value").cast(StringType()), schema=get_schema()).alias("json")) \
        .select(col("json.key"), col("json.click_data.*"), col("json.geo_data.*"), col("json.user_agent_data.*"))


def process_data(data: DataFrame):
    """
    Aggregates the data on url and country.
    :param data: DataFrame
    :return: Aggregated DataFrame
    """
    return data \
        .withWatermark("timestamp", "1 minute") \
        .groupby('url', 'country', 'timestamp').agg(
            count(col("user_id")).alias("num_clicks"),
            approx_count_distinct(col("user_id").alias("unique_users"))
        )


def write_data(data: DataFrame):
    """
    Writes the data into Elasticsearch
    :param data: The processed dataframe
    """
    data.writeStream \
        .outputMode("append") \
        .queryName("write_to_elasticsearch") \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", "/tmp/") \
        .option("es.resource", "aidetic/user_stats") \
        .option("es.nodes", "localhost") \
        .start()
