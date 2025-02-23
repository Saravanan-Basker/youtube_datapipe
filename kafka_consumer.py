#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("YouTubeKafkaConsumer").getOrCreate()

# Kafka Configuration
KAFKA_TOPIC = "youtube_data"
KAFKA_BROKER = "localhost:9092"
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Define Schema for Channel Metadata
channel_schema = StructType([
    StructField("channel_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("subscriber_count", LongType(), True),
    StructField("views_count", LongType(), True),
    StructField("total_videos", LongType(), True),
    StructField("playlist_id", StringType(), True)
])

# Define Schema for Video Statistics
video_schema = StructType([
    StructField("playlist_id", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("publish_at", StringType(), True),
    StructField("views_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("comment_count",LongType(), True),
    StructField("video_duration", StringType(), True)
])

# HDFS Paths
HDFS_CHANNEL_PATH = "hdfs://localhost:9000/user/hadoop/youtube_data/channel_stats"
HDFS_VIDEO_PATH = "hdfs://localhost:9000/user/hadoop/youtube_data/video_stats"

def process_and_store():
    for message in consumer:
        data = message.value

        if data["type"] == "channel":
            # Convert numeric fields to integers
            data["data"]["subscriber_count"] = int(data["data"]["subscriber_count"])
            data["data"]["views_count"] = int(data["data"]["views_count"])
            data["data"]["total_videos"] = int(data["data"]["total_videos"])

            df = spark.createDataFrame([data["data"]], schema=channel_schema)
            df.write.mode("append").parquet(HDFS_CHANNEL_PATH)
            print(f"Stored Channel Data: {data['data']}")

        elif data["type"] == "video":
            # Convert numeric fields to integers
            data["data"]["views_count"] = int(data["data"]["views_count"])
            data["data"]["like_count"] = int(data["data"]["like_count"])
            data["data"]["comment_count"] = int(data["data"]["comment_count"])

            df = spark.createDataFrame([data["data"]], schema=video_schema)
            df.write.mode("append").parquet(HDFS_VIDEO_PATH)
            print(f"Stored Video Data: {data['data']}")



# Run the function
process_and_store()



# In[ ]:




