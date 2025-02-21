#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json

# Initialize Spark Session
spark = SparkSession.builder.appName("YouTubeKafkaConsumer").getOrCreate()

# Kafka Configuration
KAFKA_TOPIC = "youtube_videos"
KAFKA_BROKER = "localhost:9092"
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Define Schema
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("like_count", IntegerType(), True),   # Ensure this is INT
    StructField("views_count", IntegerType(), True),  # Ensure this is INT
    StructField("comments_count", IntegerType(), True),  # Ensure this is INT
    StructField("upload_time", StringType(), True),
    StructField("video_duration", StringType(), True)
])

# HDFS Path (Ensure NameNode & Port are correct)
HDFS_PATH = "hdfs://localhost:9000/user/hadoop/youtube_data/video_stats"

def process_and_store():
    for message in consumer:
        data = message.value  # Get Kafka message
        
        # 🔹 Convert numeric values from string to int
        data["like_count"] = int(data["like_count"]) if "like_count" in data and data["like_count"].isdigit() else 0
        data["views_count"] = int(data["views_count"]) if "views_count" in data and data["views_count"].isdigit() else 0
        data["comments_count"] = int(data["comments_count"])if "comments_count" in data and data["comments_count"].isdigit() else 0
        
        # Create Spark DataFrame
        df = spark.createDataFrame([data], schema=schema)
        
        # Write to HDFS
        df.write.mode("append").parquet(HDFS_PATH)  
        
        print(f"Stored: {data}")

# Run the function
process_and_store()



# In[ ]:




