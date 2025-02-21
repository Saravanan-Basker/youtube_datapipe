#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleapiclient.discovery import build
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time


# Initialize Spark Session
spark = SparkSession.builder.appName("YouTubeETL").getOrCreate()

# YouTube API Key
API_KEY = "AIzaSyDDA-wefZOdpyQ2sEmpuiM7Ik1uoNDFyrQ"
CHANNEL_ID = "UCq-Fj5jknLsUf-MWSy4_brA"  # T-Series Channel ID
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Kafka Configuration
KAFKA_TOPIC = "youtube_videos"
KAFKA_BROKER = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_latest_videos():
    # Get latest video IDs from the T-Series channel
    request = youtube.search().list(
        part="id",
        channelId=CHANNEL_ID,
        order="date",
        maxResults=10  # Get latest 10 videos
    )
    response = request.execute()

    # Extract video IDs
    video_ids = [item['id']['videoId'] for item in response.get('items', []) if 'videoId' in item['id']]

    if not video_ids:
        print("No new videos found.")
        return

    # Fetch all video details in a single request instead of looping
    stats_request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids)  # Send all video IDs in a single request
    )
    stats_response = stats_request.execute()

    video_data = []
    for video in stats_response.get('items', []):
        data = {
            "video_id": video["id"],
            "title": video["snippet"]["title"],
            "thumbnail": video["snippet"]["thumbnails"]["high"]["url"],
            "tags": video["snippet"].get("tags", []),
            "like_count": video["statistics"].get("likeCount", 0),
            "views_count": video["statistics"].get("viewCount", 0),
            "comments_count": video["statistics"].get("commentCount", 0),
            "upload_time": video["snippet"]["publishedAt"],
            "video_duration": video["contentDetails"].get("duration", "N/A")  
        }
        video_data.append(data)
        producer.send(KAFKA_TOPIC, value=data)

    return video_data


if __name__ == "__main__":
    while True:
        fetch_latest_videos()
        time.sleep(900)  # Run every 15 minutes


# In[ ]:




