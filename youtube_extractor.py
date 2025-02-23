#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
import time

# YouTube API Key
API_KEY = "AIzaSyDDA-wefZOdpyQ2sEmpuiM7Ik1uoNDFyrQ"
CHANNELS = {
    "T-Series": "UCq-Fj5jknLsUf-MWSy4_brA",
    "Saregama": "UC_A7K2dXFsTMAciGmnNxy-Q",
    "Sony Music South": "UCn4rEMqKtwBQ6-oEwbd4PcA"
}
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Kafka Configuration
KAFKA_TOPIC = "youtube_data"
KAFKA_BROKER = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_channel_data():
    """Fetch channel metadata for all specified channels."""
    channel_data = []
    
    for name, channel_id in CHANNELS.items():
        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        )
        response = request.execute()
        
        if not response.get('items'):
            print(f"No data found for {name}")
            continue

        channel_info = response['items'][0]
        data = {
            "channel_id": channel_id,
            "channel_name": name,
            "subscriber_count": channel_info["statistics"].get("subscriberCount", 0),
            "views_count": channel_info["statistics"].get("viewCount", 0),
            "total_videos": channel_info["statistics"].get("videoCount", 0),
            "playlist_id": channel_info["contentDetails"]["relatedPlaylists"]["uploads"]
        }
        channel_data.append(data)
        producer.send(KAFKA_TOPIC, value={"type": "channel", "data": data})

    return channel_data

def fetch_video_stats(playlist_id, channel_name):
    """Fetch video statistics for the given channel's playlist."""
    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=10
    )
    response = request.execute()

    video_ids = [item["snippet"]["resourceId"]["videoId"] for item in response.get("items", [])]
    if not video_ids:
        return

    stats_request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids)
    )
    stats_response = stats_request.execute()

    for video in stats_response.get("items", []):
        data = {
            "playlist_id": playlist_id,
            "channel_name": channel_name,
            "video_id": video["id"],
            "title": video["snippet"]["title"],
            "description": video["snippet"].get("description", ""),
            "tags": video["snippet"].get("tags", []),
            "publish_at": video["snippet"]["publishedAt"],
            "views_count": video["statistics"].get("viewCount", 0),
            "like_count": video["statistics"].get("likeCount", 0),
            "comment_count": video["statistics"].get("commentCount", 0),
            "video_duration": video["contentDetails"].get("duration", "N/A")
        }
        producer.send(KAFKA_TOPIC, value={"type": "video", "data": data})

if __name__ == "__main__":
    while True:
        print("Fetching channel metadata...")
        channels = fetch_channel_data()

        for channel in channels:
            print(f"Fetching videos for {channel['channel_name']}...")
            fetch_video_stats(channel["playlist_id"], channel["channel_name"])

        print("Sleeping for 10 minutes...")
        time.sleep(600)  # Run every 10 minutes


