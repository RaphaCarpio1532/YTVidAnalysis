# Import necessary libraries
import os
import cx_Oracle
from googleapiclient.discovery import build
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import logging
from datetime import datetime, timedelta

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# API Key for YouTube
YOUTUBE_API_KEY=os.getenv('AIzaSyCo8WYxOnQhDJs1c1KdaaLB5gUCewB6e8g')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection
try:
    db_connection = cx_Oracle.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        dsn=os.getenv('DB_DSN')
    )
    cursor = db_connection.cursor()
    logging.info("Database connection established.")
except cx_Oracle.DatabaseError as err:
    logging.error(f"Error connecting to the database: {err}")
    exit()

# Function to insert video into 'videos' table
def insert_video(video_id, video_link, original_title, original_thumbnail, publish_date):
    try:
        cursor.execute('''
            INSERT INTO videos (video_id, video_link, original_title, original_thumbnail, publish_date)
            VALUES (:1, :2, :3, :4, :5)
        ''', (video_id, video_link, original_title, original_thumbnail, publish_date))
        db_connection.commit()
        logging.info(f"Video with ID {video_id} inserted.")
    except cx_Oracle.IntegrityError:
        logging.warning(f"Video with ID {video_id} already exists.")
    except cx_Oracle.DatabaseError as err:
        logging.error(f"Error inserting video: {err}")

# Function to insert iteration into 'iterations' table
def insert_iteration(video_id, title, thumbnail, start_date, views_start):
    try:
        cursor.execute('''
            INSERT INTO iterations (video_id, title, thumbnail, start_date, views_start)
            VALUES (:1, :2, :3, :4, :5)
        ''', (video_id, title, thumbnail, start_date, views_start))
        db_connection.commit()
        logging.info(f"Iteration for video ID {video_id} inserted.")
    except cx_Oracle.DatabaseError as err:
        logging.error(f"Error inserting iteration: {err}")

# Function to insert daily metrics into 'daily_metrics' table
def insert_daily_metrics(iteration_id, metric_date, views, impressions, ctr):
    try:
        cursor.execute('''
            INSERT INTO daily_metrics (iteration_id, metric_date, views, impressions, ctr)
            VALUES (:1, :2, :3, :4, :5)
        ''', (iteration_id, metric_date, views, impressions, ctr))
        db_connection.commit()
        logging.info(f"Daily metrics for iteration ID {iteration_id} inserted.")
    except cx_Oracle.DatabaseError as err:
        logging.error(f"Error inserting daily metrics: {err}")

# Create YouTube API client
def get_youtube_client():
    try:
        youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))
        logging.info("YouTube API client created.")
        return youtube
    except Exception as e:
        logging.error(f"Error creating YouTube API client: {e}")
        return None

# Get video data from YouTube API
def get_video_data(youtube, video_id):
    try:
        request = youtube.videos().list(part="snippet,statistics", id=video_id)
        response = request.execute()
        if 'items' in response and len(response['items']) > 0:
            video_info = response['items'][0]
            return video_info
        else:
            logging.warning(f"Video with ID {video_id} not found.")
            return None
    except Exception as e:
        logging.error(f"Error fetching video data: {e}")
        return None

# Function to search videos by keyword in the original title
def search_videos_by_keyword(keyword):
    try:
        query = f"""
            SELECT video_id, original_title, video_link
            FROM videos
            WHERE LOWER(original_title) LIKE :keyword
        """
        cursor.execute(query, (f"%{keyword.lower()}%",))
        results = cursor.fetchall()
        for row in results:
            logging.info(f"Video ID: {row[0]}, Title: {row[1]}, Link: {row[2]}")
    except cx_Oracle.DatabaseError as err:
        logging.error(f"Error searching videos by keyword: {err}")

# Function to get video performance using the view
def get_video_performance():
    try:
        query = "SELECT video_id, original_title, last_title, views_start, views_end FROM video_performance"
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            logging.info(f"Video ID: {row[0]}, Original Title: {row[1]}, Last Title: {row[2]}, Views Start: {row[3]}, Views End: {row[4]}")
    except cx_Oracle.DatabaseError as err:
        logging.error(f"Error fetching video performance: {err}")

# Generate example plot with Matplotlib
def generate_example_plot():
    dates = pd.date_range(start="2024-01-01", periods=30)
    views = [x * 100 for x in range(1, 31)]
    plt.figure(figsize=(10, 5))
    plt.plot(dates, views, marker='o')
    plt.xlabel("Date")
    plt.ylabel("Views")
    plt.title("Example View Count over Time")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Generate interactive plot with Plotly
def generate_plotly_plot():
    dates = pd.date_range(start="2024-01-01", periods=30)
    views = [x * 100 for x in range(1, 31)]
    trace = go.Scatter(x=dates, y=views, mode='lines+markers', name='Views')
    layout = go.Layout(title='View Count over Time', xaxis=dict(title='Date'), yaxis=dict(title='Views'))
    fig = go.Figure(data=[trace], layout=layout)
    fig.show()

# Example usage
if __name__ == "__main__":
    youtube_client = get_youtube_client()

    # Example usage for a video ID
    video_id = "dQw4w9WgXcQ"
    video_data = get_video_data(youtube_client, video_id)
    if video_data:
        insert_video(
            video_id,
            f"https://www.youtube.com/watch?v={video_id}",
            video_data['snippet']['title'],
            video_data['snippet']['thumbnails']['high']['url'],
            video_data['snippet']['publishedAt']
        )
        # Insert an iteration example
        insert_iteration(video_id, "New Title Test", "New Thumbnail URL", datetime.now(), 1000)
        # Insert daily metrics example
        insert_daily_metrics(1, datetime.now().date(), 150, 2000, 0.075)

    # Search videos by keyword
    search_videos_by_keyword("buy a house")

    # Get video performance overview
    get_video_performance()

    generate_example_plot()
    generate_plotly_plot()


