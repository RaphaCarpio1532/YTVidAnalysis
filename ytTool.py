import re
import googleapiclient.discovery
import cx_Oracle
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv


# Load environment variables from the .env file
load_dotenv()
# Function to create a connection to Oracle Database
def create_db_connection():
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        dsn = os.getenv("DB_DSN")

        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        return connection
    except cx_Oracle.Error as error:
        print("Error connecting to the database: ", error)
        return None

# Use the YouTube API key from .env
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Hardcoded YouTube API Key (replace with your own)
API_KEY = "AIzaSyDZkBabo0yNHGWuLxJJf9HqDAgqfWIYgVA"

# Function to extract video_id from YouTube URL
def get_video_id(url):
    match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11}).*", url)
    if match:
        return match.group(1)
    else:
        print("Could not extract video ID. Please check the URL.")
        return None

# Function to get video data using YouTube Data API
def get_video_data(video_id, api_key):
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
    request = youtube.videos().list(
        part="snippet,statistics",
        id=video_id
    )
    response = request.execute()
    if response["items"]:
        return response["items"][0]
    else:
        print("No data found for the video.")
        return None

# Function to insert video data into the database
def insert_video_data(video_data, connection):
    try:
        cursor = connection.cursor()
        video_id = video_data["id"]
        title = video_data["snippet"]["title"]
        thumbnail = video_data["snippet"]["thumbnails"]["default"]["url"]
        publish_date = video_data["snippet"]["publishedAt"]

        cursor.execute("""
            INSERT INTO videos (video_id, original_title, original_thumbnail, publish_date)
            VALUES (:video_id, :original_title, :original_thumbnail, TO_TIMESTAMP(:publish_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
            """,
            video_id=video_id,
            original_title=title,
            original_thumbnail=thumbnail,
            publish_date=publish_date)

        connection.commit()
        print("Video data inserted successfully.")
    except cx_Oracle.Error as error:
        print("Error inserting video data: ", error)

# Function to ask user which plot to generate
def ask_user_for_plot_option():
    print("\nWhat plot would you like to generate?")
    print("1. Views by day")
    print("2. CTR by iteration")
    print("3. Exit")

    option = input("Select an option (1, 2, or 3): ")
    return int(option)

# Function to generate a views plot
def generate_views_plot(connection):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT metric_date, views
        FROM daily_metrics
        ORDER BY metric_date
    """)
    rows = cursor.fetchall()

    dates = [row[0] for row in rows]
    views = [row[1] for row in rows]

    plt.plot(dates, views, marker='o')
    plt.xlabel('Date')
    plt.ylabel('Views')
    plt.title('Views by Day')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Main function to drive the program
if __name__ == "__main__":
    url = input("Enter the YouTube video URL: ")
    video_id = get_video_id(url)

    if video_id:
        video_data = get_video_data(video_id, API_KEY)
        if video_data:
            connection = create_db_connection()
            if connection:
                insert_video_data(video_data, connection)
                while True:
                    option = ask_user_for_plot_option()
                    if option == 1:
                        generate_views_plot(connection)
                    elif option == 2:
                        # Placeholder for future CTR plot function
                        print("CTR by iteration plot is under development.")
                    elif option == 3:
                        print("Exiting...")
                        break
                    else:
                        print("Invalid option. Please try again.")
                connection.close()

