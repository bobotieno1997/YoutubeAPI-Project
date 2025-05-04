import logging
from dotenv import load_dotenv
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from sqlalchemy import create_engine
import os
import time

# Load environment variables from a .env file (keeps credentials secure)
load_dotenv()

# Configure logging format and level
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s - %(asctime)s')

# Retrieve YouTube API key from environment variables
api_key = os.getenv("YOUTUBE_API_KEY")
if not api_key:
    raise ValueError("❌ Missing YouTube API Key.")

# Initialize the YouTube API client
youtube = googleapiclient.discovery.build(
    "youtube", "v3", developerKey=api_key
)

# Database connection parameters
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

# Validate DB environment variables
if not all([dbname, user, password, host, port]):
    raise ValueError("❌ Missing database connection parameters.")

# Create SQLAlchemy engine
try:
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    logging.info("✅ Database connection established.")
except Exception as e:
    logging.error(f"❌ Failed to connect to database: {e}")
    raise

# Query distinct playlist IDs from the database
def load_playlist():
    query = 'SELECT DISTINCT "playlistID" FROM bronze.subscribers_views_videos'
    df = pd.read_sql(query, engine)
    return df['playlistID'].tolist()

# Fetch all videos from each playlist (with pagination)
def load_video_ids():
    statistics = []
    playlists = load_playlist()
    logging.info(f"📺 Found {len(playlists)} playlist(s) to process.")

    for playlist_id in playlists:
        logging.info(f"🔍 Processing playlist: {playlist_id}")
        next_page_token = None
        while True:
            try:
                request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                statistics.append(response)
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            except Exception as e:
                logging.warning(f"⚠️ Failed to fetch playlist {playlist_id}: {e}")
                break
    return statistics

# Break list into chunks of given size
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

# Load and process video statistics from YouTube API
def load_video_statistics():
    data_list = load_video_ids()
    video_ids = []

    for data in data_list:
        for videos_list in data.get('items', []):
            video_id = videos_list.get('contentDetails', {}).get('videoId')
            if video_id:
                video_ids.append(video_id)

    if not video_ids:
        raise ValueError("❌ No video IDs found in the playlists.")

    logging.info(f"🎥 Found {len(video_ids)} video(s) to fetch statistics for.")

    all_video_info = []
    for chunk in chunked(video_ids, 50):
        try:
            request = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(chunk)
            )
            response = request.execute()
            for video in response.get('items', []):
                stats_to_keep = {
                    'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                    'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount']
                }
                video_info = {'video_id': video.get('id')}
                for section, fields in stats_to_keep.items():
                    for field in fields:
                        video_info[field] = video.get(section, {}).get(field)
                all_video_info.append(video_info)
        except Exception as e:
            logging.warning(f"⚠️ Failed to fetch video stats for chunk: {e}")
            time.sleep(1)  # Minimal retry delay

    if not all_video_info:
        raise ValueError("❌ No video statistics were retrieved.")

    return pd.DataFrame(all_video_info)

# Main logic
def main():
    df = load_video_statistics()

    if df.empty:
        raise ValueError("❌ DataFrame is empty. Nothing to upload to PostgreSQL.")

    try:
        df.to_sql(
            'video_statistics',
            engine,
            schema='bronze',
            if_exists='replace',
            index=False
        )
        logging.info("✅ Data successfully loaded into 'bronze.video_statistics'.")
    except Exception as e:
        logging.error(f"❌ Failed to load data into database: {e}")
        raise

# Entry point
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ Script terminated with error: {str(e)}")


