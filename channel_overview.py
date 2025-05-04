import logging
from dotenv import load_dotenv
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from sqlalchemy import create_engine
import os
from channel_lists import channels

# Load environment variables from a .env file (keeps credentials secure)
load_dotenv()

# Retrieve YouTube API key from environment variables
api_key = os.getenv("YOUTUBE_API_KEY")

# Configure logging format and level
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s - %(asctime)s')

# Generate list of channels
channels = channels()

def load_data():
    """
    Fetches metadata and statistics for a list of YouTube channels using the YouTube Data API v3.
    """
    api_service_name = "youtube"
    api_version = "v3"

    # Initialize the YouTube API client
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key
    )

    statistics = []

    for channel_id in channels:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        statistics.append(response)

        logging.info(f"✅ Fetched data for channel: {channel_id}")

    return statistics

def main():
    """
    Main execution function: processes YouTube data and loads it into PostgreSQL.
    """
    all_data = []
    playlists = []

    for statistics in load_data():
        if 'items' in statistics and statistics['items']:
            for item in statistics['items']:
                snippet = item.get('snippet', {})
                localized = snippet.get('localized', {})
                stats = item.get('statistics', {})
                playlist_id = item.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads', 'N/A')

                final_data = {
                    'title': snippet.get('title', 'N/A'),
                    'description': localized.get('description', 'N/A'),
                    'country': snippet.get('country', 'N/A'),
                    'subscriberCount': stats.get('subscriberCount', 'N/A'),
                    'viewCount': stats.get('viewCount', 'N/A'),
                    'videoCount': stats.get('videoCount', 'N/A'),
                    'playlistID': playlist_id
                }
                
                all_data.append(final_data)
                playlists.append(playlist_id)

    df = pd.DataFrame(all_data)

    # Retrieve PostgreSQL connection parameters from environment variables
    dbname = os.getenv('dbname')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    if not all([dbname, user, password, host, port]):
        raise ValueError("❌ Missing database connection parameters.")

    if df.empty:
        raise ValueError("❌ DataFrame is empty. Nothing to upload to PostgreSQL.")

    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        logging.info("✅ Database connection established.")
    except Exception as e:
        logging.error(f"❌ Failed to connect to database: {e}")
        raise

    try:
        df.to_sql(
            'subscribers_views_videos',
            engine,
            schema='bronze',
            if_exists='replace', 
            index=False
        )
        logging.info("✅ Data loaded into 'bronze.subscribers_views_videos' successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to load data into database: {e}")
        raise

# Entry point
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ An error occurred: {str(e)}")


