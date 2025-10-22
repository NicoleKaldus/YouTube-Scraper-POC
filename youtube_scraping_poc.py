from googleapiclient.discovery import build
import pandas as pd
from dotenv import load_dotenv, dotenv_values 
import os
import time

load_dotenv()
API_KEY = os.getenv("YT_API_KEY")


def search_videos_by_keywords(api_key, query, max_results=50):
    """Search for videos by keywords"""
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.search().list(
        part='snippet',
        q=query,
        maxResults=max_results,
        order='relevance',  # or 'date', 'rating', 'viewCount'
        type='video'
    )
    response = request.execute()
    # Get video IDs from search results
    video_ids = []
    for item in response['items']:
        video_ids.append(item['id']['videoId'])
    
    # Get detailed video info including statistics
    if video_ids:
        videos_request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids)
        )
        videos_response = videos_request.execute()
        return process_video_response(videos_response)
    
    return pd.DataFrame()

def get_trending_videos(api_key, region_code='US', max_results=50):
    """Get trending videos"""
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(
        part='snippet,statistics',
        chart='mostPopular',
        regionCode=region_code,
        maxResults=max_results
    )
    response = request.execute()
    
    videos = []
    for item in response['items']:
        video_data = {
            'videoId': item['id'],
            'title': item['snippet']['title'],
            'channelTitle': item['snippet']['channelTitle'],
            'publishedAt': item['snippet']['publishedAt'],
            'viewCount': item.get('statistics', {}).get('viewCount', 0),
            'likeCount': item.get('statistics', {}).get('likeCount', 0),
            'description': item['snippet']['description'][:200] + '...'  # Truncate description
        }
        videos.append(video_data)
    
    return pd.DataFrame(videos)

def search_by_category(api_key, category_id, max_results=50):
    """Search videos by category"""
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.videos().list(
        part='snippet,statistics',
        chart='mostPopular',
        videoCategoryId=category_id,
        maxResults=max_results
    )
    response = request.execute()
    return process_video_response(response)

def process_video_response(response):
    """Helper function to process API response"""
    videos = []
    for item in response['items']:
        if 'videoId' in item['id']:
            video_id = item['id']['videoId']
        else:
            video_id = item['id']
            
        video_data = {
            'videoId': video_id,
            'title': item['snippet']['title'],
            'channelTitle': item['snippet']['channelTitle'],
            'publishedAt': item['snippet']['publishedAt'],
            'viewCount': item.get('statistics', {}).get('viewCount', 0),
            'likeCount': item.get('statistics', {}).get('likeCount', 0),
            'description': item['snippet']['description'][:200] + '...'
        }
        videos.append(video_data)
    
    return pd.DataFrame(videos)

def surf_youtube_data(api_key):
    """Collect diverse YouTube data by surfing different categories and searches"""
    all_videos = []
    
    # 1. Get trending videos
    print("Collecting trending videos...")
    trending = get_trending_videos(api_key, max_results=25)
    all_videos.append(trending)
    
    # 2. Search by popular keywords
    keywords = ['python programming', 'machine learning', 'music', 'cooking', 'gaming']
    for keyword in keywords:
        print(f"Searching for: {keyword}")
        search_results = search_videos_by_keywords(api_key, keyword, max_results=20)
        all_videos.append(search_results)
        time.sleep(1)  # Rate limiting
    
    # 3. Search by categories (some popular category IDs)
    categories = {
        '10': 'Music',
        '20': 'Gaming', 
        '22': 'People & Blogs',
        '23': 'Comedy',
        '24': 'Entertainment'
    }
    
    for cat_id, cat_name in categories.items():
        print(f"Collecting {cat_name} videos...")
        cat_videos = search_by_category(api_key, cat_id, max_results=15)
        all_videos.append(cat_videos)
        time.sleep(1)  # Rate limiting
    
    # Combine all dataframes
    combined_df = pd.concat(all_videos, ignore_index=True)
    
    # Remove duplicates based on videoId
    combined_df = combined_df.drop_duplicates(subset=['videoId'])
    
    return combined_df

if __name__ == "__main__":
    print("Starting YouTube data surfing...")
    videos_df = surf_youtube_data(API_KEY)
    print(f"Collected {len(videos_df)} unique videos")
    print(videos_df.head())
    videos_df.to_csv('surfed_youtube_data.csv', index=False)
    print("Data saved to surfed_youtube_data.csv")