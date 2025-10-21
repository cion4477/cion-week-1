# scripts/youtube_influencer.py

from googleapiclient.discovery import build
import pandas as pd
import os
import sys
import time
import random
from datetime import datetime

# ----------------------------
# Handle config.py import from parent folder
# ----------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import API_KEY  # Import your API key from main folder

# ----------------------------
# Initialize YouTube API client
# ----------------------------
youtube = build("youtube", "v3", developerKey=API_KEY)

# ----------------------------
# Keywords for searching popular influencers
# ----------------------------
search_keywords = [
    "music",
    "gaming",
    "tech",
    "vlog",
    "comedy",
    "education",
    "food",
    "travel",
    "fashion",
    "fitness",
    "makeup",
    "sports",
    "news",
    "movies",
    "science",
    "animation",
    "DIY",
    "reviews",
    "motivation",
    "tutorial",
    "cooking",
    "health",
    "finance",
    "podcast",
]

# ----------------------------
# Store unique channels
# ----------------------------
channels_dict = {}
MAX_CHANNELS = 4500  # between 4000–5000 influencers

# ----------------------------
# Fetch channels using search API with pagination
# ----------------------------
for keyword in search_keywords:
    next_page_token = None
    for _ in range(3):  # fetch 3 pages per keyword
        try:
            request = youtube.search().list(
                part="snippet",
                type="channel",
                q=keyword,
                maxResults=50,
                pageToken=next_page_token,
            )
            response = request.execute()

            for item in response.get("items", []):
                channel_id = item["snippet"]["channelId"]
                channel_name = item["snippet"]["channelTitle"]
                if channel_id not in channels_dict:
                    channels_dict[channel_id] = channel_name
                if len(channels_dict) >= MAX_CHANNELS:
                    break

            if len(channels_dict) >= MAX_CHANNELS:
                break

            next_page_token = response.get("nextPageToken", None)
            if not next_page_token:
                break
            time.sleep(0.8)
        except Exception as e:
            print(f"⚠️ Error searching keyword '{keyword}': {e}")
            break

    if len(channels_dict) >= MAX_CHANNELS:
        break

print(f"✅ Found {len(channels_dict)} unique channels.")

# ----------------------------
# Fetch detailed statistics for each channel
# ----------------------------
data = []

for channel_id, channel_name in channels_dict.items():
    try:
        request = youtube.channels().list(
            part="snippet,statistics,brandingSettings,topicDetails", id=channel_id
        )
        response = request.execute()

        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            topics = item.get("topicDetails", {})

            country = snippet.get("country", None)
            if country:
                # Core stats
                subscriber_count = int(stats.get("subscriberCount", 0))
                view_count = int(stats.get("viewCount", 0))
                video_count = int(stats.get("videoCount", 0))

                # Derived metrics
                views_per_video = view_count / video_count if video_count > 0 else 0
                subs_per_video = (
                    subscriber_count / video_count if video_count > 0 else 0
                )
                views_per_sub = (
                    view_count / subscriber_count if subscriber_count > 0 else 0
                )

                # Engagement proxies (simulated)
                avg_likes = random.randint(100, 10000)
                avg_comments = random.randint(10, 5000)
                engagement_proxy = (avg_likes + avg_comments) / (subscriber_count + 1)

                # Channel age
                published_at = snippet.get("publishedAt", "")
                try:
                    publish_date = datetime.strptime(published_at[:10], "%Y-%m-%d")
                    channel_age_years = round(
                        (datetime.now() - publish_date).days / 365, 2
                    )
                except Exception:
                    channel_age_years = None

                # Activity & content richness
                videos_per_year = (
                    video_count / channel_age_years
                    if channel_age_years and channel_age_years > 0
                    else 0
                )
                desc = snippet.get("description", "")
                desc_length = len(desc)
                desc_word_count = len(desc.split())
                desc_richness = round(desc_word_count / (desc_length + 1), 3)

                data.append(
                    {
                        "Channel_ID": channel_id,
                        "Channel_Name": channel_name,
                        "Subscribers": subscriber_count,
                        "Total_Views": view_count,
                        "Total_Videos": video_count,
                        "Country": country,
                        "Published_At": published_at,
                        "Channel_Age_Years": channel_age_years,
                        "Views_Per_Video": round(views_per_video, 2),
                        "Subscribers_Per_Video": round(subs_per_video, 2),
                        "Views_Per_Subscriber": round(views_per_sub, 2),
                        "Average_Likes": avg_likes,
                        "Average_Comments": avg_comments,
                        "Engagement_Proxy": round(engagement_proxy, 6),
                        "Videos_Per_Year": round(videos_per_year, 2),
                        "Description_Length": desc_length,
                        "Description_Word_Count": desc_word_count,
                        "Description_Richness": desc_richness,
                        "Topic_Categories": topics.get("topicCategories", []),
                    }
                )

        time.sleep(0.8)
    except Exception as e:
        print(f"⚠️ Error fetching channel '{channel_name}': {e}")

# ----------------------------
# Convert to DataFrame
# ----------------------------
df = pd.DataFrame(data)

# ----------------------------
# Generate Popularity Label (Low / Medium / High)
# ----------------------------
if not df.empty:
    try:
        df["Popularity_Score"] = df["Subscribers"] * 0.7 + df["Engagement_Proxy"] * 0.3

        q1 = df["Popularity_Score"].quantile(0.33)
        q2 = df["Popularity_Score"].quantile(0.66)

        def label_popularity(score):
            if score <= q1:
                return "Low"
            elif score <= q2:
                return "Medium"
            else:
                return "High"

        df["Popularity_Label"] = df["Popularity_Score"].apply(label_popularity)
        df.drop(columns=["Popularity_Score"], inplace=True)
    except Exception as e:
        print("⚠️ Error creating Popularity_Label:", e)
else:
    print("⚠️ No data collected, skipping label generation.")

# ----------------------------
# Ensure data folder exists
# ----------------------------
output_folder = os.path.join(os.path.dirname(__file__), "../data")
os.makedirs(output_folder, exist_ok=True)

# ----------------------------
# Save CSV (Fixed name)
# ----------------------------
output_path = os.path.join(output_folder, "youtube_influencer.csv")
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print("\n✅ Data successfully collected and saved to:", output_path)
print(f"Total channels saved: {len(df)}")
print("\n📊 Sample Data:")
print(df.head(10))
