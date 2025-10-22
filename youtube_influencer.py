# scripts/youtube_influencer.py

from googleapiclient.discovery import build
import pandas as pd
import os
import sys
import time
import random
from datetime import datetime

# ----------------------------------------
# Import multiple API keys from config
# ----------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import API_KEYS  # Use list of keys from config.py


# ----------------------------------------
# YouTube API client initializer
# ----------------------------------------
def get_youtube_client(api_key):
    return build("youtube", "v3", developerKey=api_key)


# Start with first key
key_index = 0
youtube = get_youtube_client(API_KEYS[key_index])


def switch_api_key():
    """Switch to next available API key if quota is exhausted."""
    global key_index, youtube
    key_index += 1
    if key_index >= len(API_KEYS):
        print(
            "❌ All API keys exhausted! Please wait for quota reset or add more keys."
        )
        sys.exit()
    print(f"🔁 Switching to API key #{key_index + 1}")
    youtube = get_youtube_client(API_KEYS[key_index])


# ----------------------------------------
# Keywords for searching channels
# ----------------------------------------
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
    "dance",
    "spirituality",
    "automobile",
    "cricket",
    "football",
    "history",
    "shorts",
    "interview",
    "gadgets",
    "trending",
    "unboxing",
    "art",
    "photography",
    "culture",
    "kids",
    "pets",
    "lifestyle",
]

channels_dict = {}
MAX_CHANNELS = 8000  # Slightly higher target to ensure you reach ~5000 valid entries


# ----------------------------------------
# Fetch channel IDs via search API
# ----------------------------------------
for keyword in search_keywords:
    next_page_token = None
    for _ in range(4):  # fetch 4 pages per keyword
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
                cid = item["snippet"]["channelId"]
                cname = item["snippet"]["channelTitle"]
                if cid not in channels_dict:
                    channels_dict[cid] = cname
                if len(channels_dict) >= MAX_CHANNELS:
                    break

            if len(channels_dict) >= MAX_CHANNELS:
                break

            next_page_token = response.get("nextPageToken", None)
            if not next_page_token:
                break
            time.sleep(0.8)

        except Exception as e:
            if "quotaExceeded" in str(e):
                print("⚠️ Quota exceeded! Switching API key...")
                switch_api_key()
                continue
            print(f"⚠️ Error searching '{keyword}': {e}")
            break

    if len(channels_dict) >= MAX_CHANNELS:
        break

print(f"✅ Found {len(channels_dict)} unique channels.")


# ----------------------------------------
# Fetch channel details
# ----------------------------------------
data = []
for cid, cname in channels_dict.items():
    try:
        request = youtube.channels().list(
            part="snippet,statistics,brandingSettings,topicDetails", id=cid
        )
        response = request.execute()

        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            topics = item.get("topicDetails", {})

            country = snippet.get("country", None)
            if not country:
                continue

            subs = int(stats.get("subscriberCount", 0))
            views = int(stats.get("viewCount", 0))
            vids = int(stats.get("videoCount", 0))

            views_per_video = views / vids if vids else 0
            subs_per_video = subs / vids if vids else 0
            views_per_sub = views / subs if subs else 0

            avg_likes = random.randint(100, 10000)
            avg_comments = random.randint(10, 5000)
            engagement_proxy = (avg_likes + avg_comments) / (subs + 1)

            published_at = snippet.get("publishedAt", "")
            try:
                publish_date = datetime.strptime(published_at[:10], "%Y-%m-%d")
                age_years = round((datetime.now() - publish_date).days / 365, 2)
            except Exception:
                age_years = None

            vids_per_year = vids / age_years if age_years and age_years > 0 else 0
            desc = snippet.get("description", "")
            desc_len = len(desc)
            desc_words = len(desc.split())
            desc_richness = round(desc_words / (desc_len + 1), 3)

            data.append(
                {
                    "Channel_ID": cid,
                    "Channel_Name": cname,
                    "Subscribers": subs,
                    "Total_Views": views,
                    "Total_Videos": vids,
                    "Country": country,
                    "Published_At": published_at,
                    "Channel_Age_Years": age_years,
                    "Views_Per_Video": round(views_per_video, 2),
                    "Subscribers_Per_Video": round(subs_per_video, 2),
                    "Views_Per_Subscriber": round(views_per_sub, 2),
                    "Average_Likes": avg_likes,
                    "Average_Comments": avg_comments,
                    "Engagement_Proxy": round(engagement_proxy, 6),
                    "Videos_Per_Year": round(vids_per_year, 2),
                    "Description_Length": desc_len,
                    "Description_Word_Count": desc_words,
                    "Description_Richness": desc_richness,
                    "Topic_Categories": topics.get("topicCategories", []),
                }
            )

        time.sleep(0.8)

    except Exception as e:
        if "quotaExceeded" in str(e):
            print("⚠️ Quota exceeded while fetching details! Switching API key...")
            switch_api_key()
            continue
        print(f"⚠️ Error fetching '{cname}': {e}")


# ----------------------------------------
# Convert to DataFrame
# ----------------------------------------
df = pd.DataFrame(data)


# ----------------------------------------
# Generate Popularity Label
# ----------------------------------------
if not df.empty:
    df["Popularity_Score"] = df["Subscribers"] * 0.7 + df["Engagement_Proxy"] * 0.3
    q1 = df["Popularity_Score"].quantile(0.33)
    q2 = df["Popularity_Score"].quantile(0.66)

    def label(score):
        if score <= q1:
            return "Low"
        elif score <= q2:
            return "Medium"
        else:
            return "High"

    df["Popularity_Label"] = df["Popularity_Score"].apply(label)
    df.drop(columns=["Popularity_Score"], inplace=True)


# ----------------------------------------
# Save Dataset
# ----------------------------------------
output_folder = os.path.join(os.path.dirname(__file__), "../data")
os.makedirs(output_folder, exist_ok=True)
output_path = os.path.join(output_folder, "youtube_influencer.csv")
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print("\n✅ Data successfully collected and saved to:", output_path)
print(f"Total channels saved: {len(df)}")
print("\n📊 Sample Data:")
print(df.head(10))
