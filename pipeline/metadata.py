import os
import json
import sqlite3
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path("env/.env"))

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/videos"

def fetch_metadata_batch(video_ids: list[str]) -> list[dict]:
    """Fetch metadata for up to 50 videos in a single API call."""
    params = {
        "key": YOUTUBE_API_KEY,
        "id": ",".join(video_ids),
        "part": "snippet,contentDetails",
    }

    response = requests.get(YOUTUBE_API_URL, params=params)
    response.raise_for_status()
    items = response.json().get("items", [])

    results = []
    for item in items:
        snippet = item.get("snippet", {})
        content = item.get("contentDetails", {})

        # Parse duration from ISO 8601 format (e.g. PT5M23S → 323 seconds)
        duration = parse_duration(content.get("duration", "PT0S"))

        # Skip ads and very short videos (under 60 seconds)
        if duration < 60:
            print(f"Skipping short video (likely ad): {item['id']}")
            continue

        results.append({
            "video_id": item["id"],
            "title": snippet.get("title", ""),
            "channel": snippet.get("channelTitle", ""),
            "upload_date": snippet.get("publishedAt", ""),
            "duration": duration,
            "tags": json.dumps(snippet.get("tags", [])),
            "description": snippet.get("description", "")[:2000],
        })

    return results

def parse_duration(iso_duration: str) -> int:
    """Convert ISO 8601 duration to seconds. e.g. PT5M23S → 323"""
    import re
    pattern = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")
    match = pattern.match(iso_duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds

def save_metadata(videos: list[dict], db_path: str):
    """Insert video metadata into SQLite. Skips duplicates silently."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    for v in videos:
        c.execute("""
            INSERT OR IGNORE INTO videos
                (video_id, title, channel, upload_date, duration, tags, description, processed_at)
            VALUES
                (:video_id, :title, :channel, :upload_date, :duration, :tags, :description, :processed_at)
        """, {**v, "processed_at": datetime.now(timezone.utc).isoformat()})

    conn.commit()
    conn.close()
    print(f"Saved {len(videos)} videos to database")

def process_new_videos(new_videos: list[dict], db_path: str, batch_size: int = 50):
    """Fetch metadata for all new videos in batches of 50."""
    total = len(new_videos)
    video_ids = [v["video_id"] for v in new_videos]
    saved = []

    for i in range(0, total, batch_size):
        batch = video_ids[i:i + batch_size]
        print(f"Fetching metadata batch {i // batch_size + 1} / {(total // batch_size) + 1}")
        results = fetch_metadata_batch(batch)
        save_metadata(results, db_path)
        saved.extend(results)

    print(f"\nDone. {len(saved)} videos saved out of {total} new.")
    return saved


if __name__ == "__main__":
    from pipeline.parse import load_watch_history, init_db, get_new_videos

    DB_PATH = "data/second_brain.db"
    JSON_PATH = "data/watch-history.json"

    init_db(DB_PATH)
    all_videos = load_watch_history(JSON_PATH)
    new_videos = get_new_videos(all_videos, DB_PATH)

    process_new_videos(new_videos, DB_PATH)