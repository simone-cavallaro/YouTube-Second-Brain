import json
import sqlite3
from pathlib import Path

def load_watch_history(json_path: str) -> list[dict]:
    """Load and parse watch-history.json from Google Takeout."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    videos = []
    seen_ids = set()

    for entry in data:
        # Skip if not a watch event
        if "titleUrl" not in entry:
            continue

        # Skip ads
        details = entry.get("details", [])
        if any(d.get("name") == "From Google Ads" for d in details):
            continue

        # Extract video ID from URL
        url = entry.get("titleUrl", "")
        if "watch?v=" not in url:
            continue

        video_id = url.split("watch?v=")[-1]

        # Deduplicate re-watches in-memory
        if video_id in seen_ids:
            continue
        seen_ids.add(video_id)

        # Extract channel name
        subtitles = entry.get("subtitles", [])
        channel = subtitles[0].get("name", "Unknown") if subtitles else "Unknown"

        videos.append({
            "video_id": video_id,
            "title": entry.get("title", "").replace("Watched ", ""),
            "channel": channel,
            "watch_date": entry.get("time", ""),
        })

    return videos

def init_db(db_path: str):
    """Create SQLite database and tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id      TEXT PRIMARY KEY,
            title         TEXT,
            channel       TEXT,
            watch_date    TEXT,
            upload_date   TEXT,
            duration      INTEGER,
            tags          TEXT,
            description   TEXT,
            video_type    TEXT,
            transcript    TEXT,
            processed_at  TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id        TEXT,
            chunk_text      TEXT,
            chunk_index     INTEGER,
            timestamp_start REAL,
            timestamp_end   REAL,
            FOREIGN KEY (video_id) REFERENCES videos (video_id)
        )
    """)

    conn.commit()
    conn.close()
    print(f"Database ready at {db_path}")

def get_new_videos(videos: list[dict], db_path: str) -> list[dict]:
    """Return only videos not yet in the database."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    existing_ids = set(
        row[0] for row in c.execute("SELECT video_id FROM videos").fetchall()
    )
    conn.close()

    new = [v for v in videos if v["video_id"] not in existing_ids]

    print(f"Total in Takeout:   {len(videos)}")
    print(f"Already processed:  {len(existing_ids)}")
    print(f"New this run:       {len(new)}")

    return new

if __name__ == "__main__":
    DB_PATH = "data/second_brain.db"
    JSON_PATH = "data/watch-history.json"

    Path("data").mkdir(exist_ok=True)
    init_db(DB_PATH)

    videos = load_watch_history(JSON_PATH)
    new_videos = get_new_videos(videos, DB_PATH)

    if new_videos:
        print(f"\nFirst new video: {new_videos[0]}")