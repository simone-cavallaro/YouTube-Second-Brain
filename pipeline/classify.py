import sqlite3
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="env/.env")

DB_PATH = "data/second_brain.db"

# ─── Channel-level overrides (checked FIRST, before any keyword logic) ────────

# Always Type B — no matter what keywords appear in their titles/tags
EXCLUDED_CHANNELS = {
    "DarioMocciaArchives", "PolyMAVod", "Xxstuvi", "MarcusKron",
    "Flips", "Fierik VOD", "Sdrumox",
    # Add more gaming/streaming channels here as you discover them
}

# Always Type A — trusted technical/educational channels
TECHNICAL_CHANNELS = {
    # English tech/science
    "3Blue1Brown", "NetworkChuck", "Linus Tech Tips", "Marques Brownlee",
    "Veritasium", "TechDale", "MKBHD", "Fireship", "Andrej Karpathy",
    "Two Minute Papers", "Lex Fridman", "ArjanCodes", "Switch and Click",
    "JerryRigEverything", "Action Lab", "Ben Lambert",
    "Alex Ziskind", "Dan Ackerman", "DeadOverflow", "Techno Tim",
    "Kurzgesagt - In a Nutshell",
    # Italian educational/tech
    "Geopop", "Barbascura eXtra", "Nova Lectio", "Marcello Ascani",
    "Nicolò Balini", "GMAT Ninja Tutoring", "Jakidale", "Enkk",
}

# ─── Keyword signals (fallback when channel is not in either set) ─────────────

TITLE_KEYWORDS = [
    # English technical
    "tutorial", "how to", "how i", "step by step", "build", "explained",
    "course", "learn", "guide", "setup", "install", "coding", "programming",
    "python", "javascript", "react", "sql", "docker", "aws", "linux",
    "machine learning", "deep learning", "data science", "leetcode",
    "review", "benchmark", "upgrade", "repair",
    # Italian technical/educational
    "come fare", "guida", "spiegato", "spiegazione", "tutorial",
    "corso", "impara", "installare", "configurare", "confronto",
    "recensione", "analisi", "perché", "cosa è", "come funziona",
    "economia", "geopolitica", "storia", "scienza", "matematica",
    "fisica", "intelligenza artificiale", "gmat",
    "cosa succede", "cosa sta succedendo",
]

TAG_KEYWORDS = [
    "tutorial", "programming", "how to", "python", "explained",
    "course", "code", "coding", "software", "development", "engineering",
    "education", "science", "mathematics", "technology", "economics",
    "geopolitics", "storia", "scienza", "tecnologia", "educazione",
]

DESCRIPTION_SIGNALS = [
    "github", "source code", "code along", "follow along",
    "repository", "repo", "documentation", "docs", "notebook",
    "link in bio", "slide", "risorse", "fonti", "bibliografia",
]


def classify_video(title: str, channel: str, tags: list, description: str) -> str:
    """
    Returns 'A' (Visual/Technical) or 'B' (Audio/Conversational).
    Check order: excluded channels → whitelisted channels → keywords.
    """
    title_lower = title.lower()
    desc_lower = (description or "").lower()

    # 1. Channel exclusion list — always B (gaming, streaming, vlogs)
    if channel in EXCLUDED_CHANNELS:
        return "B"

    # 2. Channel whitelist — always A (trusted technical/educational)
    if channel in TECHNICAL_CHANNELS:
        return "A"

    # 3. Title keyword match
    if any(kw in title_lower for kw in TITLE_KEYWORDS):
        return "A"

    # 4. Tag keyword match
    tags_lower = [t.lower() for t in (tags or [])]
    if any(kw in tag for kw in TAG_KEYWORDS for tag in tags_lower):
        return "A"

    # 5. Description signal match
    if any(signal in desc_lower for signal in DESCRIPTION_SIGNALS):
        return "A"

    # Default — assume conversational/audio
    return "B"


def run_classification():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Only classify videos that haven't been classified yet
    rows = c.execute(
        "SELECT video_id, title, channel, tags, description FROM videos WHERE video_type IS NULL"
    ).fetchall()

    if not rows:
        print("No unclassified videos found.")
        conn.close()
        return

    updates = []
    for video_id, title, channel, tags_json, description in rows:
        tags = json.loads(tags_json) if tags_json else []
        vtype = classify_video(title, channel, tags, description)
        updates.append((vtype, video_id))

    c.executemany("UPDATE videos SET video_type = ? WHERE video_id = ?", updates)
    conn.commit()

    a = sum(1 for _, vid in updates if _ == "A")
    b = sum(1 for _, vid in updates if _ == "B")
    print(f"Classified {len(updates)} videos → A: {a}, B: {b}")
    conn.close()


if __name__ == "__main__":
    run_classification()