import sqlite3
import tempfile
import os
import re
import time
import random
import logging

from dotenv import load_dotenv
load_dotenv(dotenv_path="env/.env")

DB_PATH = "data/second_brain.db"
COOKIE_FILE = "data/cookies.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─── VTT parser ───────────────────────────────────────────────────────────────

def parse_vtt(vtt_text: str) -> str:
    vtt_text = re.sub(r'WEBVTT.*?\n', '', vtt_text)
    vtt_text = re.sub(r'NOTE.*?\n\n', '', vtt_text, flags=re.DOTALL)
    vtt_text = re.sub(r'\d{2}:\d{2}:\d{2}\.\d{3} --> .*\n', '', vtt_text)
    vtt_text = re.sub(r'<[^>]+>', '', vtt_text)
    lines = [line.strip() for line in vtt_text.splitlines() if line.strip()]
    deduped = []
    for line in lines:
        if not deduped or line != deduped[-1]:
            deduped.append(line)
    return " ".join(deduped)


# ─── Strategy 1: youtube-transcript-api with cookies ─────────────────────────

def fetch_transcript_api(video_id: str) -> str | None:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi

        ytt = YouTubeTranscriptApi()
        transcript_list = ytt.list(video_id)
        available = list(transcript_list)

        if not available:
            return None

        PREFERRED_LANGS = ["it", "en"]

        manual = [t for t in available if not t.is_generated]
        auto   = [t for t in available if t.is_generated]

        def pick_best(candidates):
            for lang in PREFERRED_LANGS:
                match = next((t for t in candidates if t.language_code == lang), None)
                if match:
                    return match
            return None

        chosen = pick_best(manual) or pick_best(auto)

        if chosen is None:
            chosen = (manual or auto)[0]
            log.warning(f"[{video_id}] No IT/EN transcript — using '{chosen.language}' as fallback")
        else:
            log.info(f"[{video_id}] Caption: '{chosen.language}' ({'manual' if not chosen.is_generated else 'auto'})")

        snippets = chosen.fetch()
        full_text = " ".join(s.text for s in snippets)
        return full_text.strip() or None

    except Exception as e:
        log.warning(f"[{video_id}] transcript-api failed: {type(e).__name__}: {e}")
        return None


# ─── Strategy 2: yt-dlp subtitles (ios client — no JS challenge needed) ──────

def fetch_transcript_ytdlp(video_id: str) -> str | None:
    try:
        import yt_dlp
    except ImportError:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        ydl_opts = {
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitleslangs": ["it", "en"],
            "subtitlesformat": "vtt",
            "skip_download": True,
            "outtmpl": os.path.join(tmpdir, f"{video_id}.%(ext)s"),
            "extractor_args": {"youtube": {"player_client": ["ios"]}},
            "quiet": True,
            "no_warnings": True,
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
        except Exception as e:
            log.warning(f"[{video_id}] yt-dlp subtitle failed: {e}")
            return None

        for lang in ["it", "en"]:
            for suffix in [f".{lang}.vtt", f".{lang}-orig.vtt"]:
                path = os.path.join(tmpdir, f"{video_id}{suffix}")
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as f:
                        raw = f.read()
                    text = parse_vtt(raw)
                    if text:
                        log.info(f"[{video_id}] yt-dlp subtitle ({lang})")
                        return text

        return None


# ─── Strategy 3: yt-dlp audio + Whisper ──────────────────────────────────────

def fetch_transcript_whisper(video_id: str, model) -> str | None:
    try:
        import yt_dlp
    except ImportError:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = os.path.join(tmpdir, f"{video_id}.mp3")

        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": os.path.join(tmpdir, f"{video_id}.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "64",
            }],
            "extractor_args": {"youtube": {"player_client": ["ios"]}},
            "quiet": True,
            "no_warnings": True,
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
        except Exception as e:
            log.warning(f"[{video_id}] yt-dlp audio failed: {e}")
            return None

        if not os.path.exists(audio_path):
            return None

        try:
            log.info(f"[{video_id}] Running Whisper...")
            result = model.transcribe(audio_path)
            text = result["text"].strip()
            log.info(f"[{video_id}] Whisper done (lang: {result.get('language', '?')})")
            return text or None
        except Exception as e:
            log.warning(f"[{video_id}] Whisper failed: {e}")
            return None


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_transcripts():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute(
        "SELECT video_id, title FROM videos WHERE transcript IS NULL"
    ).fetchall()

    if not rows:
        log.info("No videos need transcripts.")
        conn.close()
        return

    log.info(f"Fetching transcripts for {len(rows)} videos...")

    whisper_model   = None
    success_api     = 0
    success_ytdlp   = 0
    success_whisper = 0
    skipped         = 0

    for video_id, title in rows:
        log.info(f"── {title[:70]}")

        # Strategy 1: transcript-api with cookies (fast, reliable for captioned videos)
        transcript = fetch_transcript_api(video_id)
        if transcript:
            success_api += 1
            source = "api"
        else:
            # Strategy 2: yt-dlp ios client subtitles (no JS challenge)
            transcript = fetch_transcript_ytdlp(video_id)
            if transcript:
                success_ytdlp += 1
                source = "yt-dlp"
            else:
                # Strategy 3: Whisper (audio transcription — slowest, most complete)
                if whisper_model is None:
                    log.info("Loading Whisper model...")
                    import whisper
                    whisper_model = whisper.load_model("base")

                transcript = fetch_transcript_whisper(video_id, whisper_model)
                if transcript:
                    success_whisper += 1
                    source = "whisper"
                else:
                    skipped += 1
                    log.warning(f"[{video_id}] No transcript obtainable — skipping")
                    continue

        c.execute("UPDATE videos SET transcript = ? WHERE video_id = ?", (transcript, video_id))
        conn.commit()
        log.info(f"[{video_id}] Saved ({source}, {len(transcript):,} chars)")

        time.sleep(random.uniform(7.0, 10.0))

    conn.close()

    print(f"\n── Transcript run complete ──")
    print(f"   API            : {success_api}")
    print(f"   yt-dlp subs    : {success_ytdlp}")
    print(f"   Whisper        : {success_whisper}")
    print(f"   Skipped        : {skipped}")
    print(f"   Total          : {len(rows)}")


if __name__ == "__main__":
    run_transcripts()