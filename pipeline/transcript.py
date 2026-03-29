import sqlite3
import tempfile
import os
import logging

from dotenv import load_dotenv

load_dotenv(dotenv_path="env/.env")

DB_PATH = "data/second_brain.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ─── Strategy 1: youtube-transcript-api ──────────────────────────────────────

def fetch_transcript_api(video_id: str) -> str | None:
    """
    Fetch transcript from YouTube's caption system.
    Prefers manually uploaded captions (original language) over auto-generated.
    Falls back to any auto-generated caption if no manual one exists.
    """
    try:
        from youtube_transcript_api import (
            YouTubeTranscriptApi,
            TranscriptsDisabled,
            NoTranscriptFound,
        )

        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        available = list(transcript_list)

        if not available:
            return None

        # Prefer manually created (original language) over auto-generated
        manual = [t for t in available if not t.is_generated]
        auto   = [t for t in available if t.is_generated]
        chosen = (manual or auto)[0]

        log.info(
            f"[{video_id}] Using caption: '{chosen.language}' "
            f"({'manual' if not chosen.is_generated else 'auto-generated'})"
        )

        snippets = chosen.fetch()
        full_text = " ".join(s["text"] for s in snippets)
        return full_text.strip() or None

    except Exception as e:
        log.warning(f"[{video_id}] youtube-transcript-api failed: {type(e).__name__}: {e}")
        return None


# ─── Strategy 2: yt-dlp + Whisper ────────────────────────────────────────────

def fetch_transcript_whisper(video_id: str, model) -> str | None:
    """
    Download audio with yt-dlp, transcribe with Whisper.
    `model` is a pre-loaded Whisper model instance (loaded once per run).
    """
    try:
        import yt_dlp
    except ImportError:
        log.error("yt-dlp not installed. Run: uv add yt-dlp")
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = os.path.join(tmpdir, f"{video_id}.mp3")

        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": os.path.join(tmpdir, f"{video_id}.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "64",   # low bitrate is fine for transcription
            }],
            "quiet": True,
            "no_warnings": True,
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
        except Exception as e:
            log.warning(f"[{video_id}] yt-dlp download failed: {e}")
            return None

        if not os.path.exists(audio_path):
            log.warning(f"[{video_id}] Audio file not found after download")
            return None

        try:
            log.info(f"[{video_id}] Running Whisper transcription (this may take a minute)...")
            result = model.transcribe(audio_path)
            text = result["text"].strip()
            detected_lang = result.get("language", "unknown")
            log.info(f"[{video_id}] Whisper detected language: {detected_lang}")
            return text or None
        except Exception as e:
            log.warning(f"[{video_id}] Whisper transcription failed: {e}")
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

    # Load Whisper model once — reused for every video that needs it
    # 'base' runs well on CPU and handles Italian/English accurately.
    # Upgrade to 'small' for higher accuracy at ~2x the processing time.
    whisper_model = None

    success_api     = 0
    success_whisper = 0
    skipped         = 0

    for video_id, title in rows:
        log.info(f"── {title[:70]}")

        # Strategy 1: caption system
        transcript = fetch_transcript_api(video_id)

        if transcript:
            success_api += 1
            source = "api"
        else:
            # Strategy 2: Whisper — load model on first use
            if whisper_model is None:
                log.info("Loading Whisper model (first time — one-time download ~140MB)...")
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

        c.execute(
            "UPDATE videos SET transcript = ? WHERE video_id = ?",
            (transcript, video_id)
        )
        conn.commit()   # commit after each video — crash-safe
        log.info(f"[{video_id}] Saved ({source}, {len(transcript):,} chars)")

    conn.close()

    print(f"\n── Transcript run complete ──")
    print(f"   API success  : {success_api}")
    print(f"   Whisper      : {success_whisper}")
    print(f"   Skipped      : {skipped}")
    print(f"   Total        : {len(rows)}")


if __name__ == "__main__":
    run_transcripts()