import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(override=True)


class Config:
    """Central project configuration."""

    ROOT_DIR = Path(__file__).resolve().parents[1]
    DATA_DIR = ROOT_DIR / "data"

    SPEECH_DB_PATH = DATA_DIR / "speech_tracker" / "speeches.db"
    DOCS_DIR = ROOT_DIR / "docs"

    SPEECH_API_KEY = (
        os.getenv("GOOGLE_API_KEY_FREE_TIER")
        or os.getenv("GOOGLE_API_KEY")
    )
    DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"


config = Config()
