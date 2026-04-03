import os
from pathlib import Path
from dotenv import load_dotenv

# 환경 변수 로드 (override=True로 .env 우선 적용)
load_dotenv(override=True)

class Config:
    """중앙 집중식 프로젝트 설정 관리 클래스"""
    
    # 기본 경로 설정
    ROOT_DIR = Path(__file__).resolve().parents[1]
    DATA_DIR = ROOT_DIR / "data"
    
    # 모듈별 경로
    SPEECH_DB_PATH = DATA_DIR / "speech_tracker" / "speeches.db"
    DOCS_DIR = ROOT_DIR / "docs"
    SPEECH_DASHBOARD_PATH = DOCS_DIR / "speech_tracker" / "data.json"
    
    # LLM 설정
    SPEECH_API_KEY = os.getenv("GOOGLE_API_KEY_FREE_TIER") or os.getenv("GOOGLE_API_KEY")
    DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
    
config = Config()
