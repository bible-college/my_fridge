# backend/crawler.py
import os
import json
import logging
import re
from datetime import datetime
from yt_dlp import YoutubeDL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import DishType, Recipe, RecipeComment, RecipeChapter

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DB 연결 (Docker 내부 DNS를 통해 'db' 서비스 이름으로 접근)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://jmgjmg102:1234@db:5432/fridge_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 필터링 키워드
BLACKLIST_KEYWORDS = ["광고", "협찬"]

def get_crawled_video_ids(db_session):
    """DB에서 이미 크롤링된 영상 ID를 가져옵니다."""
    try:
        crawled_ids = db_session.query(Recipe.youtube_id).all()
        return {item[0] for item in crawled_ids}
    except Exception as e:
        logging.error(f"DB에서 크롤링된 영상 ID를 가져오는 중 오류 발생: {e}")
        return set()

def get_search_queries(db_session):
    """다양한 검색 키워드를 조합하여 동적으로 생성합니다."""
    base_queries = []
    try:
        dish_types = db_session.query(DishType).all()
        for dish in dish_types:
            base_queries.append(dish.name)
    except Exception as e:
        logging.warning(f"DishType 테이블을 가져오는 중 오류 발생: {e}. 기본 검색어를 사용합니다.")
        base_queries = ["한식", "중식", "양식"]
    
    extended_queries = set()
    for query in base_queries:
        extended_queries.add(f"{query} 레시피")
        extended_queries.add(f"{query} 만들기")
    
    # 추가적인 키워드 (예시)
    extended_queries.add("간단한 요리")
    extended_queries.add("자취생 요리")
    
    return list(extended_queries)

def get_video_info(query: str, max_videos: int = 20):
    """
    유튜브에서 영상을 검색하고 메타데이터, 자막, 댓글 등을 가져옵니다.
    """
    ydl_opts = {
        'format': 'best',
        'quiet': True,
        'extract_flat': True,
        'dump_single_json': True,
        'force_generic_extractor': True,
        'writesubtitles': True,  # 자막 다운로드 활성화
        'allsubs': True,         # 모든 언어의 자막 다운로드
        'writeinfojson': True,   # 메타데이터 JSON 파일 쓰기
        'getcomments': True,     # 댓글 정보 가져오기 활성화
        'skip_download': True,   # 실제 영상 파일은 다운로드하지 않음
        'playlistend': max_videos,
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch{max_videos}:{query}", download=False)
            return result.get('entries', [])

    except Exception as e:
        logging.error(f"'{query}' 크롤링 중 오류 발생: {e}")
        return []

def is_valid_video(video):
    """정의된 5가지 필터링 기준에 따라 영상을 검증합니다."""
    # 1. 영상 길이 (1분 이상)
    duration = video.get('duration')
    if duration is None or duration < 60:
        logging.info(f"필터링: 영상 길이가 1분 미만입니다. (ID: {video.get('id')})")
        return False
    
    # 2. 조회수 (5,000회 이상)
    view_count = video.get('view_count')
    if view_count is None or view_count < 5000:
        logging.info(f"필터링: 조회수가 5,000회 미만입니다. (ID: {video.get('id')}, 조회수: {view_count})")
        return False

    # 3. 키워드 필터링 ("광고", "협찬")
    title = video.get('title', '')
    description = video.get('description', '')
    combined_text = title + " " + description
    for keyword in BLACKLIST_KEYWORDS:
        if keyword in combined_text:
            logging.info(f"필터링: 제목 또는 설명에 '{keyword}' 키워드가 포함되어 있습니다. (ID: {video.get('id')})")
            return False

    # 4. 요리/음식 관련 카테고리 (카테고리 ID는 국가별로 다를 수 있습니다. 예시)
    category = video.get('categories', [])
    if not any(c.lower() in ["cooking", "how-to & style", "food & drink"] for c in category):
        logging.info(f"필터링: 요리 관련 카테고리가 아닙니다. (ID: {video.get('id')})")
        return False

    # 5. 한국 채널 영상 (추정)
    # youtube-dlp가 채널 국가 정보를 직접 제공하지 않으므로, 채널 제목이나 설명에서 한국어 단어를 기반으로 추정합니다.
    channel_title = video.get('channel', '')
    # 한글 자음, 모음이 포함된 경우 한국 채널로 추정 (간단한 방법)
    if not re.search('[가-힣]', channel_title):
        logging.info(f"필터링: 한국 채널로 추정되지 않습니다. (ID: {video.get('id')}, 채널명: {channel_title})")
        return False

    return True

if __name__ == "__main__":
    db_session = SessionLocal()
    
    # 1. 기존에 크롤링한 영상 ID 목록 가져오기
    crawled_ids = get_crawled_video_ids(db_session)
    logging.info(f"데이터베이스에 이미 존재하는 영상 ID: {len(crawled_ids)}개")

    # 2. 검색어 목록 동적으로 가져오기
    search_queries = get_search_queries(db_session)

    all_videos_to_process = []
    
    # 3. 검색 쿼리 실행 및 영상 정보 수집
    for query in search_queries:
        logging.info(f"'{query}'에 대한 영상 정보를 가져오는 중...")
        videos = get_video_info(query, max_videos=20)
        
        for video in videos:
            video_id = video.get('id')
            # 중복 체크
            if video_id and video_id not in crawled_ids:
                all_videos_to_process.append(video)
                crawled_ids.add(video_id) # 현재 실행에서 중복되지 않도록 추가
    
    # 4. 필터링된 영상 JSON 파일로 저장
    filtered_videos = [v for v in all_videos_to_process if is_valid_video(v)]
    
    output_path = os.path.join(os.getcwd(), 'filtered_videos.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(filtered_videos, f, ensure_ascii=False, indent=4)
    
    logging.info(f"총 {len(filtered_videos)}개의 필터링된 영상 정보가 '{output_path}'에 저장되었습니다.")
    db_session.close()