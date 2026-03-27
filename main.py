import openai
import pandas as pd
import time
import random
import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from pydub import AudioSegment
from notion_client import Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ==========================================
# 환경 변수 로드
# ==========================================
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_VOCA_DATABASE_ID = os.getenv("NOTION_VOCA_DATABASE_ID")
NOTION_AAC_DATABASE_ID = os.getenv("NOTION_AAC_DATABASE_ID")
NOTION_TASK_DATABASE_ID = os.getenv("NOTION_TASK_DATABASE_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_DRIVE_PARENT_ID = os.getenv("GOOGLE_DRIVE_PARENT_ID")
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# ==========================================
# 날짜 및 경로 설정
# ==========================================
now = datetime.now()

YY_MM = now.strftime("%y.%m")
MM_DD = now.strftime("%m.%d")
YY_MM_DD = now.strftime("%y.%m.%d")
TODAY_DATE = now.strftime('%Y-%m-%d')

BASE_DIR = Path.cwd()

# 디렉토리 구조
ORIGINAL_DIR = BASE_DIR / "words" / "original" / YY_MM
STRUCTURED_DIR = BASE_DIR / "words" / "structured" / YY_MM
TTS_DAY_DIR = BASE_DIR / "TTS" / YY_MM / MM_DD
AAC_DAY_DIR = BASE_DIR / "AAC" / YY_MM

# 폴더 생성
for directory in [ORIGINAL_DIR, STRUCTURED_DIR, TTS_DAY_DIR, AAC_DAY_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# 파일 경로
INPUT_CSV = ORIGINAL_DIR / f"{YY_MM_DD}.csv"
STRUCTURED_CSV = STRUCTURED_DIR / f"structured_{YY_MM_DD}.csv"
CLEAN_CSV = STRUCTURED_DIR / f"{YY_MM_DD}_words.csv"
FINAL_AAC_ENG = AAC_DAY_DIR / f"{YY_MM_DD}_영어.aac"
FINAL_AAC_KOR = AAC_DAY_DIR / f"{YY_MM_DD}_한글.aac"

print(f"📍 작업 날짜: {YY_MM_DD}")
print(f"📂 입력 파일: {INPUT_CSV}")
print(f"📂 출력 파일: {CLEAN_CSV}")

# ==========================================
# OpenAI 클라이언트 초기화
# ==========================================
client = openai.OpenAI(api_key=OPENAI_API_KEY)

# ==========================================
# Google Drive 및 Notion 클라이언트 초기화
# ==========================================
credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_SERVICE_ACCOUNT_JSON,
    scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)
notion_client = Client(auth=NOTION_API_KEY)

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ==========================================
# 프롬프트 템플릿
# ==========================================
PROMPT_TEMPLATE = """
## Instruction

You are generating an English collocation-based vocabulary table for immersive and practical usage.

The goal is:
- To learn high-frequency collocations used by native speakers.
- To improve conversational and professional English.
- To help the learner visualize real-life situations.
- To make expressions feel alive, not dictionary-like.
- To prioritize natural usage over forced combinations.
- To prioritize natural corpus-based usage over logical but unnatural combinations.

---

## Definition of Collocation Unit

A collocation unit must be one of the following:

- verb + object  (e.g., raise a concern)
- verb + preposition  (e.g., deal with)
- adjective + noun  (e.g., mutual agreement)
- fixed phrase (commonly used chunk in spoken or business English)

Do NOT generate:
- Rare literary expressions
- Overly academic C2-level phrases
- Logically possible but unnatural combinations

---

## Frequency & Level Constraint

- Focus on CEFR B1–C1 level spoken and business English.
- Prioritize expressions commonly used in:
  - meetings
  - daily conversation
  - workplace communication
  - exam listening/reading passages
- Avoid obscure, outdated, or rarely spoken expressions.

---

## Critical Rules

1. Focus on COLLOCATION UNITS, not isolated words.
2. Only generate high-frequency, natural native expressions.
3. Do NOT create forced or unnatural combinations.
4. Each collocation must include "{word}" naturally.
5. Assign ONLY ONE primary tone per collocation.
6. "used in" must be one of:
   - conversation
   - workplace
   - academic
   - public/service
7. Tone must be ONE of:
   - casual
   - neutral
   - business
8. Do NOT artificially create all tone levels.
9. If a natural and commonly confused synonym or collocation exists, "Nuance (Korean)" must explain the difference using an A vs B format. If no meaningful comparison exists, explain the usage context without forcing a comparison.
10. Nuance must be concise but vivid (1–3 sentences max).
11. Meaning must translate the FULL collocation into natural spoken Korean.
    Avoid dictionary-style translation.
12. Example sentence must clearly imply a real-life scene which is high-frequency, natural native expression.
13. Avoid character-specific storytelling. Use generalized situation types.
14. Translation must sound like something a real person would actually say in that situation.
15. Maintain consistency and realism across all entries.
16. If the word has multiple core meanings, ensure the table covers a balanced variety of those meanings.

---

## Output Format

| collocation unit | primary tone | used in | meaning | Nuance (Korean) | example sentence | translation |

---

Now generate the collocation table for: {word}
"""


# ==========================================
# 함수 정의
# ==========================================

def get_word_details(word):
    """OpenAI API를 호출하여 단어 상세 정보를 가져옵니다."""
    prompt = PROMPT_TEMPLATE.format(word=word)
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an AI assistant that generates structured English learning content."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"⚠️ OpenAI API 오류: {e}")
        return None


def parse_response(response_text):
    """OpenAI 응답을 테이블 형태로 파싱합니다."""
    lines = response_text.split("\n")
    table_data = []
    
    for line in lines:
        if "|" in line and "---" not in line:
            columns = [col.strip() for col in line.split("|")[1:-1]]
            if len(columns) >= 7:
                table_data.append(columns[:7])
    
    return table_data


def generate_structured_csv():
    """단어 리스트를 읽고 구조화된 CSV를 생성합니다."""
    print("\n🔹 Step 1: 구조화된 CSV 생성 중...")
    
    if not INPUT_CSV.exists():
        print(f"❌ 입력 파일을 찾을 수 없습니다: {INPUT_CSV}")
        return False
    
    df_words = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
    words = df_words["word"].tolist()
    
    structured_data = []
    
    for idx, word in enumerate(words):
        print(f"  🔸 {idx+1}/{len(words)}: {word} 처리 중...")
        response_text = get_word_details(word)
        
        if response_text:
            parsed_data = parse_response(response_text)
            structured_data.extend(parsed_data)
        
        time.sleep(1)
    
    columns = [
        "collocation unit", "primary tone", "used in", "meaning", "nuance (Korean)", 
        "example sentence", "translation"
    ]
    df_output = pd.DataFrame(structured_data, columns=columns)
    df_output.to_csv(STRUCTURED_CSV, index=False, encoding="utf-8-sig")
    
    print(f"✅ 구조화된 CSV 생성 완료: {STRUCTURED_CSV}")
    return True


def clean_csv():
    """중복 헤더를 제거하고 번호를 추가합니다."""
    print("\n🔹 Step 2: CSV 정리 중...")
    
    df = pd.read_csv(STRUCTURED_CSV, encoding="utf-8-sig")
    df_cleaned = df[df["collocation unit"] != "collocation unit"]
    df_cleaned.insert(0, "No.", range(1, len(df_cleaned) + 1))
    df_cleaned.to_csv(CLEAN_CSV, index=False, encoding="utf-8-sig")
    
    print(f"✅ CSV 정리 완료: {CLEAN_CSV}")


def generate_tts():
    """
    예문을 TTS로 변환합니다.
    - 개별 파일: 1.2x 속도 (Notion 업로드용, 영어만)
    - AAC용 임시 파일: 
      - 영어: 0.9x, 1.0x, 1.2x
      - 한글: 1.0x (번역문)
    """
    print("\n🔹 Step 3: TTS 음성 파일 생성 중...")
    
    df = pd.read_csv(CLEAN_CSV, encoding="utf-8-sig")
    
    # 영어 음성 설정
    voices_en = ["alloy", "onyx", "nova", "echo", "fable", "shimmer"]
    today_seed = datetime.today().strftime("%Y-%m-%d")
    random.seed(today_seed)
    voice_en = random.choice(voices_en)
    
    print(f"  🎤 선택된 영어 음성: {voice_en}")
    print(f"  🎤 선택된 한글 음성: alloy (고정)")
    
    # 📌 AAC용 임시 폴더 생성
    temp_aac_dir = TTS_DAY_DIR / "temp_aac"
    temp_korean_dir = TTS_DAY_DIR / "temp_korean"
    temp_aac_dir.mkdir(exist_ok=True)
    temp_korean_dir.mkdir(exist_ok=True)
    
    for index, row in df.iterrows():
        sentence_en = str(row.get("example sentence", "")).strip()
        sentence_ko = str(row.get("translation", "")).strip()
        
        if sentence_en and sentence_en.lower() != "nan":
            # ═══════════════════════════════════════
            # 1️⃣ Notion 업로드용: 영어 1.2x 고정 속도
            # ═══════════════════════════════════════
            try:
                response = client.audio.speech.create(
                    model="tts-1",
                    voice=voice_en,
                    speed=1.2,
                    input=sentence_en
                )
                
                file_name = TTS_DAY_DIR / f"{index+1}.mp3"
                response.stream_to_file(str(file_name))
                print(f"  ✅ {index+1}.mp3 저장 완료 (영어 1.2x - Notion용)")
                
            except openai.BadRequestError as e:
                print(f"  ⚠️ 영어 TTS 오류 ({index+1}): {e}")
            
            # ═══════════════════════════════════════
            # 2️⃣ AAC 통합용: 영어 다중 속도 (0.9x, 1.0x, 1.2x)
            # ═══════════════════════════════════════
            speeds = {
                'slow': 0.9,
                'normal': 1.0,
                'fast': 1.2
            }
            
            for speed_name, speed_value in speeds.items():
                try:
                    response = client.audio.speech.create(
                        model="tts-1",
                        voice=voice_en,
                        speed=speed_value,
                        input=sentence_en
                    )
                    
                    temp_file = temp_aac_dir / f"{index+1}_{speed_name}.mp3"
                    response.stream_to_file(str(temp_file))
                    
                except openai.BadRequestError as e:
                    print(f"  ⚠️ 영어 AAC용 TTS 오류 ({index+1}_{speed_name}): {e}")
            
            # ═══════════════════════════════════════
            # 3️⃣ AAC 통합용: 한글 번역 (1.0x 속도)
            # ═══════════════════════════════════════
            if sentence_ko and sentence_ko.lower() != "nan":
                try:
                    response = client.audio.speech.create(
                        model="tts-1",
                        voice="alloy",  # 한글은 alloy 권장
                        speed=1.0,
                        input=sentence_ko
                    )
                    
                    korean_file = temp_korean_dir / f"{index+1}_korean.mp3"
                    response.stream_to_file(str(korean_file))
                    print(f"  ✅ {index+1}_korean.mp3 저장 완료 (한글 1.0x)")
                    
                except openai.BadRequestError as e:
                    print(f"  ⚠️ 한글 TTS 오류 ({index+1}): {e}")
            
            # API 요청 제한 방지
            time.sleep(0.5)
    
    print("✅ TTS 생성 완료!")
    print(f"  📁 Notion용 (영어 1.2x): {TTS_DAY_DIR}")
    print(f"  📁 AAC용 영어 (다중속도): {temp_aac_dir}")
    print(f"  📁 AAC용 한글: {temp_korean_dir}")


def create_aac_files():
    """
    두 가지 AAC 파일 생성:
    1. 영어 버전: 느리게(0.9x) → 보통(1.0x) → 빠르게(1.2x)
    2. 한글 버전: 한글(1.0x) → 영어(1.2x)
    """
    print("\n🔹 Step 4: AAC 파일 생성 중...")
    
    df = pd.read_csv(CLEAN_CSV, encoding="utf-8-sig")
    temp_aac_dir = TTS_DAY_DIR / "temp_aac"
    temp_korean_dir = TTS_DAY_DIR / "temp_korean"
    
    # ═══════════════════════════════════════
    # 1️⃣ 영어 버전: 천천히 → 보통 → 빠르게
    # ═══════════════════════════════════════
    print("  🔸 영어 버전 AAC 생성 중 (느리게→보통→빠르게)...")
    
    combined_eng = AudioSegment.empty()
    gap_short = AudioSegment.silent(duration=3000)   # 3초 (같은 문장의 다른 속도 사이)
    gap_long = AudioSegment.silent(duration=7000)    # 7초 (다른 문장 사이)
    
    speeds_order = ['slow', 'normal', 'fast']
    
    for index in range(len(df)):
        for i, speed in enumerate(speeds_order):
            file_path = temp_aac_dir / f"{index+1}_{speed}.mp3"
            
            if file_path.exists():
                sound = AudioSegment.from_mp3(str(file_path))
                combined_eng += sound
                
                if i < len(speeds_order) - 1:
                    combined_eng += gap_short
        
        if index < len(df) - 1:
            combined_eng += gap_long
    
    combined_eng.export(str(FINAL_AAC_ENG), format="adts", bitrate="64k")
    print(f"  ✅ 영어 AAC 생성 완료: {FINAL_AAC_ENG}")
    
    # ═══════════════════════════════════════
    # 2️⃣ 한글 버전: 한글(1.0x) → 영어(1.2x)
    # ═══════════════════════════════════════
    print("  🔸 한글 버전 AAC 생성 중 (한글→영어)...")
    
    combined_kor = AudioSegment.empty()
    gap_between = AudioSegment.silent(duration=4000)  # 4초 (한글과 영어 사이)
    gap_long = AudioSegment.silent(duration=7000)     # 7초 (다른 문장 사이)
    
    for index in range(len(df)):
        # 한글 먼저
        korean_file = temp_korean_dir / f"{index+1}_korean.mp3"
        if korean_file.exists():
            sound_ko = AudioSegment.from_mp3(str(korean_file))
            combined_kor += sound_ko
            combined_kor += gap_between
        
        # 그 다음 영어 (빠른 속도 1.2x)
        english_file = temp_aac_dir / f"{index+1}_fast.mp3"
        if english_file.exists():
            sound_en = AudioSegment.from_mp3(str(english_file))
            combined_kor += sound_en
        
        # 다음 문장으로 넘어갈 때
        if index < len(df) - 1:
            combined_kor += gap_long
    
    combined_kor.export(str(FINAL_AAC_KOR), format="adts", bitrate="64k")
    print(f"  ✅ 한글 AAC 생성 완료: {FINAL_AAC_KOR}")
    
    print(f"\n✅ AAC 파일 생성 완료!")
    print(f"  📊 영어 버전: {len(df)}개 문장 × 3가지 속도")
    print(f"  📊 한글 버전: {len(df)}개 문장 (한글→영어)")


def create_daily_task():
    """
    TASKS DB에 오늘의 영어단어 Task를 생성
    
    Returns:
        생성된 Task 페이지의 ID (VOCA DB에서 역참조용)
    """
    print("\n🔹 Step 5-1: TASKS DB에 오늘의 학습 Task 먼저 생성 중...")
    
    if not NOTION_TASK_DATABASE_ID:
        print("  ⚠️ NOTION_TASK_DATABASE_ID가 설정되지 않았습니다. Task 생성을 건너뜁니다.")
        return None
    
    try:
        # Task 페이지 생성
        task_data = {
            "parent": {"database_id": NOTION_TASK_DATABASE_ID},
            "properties": {
                "이름": {
                    "title": [{"text": {"content": "영어단어"}}]
                },
                "날짜": {
                    "date": {"start": TODAY_DATE}
                },
                "분야": {
                    "select": {"name": "토익 및 기초"}
                },
                "상태": {
                    "select": {"name": "완료"}
                }
            }
        }
        
        response = requests.post(
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS,
            json=task_data
        )
        
        if response.status_code == 200:
            task_page_id = response.json().get("id")
            print(f"  ✅ Task 생성 완료: 영어단어 (ID: {task_page_id[:8]}...)")
            return task_page_id
        else:
            print(f"  ⚠️ Task 생성 실패: {response.status_code}")
            print(f"  📝 응답: {response.json()}")
            return None
            
    except Exception as e:
        print(f"  ⚠️ Task 생성 중 오류: {e}")
        return None


def upload_to_notion_voca(task_page_id=None):
    """
    단어장 데이터를 Notion 데이터베이스에 업로드합니다.
    
    Args:
        task_page_id: TASKS DB의 오늘 Task 페이지 ID (Relation 연결용)
    """
    print("\n🔹 Step 5-2: Notion 단어장 DB 업로드 중...")
    
    df = pd.read_csv(CLEAN_CSV, dtype=str)
    
    for _, row in df.iterrows():
        data = {
            "parent": {"database_id": NOTION_VOCA_DATABASE_ID},
            "properties": {
                "No.": {
                    "rich_text": [{"text": {"content": str(row["No."])}}]
                },
                "collocation unit": {
                    "title": [{"text": {"content": row["collocation unit"]}}]
                },
                "primary tone": {
                    "select": {"name": row["primary tone"]}
                },
                "nuance (Korean)": {
                    "rich_text": [{"text": {"content": row["nuance (Korean)"]}}]
                },
                "used in": {
                    "select": {"name": row["used in"]}
                },
                "meaning": {
                    "rich_text": [{"text": {"content": row["meaning"]}}]
                },
                "example sentence": {
                    "rich_text": [{"text": {"content": row["example sentence"]}}]
                },
                "translation": {
                    "rich_text": [{"text": {"content": row["translation"]}}]
                },
                "날짜": {"date": {"start": TODAY_DATE}},
                # 🎯 TASKS DB의 오늘 Task와 Relation 연결
                "TASK": {
                    "relation": [{"id": task_page_id}] if task_page_id else []
                }
            }
        }
        
        response = requests.post(
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS,
            json=data
        )
        
        if response.status_code == 200:
            print(f"  ✅ No.{row['No.']} 업로드 완료 (Task 연결됨)")
        else:
            print(f"  ⚠️ No.{row['No.']} 업로드 실패: {response.status_code}")
    
    print("✅ Notion 단어장 업로드 완료!")



def create_drive_folder(folder_name):
    """Google Drive에 폴더를 생성합니다."""
    folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [GOOGLE_DRIVE_PARENT_ID]
    }
    folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
    return folder.get("id")


def upload_to_google_drive(file_path, folder_id):
    """Google Drive에 파일을 업로드합니다."""
    file_name = os.path.basename(file_path)
    file_metadata = {
        "name": file_name,
        "parents": [folder_id]
    }
    
    media = MediaFileUpload(
        file_path,
        mimetype="audio/mpeg",
        resumable=True
    )
    
    request = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    )
    
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"  {file_name} 업로드 진행률: {int(status.progress() * 100)}%")
    
    file_id = response.get("id")
    
    # 공유 설정
    drive_service.permissions().create(
        fileId=file_id,
        body={"role": "reader", "type": "anyone"},
    ).execute()
    
    print(f"  ✅ {file_name} 업로드 완료")
    
    return f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"


def upload_tts_to_drive_and_notion():
    """
    TTS 파일(영어 1.2x)을 Google Drive에 업로드하고 Notion에 링크를 추가합니다.
    """
    print("\n🔹 Step 6: TTS 파일(영어 1.2x) Google Drive 업로드 및 Notion 연동 중...")
    
    folder_id = create_drive_folder(TODAY_DATE)
    mp3_files = [f for f in os.listdir(TTS_DAY_DIR) if f.endswith(".mp3") and f.isdigit()[:-4]]
    
    for file in sorted(mp3_files, key=lambda x: int(x.replace('.mp3', ''))):
        file_path = TTS_DAY_DIR / file
        file_url = upload_to_google_drive(str(file_path), folder_id)
        
        if file_url:
            # No. 추출
            key = file.replace(".mp3", "")
            
            # Notion 페이지 찾기
            query = notion_client.databases.query(
                database_id=NOTION_VOCA_DATABASE_ID,
                filter={
                    "and": [
                        {"property": "No.", "rich_text": {"equals": key}},
                        {"property": "날짜", "date": {"equals": TODAY_DATE}}
                    ]
                }
            )
            
            results = query.get("results")
            
            if results:
                page_id = results[0]["id"]
                
                notion_client.pages.update(
                    page_id=page_id,
                    properties={
                        "음성": {
                            "files": [
                                {
                                    "name": file,
                                    "external": {"url": file_url}
                                }
                            ]
                        }
                    }
                )
                print(f"  ✅ No.{key} Notion 음성 링크 업데이트 완료")
    
    print("✅ TTS 업로드 및 Notion 연동 완료!")


def upload_aac_to_drive_and_notion():
    """
    AAC 파일들을 Google Drive에 업로드하고 Notion에 추가합니다.
    - 영어 버전 (천천히→빠르게)
    - 한글 버전 (한글→영어)
    """
    print("\n🔹 Step 7: AAC 파일 Google Drive 업로드 및 Notion 추가 중...")
    
    folder_id = create_drive_folder(f"{TODAY_DATE}_AAC")
    
    # ═══════════════════════════════════════
    # 1️⃣ 영어 버전 AAC 업로드
    # ═══════════════════════════════════════
    print("  🔸 영어 버전 AAC 업로드 중...")
    aac_url_eng = upload_to_google_drive(str(FINAL_AAC_ENG), folder_id)
    
    # ═══════════════════════════════════════
    # 2️⃣ 한글 버전 AAC 업로드
    # ═══════════════════════════════════════
    print("  🔸 한글 버전 AAC 업로드 중...")
    aac_url_kor = upload_to_google_drive(str(FINAL_AAC_KOR), folder_id)
    
    # ═══════════════════════════════════════
    # 3️⃣ Notion AAC DB에 페이지 생성 (Select로 버전 구분)
    # ═══════════════════════════════════════
    if aac_url_eng:
        notion_client.pages.create(
            parent={"database_id": NOTION_AAC_DATABASE_ID},
            properties={
                "이름": {
                    "title": [{"text": {"content": f"{YY_MM_DD} 영어"}}]
                },
                "날짜": {
                    "date": {
                        "start": TODAY_DATE
                    }
                },
                "버전": {
                    "select": {"name": "영어"}
                },
                "음성": {
                    "files": [
                        {
                            "name": f"{YY_MM_DD}_영어.aac",
                            "external": {
                                "url": aac_url_eng
                            }
                        }
                    ]
                }
            }
        )
        print("  ✅ 영어 AAC Notion 업로드 완료!")
    
    if aac_url_kor:
        notion_client.pages.create(
            parent={"database_id": NOTION_AAC_DATABASE_ID},
            properties={
                "이름": {
                    "title": [{"text": {"content": f"{YY_MM_DD} 한글"}}]
                },
                "날짜": {
                    "date": {
                        "start": TODAY_DATE
                    }
                },
                "버전": {
                    "select": {"name": "한글"}
                },
                "음성": {
                    "files": [
                        {
                            "name": f"{YY_MM_DD}_한글.aac",
                            "external": {
                                "url": aac_url_kor
                            }
                        }
                    ]
                }
            }
        )
        print("  ✅ 한글 AAC Notion 업로드 완료!")
    
    print("\n✅ 모든 AAC 파일 업로드 완료!")
    print(f"  🎧 영어 버전: 느리게(0.9x) → 보통(1.0x) → 빠르게(1.2x)")
    print(f"  🎧 한글 버전: 한글(1.0x) → 영어(1.2x)")


# ==========================================
# 메인 실행 함수
# ==========================================

def main():
    """전체 워크플로우를 실행합니다."""
    print("=" * 50)
    print("🚀 Notion 단어장 자동화 시작")
    print("=" * 50)
    
    try:
        # Step 1: 구조화된 CSV 생성
        if not generate_structured_csv():
            return
        
        # Step 2: CSV 정리
        clean_csv()
        
        # Step 3: TTS 생성 (Notion용 1.2x + AAC용 다중 속도 + 한글)
        generate_tts()
        
        # Step 4: AAC 파일 생성 (영어 버전 + 한글 버전)
        create_aac_files()
        
        # Step 5-1: TASKS DB에 오늘 Task 먼저 생성
        task_page_id = create_daily_task()
        
        # Step 5-2: Notion 단어장 DB 업로드 (Task와 Relation 연결)
        upload_to_notion_voca(task_page_id)
        
        # Step 6: TTS 파일(영어 1.2x) 업로드 및 Notion 연동
        upload_tts_to_drive_and_notion()
        
        # Step 7: AAC 파일들 업로드 및 Notion 추가
        upload_aac_to_drive_and_notion()
        
        print("\n" + "=" * 50)
        print("🎉 모든 작업이 완료되었습니다!")
        print("=" * 50)
        print("\n📊 생성된 파일:")
        print(f"  - Notion 개별 TTS: 영어 1.2x (빠른 복습용)")
        print(f"  - 영어 AAC: 0.9x → 1.0x → 1.2x (점진적 학습)")
        print(f"  - 한글 AAC: 한글 → 영어 1.2x (스피킹 연습용)")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()