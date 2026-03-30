import os
import re
import requests
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from dotenv import load_dotenv
from openai import OpenAI
from fastapi import BackgroundTasks

load_dotenv()

app = FastAPI()

# 🔑 환경변수
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
CONSUME_DB_ID = os.getenv("CONSUME_DB_ID")
CATEGORY_DB_ID = os.getenv("CATEGORY_DB_ID")
PAYMENT_DB_ID = os.getenv("PAYMENT_DB_ID")
SPENDING_DB_ID = os.getenv("SPENDING_DB_ID")
DAILY_DB_ID = os.getenv("DAILY_DB_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MONTHLY_DB_ID = os.getenv("MONTHLY_DB_ID")

client = OpenAI(api_key=OPENAI_API_KEY)

headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# 🔥 문자 파싱 함수
import re

def parse_sms(text):
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    merchant, amount, card = "알 수 없음", 0, "기타카드"

    # 케이뱅크 판별
    if "[케이뱅크]" in text:
        card = "케이뱅크"
        amount_match = re.search(r"출금\s*([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0
        merchant = lines[-1].split("_")[0]

    # 하나은행 판별
    elif "하나," in text:
        card = "하나카드"
        amount_match = re.search(r"출금\s*([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0
        for i, line in enumerate(lines):
            if "출금" in line:
                merchant = lines[i+1].split("_")[0]
                break

    # KB 국민은행 판별 (줄 바꿈 스타일 대응)
    elif "[KB]" in text:
        card = "국민은행"
        for i, line in enumerate(lines):
            if "출금" in line:
                if i > 0: merchant = lines[i-1] # 출금 윗줄이 가맹점
                if i + 1 < len(lines):
                    amount_str = re.sub(r"[^\d]", "", lines[i+1])
                    amount = int(amount_str) if amount_str else 0
                break

    return merchant, amount, card

#def normalize_merchant(name):
#    name = name.lower()                  
#    name = re.sub(r"[^a-z0-9가-힣]", "", name)   # 특수문자 제거
#    return name


#def match_merchant(normalized_name):
    # MERCHANT_MAP의 key들이 normalized_name 안에 포함되어 있는지 확인
#    for key in MERCHANT_MAP:
        # 예: normalized_name이 "스타벅스강남점"이고 key가 "스타벅스"라면 매칭 성공
#        if key in normalized_name:
#            print(f"✅ 부분 일치 매핑 발견: {normalized_name} -> {MERCHANT_MAP[key]['name']}")
#            return MERCHANT_MAP[key]
#    return None

def match_merchant(merchant):
    """
    가맹점 이름에 MERCHANT_MAP의 키워드가 포함되어 있으면 매칭
    예: "스타벅스강남점" → "스타벅스" 매칭
        "GS25 역삼점" → "GS25" 매칭
    """
    # 대소문자 구분 없이 비교하기 위해 소문자로 변환
    merchant_lower = merchant.lower()
    
    # MERCHANT_MAP의 모든 키를 확인
    for key in MERCHANT_MAP:
        key_lower = key.lower()
        
        # 가맹점 이름에 키워드가 포함되어 있으면
        if key_lower in merchant_lower:
            print(f"✅ 매칭 성공: '{merchant}' → '{MERCHANT_MAP[key]['name']}' (키워드: '{key}')", flush=True)
            return MERCHANT_MAP[key]
    
    print(f"❌ 매칭 실패: '{merchant}' (GPT 사용)", flush=True)
    return None

def gpt_extract(merchant):
    prompt = f"""
다음 카드 가맹점 이름을 표준화하고 카테고리를 분류해.

출력 형식:
이름: (대표 이름 하나, 예: GS25, 스타벅스, 쿠팡 등)
카테고리: (식비, 카페, 교통, 쇼핑, 구독, 여가, 통신, 기타 중 하나)

가맹점: {merchant}
"""
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        text = res.choices[0].message.content

        name_match = re.search(r"이름:\s*(.*)", text)
        cat_match = re.search(r"카테고리:\s*(.*)", text)

        name = name_match.group(1).strip() if name_match else merchant
        category = cat_match.group(1).strip() if cat_match else "기타"

        return name, category
    except Exception as e:
        print(f"GPT 오류: {e}")
        return merchant, "기타"

# 🧠 카테고리 분류
# 1. 가맹점 매핑 테이블 (상단에 추가)
# "문자상의 이름": {"name": "노션에 표시할 이름", "category": "카테고리"}

MERCHANT_MAP = {
    "LG유플러스":{"name":"통신요금","category":"구독"},
    "네이버멤버십":{"name":"네이버멤버십,"category":"구독"},
    "와우멤버십":{"name":"쿠팡(와우멤버십),"category":"구독"},
    "위대한상상":{"name":"요기요","category":"식비"},
    "롯데컬처웍스":{"name":"롯데시네마","category":"여가"},
    "뚜레쥬르":{"name": "뚜레쥬르","category":"식비"},
    "올리브영":{"name": "올리브영","category":"쇼핑"},
    "CU":{"name": "CU", "category": "기타"},
    "씨유":{"name": "CU", "category": "기타"},
    "지에스25": {"name": "GS25", "category": "기타"},
    "gs25": {"name": "GS25", "category": "기타"},
    "스타벅스": {"name": "스타벅스", "category": "카페"},
    "쿠팡": {"name": "쿠팡", "category": "쇼핑"},
    "리앤이라마띠네": {"name": "구내식당", "category": "식비"},
    "현대그린푸드": {"name": "구내식당", "category": "식비"},
    "에스씨케이컴퍼니": {"name": "스타벅스", "category": "카페"},
    "네이버파이낸셜": {"name": "네이버페이", "category": "기타"},
}
# 2. 카테고리 분류 함수 수정
def classify_category(merchant):
    # 매핑 테이블에 있는지 먼저 확인
    if merchant in MERCHANT_MAP:
        print(f"✅ 매핑 데이터 발견: {merchant} -> {MERCHANT_MAP[merchant]['category']}")
        return MERCHANT_MAP[merchant]["category"]

    # 매핑에 없으면 기존처럼 GPT에게 물어보기
    prompt = f"""
다음 가맹점을 소비 카테고리로 분류해.
반드시 아래 중 하나만 출력: 식비, 카페, 교통, 쇼핑, 구독, 취미, 기타

가맹점: {merchant}
"""
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        return res.choices[0].message.content.strip()
    except:
        return "기타"
def clean_category(text):
    for c in ["식비", "카페", "교통", "쇼핑", "구독", "기타", "여가"]:
        if c in text:
            return c
    return "기타"

def detect_spending_type(card):
    if "케이뱅크" in card:
        return "데이트자금"
    return "생활자금"

def get_title(prop):
    if not prop or "title" not in prop:
        return ""
    arr = prop["title"]
    if not arr:
        return ""
    return arr[0].get("plain_text", "")

def get_relation_id(db_id, name):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data.get("results", []):
        title = get_title(page["properties"].get("이름"))
        if title == name:
            return page["id"]
    return None

def get_today_page():
    # 1. 한국 시간 기준 오늘 날짜 생성 (Render 서버 시간 대응)
    # 현재 서버가 2026년이라면 이에 맞춰 작동합니다.
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst) 
    today_str = now_kst.strftime("%Y-%m-%d") # "2026-03-24" 형식
    
    url = f"https://api.notion.com/v1/databases/{DAILY_DB_ID}/query"
    
    # 2. '날짜' 속성이 오늘인 데이터를 필터링
    query_data = {
        "filter": {
            "property": "날짜",  # 사진에 있는 '날짜' 컬럼 이름
            "date": {
                "equals": today_str
            }
        }
    }
    
    try:
        res = requests.post(url, headers=headers, json=query_data)
        data = res.json()
        results = data.get("results", [])

        if results:
            return results[0]["id"]
        else:
            print(f"📍 오늘 날짜({today_str})로 설정된 페이지를 찾지 못했습니다.")
            return None
    except Exception as e:
        print(f"❌ 조회 중 오류 발생: {e}")
        return None

def get_or_create_monthly_page(year: int, month: int):
    """
    해당 년월의 월별명세 페이지를 찾거나 새로 만듦
    """
    title = f"{year}년 {month}월"
    
    # 1. 이미 있는지 확인
    url = f"https://api.notion.com/v1/databases/{MONTHLY_DB_ID}/query"
    query = {
        "filter": {
            "and": [
                {
                    "property": "년도",
                    "number": {
                        "equals": year
                    }
                },
                {
                    "property": "월",
                    "number": {
                        "equals": month
                    }
                }
            ]
        }
    }
    
    try:
        res = requests.post(url, headers=headers, json=query)
        data = res.json()
        results = data.get("results", [])
        
        if results:
            print(f"✅ 기존 월별명세 발견: {title}", flush=True)
            return results[0]["id"]
        
        # 2. 없으면 새로 생성
        print(f"🆕 새 월별명세 생성: {title}", flush=True)
        create_data = {
            "parent": {"database_id": MONTHLY_DB_ID},
            "properties": {
                "제목": {"title": [{"text": {"content": title}}]},
                "년도": {"number": year},
                "월": {"number": month}
            }
        }
        
        create_res = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json=create_data
        )
        
        return create_res.json()["id"]
        
    except Exception as e:
        print(f"❌ 월별명세 처리 오류: {e}", flush=True)
        return None

# 🚀 메인 API
@app.post("/add")
def add_data(body: dict):
    import sys
    
    print("=" * 50, flush=True)
    print(f"📥 /add 호출됨", flush=True)
    print(f"body: {body}", flush=True)
    sys.stdout.flush()
    
    text = body.get("text")
    date = body.get("date")

    if not text:
        print("❌ text 없음", flush=True)
        return {"status": "no_text"}

    # 날짜 방어 강화
    if not date or date.strip() == "":
        print("⚠️ date 없음 또는 빈값 → 현재시각 사용", flush=True)
        kst = timezone(timedelta(hours=9))
        date = datetime.now(kst).isoformat()
    else:
        print(f"📅 받은 date: {date}", flush=True)
        # yyyy-MM-dd HH:mm 형식을 ISO로 변환
        try:
            kst = timezone(timedelta(hours=9))
            dt = datetime.strptime(date, "%Y-%m-%d %H:%M")
            date = dt.replace(tzinfo=kst).isoformat()
            print(f"✅ ISO 변환: {date}", flush=True)
        except Exception as e:
            print(f"❌ 날짜 변환 실패: {e}", flush=True)
            kst = timezone(timedelta(hours=9))  # 👈 여기!
            date = datetime.now(kst).isoformat()

    # 백그라운드 말고 바로 실행
    try:
        print("🚀 process_data 시작", flush=True)
        process_data(text, date)
        print("✅ 완료!", flush=True)
        return {"status": "success"}
    except Exception as e:
        print(f"💥 에러: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

def safe_process_data(text: str, date: str):
    try:
        print("🔥 BG task 시작")
        process_data(text, date)
    except Exception as e:
        print("💥 BG task 에러:", e)

def process_data(text: str, date: str):
    print("🔥 process 시작", flush=True)
    print("📩 text:", text, flush=True)
    print("📅 date:", date, flush=True)
    
    try:
        merchant, amount, card = parse_sms(text)
        print(f"🏪 파싱된 가맹점: '{merchant}'", flush=True)
    except Exception as e:
        print("parse_error:", e, flush=True)
        return

    # 👇 매칭 (한 번만!)
    mapping = match_merchant(merchant)

    if mapping:
        display_name = mapping["name"]
        category = mapping["category"]
        print(f"✅ 매핑 사용: {display_name} / {category}", flush=True)
    else:
        display_name, category = gpt_extract(merchant)
        print(f"🧠 GPT 분류: {display_name} / {category}", flush=True)
        print(f"💡 [MERCHANT_MAP 추가 추천]", flush=True)
        print(f"    \"{merchant}\": {{\"name\": \"{display_name}\", \"category\": \"{category}\"}},", flush=True)
    
    category = clean_category(category)
    amount = -abs(amount)
    spending_type = detect_spending_type(card)

    category_id = get_relation_id(CATEGORY_DB_ID, category)
    payment_id = get_relation_id(PAYMENT_DB_ID, card)
    spending_id = get_relation_id(SPENDING_DB_ID, spending_type)
    today_page_id = get_today_page()
    
    # 👇 날짜에서 년/월 추출
    dt = datetime.fromisoformat(date)
    year = dt.year
    month = dt.month
    
    # 👇 월별명세 페이지 가져오기/생성
    monthly_page_id = get_or_create_monthly_page(year, month)

    data = {
        "parent": {"database_id": CONSUME_DB_ID},
        "properties": {
            "내역": {"title": [{"text": {"content": display_name}}]},
            "금액 (기입용)": {"number": amount},
            "카테고리": {"relation": [{"id": category_id}] if category_id else []},
            "결제수단": {"relation": [{"id": payment_id}] if payment_id else []},
            "지출유형": {"relation": [{"id": spending_id}] if spending_id else []},
            "날짜": {"date": {"start": date}},
            "영수증": {"relation": [{"id": today_page_id}] if today_page_id else []},
            "월별명세": {"relation": [{"id": monthly_page_id}] if monthly_page_id else []}  # 👈 월별명세 추가!
        }
    }

    try:
        res = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json=data
        )
        print("notion status:", res.status_code, flush=True)
        print("📊 월별명세 연결: " + (f"{year}년 {month}월" if monthly_page_id else "실패"), flush=True)
    except Exception as e:
        print("notion_error:", e, flush=True)


# 파일 맨 밑에 추가
@app.get("/ping")
async def health_check():
    return {"status": "I'm alive!"}
