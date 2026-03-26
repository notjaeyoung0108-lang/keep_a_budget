import os
import re
import requests
from datetime import datetime, timedelta
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

client = OpenAI(api_key=OPENAI_API_KEY)

headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# 🔥 문자 파싱 함수
import re

def parse_sms(text, phone_date=None):
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    merchant, amount, card = "알 수 없음", 0, "기타카드"
    
    # 🕒 [시간 결정 우선순위]
    # 1. 단축어가 보내준 시간(phone_date)을 기본값으로 사용
    # 2. 만약 phone_date가 없으면 서버의 현재 한국 시간 사용
    if phone_date:
        transaction_time = phone_date
    else:
        kst_now = datetime.utcnow() + timedelta(hours=9)
        transaction_time = kst_now.isoformat()

    # --- KB 국민은행 파싱 ---
    if "[KB]" in text:
        card = "국민은행"
        # 문자 내에 구체적인 시간이 있으면 그걸로 덮어씌움 (가장 정확하니까요!)
        time_match = re.search(r"(\d{2}/\d{2})\s(\d{2}:\d{2})", text)
        if time_match:
            try:
                year = datetime.now().year # 실제 운영시는 KST 기준 연도 권장
                date_str = f"{year}/{time_match.group(1)} {time_match.group(2)}"
                dt_obj = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                transaction_time = dt_obj.isoformat()
            except:
                pass

        for i, line in enumerate(lines):
            if "출금" in line:
                if i > 0: merchant = lines[i-1]
                if i + 1 < len(lines):
                    amount_str = re.sub(r"[^\d]", "", lines[i+1])
                    amount = int(amount_str) if amount_str else 0
                break

    # --- 케이뱅크 파싱 ---
    elif "[케이뱅크]" in text:
        card = "케이뱅크"
        # 케이뱅크는 시간이 없으므로 transaction_time(단축어가 보낸 시간)을 그대로 씀
        amount_match = re.search(r"출금\s*([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0
        merchant = lines[-1].split("_")[0]

    # --- 하나카드 파싱 ---
    elif "하나," in text:
        card = "하나카드"
        amount_match = re.search(r"출금\s*([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0
        for i, line in enumerate(lines):
            if "출금" in line:
                merchant = lines[i+1].split("_")[0]
                break

    return merchant, amount, card, transaction_time

def normalize_merchant(name):
    name = name.lower()              
    name = re.sub(r"\(.*?\)", "", name)          # 괄호 제거
    name = re.sub(r"[^a-z0-9가-힣]", "", name)   # 특수문자 제거
    return name


def match_merchant(normalized_name):
    for key in MERCHANT_MAP:
        if key in normalized_name:
            return MERCHANT_MAP[key]
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
    "롯데컬처웍스":{"name":"롯데시네마","category":"여가"},
    "뚜레쥬르":{"name": "뚜레쥬르","category":"식비"},
    "올리브영":{"name": "올리브영","category":"쇼핑"},
    "CU":{"name": "CU 편의점", "category": "기타"},
    "씨유":{"name": "CU 편의점", "category": "기타"},
    "지에스25": {"name": "GS25 편의점", "category": "기타"},
    "gs25": {"name": "GS25 편의점", "category": "기타"},
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
반드시 아래 중 하나만 출력: 식비, 카페, 교통, 쇼핑, 구독, 취미, 통신, 기타

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
    for c in ["식비", "카페", "교통", "쇼핑", "구독", "기타", "여가", "통신"]:
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
    kst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = kst_now.strftime("%Y-%m-%d") # "2026-03-24" 형식
    
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

# 🚀 메인 API
@app.post("/add")
def add_data(body: dict, background_tasks: BackgroundTasks):
    text = body.get("text")
    phone_date = body.get("date") # 🎯 1. 단축어에서 보낸 'date' 추출
    
    if not text:
        return {"status": "no_text"}

    # 👉 작업을 백그라운드로 넘길 때 phone_date도 같이 넘겨줍니다.
    background_tasks.add_task(process_data, text, phone_date)

    # 👉 아이폰에는 "접수 완료!"라고 즉시 대답 (아이폰이 안 기다려도 됨)
    return {"status": "accepted"}

def process_data(text: str, phone_date: str = None):
    try:
        # 🎯 2. parse_sms가 시간까지 반환하도록 수정 (아래 parse_sms 로직 참고)
        merchant, amount, card, transaction_time = parse_sms(text, phone_date)
    except Exception as e:
        print("parse_error:", e)
        return

    # --- (중간 로직: 매핑 및 GPT 분류 부분은 동일) ---
    normalized = normalize_merchant(merchant)
    mapping = match_merchant(normalized)

    if mapping:
        display_name = mapping["name"]
        category = mapping["category"]
    else:
        # GPT 사용 시에도 merchant 정보를 넘김
        display_name, category = gpt_extract(merchant)
        # (옵션) MERCHANT_MAP에 자동 추가 로직 등...

    # --- (중간 로직: ID 조회 부분 동일) ---
    category = clean_category(category)
    amount = -abs(amount)
    spending_type = detect_spending_type(card)

    category_id = get_relation_id(CATEGORY_DB_ID, category)
    payment_id = get_relation_id(PAYMENT_DB_ID, card)
    spending_id = get_relation_id(SPENDING_DB_ID, spending_type)
    today_page_id = get_today_page()

    # 🎯 3. 노션 데이터의 "날짜" 부분에 추출된 시간 사용
    data = {
        "parent": {"database_id": CONSUME_DB_ID},
        "properties": {
            "내역": {"title": [{"text": {"content": display_name}}]},
            "금액 (기입용)": {"number": amount},
            "카테고리": {"relation": [{"id": category_id}] if category_id else []},
            "결제_수단": {"relation": [{"id": payment_id}] if payment_id else []},
            "지출유형": {"relation": [{"id": spending_id}] if spending_id else []},
            "날짜": {"date": {"start": transaction_time}}, # ⬅️ datetime.now() 대신 사용!
            "영수증": {"relation": [{"id": today_page_id}] if today_page_id else []}
        }
    }

    try:
        res = requests.post("https://api.notion.com/v1/pages", headers=headers, json=data)
        print(f"✅ 노션 전송 성공! ({display_name}, {transaction_time})")
    except Exception as e:
        print("notion_error:", e)
