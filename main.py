import os
import re
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI
from dotenv import load_dotenv
from openai import OpenAI

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

def parse_sms(text):
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    
    # 1. 케이뱅크 판별 및 파싱
    if "[케이뱅크]" in text:
        card = "케이뱅크"
        # '출금 100원'에서 숫자만 추출
        amount_match = re.search(r"출금\s*([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0
        # 마지막 줄이 가맹점
        merchant = lines[-1].split("_")[0]

    # 2. 하나은행 판별 및 파싱
    elif "하나," in text:
        card = "하나카드"
        # '출금100원'에서 숫자만 추출
        amount_match = re.search(r"출금\s*([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0
        # 하나은행은 '출금' 다음 줄(인덱스 4)이 가맹점, 그 다음이 잔액
        # 만약 줄 순서가 바뀌더라도 대응하기 위해 잔액 앞 줄을 가져옴
        merchant = ""
        for i, line in enumerate(lines):
            if "출금" in line:
                merchant = lines[i+1] # 출금 바로 다음 줄이 가맹점
                break
        merchant = merchant.split("_")[0]

    else:
        # 그 외의 경우 (예외 처리)
        return "알 수 없음", 0, "기타카드"

    return merchant, amount, card

# 🧠 카테고리 분류
# 1. 가맹점 매핑 테이블 (상단에 추가)
# "문자상의 이름": {"name": "노션에 표시할 이름", "category": "카테고리"}
MERCHANT_MAP = {
    "(주) 리앤이라마띠네": {"name": "점심식사", "category": "식비"},
    "현대그린푸드": {"name": "구내식당", "category": "식비"},
    "에스씨케이컴퍼니": {"name": "스타벅스", "category": "카페"},
    "네이버파이낸셜": {"name": "네이버페이", "category": "기타"},
    # 필요할 때마다 여기에 추가만 하면 됩니다!
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
반드시 아래 중 하나만 출력: 식비, 카페, 교통, 쇼핑, 구독, 기타

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
    for c in ["식비", "카페", "교통", "쇼핑", "구독", "기타", "취미", "통신"]:
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
def add_data(body: dict):
    text = body.get("text")
    if not text:
        return {"status": "no_text"}

    try:
        merchant, amount, card = parse_sms(text)
    except Exception as e:
        return {"status": "parse_error", "detail": str(e)}

    # --- 여기서 매핑 정보를 적용합니다 ---
    # 매핑 테이블에 있으면 이름을 바꾸고, 없으면 원래 이름을 씁니다.
    mapping = MERCHANT_MAP.get(merchant)
    display_name = mapping["name"] if mapping else merchant
    category = clean_category(classify_category(merchant))
    # -----------------------------------

    amount = -abs(amount)
    spending_type = detect_spending_type(card)

    category_id = get_relation_id(CATEGORY_DB_ID, category)
    payment_id = get_relation_id(PAYMENT_DB_ID, card)
    spending_id = get_relation_id(SPENDING_DB_ID, spending_type)
    today_page_id = get_today_page()

    data = {
        "parent": {"database_id": CONSUME_DB_ID},
        "properties": {
            "내역": {"title": [{"text": {"content": display_name}}]}, # 변환된 이름 사용
            "금액 (기입용)": {"number": amount},
            "카테고리": {"relation": [{"id": category_id}] if category_id else []},
            "결제수단": {"relation": [{"id": payment_id}] if payment_id else []},
            "지출유형": {"relation": [{"id": spending_id}] if spending_id else []},
            "날짜": {"date": {"start": datetime.now().isoformat()}},
            "영수증": {"relation": [{"id": today_page_id}] if today_page_id else []}
        }
    }
    res = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json=data
    )

    print("STATUS:", res.status_code)
    print("RESPONSE:", res.text)

    return {
        "status": "ok",
        "merchant": merchant,
        "amount": amount,
        "card": card,
        "category": category,
        "spending_type": spending_type
    }
    
# 파일 맨 밑에 추가
@app.get("/")
async def health_check():
    return {"status": "I'm alive!"}