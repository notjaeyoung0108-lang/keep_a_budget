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
def parse_sms(text):
    lines = text.split("\n")

    # 카드
    card = lines[1].replace("[", "").replace("]", "").strip()

    # 금액
    amount_line = lines[3]
    amount = re.sub(r"[^\d]", "", amount_line)

    # 가맹점
    merchant_line = lines[-1]
    merchant = merchant_line.split("_")[0]

    return merchant, int(amount), card

# 🧠 카테고리 분류
def classify_category(merchant):
    prompt = f"""
다음 가맹점을 소비 카테고리로 분류해.

반드시 아래 중 하나만 출력:
식비, 카페, 교통, 쇼핑, 구독, 기타

가맹점:
{merchant}
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
    for c in ["식비", "카페", "교통", "쇼핑", "구독", "기타"]:
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

    amount = -abs(amount)

    category = clean_category(classify_category(merchant))
    spending_type = detect_spending_type(card)

    category_id = get_relation_id(CATEGORY_DB_ID, category)
    payment_id = get_relation_id(PAYMENT_DB_ID, card)
    spending_id = get_relation_id(SPENDING_DB_ID, spending_type)
    today_page_id = get_today_page()

    data = {
        "parent": {"database_id": CONSUME_DB_ID},
        "properties": {
            "내역": {"title": [{"text": {"content": merchant}}]},
            "금액": {"number": amount},
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