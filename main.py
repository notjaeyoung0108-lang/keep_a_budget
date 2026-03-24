import os
import requests
from datetime import datetime
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

# 🧠 GPT 카테고리 분류
def classify_category(merchant):
    prompt = f"""
다음 가맹점을 소비 카테고리로 분류해.

반드시 아래 단어 중 하나만 출력:
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

# 🧼 결과 정리
def clean_category(text):
    categories = ["식비", "카페", "교통", "쇼핑", "구독", "기타"]
    for c in categories:
        if c in text:
            return c
    return "기타"

# 💳 카드 → 지출유형
def detect_spending_type(card):
    if card == "케이뱅크":
        return "데이트자금"
    elif card == "하나카드":
        return "생활자금"
    return "생활자금"

# 🔐 Notion title 안전 추출
def get_title(prop):
    if not prop or "title" not in prop:
        return ""
    arr = prop["title"]
    if not arr:
        return ""
    return arr[0].get("plain_text", "")

# 📂 공통 relation 조회
def get_relation_id(db_id, name):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data.get("results", []):
        title = get_title(page["properties"].get("이름"))
        if title == name:
            return page["id"]
    return None

# 📅 오늘 페이지
def get_today_page():
    today = datetime.now().strftime("%Y-%m-%d")

    url = f"https://api.notion.com/v1/databases/{DAILY_DB_ID}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data.get("results", []):
        title = get_title(page["properties"].get("이름"))
        if today in title:
            return page["id"]
    return None

# 🚀 메인 API
@app.post("/add")
def add_data(body: dict):

    # ✅ 단축어에서 전달된 값
    merchant = body.get("merchant")   # ex: 스타벅스
    amount = body.get("amount")       # ex: 4500
    card = body.get("card")           # ex: 케이뱅크

    if not merchant or not amount:
        return {"status": "invalid_input"}

    try:
        amount = -abs(int(amount))
    except:
        return {"status": "amount_error"}

    # 🧠 카테고리
    category_raw = classify_category(merchant)
    category_name = clean_category(category_raw)

    # 💳 지출유형
    spending_type = detect_spending_type(card)

    # 🔗 relation ID
    category_id = get_relation_id(CATEGORY_DB_ID, category_name)
    payment_id = get_relation_id(PAYMENT_DB_ID, card)
    spending_id = get_relation_id(SPENDING_DB_ID, spending_type)
    today_page_id = get_today_page()

    # 📦 Notion 데이터
    data = {
        "parent": {"database_id": CONSUME_DB_ID},
        "properties": {
            "내역": {
                "title": [{"text": {"content": merchant}}]
            },
            "금액 (기입란)": {
                "number": amount
            },
            "카테고리": {
                "relation": [{"id": category_id}] if category_id else []
            },
            "결제수단": {
                "relation": [{"id": payment_id}] if payment_id else []
            },
            "지출유형": {
                "relation": [{"id": spending_id}] if spending_id else []
            },
            "날짜": {
                "date": {"start": datetime.now().isoformat()}
            },
            "영수증": {
                "relation": [{"id": today_page_id}] if today_page_id else []
            }
        }
    }

    # 🔥 Notion 요청
    res = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json=data
    )

    print("STATUS:", res.status_code)
    print("RESPONSE:", res.text)

    if res.status_code != 200:
        return {
            "status": "notion_error",
            "detail": res.text
        }

    return {
        "status": "ok",
        "merchant": merchant,
        "amount": amount,
        "category": category_name,
        "card": card,
        "spending_type": spending_type
    }