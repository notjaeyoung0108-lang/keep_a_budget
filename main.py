import os
import re
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
DAILY_DB_ID = os.getenv("DAILY_DB_ID")
PAYMENT_DB_ID = os.getenv("PAYMENT_DB_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# 🧠 GPT 카테고리 분류
def classify_category(text):
    prompt = f"""
다음 소비 내역을 카테고리로 분류해.
카테고리는 아래 중 하나만 선택:
식비, 카페, 교통, 쇼핑, 구독, 기타

문자:
{text}
"""
    res = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    return res.choices[0].message.content.strip()

# 💰 금액 추출 → 무조건 음수
def extract_amount(text):
    nums = re.findall(r'\d[\d,]*', text)
    if not nums:
        return 0
    amount = int(nums[0].replace(",", ""))
    return -abs(amount)

# 💳 카드 감지
def detect_card(text):
    if "케이뱅크" in text:
        return "케이뱅크"
    elif "하나" in text:
        return "하나카드"
    return "기타"

# 📂 카테고리 DB에서 ID 찾기
def get_category_id(category_name):
    url = f"https://api.notion.com/v1/databases/{CATEGORY_DB_ID}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data["results"]:
        title = page["properties"]["이름"]["title"][0]["text"]["content"]
        if title == category_name:
            return page["id"]
    return None

# 💳 결제수단 DB에서 ID 찾기
def get_payment_id(card_name):
    url = f"https://api.notion.com/v1/databases/{PAYMENT_DB_ID}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data["results"]:
        title = page["properties"]["이름"]["title"][0]["text"]["content"]
        if title == card_name:
            return page["id"]
    return None

# 🧾 오늘 페이지 찾기
def get_today_page():
    today = datetime.now().strftime("%Y-%m-%d")

    url = f"https://api.notion.com/v1/databases/{DAILY_DB_ID}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data["results"]:
        title = page["properties"]["이름"]["title"][0]["text"]["content"]
        if today in title:
            return page["id"]
    return None

# 🚫 중복 체크
def is_duplicate(text):
    url = f"https://api.notion.com/v1/databases/{CONSUME_DB_ID}/query"
    res = requests.post(url, headers=headers)
    data = res.json()

    for page in data["results"]:
        stored = page["properties"]["원문"]["rich_text"]
        if stored and text == stored[0]["text"]["content"]:
            return True
    return False

# 🚀 메인 API
@app.post("/add")
def add_data(body: dict):
    text = body.get("text")

    if not text:
        return {"status": "no_text"}

    if is_duplicate(text):
        return {"status": "duplicate"}

    amount = extract_amount(text)
    category_name = classify_category(text)
    card = detect_card(text)

    category_id = get_category_id(category_name)
    payment_id = get_payment_id(card)
    today_page_id = get_today_page()

    data = {
        "parent": {"database_id": CONSUME_DB_ID},
        "properties": {
            "이름": {
                "title": [{"text": {"content": text[:20]}}]
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
            "날짜": {
                "date": {"start": datetime.now().isoformat()}
            },
            "원문": {
                "rich_text": [{"text": {"content": text}}]
            },
            "오늘": {
                "relation": [{"id": today_page_id}] if today_page_id else []
            }
        }
    }

    requests.post("https://api.notion.com/v1/pages", headers=headers, json=data)

    return {
        "status": "ok",
        "amount": amount,
        "category": category_name,
        "card": card
    }