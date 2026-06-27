import os
import re
import json
import time
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

# 📲 디스코드 웹훅 URL (.env 에서 읽음)
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

client = OpenAI(api_key=OPENAI_API_KEY)

headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# 🔥 문자 파싱 함수
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
                if i > 0:
                    merchant = lines[i-1]  # 출금 윗줄이 가맹점
                if i + 1 < len(lines):
                    amount_str = re.sub(r"[^\d]", "", lines[i+1])
                    amount = int(amount_str) if amount_str else 0
                break

    # 현대카드 판별
    elif "현대카드" in text:
        card = "현대카드"
        amount_match = re.search(r"([\d,]+)원", text)
        amount = int(re.sub(r"[^\d]", "", amount_match.group(1))) if amount_match else 0

        for i, line in enumerate(lines):
            if re.match(r"\d{2}/\d{2}\s+\d{2}:\d{2}", line):  # "04/01 08:48" 형태
                if i + 1 < len(lines):
                    merchant = lines[i+1]
                break

    return merchant, amount, card


def match_merchant(merchant):
    """
    가맹점 이름에 MERCHANT_MAP의 키워드가 포함되어 있으면 매칭
    예: "스타벅스강남점" → "스타벅스" 매칭
    "GS25 역삼점" → "GS25" 매칭
    """
    merchant_lower = merchant.lower()

    for key in MERCHANT_MAP:
        key_lower = key.lower()
        if key_lower in merchant_lower:
            print(f"✅ 매칭 성공: '{merchant}' → '{MERCHANT_MAP[key]['name']}' (키워드: '{key}')", flush=True)
            return MERCHANT_MAP[key]

    print(f"❌ 매칭 실패: '{merchant}' (GPT 사용)", flush=True)
    return None


# 카테고리 목록 캐시 (Notion 카테고리 DB에서 자동 조회)
_cached_categories = None
FALLBACK_CATEGORIES = ["식비", "카페", "교통", "쇼핑", "구독", "여가", "통신", "기타"]


def get_category_names():
    """Notion 카테고리 DB의 실제 카테고리 이름 목록을 가져온다. 실패 시 기본값."""
    global _cached_categories
    if _cached_categories:
        return _cached_categories

    try:
        url = f"https://api.notion.com/v1/databases/{CATEGORY_DB_ID}/query"
        res = requests.post(url, headers=headers)
        names = [
            get_title(page["properties"].get("이름"))
            for page in res.json().get("results", [])
        ]
        names = [n for n in names if n]
        if names:
            _cached_categories = names
            print(f"📂 카테고리 목록 로드: {names}", flush=True)
            return names
    except Exception as e:
        print(f"❌ 카테고리 조회 실패(기본값 사용): {e}", flush=True)

    return FALLBACK_CATEGORIES


GPT_SYSTEM_PROMPT = """너는 한국 카드 결제 가맹점 이름을 정리하는 분류기다.
입력으로 카드사가 보내는 가맹점 원문(지점명·법인명·약어 섞임)이 들어온다.

해야 할 일:
1) name: 사람이 알아보는 대표 상호명 하나로 정규화.
   - 지점/번호/괄호 제거: "스타벅스강남R점" → "스타벅스", "GS25 역삼점" → "GS25"
   - 법인명은 브랜드로 환원: "에스씨케이컴퍼니" → "스타벅스", "위대한상상" → "요기요"
2) category: 반드시 주어진 보기 중 하나만 선택. 애매하면 가장 가까운 것, 정말 모르면 "기타".

분류 기준:
- 식비: 식당·배달·편의점 음식·베이커리
- 카페: 커피·디저트 전문점
- 교통: 택시·버스·지하철·주유·통행료·주차
- 쇼핑: 마트·온라인쇼핑·의류·생활용품·화장품
- 구독: 통신요금·멤버십·정기결제(넷플릭스 등)
- 여가: 영화·공연·게임·여행·취미
"""


def gpt_extract(merchant):
    categories = get_category_names()
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "대표 상호명 하나 (지점/법인명 정규화)"},
            "category": {"type": "string", "enum": categories},
        },
        "required": ["name", "category"],
        "additionalProperties": False,
    }

    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": GPT_SYSTEM_PROMPT},
                {"role": "user", "content": f"가맹점: {merchant}\n선택 가능한 카테고리: {categories}"},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "merchant_classification",
                    "strict": True,
                    "schema": schema,
                },
            },
        )
        data = json.loads(res.choices[0].message.content)
        return data.get("name") or merchant, data.get("category") or "기타"
    except Exception as e:
        print(f"GPT 오류: {e}", flush=True)
        return merchant, "기타"


# 🧠 가맹점 매핑 테이블
MERCHANT_MAP = {
    "LG유플러스": {"name": "통신요금", "category": "구독"},
    "네이버멤버십": {"name": "네이버멤버십", "category": "구독"},
    "와우멤버십": {"name": "쿠팡(와우멤버십)", "category": "구독"},
    "위대한상상": {"name": "요기요", "category": "식비"},
    "롯데컬처웍스": {"name": "롯데시네마", "category": "여가"},
    "뚜레쥬르": {"name": "뚜레쥬르", "category": "식비"},
    "올리브영": {"name": "올리브영", "category": "쇼핑"},
    "CU": {"name": "CU", "category": "기타"},
    "씨유": {"name": "CU", "category": "기타"},
    "지에스25": {"name": "GS25", "category": "기타"},
    "gs25": {"name": "GS25", "category": "기타"},
    "스타벅스": {"name": "스타벅스", "category": "카페"},
    "쿠팡": {"name": "쿠팡", "category": "쇼핑"},
    "리앤이라마띠네": {"name": "구내식당", "category": "식비"},
    "현대그린푸드": {"name": "구내식당", "category": "식비"},
    "에스씨케이컴퍼니": {"name": "스타벅스", "category": "카페"},
    "네이버파이낸셜": {"name": "네이버페이", "category": "기타"},
}


def detect_spending_type(card):
    if "케이뱅크" in card:
        return "데이트자금"
    elif "현대카드" in card:
        return "과소비"
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
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    today_str = now_kst.strftime("%Y-%m-%d")

    url = f"https://api.notion.com/v1/databases/{DAILY_DB_ID}/query"
    query_data = {
        "filter": {
            "property": "날짜",
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
    title = f"{year}년 {month}월"

    url = f"https://api.notion.com/v1/databases/{MONTHLY_DB_ID}/query"
    query = {
        "filter": {
            "and": [
                {
                    "property": "년도",
                    "number": {"equals": year}
                },
                {
                    "property": "월",
                    "number": {"equals": month}
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


def _num_from_str(s):
    """'🍞 -68,070원' 같은 문자열에서 숫자(마이너스 포함)만 뽑아 정수로."""
    if not s:
        return None
    cleaned = re.sub(r"[^\d\-]", "", s)
    if not cleaned or cleaned == "-":
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _extract_number(prop):
    """number / formula / rollup / 텍스트 속성에서 숫자값을 꺼낸다."""
    if not prop:
        return None
    t = prop.get("type")
    if t == "number":
        return prop.get("number")
    if t == "formula":
        f = prop.get("formula", {})
        ft = f.get("type")
        if ft == "number":
            return f.get("number")
        if ft == "string":
            return _num_from_str(f.get("string"))
        return None
    if t == "rollup":
        r = prop.get("rollup", {})
        rt = r.get("type")
        if rt == "number":
            return r.get("number")
        # "원본 표시" 등 array 형태 롤업: 안쪽 항목에서 숫자 추출
        if rt == "array":
            for item in r.get("array", []):
                val = _extract_number(item)
                if val is not None:
                    return val
            return None
    # 텍스트(rich_text) 또는 제목(title) 속성이면 문자열에서 숫자만 추출
    if t in ("rich_text", "title"):
        arr = prop.get(t, [])
        s = "".join(item.get("plain_text", "") for item in arr)
        return _num_from_str(s)
    return None


def get_balance(page_id, prop_name="잔액", verbose=True):
    """해당 페이지에서 '잔액' 속성값을 읽어 반환. 없으면 None."""
    if not page_id:
        return None
    try:
        res = requests.get(f"https://api.notion.com/v1/pages/{page_id}", headers=headers)
        props = res.json().get("properties", {})
        prop = props.get(prop_name)
        value = _extract_number(prop)
        if value is None and verbose:
            # 디버그: 어떤 속성이 있는지 / 잔액 원본이 어떻게 생겼는지
            print(f"🔍 '{prop_name}' 추출 실패. 페이지 속성들: {list(props.keys())}", flush=True)
            print(f"🔍 '{prop_name}' 원본: {json.dumps(prop, ensure_ascii=False)}", flush=True)
        return value
    except Exception as e:
        print(f"❌ 잔액 조회 실패: {e}", flush=True)
        return None


# 📲 디스코드 알림
def send_discord(display_name, time_str, amount, balance_str, balance):
    """결제 기입 완료 시 디스코드 채널로 알림 전송."""
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ 디스코드 웹훅 URL 없음", flush=True)
        return

    # 잔액 음수면 빨강, 아니면 파랑 (임베드 좌측 색상 바)
    color = 0xE74C3C if (balance is not None and balance < 0) else 0x3498DB

    description = (
        f"💸 결제내역 : {time_str} / {display_name} / {abs(amount):,}원\n"
        f"🏦 남은금액 : {balance_str}"
    )

    payload = {"embeds": [{"description": description, "color": color}]}

    try:
        res = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        print(f"📲 디스코드 status: {res.status_code}", flush=True)
        if res.status_code not in (200, 204):
            print(f"📲 디스코드 응답: {res.text}", flush=True)
    except Exception as e:
        print(f"❌ 디스코드 전송 실패: {e}", flush=True)


def notify_entry_done(page_id, display_name, amount, date, monthly_page_id):
    """기입 완료 시 디스코드로 알림을 보낸다."""
    # 시간 (HH시MM분)
    try:
        time_str = datetime.fromisoformat(date).strftime("%H시%M분")
    except Exception:
        time_str = ""

    # 잔액(롤업/수식)은 결제 직후 계산이 안 끝났을 수 있음
    # → 먼저 5초 기다려 노션 계산이 반영되게 한 뒤 읽고, 그래도 없으면 재시도
    time.sleep(10)

    balance = None
    source = None
    for attempt in range(5):
        verbose = (attempt == 4)  # 마지막 시도에만 디버그 로그
        balance = get_balance(page_id, verbose=verbose)
        if balance is not None:
            source = "소비페이지"
        else:
            balance = get_balance(monthly_page_id, verbose=verbose)
            if balance is not None:
                source = "월별명세"
        if balance is not None:
            print(f"💰 잔액 확인(시도 {attempt + 1}회, 출처: {source}): {balance:,}원", flush=True)
            break
        time.sleep(1.5)

    balance_str = f"{balance:,.0f}원" if balance is not None else "조회불가"

    send_discord(display_name, time_str, amount, balance_str, balance)


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

    if not date or date.strip() == "":
        print("⚠️ date 없음 또는 빈값 → 현재시각 사용", flush=True)
        kst = timezone(timedelta(hours=9))
        date = datetime.now(kst).isoformat()
    else:
        print(f"📅 받은 date: {date}", flush=True)
        try:
            kst = timezone(timedelta(hours=9))
            dt = datetime.strptime(date, "%Y-%m-%d %H:%M")
            date = dt.replace(tzinfo=kst).isoformat()
            print(f"✅ ISO 변환: {date}", flush=True)
        except Exception as e:
            print(f"❌ 날짜 변환 실패: {e}", flush=True)
            kst = timezone(timedelta(hours=9))
            date = datetime.now(kst).isoformat()

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

    amount = -abs(amount)
    spending_type = detect_spending_type(card)

    category_id = get_relation_id(CATEGORY_DB_ID, category)
    payment_id = get_relation_id(PAYMENT_DB_ID, card)
    spending_id = get_relation_id(SPENDING_DB_ID, spending_type)
    today_page_id = get_today_page()

    dt = datetime.fromisoformat(date)
    year = dt.year
    month = dt.month

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
            "월별명세": {"relation": [{"id": monthly_page_id}] if monthly_page_id else []}
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

        # 🔔 기입 성공 시 디스코드 알림
        if res.status_code in (200, 201):
            new_page_id = res.json().get("id")
            notify_entry_done(new_page_id, display_name, amount, date, monthly_page_id)
    except Exception as e:
        print("notion_error:", e, flush=True)


@app.get("/ping")
async def health_check():
    return {"status": "I'm alive!"}
