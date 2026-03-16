"""
scraper.py — mabuispa.com コアスクレイパー
対象: https://mabuispa.com/schedule?day=YYYY-MM-DD
CMS: 独自 Rails 製（SSR）
店舗: 赤羽ルーム (room8) / 蕨ルーム (room9)

【JewelrySpa との主な差異】
- URL パラメータ: ?day=YYYY-MM-DD（&from 不要）
- 日別 Ajax API (/today_plus_schedule/{date}) は常に 204 を返すため使用不可
  → メインページから直接 scheduleContent をパース
- セッション Cookie (_three_m_session) が必要
- 予約満了時は時刻情報なし（「予約満了」テキストのみ）
- 店舗識別: room8=赤羽ルーム / room9=蕨ルーム（クラス名で判定）
- 身長・カップは h3.itemName > span に含まれる
"""

import re
import time
import logging
from datetime import date, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL      = "https://mabuispa.com"
SCHEDULE_URL  = f"{BASE_URL}/schedule"
THERAPIST_URL = f"{BASE_URL}/therapist"

# room クラス → 店舗名マッピング
ROOM_MAP = {
    "room8": "赤羽ルーム",
    "room9": "蕨ルーム",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9",
}

SLEEP_BETWEEN_REQUESTS = 2.0   # サーバー負荷軽減


# ─────────────────────────────────────────────────────────────────────────────
# セッション管理
# ─────────────────────────────────────────────────────────────────────────────

def _make_client() -> httpx.Client:
    """Cookie つきの永続セッションを返す。最初の GET でセッション確立。"""
    client = httpx.Client(
        follow_redirects=True,
        headers=REQUEST_HEADERS,
        timeout=30,
    )
    # セッション Cookie (_three_m_session) を取得
    client.get(SCHEDULE_URL)
    return client


# ─────────────────────────────────────────────────────────────────────────────
# 時刻正規化: 深夜帯を 25:00〜29:00 に正規化
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_time(t: str) -> str:
    """
    'HH:MM' を受け取り、深夜帯（0:00〜5:00）を 24:00〜29:00 に変換して返す。
    ソート・集計が日付を跨がずに行えるようになる。
    """
    if not t:
        return t
    m = re.match(r'^(\d{1,2}):(\d{2})$', t.strip())
    if not m:
        return t
    h, mn = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 5:
        h += 24
    return f"{h:02d}:{mn:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# HTML パーサー
# ─────────────────────────────────────────────────────────────────────────────

def _parse_stats(span_text: str) -> tuple[Optional[int], Optional[str]]:
    """
    'T.150 Cカップ' → (150, 'C')
    'T.174 Gカップ' → (174, 'G')
    'T.153 Ⅽカップ' (U+216D) → (153, 'C')  ← NFKC正規化で対応
    """
    import unicodedata
    span_text = unicodedata.normalize('NFKC', span_text)
    height = cup = None
    hm = re.search(r'T\.?(\d{2,3})', span_text)
    if hm:
        height = int(hm.group(1))
    cm = re.search(r'([A-Za-z])カップ', span_text)
    if cm:
        cup = cm.group(1).upper()
    return height, cup


def _parse_items(html: str, schedule_date: str) -> list[dict]:
    """
    scheduleContent div 内の .item を全てパースして辞書のリストで返す。

    返却キー:
        therapist_id, name, age, height_cm, cup_size,
        location, start_time, end_time, is_fully_booked, schedule_date
    """
    soup = BeautifulSoup(html, 'html.parser')
    sc   = soup.find('div', class_='scheduleContent')
    if not sc:
        logger.warning(f"scheduleContent not found for {schedule_date}")
        return []

    results = []
    for item in sc.find_all('div', class_='item'):
        # ── therapist_id ──────────────────────────────────────────────────
        link = item.find('a', href=re.compile(r'/therapist/\d+'))
        if not link:
            continue
        tid_m = re.search(r'/therapist/(\d+)', link['href'])
        if not tid_m:
            continue
        therapist_id = int(tid_m.group(1))

        # ── 名前・年齢・身長・カップ ──────────────────────────────────────
        name_tag = item.find('h3', class_='itemName')
        name     = ''
        age      = None
        height   = None
        cup      = None
        if name_tag:
            # テキスト全体: '水原 (38歳) \nT.174 Gカップ'
            raw_full = name_tag.get_text(' ', strip=True)
            nm = re.match(r'(.+?)\s*\((\d+)歳\)', raw_full)
            if nm:
                name = nm.group(1).strip()
                age  = int(nm.group(2))
            else:
                name = raw_full.split('(')[0].strip()

            span = name_tag.find('span')
            if span:
                height, cup = _parse_stats(span.get_text(strip=True))

        # ── 店舗 (room クラスから判定) ────────────────────────────────────
        room_span = item.find('span', class_=re.compile(r'scheduleTypeRoom'))
        location  = ''
        if room_span:
            for cls in room_span.get('class', []):
                if cls in ROOM_MAP:
                    location = ROOM_MAP[cls]
                    break
            if not location:
                location = room_span.get_text(strip=True)  # fallback

        # ── 予約満了判定・時刻 ────────────────────────────────────────────
        is_fully_booked = False
        start_time      = None
        end_time        = None

        p_tag = item.find('p')
        if p_tag:
            p_text = p_tag.get_text(' ', strip=True)
            if '予約満了' in p_text:
                is_fully_booked = True
            else:
                tm = re.search(
                    r'(\d{1,2}:\d{2})\s*[~～〜\-]\s*(\d{1,2}:\d{2})', p_text
                )
                if tm:
                    start_time = _normalize_time(tm.group(1))
                    end_time   = _normalize_time(tm.group(2))

        results.append({
            'therapist_id'   : therapist_id,
            'name'           : name,
            'age'            : age,
            'height_cm'      : height,
            'cup_size'       : cup,
            'location'       : location,
            'start_time'     : start_time,
            'end_time'       : end_time,
            'is_fully_booked': is_fully_booked,
            'schedule_date'  : schedule_date,
        })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 公開 API
# ─────────────────────────────────────────────────────────────────────────────

def scrape_day(
    target_date: str,
    client: Optional[httpx.Client] = None,
) -> list[dict]:
    """
    1日分のスケジュールをスクレイプして返す。
    target_date: 'YYYY-MM-DD'
    """
    own_client = client is None
    if own_client:
        client = _make_client()
    try:
        url  = f"{SCHEDULE_URL}?day={target_date}"
        resp = client.get(url, headers={"Referer": SCHEDULE_URL})
        resp.raise_for_status()
        return _parse_items(resp.text, target_date)
    except Exception as e:
        logger.error(f"scrape_day({target_date}) failed: {e}")
        return []
    finally:
        if own_client:
            client.close()


def scrape_week(
    start_date: Optional[str] = None,
) -> dict[str, list[dict]]:
    """
    7日分を一括スクレイプして {date_str: [records]} の辞書で返す。
    start_date が None なら今日から。
    """
    if start_date is None:
        start = date.today()
    else:
        start = date.fromisoformat(start_date)

    client  = _make_client()
    results = {}
    try:
        for i in range(7):
            d       = start + timedelta(days=i)
            ds      = d.isoformat()
            records = scrape_day(ds, client=client)
            results[ds] = records
            logger.info(f"scrape_week: {ds} → {len(records)} records")
            if i < 6:
                time.sleep(SLEEP_BETWEEN_REQUESTS)
    finally:
        client.close()
    return results


def scrape_today() -> list[dict]:
    """当日分のみスクレイプ。"""
    return scrape_day(date.today().isoformat())


# ─────────────────────────────────────────────────────────────────────────────
# 単体テスト
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    records = scrape_today()
    print(f"Today: {len(records)} records")
    for r in records:
        booked = "【満了】" if r['is_fully_booked'] else f"{r['start_time']}〜{r['end_time']}"
        print(f"  {r['therapist_id']:>5} {r['name']:6} {r['location']:8} {booked}")
