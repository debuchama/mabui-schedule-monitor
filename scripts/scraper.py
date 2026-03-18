"""
scraper.py — mabuispa.com コアスクレイパー
"""
import re, time, logging, unicodedata, json
from datetime import date, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL     = "https://mabuispa.com"
SCHEDULE_URL = f"{BASE_URL}/schedule"
ROOM_MAP     = {"room8": "赤羽ルーム", "room9": "蕨ルーム"}
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ja,en-US;q=0.9",
}
SLEEP_BETWEEN = 2.0

def _make_client():
    client = httpx.Client(follow_redirects=True, headers=REQUEST_HEADERS, timeout=30)
    client.get(SCHEDULE_URL)
    return client

def _normalize_time(t):
    if not t: return t
    m = re.match(r'^(\d{1,2}):(\d{2})$', t.strip())
    if not m: return t
    h, mn = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 5: h += 24
    return f"{h:02d}:{mn:02d}"

def _parse_stats(span_text):
    span_text = unicodedata.normalize('NFKC', span_text)
    height = cup = None
    hm = re.search(r'T\.?(\d{2,3})', span_text)
    if hm: height = int(hm.group(1))
    cm = re.search(r'([A-Za-z])カップ', span_text)
    if cm: cup = cm.group(1).upper()
    return height, cup

def _parse_items(html, schedule_date):
    soup = BeautifulSoup(html, 'html.parser')
    sc   = soup.find('div', class_='scheduleContent')
    if not sc:
        logger.warning(f"scheduleContent not found for {schedule_date}")
        return []

    results = []
    for item in sc.find_all('div', class_='item'):
        link = item.find('a', href=re.compile(r'/therapist/\d+'))
        if not link: continue
        tid_m = re.search(r'/therapist/(\d+)', link['href'])
        if not tid_m: continue
        therapist_id = int(tid_m.group(1))

        name_tag = item.find('h3', class_='itemName')
        name = age = height = cup = None
        if name_tag:
            raw = name_tag.get_text(' ', strip=True)
            nm  = re.match(r'(.+?)\s*\((\d+)歳\)', raw)
            if nm:
                name = nm.group(1).strip()
                age  = int(nm.group(2))
            else:
                name = raw.split('(')[0].strip()
            span = name_tag.find('span')
            if span: height, cup = _parse_stats(span.get_text(strip=True))

        # キャッチコピータグ (mark > label)
        tags = [lbl.get_text(strip=True)
                for lbl in item.find_all('label')
                if lbl.get_text(strip=True)]

        room_span = item.find('span', class_=re.compile(r'scheduleTypeRoom'))
        location  = ''
        if room_span:
            for cls in room_span.get('class', []):
                if cls in ROOM_MAP:
                    location = ROOM_MAP[cls]; break
            if not location:
                location = room_span.get_text(strip=True)

        is_fully_booked = False
        start_time = end_time = None
        p_tag = item.find('p')
        if p_tag:
            p_text = p_tag.get_text(' ', strip=True)
            if '予約満了' in p_text:
                is_fully_booked = True
            else:
                tm = re.search(r'(\d{1,2}:\d{2})\s*[~～〜\-]\s*(\d{1,2}:\d{2})', p_text)
                if tm:
                    start_time = _normalize_time(tm.group(1))
                    end_time   = _normalize_time(tm.group(2))

        results.append({
            'therapist_id'   : therapist_id,
            'name'           : name,
            'age'            : age,
            'height_cm'      : height,
            'cup_size'       : cup,
            'tags'           : json.dumps(tags, ensure_ascii=False),
            'location'       : location,
            'start_time'     : start_time,
            'end_time'       : end_time,
            'is_fully_booked': is_fully_booked,
            'schedule_date'  : schedule_date,
        })
    return results

def scrape_day(target_date, client=None):
    own = client is None
    if own: client = _make_client()
    try:
        url  = f"{SCHEDULE_URL}?day={target_date}"
        resp = client.get(url, headers={"Referer": SCHEDULE_URL})
        resp.raise_for_status()
        return _parse_items(resp.text, target_date)
    except Exception as e:
        logger.error(f"scrape_day({target_date}) failed: {e}")
        return []
    finally:
        if own: client.close()

def scrape_week(start_date=None):
    if start_date is None:
        start = date.today()
    else:
        start = date.fromisoformat(start_date)
    client  = _make_client()
    results = {}
    try:
        for i in range(7):
            d  = start + timedelta(days=i)
            ds = d.isoformat()
            records = scrape_day(ds, client=client)
            results[ds] = records
            logger.info(f"scrape_week: {ds} → {len(records)} records")
            if i < 6: time.sleep(SLEEP_BETWEEN)
    finally:
        client.close()
    return results

def scrape_today():
    return scrape_day(date.today().isoformat())

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    records = scrape_today()
    print(f"Today: {len(records)} records")
    for r in records:
        booked = "【満了】" if r['is_fully_booked'] else f"{r['start_time']}〜{r['end_time']}"
        tags   = json.loads(r['tags'])
        print(f"  {r['therapist_id']:>5} {r['name']:6} {r['location']:8} {booked}  tags={tags[:2]}")
