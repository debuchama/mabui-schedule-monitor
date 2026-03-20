"""
scrape_info.py — INFOページ & スタッフ個別ページ収集

① INFOページ (mabuispa.com/) から当日スケジュールを構造化
  - 残枠数、事前予約完売フラグを info_schedule テーブルに保存
  
② 今日出勤の全スタッフの個別ページから週間スケジュールを取得
  - weekly_schedule テーブルに保存（スケジュールページの欠陥を補完）
"""
import sys, os, re, time, logging, json
from datetime import datetime, timezone, timedelta, date

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper  import _make_client, _normalize_time, ROOM_MAP

JST    = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)

def now_jst():
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')

def today_jst():
    return datetime.now(JST).date().isoformat()

# ── INFOページ ─────────────────────────────────────────────────────────────

def _parse_info_page(html: str, scraped_at: str) -> list[dict]:
    """
    itemNoticesCont から当日スケジュールを抽出。
    返すキー: schedule_date, name_raw, location, start_time, end_time,
              note, is_soldout, remaining
    """
    soup = BeautifulSoup(html, 'html.parser')
    cont = soup.find('div', class_='itemNoticesCont')
    if not cont:
        logger.warning("itemNoticesCont not found")
        return []

    text_lines = [l.strip() for l in cont.get_text(separator='\n').splitlines() if l.strip()]

    date_pattern  = re.compile(r'^(\d+)/(\d+)[\(（]([月火水木金土日])[\)）]')
    # 🌸 or 💮 or similar emoji before name
    staff_pattern = re.compile(
        r'[\U0001F300-\U0001FFFF\U00002600-\U000027BF]?\s*'  # emoji (optional)
        r'(\S{1,6}?)\s+'                                       # name
        r'(\d{1,2}:\d{2})[-〜～](?:翌)?(\d{1,2}:\d{2})'     # time
        r'\s*【([^】]+)】'                                     # room
        r'\s*(.*)'                                             # note
    )

    current_date = None
    results      = []
    pending_name = None  # 直前のスタッフ名（次行に※が来るパターン用）

    for line in text_lines:
        # 日付行
        dm = date_pattern.match(line)
        if dm:
            m_month, m_day = int(dm.group(1)), int(dm.group(2))
            year = datetime.now(JST).year
            current_date = f'{year}-{m_month:02d}-{m_day:02d}'
            pending_name = None
            continue

        if current_date is None:
            continue

        # スタッフ行
        sm = staff_pattern.match(line)
        if sm:
            name_raw, start, end, room_raw, note = sm.groups()
            note = note.strip()

            # ルーム名正規化
            location = '赤羽ルーム' if '赤羽' in room_raw else '蕨ルーム' if '蕨' in room_raw else room_raw

            # 時刻正規化
            start_n = _normalize_time(start)
            end_n   = _normalize_time(end)

            # 残枠・満了フラグをnote文字列から解析
            is_soldout = 1 if re.search(r'事前予約完売|完売', note) else 0
            rem_m      = re.search(r'残(\d+)枠', note)
            remaining  = int(rem_m.group(1)) if rem_m else (0 if is_soldout else None)

            entry = {
                'scraped_at'   : scraped_at,
                'schedule_date': current_date,
                'name_raw'     : name_raw,
                'location'     : location,
                'start_time'   : start_n,
                'end_time'     : end_n,
                'note'         : note,
                'is_soldout'   : is_soldout,
                'remaining'    : remaining,
            }
            results.append(entry)
            pending_name = name_raw
            continue

        # ※メモが次行に来るパターン (「🌸水原...」の次行に「※事前予約完売」)
        if pending_name and line.startswith('※'):
            for entry in reversed(results):
                if entry['name_raw'] == pending_name and entry['schedule_date'] == current_date:
                    combined = (entry['note'] + ' ' + line).strip()
                    entry['note'] = combined
                    if re.search(r'事前予約完売|完売', line):
                        entry['is_soldout'] = 1
                        entry['remaining']  = 0
                    rm = re.search(r'残(\d+)枠', line)
                    if rm:
                        entry['remaining'] = int(rm.group(1))
                    break

    return results


def scrape_info(client=None) -> list[dict]:
    own = client is None
    if own:
        client = _make_client()
    try:
        r = client.get('https://mabuispa.com/')
        r.raise_for_status()
        return _parse_info_page(r.text, now_jst())
    except Exception as e:
        logger.error(f"scrape_info failed: {e}")
        return []
    finally:
        if own:
            client.close()


# ── スタッフ個別ページ週間スケジュール ───────────────────────────────────────

def _parse_weekly_schedule(html: str, therapist_id: int, scraped_at: str) -> list[dict]:
    """
    /therapist/{id} ページの timeTable から週間スケジュールを抽出。
    返すキー: therapist_id, schedule_date, location, start_time, end_time, status
    """
    soup  = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', class_='timeTable')
    if not table:
        return []

    # ヘッダーから日付を抽出  "3/20(金)" → "2026-03-20"
    year   = datetime.now(JST).year
    headers = []
    for th in table.find_all('th'):
        text = th.get_text(strip=True)
        m    = re.match(r'(\d+)/(\d+)', text)
        if m:
            headers.append(f'{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}')
        else:
            headers.append(None)

    results = []
    for i, td in enumerate(table.find_all('td')):
        if i >= len(headers) or headers[i] is None:
            continue
        sched_date = headers[i]

        room_span = td.find('span', class_=re.compile('rooms'))
        room_raw  = room_span.get_text(strip=True) if room_span else ''
        location  = '赤羽ルーム' if '赤羽' in room_raw else '蕨ルーム' if '蕨' in room_raw else ''
        cell_text = re.sub(r'\s+', ' ', td.get_text(separator=' ', strip=True))

        if 'お休み' in cell_text:
            status = 'off'
            start = end = None
        elif '予約満了' in cell_text:
            status = 'fully_booked'
            start = end = None
        else:
            tm = re.search(r'(\d{1,2}:\d{2})[^\d]+(\d{1,2}:\d{2})', cell_text)
            if tm:
                status = 'working'
                start  = _normalize_time(tm.group(1))
                end    = _normalize_time(tm.group(2))
            else:
                continue  # 不明なセルはスキップ

        results.append({
            'scraped_at'   : scraped_at,
            'therapist_id' : therapist_id,
            'schedule_date': sched_date,
            'location'     : location,
            'start_time'   : start,
            'end_time'     : end,
            'status'       : status,
        })
    return results


def scrape_all_weekly(therapist_ids: list[int], client=None) -> list[dict]:
    """全スタッフの週間スケジュールを取得"""
    own = client is None
    if own:
        client = _make_client()
    at      = now_jst()
    results = []
    try:
        for i, tid in enumerate(therapist_ids):
            try:
                r = client.get(f'https://mabuispa.com/therapist/{tid}')
                r.raise_for_status()
                records = _parse_weekly_schedule(r.text, tid, at)
                results.extend(records)
                logger.debug(f"  therapist/{tid}: {len(records)} days")
            except Exception as e:
                logger.warning(f"  therapist/{tid} failed: {e}")
            if i < len(therapist_ids) - 1:
                time.sleep(1.0)
    finally:
        if own:
            client.close()
    return results


# ── DB 保存 ────────────────────────────────────────────────────────────────

def save_info_schedule(conn, records: list[dict]):
    cur = conn.cursor()
    # まず therapist_id を名前で逆引き
    name_to_id = dict(cur.execute("SELECT name, therapist_id FROM therapists").fetchall())
    for rec in records:
        tid = name_to_id.get(rec['name_raw'])
        cur.execute("""
            INSERT INTO info_schedule
                (scraped_at,schedule_date,therapist_id,name_raw,location,
                 start_time,end_time,note,is_soldout,remaining)
            VALUES(:scraped_at,:schedule_date,:therapist_id,:name_raw,:location,
                   :start_time,:end_time,:note,:is_soldout,:remaining)
            ON CONFLICT(schedule_date,name_raw) DO UPDATE SET
                scraped_at=excluded.scraped_at, therapist_id=COALESCE(excluded.therapist_id,therapist_id),
                location=excluded.location, start_time=excluded.start_time, end_time=excluded.end_time,
                note=excluded.note, is_soldout=excluded.is_soldout, remaining=excluded.remaining
        """, {**rec, 'therapist_id': tid})
    conn.commit()


def save_weekly_schedule(conn, records: list[dict]):
    cur = conn.cursor()
    for rec in records:
        cur.execute("""
            INSERT INTO weekly_schedule
                (scraped_at,therapist_id,schedule_date,location,start_time,end_time,status)
            VALUES(:scraped_at,:therapist_id,:schedule_date,:location,:start_time,:end_time,:status)
            ON CONFLICT(therapist_id,schedule_date) DO UPDATE SET
                scraped_at=excluded.scraped_at, location=excluded.location,
                start_time=excluded.start_time, end_time=excluded.end_time, status=excluded.status
        """, rec)
    conn.commit()


# ── メインエントリ ─────────────────────────────────────────────────────────

def run(db_path=DB_PATH):
    init_db(db_path)
    conn = get_connection(db_path)

    client = _make_client()
    try:
        # 1. INFOページ
        logger.info("scrape_info: INFO page")
        info_records = scrape_info(client)
        save_info_schedule(conn, info_records)
        logger.info(f"  info_schedule: {len(info_records)} records saved")

        # 2. 今日出勤中のスタッフIDを daily_schedules から取得
        today = today_jst()
        today_ids = [r[0] for r in conn.execute(
            "SELECT DISTINCT therapist_id FROM daily_schedules WHERE schedule_date=?", (today,)
        ).fetchall()]
        # さらに全セラピストも対象に（INFO記載の未登録スタッフ対応）
        all_known_ids = [r[0] for r in conn.execute(
            "SELECT therapist_id FROM therapists WHERE is_active=1"
        ).fetchall()]
        target_ids = sorted(set(today_ids + all_known_ids))

        logger.info(f"scrape_info: weekly schedules for {len(target_ids)} therapists")
        weekly_records = scrape_all_weekly(target_ids, client)
        save_weekly_schedule(conn, weekly_records)
        logger.info(f"  weekly_schedule: {len(weekly_records)} records saved")

    finally:
        client.close()
        conn.close()

    return {'info': len(info_records), 'weekly': len(weekly_records)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    result = run()
    print(f"Done: {result}")
