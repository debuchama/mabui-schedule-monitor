"""
daily_monitor.py — 当日空き状況モニター

シフト終了検出:
  前回スクレイプに存在したセラピストが今回のスクレイプ結果から消えた場合、
  そのセラピストのシフトが終了したと判定し 'shift_ended' スナップショットを挿入する。
  これにより daily_schedules の fully_booked レコードがシフト終了後も残り続ける問題を解決する。
"""
import sys, os, time, logging, argparse
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper  import scrape_today

JST    = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)

def now_jst():
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')

def upsert_therapist(cur, rec, scraped_at):
    cur.execute("""
        INSERT INTO therapists(therapist_id,name,age,height_cm,cup_size,tags,first_seen,last_seen,is_active)
        VALUES(:therapist_id,:name,:age,:height_cm,:cup_size,:tags,:scraped_at,:scraped_at,1)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name=COALESCE(name,excluded.name), age=COALESCE(age,excluded.age),
            height_cm=COALESCE(height_cm,excluded.height_cm), cup_size=COALESCE(cup_size,excluded.cup_size),
            tags=COALESCE(excluded.tags,tags), last_seen=excluded.last_seen, is_active=1
    """, {**rec, 'scraped_at': scraped_at})

def get_last_snapshot(cur, therapist_id, schedule_date):
    row = cur.execute("""
        SELECT status FROM availability_snapshots
        WHERE therapist_id=? AND schedule_date=? ORDER BY id DESC LIMIT 1
    """, (therapist_id, schedule_date)).fetchone()
    return dict(row)['status'] if row else None

def get_today_active_ids(cur, schedule_date):
    """今日 shift_ended でないスナップショットが存在するtherapist_id一覧"""
    rows = cur.execute("""
        SELECT DISTINCT therapist_id FROM availability_snapshots
        WHERE schedule_date=? AND status != 'shift_ended'
    """, (schedule_date,)).fetchall()
    return {r[0] for r in rows}

def get_today_db_ids(cur, schedule_date):
    """daily_schedules に今日のレコードがある therapist_id 一覧"""
    rows = cur.execute(
        "SELECT therapist_id FROM daily_schedules WHERE schedule_date=?", (schedule_date,)
    ).fetchall()
    return {r[0] for r in rows}

def get_shift_info(cur, therapist_id, schedule_date):
    """シフト時刻をスナップショット履歴から取得（最初の available から）"""
    row = cur.execute("""
        SELECT start_time, end_time FROM availability_snapshots
        WHERE therapist_id=? AND schedule_date=? AND status='available'
        ORDER BY id ASC LIMIT 1
    """, (therapist_id, schedule_date)).fetchone()
    return dict(row) if row else {'start_time': None, 'end_time': None}

def insert_snapshot(cur, therapist_id, schedule_date, location, status, start_time, end_time, checked_at):
    cur.execute("""
        INSERT INTO availability_snapshots
            (checked_at,therapist_id,schedule_date,location,status,start_time,end_time)
        VALUES(?,?,?,?,?,?,?)
    """, (checked_at, therapist_id, schedule_date, location, status, start_time, end_time))

def get_location(cur, therapist_id, schedule_date):
    row = cur.execute(
        "SELECT location FROM daily_schedules WHERE therapist_id=? AND schedule_date=?",
        (therapist_id, schedule_date)
    ).fetchone()
    return row[0] if row else ''

def run_once(db_path=DB_PATH):
    init_db(db_path)
    conn  = get_connection(db_path)
    cur   = conn.cursor()
    at    = now_jst()
    today = datetime.now(JST).date().isoformat()

    records   = scrape_today()
    scraped_ids = {r['therapist_id'] for r in records}
    changes   = 0

    # ── 現在出勤中のスタッフを処理 ──────────────────────────────────────
    for rec in records:
        upsert_therapist(cur, rec, at)
        tid        = rec['therapist_id']
        cur_status = 'fully_booked' if rec['is_fully_booked'] else 'available'
        last       = get_last_snapshot(cur, tid, today)

        if last is None:
            logger.info(f"  NEW  {rec['name']} ({rec['location']}) → {cur_status}")
            changes += 1
        elif last == 'shift_ended':
            # 一度終了したが再出勤（スケジュール変更等）
            logger.info(f"  REAPPEAR {rec['name']} → {cur_status}")
            changes += 1
        elif last != cur_status:
            logger.info(f"  CHANGE {rec['name']}: {last} → {cur_status}")
            changes += 1

        insert_snapshot(cur, tid, today, rec['location'], cur_status,
                        rec['start_time'], rec['end_time'], at)

    # ── シフト終了検出 ────────────────────────────────────────────────────
    # 今日のDBに登録されていてスナップショットも存在するが、今回のスクレイプにいない人
    active_snapshot_ids = get_today_active_ids(cur, today)
    db_ids              = get_today_db_ids(cur, today)
    disappeared_ids     = (active_snapshot_ids | db_ids) - scraped_ids

    for tid in disappeared_ids:
        last = get_last_snapshot(cur, tid, today)
        if last == 'shift_ended':
            continue  # すでに記録済み

        # シフト時刻をスナップショット履歴から復元
        shift = get_shift_info(cur, tid, today)
        loc   = get_location(cur, tid, today)

        # therapist 名を取得してログ出力
        row = cur.execute("SELECT name FROM therapists WHERE therapist_id=?", (tid,)).fetchone()
        name = row[0] if row else str(tid)
        logger.info(f"  SHIFT_ENDED {name} (ID:{tid}) → shift_ended")

        insert_snapshot(cur, tid, today, loc, 'shift_ended',
                        shift['start_time'], shift['end_time'], at)
        changes += 1

    cur.execute(
        "INSERT INTO scrape_logs(run_at,task_type,target_date,records_found,success) VALUES(?,?,?,?,1)",
        (at, 'daily_monitor', today, len(records)))
    conn.commit()
    conn.close()
    logger.info(f"daily_monitor: scraped={len(records)}, changes={changes}, ended={len(disappeared_ids)}")
    return changes

def run_loop(interval_minutes, db_path=DB_PATH):
    logger.info(f"daily_monitor: loop mode, interval={interval_minutes}min")
    while True:
        try: run_once(db_path)
        except Exception as e: logger.error(f"error: {e}")
        time.sleep(interval_minutes * 60)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--loop', type=int, default=0)
    parser.add_argument('--db', default=DB_PATH)
    args = parser.parse_args()
    if args.loop > 0: run_loop(args.loop, args.db)
    else: run_once(args.db)
