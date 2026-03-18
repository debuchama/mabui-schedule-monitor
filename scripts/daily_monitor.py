"""
daily_monitor.py — 当日空き状況モニター（スナップショット蓄積）
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
        INSERT INTO therapists (therapist_id,name,age,height_cm,cup_size,
                                tags,first_seen,last_seen,is_active)
        VALUES (:therapist_id,:name,:age,:height_cm,:cup_size,
                :tags,:scraped_at,:scraped_at,1)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name      = COALESCE(name,      excluded.name),
            age       = COALESCE(age,       excluded.age),
            height_cm = COALESCE(height_cm, excluded.height_cm),
            cup_size  = COALESCE(cup_size,  excluded.cup_size),
            tags      = COALESCE(excluded.tags, tags),
            last_seen = excluded.last_seen,
            is_active = 1
    """, {**rec, 'scraped_at': scraped_at})

def get_prev_snapshot(cur, therapist_id, schedule_date):
    row = cur.execute("""
        SELECT status FROM availability_snapshots
        WHERE therapist_id=? AND schedule_date=? ORDER BY id DESC LIMIT 1
    """, (therapist_id, schedule_date)).fetchone()
    return dict(row) if row else None

def insert_snapshot(cur, rec, checked_at):
    cur.execute("""
        INSERT INTO availability_snapshots
            (checked_at,therapist_id,schedule_date,location,status,start_time,end_time)
        VALUES(?,?,?,?,?,?,?)
    """, (checked_at, rec['therapist_id'], rec['schedule_date'], rec['location'],
          'fully_booked' if rec['is_fully_booked'] else 'available',
          rec['start_time'], rec['end_time']))

def run_once(db_path=DB_PATH):
    init_db(db_path)
    conn  = get_connection(db_path)
    cur   = conn.cursor()
    at    = now_jst()
    today = date.today().isoformat()

    records = scrape_today()
    changes = 0
    for rec in records:
        upsert_therapist(cur, rec, at)
        cur_status = 'fully_booked' if rec['is_fully_booked'] else 'available'
        prev = get_prev_snapshot(cur, rec['therapist_id'], today)
        if prev is None:
            logger.info(f"  NEW  {rec['name']} ({rec['location']}) → {cur_status}")
            changes += 1
        elif prev['status'] != cur_status:
            logger.info(f"  CHANGE {rec['name']}: {prev['status']} → {cur_status}")
            changes += 1
        insert_snapshot(cur, rec, at)

    cur.execute(
        "INSERT INTO scrape_logs(run_at,task_type,target_date,records_found,success) VALUES(?,?,?,?,1)",
        (at,'daily_monitor',today,len(records)))
    conn.commit()
    conn.close()
    logger.info(f"daily_monitor: {len(records)} records, {changes} changes")
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
