"""
weekly_collector.py — 7日分スケジュール一括収集
当日分は最後に強制再スクレイプして上書きする。
"""
import sys, os, logging, json
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper  import scrape_week, scrape_day

JST    = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)

def now_jst():
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')

def upsert_therapist(cur, rec, scraped_at):
    cur.execute("""
        INSERT INTO therapists (therapist_id, name, age, height_cm, cup_size,
                                tags, first_seen, last_seen, is_active)
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

def upsert_schedule(cur, rec, scraped_at):
    cur.execute("""
        INSERT INTO daily_schedules
            (therapist_id,schedule_date,location,start_time,end_time,is_fully_booked,scraped_at)
        VALUES
            (:therapist_id,:schedule_date,:location,:start_time,:end_time,:is_fully_booked,:scraped_at)
        ON CONFLICT(therapist_id,schedule_date) DO UPDATE SET
            location=excluded.location, start_time=excluded.start_time,
            end_time=excluded.end_time, is_fully_booked=excluded.is_fully_booked,
            scraped_at=excluded.scraped_at
    """, {**rec, 'scraped_at': scraped_at})

def save_records(conn, cur, records, scraped_at):
    for rec in records:
        upsert_therapist(cur, rec, scraped_at)
        upsert_schedule(cur, rec, scraped_at)
    conn.commit()

def run(db_path=DB_PATH):
    init_db(db_path)
    conn  = get_connection(db_path)
    cur   = conn.cursor()
    at    = now_jst()
    today = datetime.now(JST).date().isoformat()

    logger.info("weekly_collector: start")
    week_data   = scrape_week()
    total_saved = 0

    for date_str, records in week_data.items():
        success = True; err_msg = None
        try:
            save_records(conn, cur, records, at)
            total_saved += len(records)
        except Exception as e:
            conn.rollback(); success = False; err_msg = str(e)
            logger.error(f"  {date_str} error: {e}")
        cur.execute(
            "INSERT INTO scrape_logs(run_at,task_type,target_date,records_found,success,error_message) VALUES(?,?,?,?,?,?)",
            (at,'weekly',date_str,len(records),int(success),err_msg))
        conn.commit()
        logger.info(f"  {date_str}: {len(records)} records (ok={success})")

    # 当日分を強制上書き
    logger.info(f"weekly_collector: 当日({today})強制上書き")
    today_records = scrape_day(today)
    if today_records:
        cur.execute("DELETE FROM daily_schedules WHERE schedule_date=?", (today,))
        conn.commit()
        try:
            save_records(conn, cur, today_records, at)
            logger.info(f"  today overwrite: {len(today_records)} records")
        except Exception as e:
            conn.rollback(); logger.error(f"  today overwrite error: {e}")

    conn.close()
    logger.info(f"weekly_collector: done. total={total_saved}")
    return total_saved

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    run()
