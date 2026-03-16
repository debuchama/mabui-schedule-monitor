"""
weekly_collector.py — 7日分スケジュール一括収集
therapists / daily_schedules テーブルを UPSERT で更新する。
"""

import sys
import os
import logging
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper import scrape_week

JST = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)


def now_jst() -> str:
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')


def upsert_therapist(cur, record: dict, scraped_at: str):
    cur.execute("""
        INSERT INTO therapists (therapist_id, name, age, height_cm, cup_size,
                                first_seen, last_seen, is_active)
        VALUES (:therapist_id, :name, :age, :height_cm, :cup_size,
                :scraped_at, :scraped_at, 1)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name      = COALESCE(name,      excluded.name),
            age       = COALESCE(age,       excluded.age),
            height_cm = COALESCE(height_cm, excluded.height_cm),
            cup_size  = COALESCE(cup_size,  excluded.cup_size),
            last_seen = excluded.last_seen,
            is_active = 1
    """, {**record, 'scraped_at': scraped_at})


def upsert_schedule(cur, record: dict, scraped_at: str):
    cur.execute("""
        INSERT INTO daily_schedules
            (therapist_id, schedule_date, location,
             start_time, end_time, is_fully_booked, scraped_at)
        VALUES
            (:therapist_id, :schedule_date, :location,
             :start_time, :end_time, :is_fully_booked, :scraped_at)
        ON CONFLICT(therapist_id, schedule_date) DO UPDATE SET
            location        = excluded.location,
            start_time      = excluded.start_time,
            end_time        = excluded.end_time,
            is_fully_booked = excluded.is_fully_booked,
            scraped_at      = excluded.scraped_at
    """, {**record, 'scraped_at': scraped_at})


def run(db_path: str = DB_PATH):
    init_db(db_path)
    conn = get_connection(db_path)
    cur  = conn.cursor()
    at   = now_jst()

    logger.info("weekly_collector: start")
    week_data   = scrape_week()
    total_saved = 0

    for date_str, records in week_data.items():
        success = True
        err_msg = None
        try:
            for rec in records:
                upsert_therapist(cur, rec, at)
                upsert_schedule(cur, rec, at)
            conn.commit()
            total_saved += len(records)
        except Exception as e:
            conn.rollback()
            success = False
            err_msg = str(e)
            logger.error(f"weekly_collector: {date_str} error: {e}")

        cur.execute("""
            INSERT INTO scrape_logs
                (run_at, task_type, target_date, records_found, success, error_message)
            VALUES (?, 'weekly', ?, ?, ?, ?)
        """, (at, date_str, len(records), int(success), err_msg))
        conn.commit()
        logger.info(f"  {date_str}: {len(records)} records (ok={success})")

    conn.close()
    logger.info(f"weekly_collector: done. total={total_saved}")
    return total_saved


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    run()
