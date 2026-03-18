"""
weekly_collector.py — 7日分スケジュール一括収集
therapists / daily_schedules テーブルを UPSERT で更新する。

【重要】
当日のスケジュールは前日時点では確定していないため、
weekly_collect 実行時に「今日より前の日付はスキップ、今日以降は収集」とすると
当日が取り残される。そこで:
  - 7日分を収集する際に当日分も含める
  - さらに最後に当日分を単独で再スクレイプして強制上書き (delete + insert)
  これにより「前日の週次収集で先取りした今日分の古いデータ」を確実に最新化する。
"""

import sys
import os
import logging
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper import scrape_week, scrape_day

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


def save_records(conn, cur, records: list, scraped_at: str):
    for rec in records:
        upsert_therapist(cur, rec, scraped_at)
        upsert_schedule(cur, rec, scraped_at)
    conn.commit()


def run(db_path: str = DB_PATH):
    init_db(db_path)
    conn = get_connection(db_path)
    cur  = conn.cursor()
    at   = now_jst()
    today = date.today().isoformat()

    logger.info("weekly_collector: start (7日分収集)")
    week_data   = scrape_week()
    total_saved = 0

    for date_str, records in week_data.items():
        success = True
        err_msg = None
        try:
            save_records(conn, cur, records, at)
            total_saved += len(records)
        except Exception as e:
            conn.rollback()
            success = False
            err_msg = str(e)
            logger.error(f"  {date_str} error: {e}")

        cur.execute("""
            INSERT INTO scrape_logs
                (run_at, task_type, target_date, records_found, success, error_message)
            VALUES (?, 'weekly', ?, ?, ?, ?)
        """, (at, date_str, len(records), int(success), err_msg))
        conn.commit()
        logger.info(f"  {date_str}: {len(records)} records (ok={success})")

    # ── 当日分を強制上書き ──────────────────────────────────────────────
    # 前日の週次収集で書き込んだ「今日分の古いデータ」を削除して再挿入する
    logger.info(f"weekly_collector: 当日({today})を強制再スクレイプして上書き")
    today_records = scrape_day(today)

    if today_records:
        # 当日分を一旦全削除してから再挿入（出勤者の増減を正確に反映）
        cur.execute("DELETE FROM daily_schedules WHERE schedule_date = ?", (today,))
        conn.commit()
        try:
            save_records(conn, cur, today_records, at)
            logger.info(f"  today overwrite: {len(today_records)} records")
        except Exception as e:
            conn.rollback()
            logger.error(f"  today overwrite error: {e}")

    conn.close()
    logger.info(f"weekly_collector: done. total={total_saved}")
    return total_saved


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    run()
