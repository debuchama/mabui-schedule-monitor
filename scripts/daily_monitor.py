"""
daily_monitor.py — 当日空き状況モニター
availability_snapshots テーブルに追記し、状態変化をログ出力する。
--loop N オプションで N分間隔の継続監視も可能。
"""

import sys
import os
import time
import logging
import argparse
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper import scrape_today

JST    = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)


def now_jst() -> str:
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')


def get_previous_snapshot(cur, therapist_id: int, schedule_date: str) -> dict | None:
    """直前のスナップショットを返す。"""
    row = cur.execute("""
        SELECT status, start_time, end_time
        FROM availability_snapshots
        WHERE therapist_id = ? AND schedule_date = ?
        ORDER BY id DESC LIMIT 1
    """, (therapist_id, schedule_date)).fetchone()
    return dict(row) if row else None


def insert_snapshot(cur, rec: dict, checked_at: str):
    status = 'fully_booked' if rec['is_fully_booked'] else 'available'
    cur.execute("""
        INSERT INTO availability_snapshots
            (checked_at, therapist_id, schedule_date, location,
             status, start_time, end_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        checked_at,
        rec['therapist_id'],
        rec['schedule_date'],
        rec['location'],
        status,
        rec['start_time'],
        rec['end_time'],
    ))


def run_once(db_path: str = DB_PATH) -> int:
    """1回チェックして変化数を返す。"""
    init_db(db_path)
    conn = get_connection(db_path)
    cur  = conn.cursor()
    at   = now_jst()
    today = date.today().isoformat()

    records = scrape_today()
    changes = 0

    for rec in records:
        tid  = rec['therapist_id']
        prev = get_previous_snapshot(cur, tid, today)
        cur_status = 'fully_booked' if rec['is_fully_booked'] else 'available'

        # 変化検出
        if prev is None:
            logger.info(f"  NEW  {rec['name']} ({rec['location']}) → {cur_status}")
            changes += 1
        elif prev['status'] != cur_status:
            logger.info(
                f"  CHANGE {rec['name']} ({rec['location']}): "
                f"{prev['status']} → {cur_status}"
            )
            changes += 1

        insert_snapshot(cur, rec, at)

    # ログ記録
    cur.execute("""
        INSERT INTO scrape_logs
            (run_at, task_type, target_date, records_found, success)
        VALUES (?, 'daily_monitor', ?, ?, 1)
    """, (at, today, len(records)))

    conn.commit()
    conn.close()
    logger.info(f"daily_monitor: {len(records)} records, {changes} changes")
    return changes


def run_loop(interval_minutes: int, db_path: str = DB_PATH):
    logger.info(f"daily_monitor: loop mode, interval={interval_minutes}min")
    while True:
        try:
            run_once(db_path)
        except Exception as e:
            logger.error(f"daily_monitor error: {e}")
        time.sleep(interval_minutes * 60)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('--loop', type=int, default=0,
                        help='N分間隔で継続監視 (0=1回のみ)')
    parser.add_argument('--db', default=DB_PATH)
    args = parser.parse_args()

    if args.loop > 0:
        run_loop(args.loop, args.db)
    else:
        run_once(args.db)
