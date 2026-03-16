"""
export_data.py — SQLite → dashboard_data.json エクスポート
ダッシュボード描画に必要な全データを1ファイルに集約する。
"""

import sys
import os
import json
import logging
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, DB_PATH

logger = logging.getLogger(__name__)

DATA_DIR       = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_JSON    = os.path.join(DATA_DIR, 'dashboard_data.json')
FAVORITES_JSON = os.path.join(DATA_DIR, 'favorites.json')


def q(conn, sql, params=()):
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def export_all(db_path: str = DB_PATH, out_path: str = OUTPUT_JSON) -> dict:
    conn  = get_connection(db_path)
    today = date.today().isoformat()
    week_dates = [(date.today() + timedelta(days=i)).isoformat() for i in range(7)]
    past30_start = (date.today() - timedelta(days=30)).isoformat()

    # ── 1. therapists ─────────────────────────────────────────────────────────
    therapists = q(conn, "SELECT * FROM therapists WHERE is_active=1 ORDER BY name")

    # ── 2. weekly_schedules ───────────────────────────────────────────────────
    weekly_schedules = q(conn, """
        SELECT ds.*, t.name AS therapist_name, t.age, t.height_cm, t.cup_size
        FROM daily_schedules ds
        JOIN therapists t ON t.therapist_id = ds.therapist_id
        WHERE ds.schedule_date BETWEEN ? AND ?
        ORDER BY ds.schedule_date, ds.start_time NULLS LAST
    """, (week_dates[0], week_dates[-1]))

    # ── 3. daily_location_summary (店舗×日) ──────────────────────────────────
    daily_location_summary = q(conn, """
        SELECT schedule_date, location, COUNT(*) AS staff_count,
               SUM(is_fully_booked) AS booked_count
        FROM daily_schedules
        WHERE schedule_date BETWEEN ? AND ?
        GROUP BY schedule_date, location
        ORDER BY schedule_date, location
    """, (week_dates[0], week_dates[-1]))

    # ── 4. therapist_stats (直近30日) ─────────────────────────────────────────
    therapist_stats = q(conn, """
        SELECT
            t.therapist_id, t.name,
            COUNT(ds.id)            AS shift_count,
            ROUND(AVG(
                CASE
                    WHEN ds.start_time IS NOT NULL AND ds.end_time IS NOT NULL
                    THEN (
                        CAST(SUBSTR(ds.end_time,1,2) AS REAL) * 60
                        + CAST(SUBSTR(ds.end_time,4,2) AS REAL)
                        - CAST(SUBSTR(ds.start_time,1,2) AS REAL) * 60
                        - CAST(SUBSTR(ds.start_time,4,2) AS REAL)
                    )
                    ELSE NULL
                END
            ) / 60.0, 2)            AS avg_hours,
            GROUP_CONCAT(DISTINCT ds.location) AS locations
        FROM daily_schedules ds
        JOIN therapists t ON t.therapist_id = ds.therapist_id
        WHERE ds.schedule_date >= ?
        GROUP BY t.therapist_id
        ORDER BY shift_count DESC
    """, (past30_start,))

    # ── 5. shift_coverage (昼夜バランス) ──────────────────────────────────────
    shift_coverage = q(conn, """
        SELECT schedule_date,
               SUM(CASE WHEN CAST(SUBSTR(start_time,1,2) AS INTEGER) < 18
                        AND start_time IS NOT NULL THEN 1 ELSE 0 END) AS day_count,
               SUM(CASE WHEN CAST(SUBSTR(start_time,1,2) AS INTEGER) >= 18
                        OR  start_time IS NULL THEN 1 ELSE 0 END) AS night_count
        FROM daily_schedules
        WHERE schedule_date BETWEEN ? AND ?
        GROUP BY schedule_date
        ORDER BY schedule_date
    """, (week_dates[0], week_dates[-1]))

    # ── 6. today_snapshots ────────────────────────────────────────────────────
    today_snapshots = q(conn, """
        SELECT s.*, t.name AS therapist_name
        FROM availability_snapshots s
        JOIN therapists t ON t.therapist_id = s.therapist_id
        WHERE s.schedule_date = ?
        ORDER BY s.checked_at, t.name
    """, (today,))

    # ── 7. scrape_logs ────────────────────────────────────────────────────────
    scrape_logs = q(conn, """
        SELECT * FROM scrape_logs
        ORDER BY id DESC LIMIT 20
    """)

    # ── 8. booking_events (予約満了イベント一覧) ──────────────────────────────
    booking_events = q(conn, """
        SELECT
            s.checked_at,
            s.therapist_id,
            t.name AS therapist_name,
            s.schedule_date,
            s.location,
            s.start_time,
            s.end_time
        FROM availability_snapshots s
        JOIN therapists t ON t.therapist_id = s.therapist_id
        WHERE s.status = 'fully_booked'
        ORDER BY s.checked_at DESC
        LIMIT 100
    """)

    # ── 9. popularity_ranking ─────────────────────────────────────────────────
    # 「最初の available → fully_booked 変化」の時間差を therapist 毎に集計
    popularity_raw = q(conn, """
        WITH ranked AS (
            SELECT
                therapist_id, schedule_date,
                status, checked_at,
                ROW_NUMBER() OVER (
                    PARTITION BY therapist_id, schedule_date, status
                    ORDER BY checked_at
                ) AS rn
            FROM availability_snapshots
        ),
        first_avail AS (
            SELECT therapist_id, schedule_date, checked_at AS avail_at
            FROM ranked WHERE status='available' AND rn=1
        ),
        first_booked AS (
            SELECT therapist_id, schedule_date, checked_at AS booked_at
            FROM ranked WHERE status='fully_booked' AND rn=1
        )
        SELECT
            fa.therapist_id,
            t.name,
            COUNT(*) AS booked_days,
            ROUND(AVG(
                (JULIANDAY(fb.booked_at) - JULIANDAY(fa.avail_at)) * 24.0
            ), 2) AS avg_hours_to_book
        FROM first_avail fa
        JOIN first_booked fb
          ON fa.therapist_id = fb.therapist_id
         AND fa.schedule_date = fb.schedule_date
         AND fb.booked_at > fa.avail_at
        JOIN therapists t ON t.therapist_id = fa.therapist_id
        GROUP BY fa.therapist_id
        HAVING booked_days >= 1
        ORDER BY avg_hours_to_book ASC
    """)

    # ── 10. favorites ─────────────────────────────────────────────────────────
    favorites_config = []
    if os.path.exists(FAVORITES_JSON):
        with open(FAVORITES_JSON, encoding='utf-8') as f:
            favorites_config = json.load(f)

    fav_ids = [fav['therapist_id'] for fav in favorites_config]
    favorites_schedule = []
    if fav_ids:
        placeholders = ','.join('?' * len(fav_ids))
        favorites_schedule = q(conn, f"""
            SELECT ds.*, t.name AS therapist_name
            FROM daily_schedules ds
            JOIN therapists t ON t.therapist_id = ds.therapist_id
            WHERE ds.therapist_id IN ({placeholders})
              AND ds.schedule_date BETWEEN ? AND ?
            ORDER BY ds.schedule_date, ds.start_time NULLS LAST
        """, (*fav_ids, week_dates[0], week_dates[-1]))

    conn.close()

    payload = {
        'generated_at'          : __import__('datetime').datetime.now().isoformat(),
        'today'                 : today,
        'week_dates'            : week_dates,
        'therapists'            : therapists,
        'weekly_schedules'      : weekly_schedules,
        'daily_location_summary': daily_location_summary,
        'therapist_stats'       : therapist_stats,
        'shift_coverage'        : shift_coverage,
        'today_snapshots'       : today_snapshots,
        'scrape_logs'           : scrape_logs,
        'booking_events'        : booking_events,
        'popularity_ranking'    : popularity_raw,
        'favorites_config'      : favorites_config,
        'favorites_schedule'    : favorites_schedule,
    }

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"export_data: written → {os.path.abspath(out_path)}")
    return payload


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    export_all()
