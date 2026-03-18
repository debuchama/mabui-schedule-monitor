"""
export_data.py — SQLite → dashboard_data.json エクスポート
"""
import sys, os, json, logging
from datetime import date, timedelta, datetime

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, DB_PATH

logger   = logging.getLogger(__name__)
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_JSON    = os.path.join(DATA_DIR, 'dashboard_data.json')
FAVORITES_JSON = os.path.join(DATA_DIR, 'favorites.json')

def q(conn, sql, params=()):
    return [dict(r) for r in conn.execute(sql, params).fetchall()]

def export_all(db_path=DB_PATH, out_path=OUTPUT_JSON):
    conn        = get_connection(db_path)
    today       = date.today().isoformat()
    week_dates  = [(date.today() + timedelta(days=i)).isoformat() for i in range(7)]
    past30      = (date.today() - timedelta(days=30)).isoformat()
    past7       = (date.today() - timedelta(days=7)).isoformat()

    # 1. therapists
    therapists = q(conn, "SELECT * FROM therapists WHERE is_active=1 ORDER BY name")

    # 2. weekly_schedules
    weekly_schedules = q(conn, """
        SELECT ds.*, t.name AS therapist_name, t.age, t.height_cm, t.cup_size, t.tags
        FROM daily_schedules ds JOIN therapists t ON t.therapist_id=ds.therapist_id
        WHERE ds.schedule_date BETWEEN ? AND ?
        ORDER BY ds.schedule_date, ds.start_time NULLS LAST
    """, (week_dates[0], week_dates[-1]))

    # 3. daily_location_summary
    daily_location_summary = q(conn, """
        SELECT schedule_date, location, COUNT(*) AS staff_count,
               SUM(is_fully_booked) AS booked_count
        FROM daily_schedules
        WHERE schedule_date BETWEEN ? AND ?
        GROUP BY schedule_date, location ORDER BY schedule_date, location
    """, (week_dates[0], week_dates[-1]))

    # 4. therapist_stats (直近30日)
    therapist_stats = q(conn, """
        SELECT t.therapist_id, t.name, t.tags,
               COUNT(ds.id) AS shift_count,
               ROUND(AVG(CASE
                 WHEN ds.start_time IS NOT NULL AND ds.end_time IS NOT NULL
                 THEN (CAST(SUBSTR(ds.end_time,1,2) AS REAL)*60
                       + CAST(SUBSTR(ds.end_time,4,2) AS REAL)
                       - CAST(SUBSTR(ds.start_time,1,2) AS REAL)*60
                       - CAST(SUBSTR(ds.start_time,4,2) AS REAL))/60.0
                 ELSE NULL END), 2) AS avg_hours,
               GROUP_CONCAT(DISTINCT ds.location) AS locations
        FROM daily_schedules ds JOIN therapists t ON t.therapist_id=ds.therapist_id
        WHERE ds.schedule_date >= ?
        GROUP BY t.therapist_id ORDER BY shift_count DESC
    """, (past30,))

    # 5. shift_coverage
    shift_coverage = q(conn, """
        SELECT schedule_date,
               SUM(CASE WHEN CAST(SUBSTR(start_time,1,2) AS INTEGER)<18
                        AND start_time IS NOT NULL THEN 1 ELSE 0 END) AS day_count,
               SUM(CASE WHEN CAST(SUBSTR(start_time,1,2) AS INTEGER)>=18
                        OR start_time IS NULL THEN 1 ELSE 0 END) AS night_count
        FROM daily_schedules
        WHERE schedule_date BETWEEN ? AND ?
        GROUP BY schedule_date ORDER BY schedule_date
    """, (week_dates[0], week_dates[-1]))

    # 6. today_snapshots
    today_snapshots = q(conn, """
        SELECT s.*, t.name AS therapist_name
        FROM availability_snapshots s JOIN therapists t ON t.therapist_id=s.therapist_id
        WHERE s.schedule_date=? ORDER BY s.checked_at, t.name
    """, (today,))

    # 7. scrape_logs
    scrape_logs = q(conn, "SELECT * FROM scrape_logs ORDER BY id DESC LIMIT 20")

    # 8. booking_events
    booking_events = q(conn, """
        SELECT s.checked_at, s.therapist_id, t.name AS therapist_name,
               s.schedule_date, s.location, s.start_time, s.end_time
        FROM availability_snapshots s JOIN therapists t ON t.therapist_id=s.therapist_id
        WHERE s.status='fully_booked' ORDER BY s.checked_at DESC LIMIT 100
    """)

    # 9. popularity_ranking
    popularity_ranking = q(conn, """
        WITH ranked AS (
            SELECT therapist_id, schedule_date, status, checked_at,
                   ROW_NUMBER() OVER(PARTITION BY therapist_id,schedule_date,status ORDER BY checked_at) AS rn
            FROM availability_snapshots
        ),
        fa AS (SELECT therapist_id,schedule_date,checked_at AS avail_at  FROM ranked WHERE status='available'    AND rn=1),
        fb AS (SELECT therapist_id,schedule_date,checked_at AS booked_at FROM ranked WHERE status='fully_booked' AND rn=1)
        SELECT fa.therapist_id, t.name,
               COUNT(*)  AS booked_days,
               ROUND(AVG((JULIANDAY(fb.booked_at)-JULIANDAY(fa.avail_at))*24.0),2) AS avg_hours_to_book
        FROM fa JOIN fb ON fa.therapist_id=fb.therapist_id AND fa.schedule_date=fb.schedule_date
                       AND fb.booked_at>fa.avail_at
                JOIN therapists t ON t.therapist_id=fa.therapist_id
        GROUP BY fa.therapist_id HAVING booked_days>=1
        ORDER BY avg_hours_to_book ASC
    """)

    # 10. favorites
    favorites_config = []
    if os.path.exists(FAVORITES_JSON):
        with open(FAVORITES_JSON, encoding='utf-8') as f:
            favorites_config = json.load(f)
    fav_ids = [fav['therapist_id'] for fav in favorites_config]
    favorites_schedule = []
    if fav_ids:
        ph = ','.join('?'*len(fav_ids))
        favorites_schedule = q(conn, f"""
            SELECT ds.*, t.name AS therapist_name
            FROM daily_schedules ds JOIN therapists t ON t.therapist_id=ds.therapist_id
            WHERE ds.therapist_id IN ({ph}) AND ds.schedule_date BETWEEN ? AND ?
            ORDER BY ds.schedule_date, ds.start_time NULLS LAST
        """, (*fav_ids, week_dates[0], week_dates[-1]))

    # ── NEW: 11. 時間帯別予約圧力ヒートマップ ────────────────────────────
    # checked_at の時刻を1時間単位に丸めて、fully_booked 率を集計
    hourly_pressure = q(conn, """
        SELECT
            s.therapist_id,
            t.name AS therapist_name,
            t.tags,
            CAST(SUBSTR(s.checked_at, 12, 2) AS INTEGER) AS hour_utc,
            COUNT(*) AS total_checks,
            SUM(CASE WHEN s.status='fully_booked' THEN 1 ELSE 0 END) AS booked_checks
        FROM availability_snapshots s
        JOIN therapists t ON t.therapist_id = s.therapist_id
        WHERE s.schedule_date >= ?
        GROUP BY s.therapist_id, hour_utc
        ORDER BY s.therapist_id, hour_utc
    """, (past7,))
    # hour_utc → JST (+9)、深夜補正
    for row in hourly_pressure:
        h_jst = (row['hour_utc'] + 9) % 24
        row['hour_jst'] = h_jst if h_jst >= 6 else h_jst + 24  # 0〜5時→24〜29

    # ── NEW: 12. 曜日別出勤パターン ──────────────────────────────────────
    weekday_pattern = q(conn, """
        SELECT
            t.therapist_id, t.name,
            CASE CAST(strftime('%w', schedule_date) AS INTEGER)
                WHEN 0 THEN '日' WHEN 1 THEN '月' WHEN 2 THEN '火'
                WHEN 3 THEN '水' WHEN 4 THEN '木' WHEN 5 THEN '金'
                WHEN 6 THEN '土'
            END AS weekday,
            CAST(strftime('%w', schedule_date) AS INTEGER) AS weekday_num,
            COUNT(*) AS shift_count
        FROM daily_schedules ds JOIN therapists t ON t.therapist_id=ds.therapist_id
        WHERE ds.schedule_date >= ?
        GROUP BY t.therapist_id, weekday_num
        ORDER BY t.name, weekday_num
    """, (past30,))

    # ── NEW: 13. 満了速度トレンド (日別平均) ──────────────────────────────
    booking_trend = q(conn, """
        WITH ranked AS (
            SELECT therapist_id, schedule_date, status, checked_at,
                   ROW_NUMBER() OVER(PARTITION BY therapist_id,schedule_date,status ORDER BY checked_at) AS rn
            FROM availability_snapshots
        ),
        fa AS (SELECT therapist_id,schedule_date,checked_at AS avail_at  FROM ranked WHERE status='available'    AND rn=1),
        fb AS (SELECT therapist_id,schedule_date,checked_at AS booked_at FROM ranked WHERE status='fully_booked' AND rn=1)
        SELECT fa.schedule_date,
               COUNT(*)  AS booked_count,
               ROUND(AVG((JULIANDAY(fb.booked_at)-JULIANDAY(fa.avail_at))*24.0),2) AS avg_hours_to_book,
               ROUND(MIN((JULIANDAY(fb.booked_at)-JULIANDAY(fa.avail_at))*24.0),2) AS min_hours_to_book
        FROM fa JOIN fb ON fa.therapist_id=fb.therapist_id AND fa.schedule_date=fb.schedule_date
                       AND fb.booked_at>fa.avail_at
        GROUP BY fa.schedule_date ORDER BY fa.schedule_date
    """)

    # ── NEW: 14. タグ別人気度相関 ─────────────────────────────────────────
    tag_popularity = q(conn, """
        WITH ranked AS (
            SELECT therapist_id, schedule_date, status, checked_at,
                   ROW_NUMBER() OVER(PARTITION BY therapist_id,schedule_date,status ORDER BY checked_at) AS rn
            FROM availability_snapshots
        ),
        fa AS (SELECT therapist_id,schedule_date,checked_at AS avail_at  FROM ranked WHERE status='available'    AND rn=1),
        fb AS (SELECT therapist_id,schedule_date,checked_at AS booked_at FROM ranked WHERE status='fully_booked' AND rn=1),
        speed AS (
            SELECT fa.therapist_id,
                   ROUND(AVG((JULIANDAY(fb.booked_at)-JULIANDAY(fa.avail_at))*24.0),2) AS avg_h
            FROM fa JOIN fb ON fa.therapist_id=fb.therapist_id AND fa.schedule_date=fb.schedule_date
                           AND fb.booked_at>fa.avail_at
            GROUP BY fa.therapist_id
        )
        SELECT t.tags, speed.avg_h, t.name
        FROM speed JOIN therapists t ON t.therapist_id=speed.therapist_id
        WHERE t.tags IS NOT NULL
    """)
    # タグ別に平均速度を集計（Python側で処理）
    from collections import defaultdict
    tag_speed_map = defaultdict(list)
    for row in tag_popularity:
        try:
            tags = json.loads(row['tags'] or '[]')
            for tag in tags:
                if tag: tag_speed_map[tag].append(row['avg_h'])
        except Exception: pass
    tag_popularity_agg = [
        {'tag': tag, 'avg_hours': round(sum(vs)/len(vs), 2), 'count': len(vs)}
        for tag, vs in tag_speed_map.items() if len(vs) >= 1
    ]
    tag_popularity_agg.sort(key=lambda x: x['avg_hours'])

    # ── NEW: 15. 在籍履歴・新人/復帰アラート ─────────────────────────────
    # 直近7日以内に first_seen の人 = 新人
    # last_seen が7日以上前で今週出勤している人 = 復帰
    newcomers = q(conn, """
        SELECT therapist_id, name, first_seen FROM therapists
        WHERE first_seen >= ? AND is_active=1 ORDER BY first_seen DESC
    """, (past7,))
    returnees = q(conn, """
        SELECT t.therapist_id, t.name, t.last_seen
        FROM therapists t
        JOIN daily_schedules ds ON t.therapist_id=ds.therapist_id
        WHERE ds.schedule_date BETWEEN ? AND ?
          AND t.first_seen < ?
          AND t.last_seen < ?
        GROUP BY t.therapist_id
    """, (week_dates[0], week_dates[-1], past7, past7))

    # ── NEW: 16. お気に入り専用詳細 ──────────────────────────────────────
    favorites_detail = []
    if fav_ids:
        ph = ','.join('?'*len(fav_ids))
        fav_snaps = q(conn, f"""
            SELECT s.*, t.name AS therapist_name
            FROM availability_snapshots s JOIN therapists t ON t.therapist_id=s.therapist_id
            WHERE s.therapist_id IN ({ph}) AND s.schedule_date >= ?
            ORDER BY s.checked_at
        """, (*fav_ids, past30))
        fav_stats = q(conn, f"""
            SELECT t.therapist_id, t.name, t.tags,
                   COUNT(DISTINCT ds.schedule_date) AS shift_count,
                   GROUP_CONCAT(DISTINCT ds.location) AS locations
            FROM therapists t LEFT JOIN daily_schedules ds ON t.therapist_id=ds.therapist_id
            WHERE t.therapist_id IN ({ph}) AND (ds.schedule_date IS NULL OR ds.schedule_date >= ?)
            GROUP BY t.therapist_id
        """, (*fav_ids, past30))
        favorites_detail = {
            'snapshots': fav_snaps,
            'stats': fav_stats,
        }

    conn.close()

    payload = {
        'generated_at'          : datetime.now().isoformat(),
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
        'popularity_ranking'    : popularity_ranking,
        'favorites_config'      : favorites_config,
        'favorites_schedule'    : favorites_schedule,
        'favorites_detail'      : favorites_detail,
        # NEW
        'hourly_pressure'       : hourly_pressure,
        'weekday_pattern'       : weekday_pattern,
        'booking_trend'         : booking_trend,
        'tag_popularity'        : tag_popularity_agg,
        'newcomers'             : newcomers,
        'returnees'             : returnees,
    }

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"export_data: written → {os.path.abspath(out_path)}")
    return payload

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    export_all()
