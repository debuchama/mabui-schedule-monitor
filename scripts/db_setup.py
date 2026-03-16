"""
db_setup.py — SQLiteスキーマ定義・初期化
mabuispa.com 専用 (赤羽ルーム / 蕨ルーム 2店舗)
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'mabuispa.db')


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = get_connection(db_path)
    cur = conn.cursor()

    # ── therapists ────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS therapists (
            therapist_id  INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            age           INTEGER,
            height_cm     INTEGER,
            cup_size      TEXT,
            first_seen    TEXT,
            last_seen     TEXT,
            is_active     INTEGER DEFAULT 1
        )
    """)

    # ── daily_schedules ───────────────────────────────────────────────────────
    # UNIQUE(therapist_id, schedule_date) → 同日再スクレイプは UPSERT 上書き
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_schedules (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            therapist_id     INTEGER NOT NULL REFERENCES therapists(therapist_id),
            schedule_date    TEXT    NOT NULL,
            location         TEXT    NOT NULL,
            start_time       TEXT,
            end_time         TEXT,
            is_fully_booked  INTEGER DEFAULT 0,
            scraped_at       TEXT    NOT NULL,
            UNIQUE(therapist_id, schedule_date)
        )
    """)

    # ── availability_snapshots ────────────────────────────────────────────────
    # 追記専用。「何時に予約が埋まったか」を遡れる。
    cur.execute("""
        CREATE TABLE IF NOT EXISTS availability_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at    TEXT    NOT NULL,
            therapist_id  INTEGER NOT NULL REFERENCES therapists(therapist_id),
            schedule_date TEXT    NOT NULL,
            location      TEXT    NOT NULL,
            status        TEXT    NOT NULL CHECK(status IN ('available', 'fully_booked')),
            start_time    TEXT,
            end_time      TEXT
        )
    """)

    # ── scrape_logs ───────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_logs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at         TEXT    NOT NULL,
            task_type      TEXT    NOT NULL,
            target_date    TEXT,
            records_found  INTEGER DEFAULT 0,
            success        INTEGER DEFAULT 1,
            error_message  TEXT
        )
    """)

    # ── インデックス ──────────────────────────────────────────────────────────
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ds_date     ON daily_schedules(schedule_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ds_tid      ON daily_schedules(therapist_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_date   ON availability_snapshots(schedule_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_tid    ON availability_snapshots(therapist_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_chk    ON availability_snapshots(checked_at)")

    conn.commit()
    conn.close()
    print(f"[db_setup] DB initialized: {os.path.abspath(db_path)}")


if __name__ == '__main__':
    init_db()
