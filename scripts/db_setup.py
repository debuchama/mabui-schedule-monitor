"""db_setup.py — SQLiteスキーマ定義・初期化"""
import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'mabuispa.db')

def get_connection(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path=DB_PATH):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = get_connection(db_path)
    cur  = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS therapists (
        therapist_id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER,
        height_cm INTEGER, cup_size TEXT, tags TEXT,
        first_seen TEXT, last_seen TEXT, is_active INTEGER DEFAULT 1)""")
    cols = [r[1] for r in cur.execute("PRAGMA table_info(therapists)").fetchall()]
    if 'tags' not in cols:
        cur.execute("ALTER TABLE therapists ADD COLUMN tags TEXT")

    cur.execute("""CREATE TABLE IF NOT EXISTS daily_schedules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        therapist_id INTEGER NOT NULL REFERENCES therapists(therapist_id),
        schedule_date TEXT NOT NULL, location TEXT NOT NULL,
        start_time TEXT, end_time TEXT, is_fully_booked INTEGER DEFAULT 0,
        scraped_at TEXT NOT NULL, UNIQUE(therapist_id, schedule_date))""")

    cur.execute("""CREATE TABLE IF NOT EXISTS availability_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, checked_at TEXT NOT NULL,
        therapist_id INTEGER NOT NULL REFERENCES therapists(therapist_id),
        schedule_date TEXT NOT NULL, location TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('available','fully_booked','shift_ended')),
        start_time TEXT, end_time TEXT)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS scrape_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_at TEXT NOT NULL,
        task_type TEXT NOT NULL, target_date TEXT,
        records_found INTEGER DEFAULT 0, success INTEGER DEFAULT 1, error_message TEXT)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS info_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT, scraped_at TEXT NOT NULL,
        schedule_date TEXT NOT NULL,
        therapist_id INTEGER REFERENCES therapists(therapist_id),
        name_raw TEXT NOT NULL, location TEXT, start_time TEXT, end_time TEXT,
        note TEXT, is_soldout INTEGER DEFAULT 0, remaining INTEGER,
        UNIQUE(schedule_date, name_raw))""")

    cur.execute("""CREATE TABLE IF NOT EXISTS weekly_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT, scraped_at TEXT NOT NULL,
        therapist_id INTEGER NOT NULL REFERENCES therapists(therapist_id),
        schedule_date TEXT NOT NULL, location TEXT,
        start_time TEXT, end_time TEXT,
        status TEXT NOT NULL CHECK(status IN ('working','fully_booked','off')),
        UNIQUE(therapist_id, schedule_date))""")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_ds_date   ON daily_schedules(schedule_date)",
        "CREATE INDEX IF NOT EXISTS idx_ds_tid    ON daily_schedules(therapist_id)",
        "CREATE INDEX IF NOT EXISTS idx_snap_date ON availability_snapshots(schedule_date)",
        "CREATE INDEX IF NOT EXISTS idx_snap_tid  ON availability_snapshots(therapist_id)",
        "CREATE INDEX IF NOT EXISTS idx_snap_chk  ON availability_snapshots(checked_at)",
        "CREATE INDEX IF NOT EXISTS idx_info_date ON info_schedule(schedule_date)",
        "CREATE INDEX IF NOT EXISTS idx_ws_tid    ON weekly_schedule(therapist_id)",
        "CREATE INDEX IF NOT EXISTS idx_ws_date   ON weekly_schedule(schedule_date)",
    ]:
        cur.execute(sql)

    conn.commit()
    conn.close()
    print(f"[db_setup] initialized: {os.path.abspath(db_path)}")

if __name__ == '__main__':
    init_db()
