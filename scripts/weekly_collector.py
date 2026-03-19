"""
weekly_collector.py — スケジュール収集

【重要な仕様】
mabuispa.com は当日のスケジュールのみ確定している。
/schedule?day=YYYY-MM-DD に未来日程を渡すと「今日と同じデータ」を返すため、
7日分を取得しても 6日分は誤ったデータになる。

このため収集は「当日分のみ」とし、daily_schedules を毎朝上書きする。
"""
import sys, os, logging
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from db_setup import get_connection, init_db, DB_PATH
from scraper  import scrape_day

JST    = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)

def now_jst():
    return datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')

def today_jst():
    return datetime.now(JST).date().isoformat()

def upsert_therapist(cur, rec, scraped_at):
    cur.execute("""
        INSERT INTO therapists(therapist_id,name,age,height_cm,cup_size,tags,first_seen,last_seen,is_active)
        VALUES(:therapist_id,:name,:age,:height_cm,:cup_size,:tags,:scraped_at,:scraped_at,1)
        ON CONFLICT(therapist_id) DO UPDATE SET
            name=COALESCE(name,excluded.name), age=COALESCE(age,excluded.age),
            height_cm=COALESCE(height_cm,excluded.height_cm),
            cup_size=COALESCE(cup_size,excluded.cup_size),
            tags=COALESCE(excluded.tags,tags),
            last_seen=excluded.last_seen, is_active=1
    """, {**rec, 'scraped_at': scraped_at})

def run(db_path=DB_PATH):
    init_db(db_path)
    conn  = get_connection(db_path)
    cur   = conn.cursor()
    at    = now_jst()
    today = today_jst()

    logger.info(f"weekly_collector: 当日({today})を収集")
    records = scrape_day(today)

    if records:
        # 当日分を全削除して再挿入（出勤者の増減を正確に反映）
        cur.execute("DELETE FROM daily_schedules WHERE schedule_date=?", (today,))
        conn.commit()
        for rec in records:
            upsert_therapist(cur, rec, at)
            cur.execute("""
                INSERT INTO daily_schedules
                    (therapist_id,schedule_date,location,start_time,end_time,is_fully_booked,scraped_at)
                VALUES(:therapist_id,:schedule_date,:location,:start_time,:end_time,:is_fully_booked,:scraped_at)
                ON CONFLICT(therapist_id,schedule_date) DO UPDATE SET
                    location=excluded.location, start_time=excluded.start_time,
                    end_time=excluded.end_time, is_fully_booked=excluded.is_fully_booked,
                    scraped_at=excluded.scraped_at
            """, {**rec, 'scraped_at': at})
        conn.commit()
        logger.info(f"  {today}: {len(records)} records saved")

    cur.execute(
        "INSERT INTO scrape_logs(run_at,task_type,target_date,records_found,success) VALUES(?,?,?,?,1)",
        (at,'weekly',today,len(records)))
    conn.commit()
    conn.close()
    logger.info(f"weekly_collector: done. {len(records)} records")
    return len(records)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    run()
