# MABUI Tokyo — Schedule Monitor

赤羽ルーム・蕨ルームの出勤スケジュールを自動スクレイプし、GitHubPages上でダッシュボード表示するシステム。

## アーキテクチャ

```
mabuispa.com (独自 Rails CMS, SSR)
    │  httpx + BeautifulSoup
    ▼
GitHub Actions (無料)
    ① weekly_collect.yml     毎日 JST 6:00
       7日分スクレイプ → DB → JSON → HTML → GitHub Pages
    ② availability_monitor.yml
       JST 12:00〜翌5:00 / 30分ごと スナップショット取得
    │
    ▼
GitHub Repository
    data/mabuispa.db          SQLite (WAL)
    data/favorites.json       お気に入り設定
    docs/index.html           ダッシュボード (GitHub Pages)
```

## 対象サイト仕様メモ

| 項目 | 値 |
|---|---|
| スケジュールURL | `https://mabuispa.com/schedule?day=YYYY-MM-DD` |
| 日別AjaxAPI | `/today_plus_schedule/{date}` → 204（使用不可）|
| セッション | `_three_m_session` Cookie が必要 |
| 店舗 | 赤羽ルーム (`room8`) / 蕨ルーム (`room9`) |
| 予約満了 | 「予約満了」テキストのみ（時刻なし） |
| セラピストURL | `https://mabuispa.com/therapist/{id}` |

## ローカル開発

```bash
pip install -r requirements.txt
python scripts/db_setup.py              # DB初期化
python scripts/weekly_collector.py      # 7日分収集
python scripts/daily_monitor.py         # 当日スナップショット1回
python scripts/daily_monitor.py --loop 15  # 15分間隔で継続
python scripts/export_data.py           # JSONエクスポート
python build_dashboard.py               # 一括ビルド
python build_dashboard.py --today       # 当日分のみ
python build_dashboard.py --skip-scrape # エクスポート+HTML生成のみ
```

## お気に入り設定

`data/favorites.json` を編集して therapist_id を追加:

```json
[
  { "therapist_id": 48, "name": "水原", "note": "蕨ルーム" }
]
```

therapist_id は公式サイト `https://mabuispa.com/therapist/{id}` の URL から取得。

## GitHub Pages 設定

- Source: `Deploy from a branch`
- Branch: `main` / Folder: `/docs`

## GitHub Actions 無料枠

| ワークフロー | 頻度 | 月間実行回数 | 所要時間 |
|---|---|---|---|
| weekly_collect | 毎日1回 | 30回 | ~60s |
| availability_monitor | 30分×17h | ~1,020回 | ~30s |

Public リポジトリなら Actions 無制限。
Private でも月間約 1,000 分以内で収まる見込み。
