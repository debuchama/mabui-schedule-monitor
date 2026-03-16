#!/usr/bin/env python3
"""
build_dashboard.py — 一括ビルドスクリプト
収集 → エクスポート → ダッシュボードHTML生成 → docs/index.html コピー
"""

import sys
import os
import json
import shutil
import logging
import argparse

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
SCRIPTS   = os.path.join(BASE_DIR, 'scripts')
DATA_DIR  = os.path.join(BASE_DIR, 'data')
DASH_DIR  = os.path.join(BASE_DIR, 'dashboards')
DOCS_DIR  = os.path.join(BASE_DIR, 'docs')

sys.path.insert(0, SCRIPTS)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)


def build_html(json_path: str, template_path: str, out_path: str):
    with open(json_path, encoding='utf-8') as f:
        data_str = f.read()
    with open(template_path, encoding='utf-8') as f:
        template = f.read()
    html = template.replace('DASHBOARD_DATA_PLACEHOLDER', data_str)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"build_html: written → {out_path}")


def main():
    parser = argparse.ArgumentParser(description='MABUI monitor ビルドスクリプト')
    parser.add_argument('--today', action='store_true', help='当日分のみ（週次収集スキップ）')
    parser.add_argument('--skip-scrape', action='store_true', help='スクレイプスキップ（エクスポートのみ）')
    args = parser.parse_args()

    from db_setup import init_db, DB_PATH

    # ── DB 初期化 ──────────────────────────────────────────────────────────
    init_db(DB_PATH)

    # ── スクレイプ ─────────────────────────────────────────────────────────
    if not args.skip_scrape:
        if args.today:
            logger.info("=== daily_monitor ===")
            from daily_monitor import run_once
            run_once(DB_PATH)
        else:
            logger.info("=== weekly_collector ===")
            from weekly_collector import run
            run(DB_PATH)
            logger.info("=== daily_monitor ===")
            from daily_monitor import run_once
            run_once(DB_PATH)

    # ── JSON エクスポート ─────────────────────────────────────────────────
    logger.info("=== export_data ===")
    from export_data import export_all
    json_path = os.path.join(DATA_DIR, 'dashboard_data.json')
    export_all(DB_PATH, json_path)

    # ── HTML ビルド ───────────────────────────────────────────────────────
    logger.info("=== build HTML ===")
    template_path = os.path.join(DASH_DIR, 'dashboard_template.html')
    dashboard_out = os.path.join(DASH_DIR, 'dashboard.html')
    docs_out      = os.path.join(DOCS_DIR, 'index.html')

    build_html(json_path, template_path, dashboard_out)

    os.makedirs(DOCS_DIR, exist_ok=True)
    shutil.copy2(dashboard_out, docs_out)
    logger.info(f"Copied → {docs_out}")

    logger.info("=== BUILD COMPLETE ===")


if __name__ == '__main__':
    main()
