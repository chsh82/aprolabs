"""검수 결과가 쌓이는 edition_store.db를 매일 백업 - 2026-09-28 배포 [7].

sqlite3의 온라인 백업 API(Connection.backup)를 쓴다 - 서비스가 켜져서 DB에
계속 쓰고 있는 중에도 파일을 그냥 cp하는 것과 달리 손상 없이 안전하게
복제된다. 14일 넘은 백업은 지운다.

cron 예시(매일 새벽 4시): 서버에서 `crontab -e`로 아래 한 줄 추가
  0 4 * * * /home/chsh82/aprolabs/venv/bin/python3 /home/chsh82/aprolabs/momo_b2b_tablet/scripts/backup_edition_db.py
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "edition" / "edition_store.db"
BACKUP_DIR = Path(__file__).resolve().parent.parent / "edition_store_backups"
KEEP_DAYS = 14


def main() -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUP_DIR / f"edition_store_{ts}.db"

    src_conn = sqlite3.connect(DB_PATH)
    dest_conn = sqlite3.connect(dest)
    try:
        src_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        src_conn.close()
    print(f"백업 완료: {dest} ({dest.stat().st_size:,} bytes)")

    cutoff = time.time() - KEEP_DAYS * 86400
    removed = 0
    for f in BACKUP_DIR.glob("edition_store_*.db"):
        if f.stat().st_mtime < cutoff:
            f.unlink()
            removed += 1
    if removed:
        print(f"{KEEP_DAYS}일 넘은 백업 {removed}개 삭제")


if __name__ == "__main__":
    main()
