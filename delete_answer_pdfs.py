"""
delete_answer_pdfs.py — 정답해설 PDF 레코드 시스템에서 제거
============================================================
사용법:
  python3 delete_answer_pdfs.py --investigate   # 조사만 (변경 없음)
  python3 delete_answer_pdfs.py --backup        # JSON 백업 생성
  python3 delete_answer_pdfs.py --delete        # dry-run (삭제 대상 출력)
  python3 delete_answer_pdfs.py --delete --apply  # 실제 삭제 실행
"""
import sqlite3, json, os, argparse, shutil
from datetime import datetime

DB_PATH    = '/home/chsh82/aprolabs/aprolabs.db'
BACKUP_DIR = '/home/chsh82/aprolabs'
UPLOADS    = '/home/chsh82/aprolabs/uploads/suneung'

FILTER_SQL = "filename LIKE '%정답%' OR filename LIKE '%해설%'"


def get_jobs(db):
    return db.execute(
        f"SELECT * FROM pipeline_jobs WHERE {FILTER_SQL}"
    ).fetchall()


def get_tables(db):
    return [r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]


def find_related(db, tables, job_ids):
    """job_id / pipeline_job_id 참조 레코드 수집."""
    refs = {}  # table → [(col, job_id, count)]
    for table in tables:
        if table == 'pipeline_jobs':
            continue
        try:
            cols = [c[1] for c in db.execute(f'PRAGMA table_info({table})').fetchall()]
        except Exception:
            continue
        for col in ('job_id', 'pipeline_job_id'):
            if col not in cols:
                continue
            for jid in job_ids:
                cnt = db.execute(
                    f'SELECT COUNT(*) FROM {table} WHERE {col}=?', (jid,)
                ).fetchone()[0]
                if cnt:
                    refs.setdefault(table, []).append((col, jid, cnt))
    return refs


# ── --investigate ─────────────────────────────────────────────────────────────

def investigate():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    tables = get_tables(db)
    jobs   = get_jobs(db)

    print("=" * 60)
    print("STEP 1: 조사 결과")
    print("=" * 60)

    print(f"\n전체 테이블: {tables}\n")

    print(f"정답해설 레코드: {len(jobs)}건")
    job_ids = []
    for r in jobs:
        job_ids.append(r['id'])
        fp = r['file_path']
        img_dir = os.path.join(UPLOADS, r['id'], 'images') if fp else None
        img_cnt = len(os.listdir(img_dir)) if img_dir and os.path.isdir(img_dir) else 0
        print(f"\n  id       : {r['id']}")
        print(f"  filename : {r['filename']}")
        print(f"  file_path: {fp}")
        print(f"  status   : {r['status']}")
        print(f"  images   : {img_cnt}개 ({img_dir or 'N/A'})")

    refs = find_related(db, tables, job_ids)
    print(f"\n연관 테이블 참조:")
    if refs:
        for table, items in refs.items():
            for col, jid, cnt in items:
                print(f"  {table}.{col} = {jid}: {cnt}건")
    else:
        print("  없음 — pipeline_jobs 단독 삭제로 충분")

    db.close()


# ── --backup ──────────────────────────────────────────────────────────────────

def backup():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    jobs = get_jobs(db)

    # DB 파일 전체 복사
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    db_bak = os.path.join(BACKUP_DIR, f'aprolabs.db.bak_before_delete_{ts}')
    shutil.copy2(DB_PATH, db_bak)
    print(f"DB 백업: {db_bak}")

    # 레코드 JSON 백업
    records = []
    for r in jobs:
        records.append(dict(r))
    json_bak = os.path.join(BACKUP_DIR, f'backup_answer_pdfs_{ts}.json')
    with open(json_bak, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    print(f"레코드 백업 ({len(records)}건): {json_bak}")

    db.close()


# ── --delete [--apply] ────────────────────────────────────────────────────────

def delete(apply: bool):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    tables = get_tables(db)
    jobs   = get_jobs(db)
    job_ids = [r['id'] for r in jobs]

    print("=" * 60)
    print(f"STEP 3: 삭제 {'실행' if apply else 'dry-run'}")
    print("=" * 60)

    if not jobs:
        print("삭제 대상 없음.")
        db.close()
        return

    refs = find_related(db, tables, job_ids)

    # 연관 테이블 먼저
    for table, items in refs.items():
        for col, jid, cnt in items:
            print(f"\n  [{table}] {col}={jid} {cnt}건 삭제")
            if apply:
                db.execute(f'DELETE FROM {table} WHERE {col}=?', (jid,))

    # pipeline_jobs
    for r in jobs:
        print(f"\n  [pipeline_jobs] {r['filename']} ({r['id']}) 삭제")
        # uploads 이미지 폴더
        img_dir = os.path.join(UPLOADS, r['id'])
        if os.path.isdir(img_dir):
            print(f"  [uploads] {img_dir} 디렉토리 삭제")
            if apply:
                shutil.rmtree(img_dir)
        # PDF 파일
        if r['file_path']:
            pdf_path = os.path.join('/home/chsh82/aprolabs', r['file_path'])
            if os.path.isfile(pdf_path):
                print(f"  [uploads] {pdf_path} 삭제")
                if apply:
                    os.remove(pdf_path)

    if apply:
        db.execute(f'DELETE FROM pipeline_jobs WHERE {FILTER_SQL}')
        db.commit()
        # 검증
        remaining = db.execute(
            f'SELECT COUNT(*) FROM pipeline_jobs WHERE {FILTER_SQL}'
        ).fetchone()[0]
        print(f"\n  삭제 완료. 남은 정답해설 레코드: {remaining}건")
    else:
        print(f"\n[dry-run] 위 항목이 삭제됩니다. --apply 추가 시 실제 실행.")

    db.close()


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--investigate', action='store_true')
    parser.add_argument('--backup',      action='store_true')
    parser.add_argument('--delete',      action='store_true')
    parser.add_argument('--apply',       action='store_true', help='--delete와 함께 사용')
    args = parser.parse_args()

    if args.investigate:
        investigate()
    elif args.backup:
        backup()
    elif args.delete:
        delete(apply=args.apply)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
