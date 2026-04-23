"""
run_patches.py — patch_bogi_hwajak 자동 스캔 + dry-run / apply
================================================================
사용법:
  python3 run_patches.py            # dry-run (기본)
  python3 run_patches.py --apply    # 실제 적용
"""
import sqlite3, json, re, argparse

DB = '/home/chsh82/aprolabs/aprolabs.db'
STEM_END_RE = re.compile(r'(것은\?(?:\s*\[\d점\])?)\s*\n\n', re.DOTALL)

SEARCH_TERMS = [
    '2024학년도 수능_국어(언매)',
    '2025 10월 학력평가 국어(언매) 문제',
    '2025 10월 학력평가 국어(화작) 문제',
    '2025 7월 학력평가 국어(언매) 문제',
    '2025 7월 학력평가 국어(화작) 문제',
    '2026 9월 모의평가 국어(언매) 문제',
    '2026 9월 모의평가 국어(화작) 문제',
    '2026 수능 국어(언매) 문제',
]

def scan_and_patch(dry_run: bool):
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row

    grand_total = 0

    for term in SEARCH_TERMS:
        rows = db.execute(
            "SELECT id, filename, segments FROM pipeline_jobs "
            "WHERE filename LIKE ? AND filename NOT LIKE '%정답%'",
            (f'%{term}%',)
        ).fetchall()

        for row in rows:
            fname = row['filename']
            segs = json.loads(row['segments'])
            qs_raw = segs.get('questions', '[]')
            qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw

            hits = []
            for q in qs:
                if not isinstance(q, dict): continue
                stem = str(q.get('stem') or '')
                bogi = str(q.get('bogi') or '').strip()
                if STEM_END_RE.search(stem) and (not bogi or bogi == 'None'):
                    hits.append(q)

            print(f'\n{"="*60}')
            print(f'파일: {fname}')

            if not hits:
                print('  → 패치 대상 없음')
                continue

            print(f'  패치 대상: {len(hits)}건  '
                  f'(Q{", Q".join(str(q.get("number","?")) for q in hits)})')

            changed = []
            for q in hits:
                stem = str(q.get('stem') or '')
                m = STEM_END_RE.search(stem)
                if not m:
                    continue
                new_stem = stem[:m.end(1)].strip()
                new_bogi = stem[m.end():].strip()
                if not new_bogi:
                    print(f'  Q{q.get("number")}: bogi 내용 없음 — skip')
                    continue

                print(f'  Q{q.get("number")}:')
                print(f'    stem BEFORE: {repr(stem[:80])}')
                print(f'    stem AFTER : {repr(new_stem)}')
                print(f'    bogi NEW   : {repr(new_bogi[:80])}')

                if not dry_run:
                    q['stem'] = new_stem
                    q['bogi'] = new_bogi
                    changed.append(q.get('number'))

            grand_total += len(hits)

            if not dry_run and changed:
                segs_copy = dict(segs)
                if isinstance(segs.get('questions'), str):
                    segs_copy['questions'] = json.dumps(qs, ensure_ascii=False)
                else:
                    segs_copy['questions'] = qs
                db.execute(
                    "UPDATE pipeline_jobs SET segments = ? WHERE id = ?",
                    (json.dumps(segs_copy, ensure_ascii=False), row['id'])
                )
                db.commit()
                print(f'  → DB 업데이트 완료 (Q{changed})')

    db.close()
    print(f'\n{"="*60}')
    print(f'전체 패치 대상: {grand_total}건')
    if dry_run:
        print('※ dry-run 완료. --apply 추가 시 실제 적용.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='실제 DB 적용 (기본: dry-run)')
    args = parser.parse_args()
    scan_and_patch(dry_run=not args.apply)
