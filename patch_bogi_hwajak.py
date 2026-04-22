"""
patch_bogi_hwajak.py — 2024 화작 Q15/Q36/Q40/Q42 bogi 분리 패치
stem에서 발문 종결("것은?\n\n") 이후를 bogi로 분리.
"""
import json, sqlite3, re
from datetime import datetime

DB_PATH  = '/home/chsh82/aprolabs/aprolabs.db'
FILENAME = '2024학년도 수능_국어(화작) 문제.pdf'
TARGETS  = {15, 36, 40, 42}

# 발문 종결 후 bogi 시작점: "것은?" 또는 "것은? [3점]" 뒤의 \n\n
STEM_END_RE = re.compile(r'(것은\?(?:\s*\[\d점\])?)\s*\n\n', re.DOTALL)

import sys
dry_run = '--dry-run' in sys.argv

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
row = db.execute(
    "SELECT id, segments FROM pipeline_jobs WHERE filename=?", (FILENAME,)
).fetchone()

if not row:
    print(f"파일 없음: {FILENAME}")
    exit(1)

job_id = row['id']
segs = json.loads(row['segments'])
qs_raw = segs.get('questions', '[]')
qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw

fixed = []
for q in qs:
    n = q.get('number')
    if n not in TARGETS:
        continue

    stem = str(q.get('stem') or '')
    current_bogi = q.get('bogi')

    m = STEM_END_RE.search(stem)
    if not m:
        print(f"Q{n}: 분리자 없음 — 수동 확인 필요")
        print(f"  stem={repr(stem[:120])}")
        continue

    new_stem = stem[:m.end(1)].strip()   # "것은?" 까지
    new_bogi = stem[m.end():].strip()    # \n\n 이후

    if not new_bogi:
        print(f"Q{n}: bogi 내용 없음 — 스킵")
        continue

    print(f"Q{n}")
    print(f"  BEFORE stem ({len(stem)}자): {repr(stem[:100])}")
    print(f"  AFTER  stem ({len(new_stem)}자): {repr(new_stem[:100])}")
    print(f"  NEW    bogi ({len(new_bogi)}자): {repr(new_bogi[:100])}")
    print()

    if not dry_run:
        q['stem'] = new_stem
        q['bogi'] = new_bogi
        fixed.append(n)

if not dry_run and fixed:
    segs['questions'] = json.dumps(qs, ensure_ascii=False)
    db.execute(
        "UPDATE pipeline_jobs SET segments=?, updated_at=? WHERE id=?",
        (json.dumps(segs, ensure_ascii=False), datetime.utcnow().isoformat(), job_id)
    )
    db.commit()
    print(f"[APPLIED] {sorted(fixed)}건 수정 완료")
elif dry_run:
    print("[DRY-RUN] DB 수정 없음")

db.close()
