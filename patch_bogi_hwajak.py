"""
patch_bogi_hwajak.py — bogi 미분리 패치 ("다음은..." 형태 발문)
stem에서 발문 종결("것은?\n\n") 이후를 bogi로 분리.

사용법:
  python3 patch_bogi_hwajak.py --dry-run [--file FILENAME] [--targets 15,36,40]
"""
import json, sqlite3, re, argparse
from datetime import datetime

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'

# 발문 종결 후 bogi 시작점: "것은?" 또는 "것은? [3점]" 뒤의 \n\n
STEM_END_RE = re.compile(r'(것은\?(?:\s*\[\d점\])?)\s*\n\n', re.DOTALL)

parser = argparse.ArgumentParser()
parser.add_argument('--dry-run', action='store_true')
parser.add_argument('--file', default='2024학년도 수능_국어(화작) 문제.pdf', help='파일명 (부분 일치)')
parser.add_argument('--targets', default='15,36,40,42', help='쉼표 구분 문항번호')
args = parser.parse_args()

dry_run = args.dry_run
targets = set(int(x) for x in args.targets.split(',') if x.strip())

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
row = db.execute(
    "SELECT id, filename, segments FROM pipeline_jobs WHERE filename LIKE ?",
    (f'%{args.file}%',)
).fetchone()

if not row:
    print(f"파일 없음: {args.file}")
    exit(1)

print(f"파일: {row['filename']}")
job_id = row['id']
segs = json.loads(row['segments'])
qs_raw = segs.get('questions', '[]')
qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw

fixed = []
for q in qs:
    n = q.get('number')
    if n not in targets:
        continue

    stem = str(q.get('stem') or '')
    current_bogi = q.get('bogi')

    if current_bogi and str(current_bogi).strip() and str(current_bogi) != 'None':
        print(f"Q{n}: bogi 이미 있음 — 스킵 ({repr(str(current_bogi)[:40])})")
        continue

    m = STEM_END_RE.search(stem)
    if not m:
        print(f"Q{n}: 분리자 없음 — 수동 확인 필요")
        print(f"  stem={repr(stem[:120])}")
        continue

    new_stem = stem[:m.end(1)].strip()
    new_bogi = stem[m.end():].strip()

    if not new_bogi:
        print(f"Q{n}: bogi 내용 없음 — 스킵")
        continue

    print(f"Q{n}")
    print(f"  BEFORE stem ({len(stem)}자): {repr(stem[:100])}")
    print(f"  AFTER  stem ({len(new_stem)}자): {repr(new_stem)}")
    print(f"  NEW    bogi ({len(new_bogi)}자): {repr(new_bogi[:120])}")
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
