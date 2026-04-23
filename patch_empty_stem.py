"""
patch_empty_stem.py — <보기> 분리 버그로 인한 빈 stem 수정
segmenter가 발문 중간 <보기>를 bogi 마커로 잘못 인식하여 발생한 문제 수정.

구조:
  stem = "" 또는 발문 앞부분
  bogi = [발문 나머지(것은?까지)] + <img...> (실제 보기 이미지)

수정:
  new_stem = (기존 stem) + "<보기>" + (bogi에서 것은? 까지)
  new_bogi = <img...> 또는 [A:START]\n<img...> 부분

사용법:
  python3 patch_empty_stem.py --dry-run [--file FILENAME] [--targets 3,7,...]
"""
import json, sqlite3, re, argparse
from datetime import datetime

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
_QNUM_PREFIX = re.compile(r'^\d+\.\s*[\u3000\s]*')
# 발문 종결 (이 위치까지가 stem)
STEM_END_RE = re.compile(r'것은\?(?:\s*\[\d점\])?')
# 실제 bogi 시작: <img 또는 [A:START]
BOGI_START_RE = re.compile(r'(\[A:START\]\s*)?<img\s')

parser = argparse.ArgumentParser()
parser.add_argument('--dry-run', action='store_true')
parser.add_argument('--file', default='2024학년도 수능_국어(화작) 문제.pdf')
parser.add_argument('--targets', default='3,7,10,16,21,23,27,31,34,37,44,45')
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

print(f"파일: {row['filename']}\n")
job_id = row['id']
segs = json.loads(row['segments'])
qs_raw = segs.get('questions', '[]')
qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw

fixed = []
skipped = []

for q in qs:
    n = q.get('number')
    if n not in targets:
        continue

    stem_raw = _QNUM_PREFIX.sub('', str(q.get('stem') or '')).strip()
    bogi = str(q.get('bogi') or '').strip()

    print(f"Q{n}")

    if not bogi:
        print(f"  SKIP: bogi 비어있음\n")
        skipped.append((n, 'bogi_empty'))
        continue

    # bogi에서 실제 이미지 시작점 찾기
    img_m = BOGI_START_RE.search(bogi)
    if not img_m:
        print(f"  SKIP: bogi에 <img> 없음 ({repr(bogi[:60])})\n")
        skipped.append((n, 'no_img_in_bogi'))
        continue

    img_start = img_m.start()
    stem_continuation = bogi[:img_start].strip()  # 발문 나머지
    new_bogi = bogi[img_m.start():].strip()        # <img...> 또는 [A:START]\n<img...>

    # 발문 나머지에서 "것은?" 확인
    stem_end_m = STEM_END_RE.search(stem_continuation)
    if not stem_end_m:
        print(f"  SKIP: 발문 종결('것은?') 없음 ({repr(stem_continuation[:60])})\n")
        skipped.append((n, 'no_stem_end'))
        continue

    stem_part = stem_continuation[:stem_end_m.end()].strip()

    # 전체 발문 재구성: (기존 stem) + "<보기>" + (bogi에서 나온 발문 나머지)
    if stem_raw:
        new_stem = stem_raw + ' <보기>' + stem_part
    else:
        new_stem = '<보기>' + stem_part

    print(f"  OLD stem: {repr(stem_raw[:60]) if stem_raw else '(empty)'}")
    print(f"  OLD bogi: {repr(bogi[:80])}")
    print(f"  NEW stem: {repr(new_stem[:100])}")
    print(f"  NEW bogi: {repr(new_bogi[:80])}")
    print()

    if not dry_run:
        q['stem'] = new_stem
        q['bogi'] = new_bogi
        fixed.append(n)

print("=" * 60)
if not dry_run and fixed:
    segs['questions'] = json.dumps(qs, ensure_ascii=False)
    db.execute(
        "UPDATE pipeline_jobs SET segments=?, updated_at=? WHERE id=?",
        (json.dumps(segs, ensure_ascii=False), datetime.utcnow().isoformat(), job_id)
    )
    db.commit()
    print(f"[APPLIED] {sorted(fixed)}건 수정 완료")
elif dry_run:
    print(f"[DRY-RUN] DB 수정 없음 (수정 예정: {len(targets) - len(skipped)}건)")

if skipped:
    print(f"스킵: {skipped}")

db.close()
