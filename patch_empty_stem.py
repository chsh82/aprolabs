"""
patch_empty_stem.py — 빈 stem 수정 패치
bogi 필드에서 발문 본문을 추출해 stem으로 이동.

대상: stem이 비어있고 bogi가 있는 문항들
패턴: bogi = "[실제보기내용]\n\n[발문본문]것은?"
      → stem = "[발문본문]것은?"
      → bogi = "[실제보기내용]"

또는: bogi에 발문이 포함되어 있고 stem이 빈 경우.

사용법:
  python3 patch_empty_stem.py --dry-run [--file FILENAME] [--targets 3,7,10,...]
"""
import json, sqlite3, re, argparse
from datetime import datetime

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
_QNUM_PREFIX = re.compile(r'^\d+\.\s*[\u3000\s]*')
# 발문 종결 패턴: "것은?" 으로 끝나는 문장
STEM_END_RE = re.compile(r'것은\?(?:\s*\[\d점\])?')

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

    stem = _QNUM_PREFIX.sub('', str(q.get('stem') or '')).strip()
    bogi = str(q.get('bogi') or '').strip()

    print(f"=== Q{n} ===")
    print(f"  stem({len(stem)}자): {repr(stem[:80])}")
    print(f"  bogi({len(bogi)}자): {repr(bogi[:120])}")

    if not bogi:
        print(f"  → SKIP: bogi도 비어있음\n")
        skipped.append((n, 'bogi_empty'))
        continue

    # 전략: bogi 안에서 마지막 "것은?" 위치를 찾아
    # bogi = [실제보기내용 + \n\n + 발문] 또는 [발문만] 형태 파악
    stem_match = None
    # bogi 안에서 "것은?" 으로 끝나는 발문 찾기
    for m in STEM_END_RE.finditer(bogi):
        stem_match = m  # 마지막 매칭 사용

    if stem_match is None:
        print(f"  → SKIP: bogi에 발문('것은?') 없음 — 구조 불명확\n")
        skipped.append((n, 'no_stem_in_bogi'))
        continue

    stem_end = stem_match.end()

    # 발문이 bogi 맨 앞에 있는지, 중간/끝에 있는지 판단
    # "\n\n" 구분자로 발문과 실제보기 분리 시도
    # 경우 A: 발문\n\n실제보기
    # 경우 B: 실제보기\n\n발문  ← 이 경우가 더 흔함 (bogi박스 먼저, 발문 나중)
    # 경우 C: bogi에 발문만 (실제보기 없음)

    # "것은?" 이후에 내용이 있으면 → 발문이 중간에 있음 (경우 A 가능)
    after_stem = bogi[stem_end:].strip()
    before_stem = bogi[:stem_match.start()].strip()

    # 발문이 마지막에 위치 (경우 B): before_stem = 실제보기, stem_text = 발문
    # "\n\n" 또는 bogi 시작부터 발문까지
    # 발문 시작점 추정: "\n\n" 구분 또는 bogi 전체가 발문
    double_newline = bogi.rfind('\n\n', 0, stem_match.start())
    if double_newline != -1:
        actual_bogi = bogi[:double_newline].strip()
        extracted_stem = bogi[double_newline:stem_end].strip()
    else:
        # 구분자 없음 → 전체가 발문이거나 구분 불가
        actual_bogi = ''
        extracted_stem = bogi[:stem_end].strip()

    print(f"  → 추출 발문: {repr(extracted_stem[:80])}")
    print(f"  → 남은 bogi: {repr(actual_bogi[:80])}")
    if after_stem:
        print(f"  → 발문 뒤 잔여: {repr(after_stem[:60])} ← 주의 필요")
    print()

    if not extracted_stem:
        skipped.append((n, 'extraction_failed'))
        continue

    if not dry_run:
        q['stem'] = extracted_stem
        q['bogi'] = actual_bogi if actual_bogi else None
        fixed.append(n)

print("=" * 60)
if not dry_run and fixed:
    segs['questions'] = json.dumps(qs, ensure_ascii=False)
    db.execute(
        "UPDATE pipeline_jobs SET segments=?, updated_at=? WHERE id=?",
        (json.dumps(segs, ensure_ascii=False), datetime.utcnow().isoformat(), job_id)
    )
    db.commit()
    print(f"[APPLIED] {sorted(fixed)}건 수정")
elif dry_run:
    print(f"[DRY-RUN] 수정 예정 후보: {len([n for n,_ in skipped if False] + fixed)}건")

print(f"스킵: {skipped}")
db.close()
