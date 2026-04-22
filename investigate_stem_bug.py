"""
investigate_stem_bug.py — 유형B stem 버그 패턴 조사
DB에서 Q3/Q7/Q10/Q16/Q21/Q23/Q27/Q31/Q34/Q37/Q44/Q45의 실제 stem 내용 확인
수정 없이 보고만.
"""
import json, sqlite3, re

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
FILENAME_FILTER = '2024학년도 수능_국어(화작) 문제.pdf'
TARGETS = {3, 7, 10, 16, 21, 23, 27, 31, 34, 37, 44, 45}

_QNUM_PREFIX = re.compile(r'^\d+\.\s*[\u3000\s]*')

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
row = db.execute(
    "SELECT segments FROM pipeline_jobs WHERE filename = ?", (FILENAME_FILTER,)
).fetchone()

if not row:
    print(f"파일 없음: {FILENAME_FILTER}")
    exit(1)

segs = json.loads(row['segments'])
qs_raw = segs.get('questions', '[]')
qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
ps_raw = segs.get('passages', '[]')
ps = json.loads(ps_raw) if isinstance(ps_raw, str) else ps_raw

print(f"=== 유형B stem 버그 조사 ({FILENAME_FILTER}) ===\n")
print(f"{'Q':>3}  {'stem 길이':>6}  {'stem 내용(100자)':50}  {'bogi 유무'}")
print("-" * 90)

empty_stem = []    # stem이 번호만
short_stem = []    # stem이 50자 미만 (부분 추출)
bogi_in_stem = []  # stem에 < 보 기 > 포함

for q in qs:
    n = q.get('number')
    if n not in TARGETS:
        continue
    stem = str(q.get('stem') or '')
    bogi = str(q.get('bogi') or '')
    stem_stripped = _QNUM_PREFIX.sub('', stem).strip()

    category = ''
    if not stem_stripped:
        category = '⚠ EMPTY'
        empty_stem.append(n)
    elif len(stem_stripped) < 10:
        category = '⚠ 번호만'
        empty_stem.append(n)
    elif re.search(r'<\s*보\s*기\s*>', stem):
        category = '★ bogi-in-stem'
        bogi_in_stem.append(n)
    elif len(stem_stripped) < 50:
        category = '△ 짧음'
        short_stem.append(n)
    else:
        category = '✓ 정상'

    bogi_flag = '있음' if bogi and bogi.strip() and bogi != 'None' else '없음'
    print(f"Q{n:>2}  {len(stem_stripped):>6}자  {category:<16} {repr(stem_stripped[:60]):<60}  bogi={bogi_flag}")

db.close()

print("\n=== 패턴 요약 ===")
print(f"  번호만/비어있음: {sorted(empty_stem)}  ({len(empty_stem)}건)")
print(f"  bogi-in-stem:   {sorted(bogi_in_stem)}  ({len(bogi_in_stem)}건)")
print(f"  짧은 stem:      {sorted(short_stem)}  ({len(short_stem)}건)")
print()

# 공통 패턴 추측
all_empty_vision = [3, 16, 21, 23, 27, 31, 34, 44, 45]  # sim=0.0 in report
all_partial = [7, 10, 37]  # sim<0.5 but not 0

print("=== 추정 원인 ===")
print("  sim=0.0 그룹 (DB='N.'): Q3/16/21/23/27/31/34/44/45")
print("  → stem 앞에 <보기> 블록이 있어 segmenter가 stem 시작점을 못 찾음?")
print("  → 또는 해당 문항이 다음 페이지에서 시작해 segmenter가 연결 못 함?")
print()
print("  sim<0.5 그룹 (DB 부분 추출): Q7/10/37")
print("  → 원문자 불일치(㉠→㉡) 또는 stem이 도중에 잘림")
