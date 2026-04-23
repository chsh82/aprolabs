"""
classify_mismatches.py — 미처리 파일 불일치 패턴 분류 (수정 없음)
=================================================================
QA 리포트 + DB segments를 조합하여 각 불일치를 유형 분류.

보기불일치:
  X: stem에 "것은?\n\n..." 또는 "것은? \n..." 패턴 → patch_bogi_hwajak 대상
  Y: 기타

발문불일치:
  A: vision='' 또는 prefix·공백 차이 (오탐)
  B: DB stem이 비거나 <보기> 분리 버그 (빈 stem / 부분 stem) → patch_empty_stem 대상
  Z: 기타 (새 패턴)

사용법:
  python3 classify_mismatches.py
"""
import json, glob, os, re, sqlite3, difflib
from collections import defaultdict

GOLDEN_DIR = '/home/chsh82/aprolabs/golden_tests'
DB_PATH    = '/home/chsh82/aprolabs/aprolabs.db'

# 분류 대상 파일 (이미 처리된 파일 제외)
TARGET_FILES = {
    '2025 10월 학력평가 국어(언매) 문제.pdf',
    '2025 10월 학력평가 국어(화작) 문제.pdf',
    '2025 7월 학력평가 국어(언매) 문제.pdf',
    '2025 7월 학력평가 국어(화작) 문제.pdf',
    '2026 9월 모의평가 국어(언매) 문제.pdf',
    '2026 9월 모의평가 국어(화작) 문제.pdf',
}
# 국어() 파일은 이름 패턴으로 추가 매칭
TARGET_PATTERN = re.compile(r'국어\(\)')

# 패턴
STEM_END_NN  = re.compile(r'것은\?(?:\s*\[\d점\])?\s*\n\n')          # 유형X: \n\n 분리
STEM_END_SP  = re.compile(r'것은\?(?:\s*\[\d점\])?\s+[\u3000<]')      # 유형X: 공백+내용
BOGI_IN_STEM = re.compile(r'<보기>')                                  # 유형B: stem에 <보기>
QNUM_PREFIX  = re.compile(r'^\d+\.\s*[\u3000 ]*')

def sim(a, b):
    a = QNUM_PREFIX.sub('', re.sub(r'\s+', ' ', str(a or ''))).strip()
    b = QNUM_PREFIX.sub('', re.sub(r'\s+', ' ', str(b or ''))).strip()
    if not a and not b: return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()

# ── DB 로드 ───────────────────────────────────────────────────────────────
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
jobs = db.execute(
    "SELECT id, filename, segments FROM pipeline_jobs WHERE segments IS NOT NULL"
).fetchall()
db.close()

db_segs = {}  # filename → {q_num: q_dict}
for job in jobs:
    fname = job['filename']
    if fname not in TARGET_FILES and not TARGET_PATTERN.search(fname):
        continue
    try:
        segs = json.loads(job['segments'])
        qs_raw = segs.get('questions', '[]')
        qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
        db_segs[fname] = {q.get('number'): q for q in qs if isinstance(q, dict)}
    except Exception:
        db_segs[fname] = {}

# ── 최신 QA 리포트 로드 ───────────────────────────────────────────────────
def latest_reports():
    paths = sorted(glob.glob(f'{GOLDEN_DIR}/qa_report_*.json'))
    latest = {}
    for path in paths:
        try:
            data = json.load(open(path, encoding='utf-8'))
            ts = os.path.basename(path).replace('qa_report_', '').replace('.json', '')
            for fr in data:
                fn = fr.get('filename', '')
                if fn not in latest or ts > latest[fn][0]:
                    latest[fn] = (ts, fr)
        except Exception:
            pass
    return latest

reports = latest_reports()

# ── 분류 ──────────────────────────────────────────────────────────────────
results = {}   # filename → {bogi_X, bogi_Y, stem_A, stem_B, stem_Z, details}

for fname in sorted(TARGET_FILES | {f for f in reports if TARGET_PATTERN.search(f)}):
    if fname not in reports:
        print(f"  [SKIP] 리포트 없음: {fname[-45:]}")
        continue

    ts, fr = reports[fname]
    q_map = db_segs.get(fname, {})

    bogi_X = bogi_Y = stem_A = stem_B = stem_Z = 0
    details_bogi = []
    details_stem = []

    for m in fr.get('mismatches', []):
        qnum = m.get('question_number')
        q_db = q_map.get(qnum, {})
        db_stem = str(q_db.get('stem', '') or '')
        db_bogi = str(q_db.get('bogi', '') or '')

        for issue in m.get('issues', []):
            field   = issue.get('field', '')
            vision  = str(issue.get('vision', ''))
            db_val  = str(issue.get('db', ''))

            # ── 보기 불일치 ────────────────────────────────────────────
            if field == 'bogi':
                # 유형X: stem에 것은?\n\n 또는 공백+<img가 있는 경우
                if STEM_END_NN.search(db_stem) or STEM_END_SP.search(db_stem):
                    bogi_X += 1
                    details_bogi.append(('X', qnum, db_stem[:100]))
                else:
                    bogi_Y += 1
                    details_bogi.append(('Y', qnum, db_stem[:100]))

            # ── 발문 불일치 ────────────────────────────────────────────
            elif field == 'stem':
                # 유형A: vision이 빈 값 (Gemini 비결정성 오탐)
                if not vision or vision == 'None':
                    stem_A += 1
                    details_stem.append(('A-empty_vision', qnum, repr(vision[:40]), repr(db_stem[:60])))
                    continue

                # prefix 제거 후 유사도
                s = sim(vision, db_stem)

                # 유형B: DB stem이 비어있거나 아주 짧음 (번호만)
                db_stripped = QNUM_PREFIX.sub('', db_stem).strip()
                if len(db_stripped) < 5:
                    stem_B += 1
                    details_stem.append(('B-empty_db', qnum, repr(vision[:60]), repr(db_stem[:60])))
                # 유형B: stem에 <보기> 포함 (bogi 분리 버그)
                elif BOGI_IN_STEM.search(db_stem) and '<img' not in db_bogi:
                    stem_B += 1
                    details_stem.append(('B-bogi_in_stem', qnum, repr(vision[:60]), repr(db_stem[:60])))
                # 유형A: prefix 제거 후 유사도 높음 (오탐)
                elif s >= 0.80:
                    stem_A += 1
                    details_stem.append(('A-prefix_diff', qnum, f'sim={s:.2f}', repr(db_stem[:60])))
                else:
                    stem_Z += 1
                    details_stem.append(('Z', qnum, repr(vision[:70]), repr(db_stem[:70])))

    results[fname] = {
        'ts': ts,
        'bogi_X': bogi_X, 'bogi_Y': bogi_Y,
        'stem_A': stem_A, 'stem_B': stem_B, 'stem_Z': stem_Z,
        'details_bogi': details_bogi,
        'details_stem': details_stem,
    }

# ── 출력 ──────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 1: 파일별 불일치 유형 분류")
print("=" * 80)
print(f"\n{'파일':<48} {'보기X':>5} {'보기Y':>5} {'발A':>5} {'발B':>5} {'발Z':>5}")
print("-" * 80)

total = defaultdict(int)
for fname, r in sorted(results.items()):
    short = fname[-48:]
    print(f"{short:<48} {r['bogi_X']:>5} {r['bogi_Y']:>5} {r['stem_A']:>5} {r['stem_B']:>5} {r['stem_Z']:>5}  [{r['ts']}]")
    for k in ('bogi_X','bogi_Y','stem_A','stem_B','stem_Z'):
        total[k] += r[k]

print("-" * 80)
print(f"{'합계':<48} {total['bogi_X']:>5} {total['bogi_Y']:>5} {total['stem_A']:>5} {total['stem_B']:>5} {total['stem_Z']:>5}")

grand = sum(total.values())
known = total['bogi_X'] + total['stem_A'] + total['stem_B']
other = total['bogi_Y'] + total['stem_Z']
print(f"\n  전체 {grand}건 중 기타(Y+Z): {other}건 ({other/grand*100:.1f}%)")
if other/grand < 0.10:
    print("  → Case A: 일괄 배치 안전")
elif other/grand < 0.30:
    print("  → Case B: 유형별 순차 처리")
else:
    print("  → Case C: 새 유형 조사 필요")

# ── STEP 3: 기타(Y+Z) 상세 ───────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 3: 기타(보기Y + 발문Z) 상세")
print("=" * 80)

for fname, r in sorted(results.items()):
    y_items = [(t,q,d) for t,q,d in r['details_bogi'] if t == 'Y']
    z_items = [(t,q,v,d) for t,q,v,d in r['details_stem'] if t == 'Z']
    if not y_items and not z_items:
        continue
    print(f"\n  [{fname[-45:]}]")
    for t,q,d in y_items:
        print(f"    보기Y Q{q}: db_stem={repr(d[:80])}")
    for t,q,v,d in z_items:
        print(f"    발문Z Q{q}:")
        print(f"      vision={v}")
        print(f"      db    ={d}")

# ── STEP 3b: 유형B 상세 (stem_B 내역) ──────────────────────────────────
print("\n" + "=" * 80)
print("STEP 3b: 유형B 상세 (patch_empty_stem 대상 확인)")
print("=" * 80)
for fname, r in sorted(results.items()):
    b_items = r['details_stem']
    b_items = [(t,q,v,d) for t,q,v,d in b_items if t.startswith('B')]
    if not b_items:
        continue
    print(f"\n  [{fname[-45:]}]  ({len(b_items)}건)")
    for t,q,v,d in b_items:
        print(f"    Q{q} [{t}]: vision={v[:60]}  db={d[:60]}")

print()
