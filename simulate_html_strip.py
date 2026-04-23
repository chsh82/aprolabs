"""
simulate_html_strip.py — HTML 태그 정규화 적용 시 분류 변화 시뮬레이션
=======================================================================
현재 QA 리포트 + DB를 기반으로 두 가지 sim() 버전을 비교:

  OLD: 공백 + prefix 제거만 (현재 auto_qa_agent 코드)
  NEW: 공백 + prefix + HTML 서식 태그 제거

결과: 각 mismatch가 OLD/NEW 기준으로 어떻게 재분류되는지 비교.

수정 없음 — 순수 시뮬레이션.
"""
import json, glob, os, re, sqlite3, difflib
from collections import defaultdict

GOLDEN_DIR = '/home/chsh82/aprolabs/golden_tests'
DB_PATH    = '/home/chsh82/aprolabs/aprolabs.db'

TARGET_FILES = {
    '2025 10월 학력평가 국어(언매) 문제.pdf',
    '2025 10월 학력평가 국어(화작) 문제.pdf',
    '2025 7월 학력평가 국어(언매) 문제.pdf',
    '2025 7월 학력평가 국어(화작) 문제.pdf',
    '2026 9월 모의평가 국어(언매) 문제.pdf',
    '2026 9월 모의평가 국어(화작) 문제.pdf',
}
TARGET_PATTERN = re.compile(r'국어\(\)')

# ── 두 가지 정규화 함수 ───────────────────────────────────────────────────
QNUM_RE       = re.compile(r'^\d+\.\s*[\u3000\s]*')
HTML_FORMAT   = re.compile(r'</?(?:u|b|i|em|strong)\b[^>]*>')

def _norm_old(text: str) -> str:
    """현재 버전: 공백 + prefix만"""
    t = re.sub(r'\s+', ' ', str(text or '')).strip()
    t = QNUM_RE.sub('', t).strip()
    return t

def _norm_new(text: str) -> str:
    """개선 버전: 공백 + prefix + HTML 서식 태그 제거"""
    t = re.sub(r'\s+', ' ', str(text or '')).strip()
    t = QNUM_RE.sub('', t).strip()
    t = HTML_FORMAT.sub('', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def sim_old(a, b) -> float:
    a, b = _norm_old(a), _norm_old(b)
    if not a and not b: return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def sim_new(a, b) -> float:
    a, b = _norm_new(a), _norm_new(b)
    if not a and not b: return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()

QA_THRESHOLD   = 0.60  # auto_qa_agent의 stem 불일치 기준 (< 이면 불일치)
CLASS_THRESHOLD = 0.80  # 유형A 분류 기준 (≥ 이면 A)

# ── DB 로드 ───────────────────────────────────────────────────────────────
db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
jobs = db.execute(
    "SELECT id, filename, segments FROM pipeline_jobs WHERE segments IS NOT NULL"
).fetchall()
db.close()

db_segs = {}
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

# ── 최신 QA 리포트 ────────────────────────────────────────────────────────
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

# ── 시뮬레이션 ────────────────────────────────────────────────────────────
BOGI_IN_STEM = re.compile(r'<보기>')
STEM_END_NN  = re.compile(r'것은\?(?:\s*\[\d점\])?\s*\n\n')
STEM_END_SP  = re.compile(r'것은\?(?:\s*\[\d점\])?\s+[\u3000<]')

print("\n" + "=" * 80)
print("STEP 3: sim() HTML 정규화 전/후 분류 변화 시뮬레이션")
print("=" * 80)
print(f"\n  임계값: QA_THRESHOLD={QA_THRESHOLD}, CLASS_THRESHOLD={CLASS_THRESHOLD}")
print(f"  (OLD=현재코드, NEW=HTML서식태그 추가 제거)\n")

grand = defaultdict(int)  # 전체 집계
file_results = {}

for fname in sorted(TARGET_FILES | {f for f in reports if TARGET_PATTERN.search(f)}):
    if fname not in reports:
        continue
    ts, fr = reports[fname]
    q_map = db_segs.get(fname, {})

    # 결과 카테고리
    results = {
        'bogi_X':   0, 'bogi_Y':   0,
        # 발문: "OLD/NEW" 조합
        'stem_disappears':    0,   # OLD 불일치, NEW 해소 (oetam)
        'stem_A_remains':     0,   # NEW sim ≥ CLASS (A급 오탐, 여전히 불일치 라벨)
        'stem_B_real':        0,   # empty DB stem / img-in-stem
        'stem_Z_real':        0,   # 내용 자체가 다름
    }
    detail_disappears = []
    detail_remain     = []

    for m in fr.get('mismatches', []):
        qnum = m.get('question_number')
        q_db = q_map.get(qnum, {})
        db_stem = str(q_db.get('stem', '') or '')
        db_bogi = str(q_db.get('bogi', '') or '')

        for issue in m.get('issues', []):
            field  = issue.get('field', '')
            vision = str(issue.get('vision', ''))
            db_val = str(issue.get('db', ''))

            # ── 보기 불일치 ──────────────────────────────────────────────
            if field == 'bogi':
                if STEM_END_NN.search(db_stem) or STEM_END_SP.search(db_stem):
                    results['bogi_X'] += 1
                else:
                    results['bogi_Y'] += 1

            # ── 발문 불일치 ──────────────────────────────────────────────
            elif field == 'stem':
                db_stripped = QNUM_RE.sub('', db_stem).strip()
                is_empty_db = len(db_stripped) < 5
                has_img_in_stem = '\n\n<img' in db_stem or '\n<img' in db_stem

                # ① 빈 DB stem
                if is_empty_db:
                    results['stem_B_real'] += 1
                    detail_remain.append(('B-empty', qnum, vision[:60], db_stem[:60]))
                # ② img-in-stem → patch_bogi_hwajak 대상
                elif has_img_in_stem:
                    results['stem_B_real'] += 1
                    detail_remain.append(('B-img', qnum, vision[:60], db_stem[:60]))
                else:
                    s_old = sim_old(vision, db_stem)
                    s_new = sim_new(vision, db_stem)

                    if s_new >= QA_THRESHOLD:
                        # NEW 정규화 후 임계값 넘음 → 재실행 시 사라질 것
                        results['stem_disappears'] += 1
                        detail_disappears.append((qnum, s_old, s_new, vision[:60], db_stem[:60]))
                    elif s_new >= CLASS_THRESHOLD * 0.75:  # 0.60 이하지만 유사한 경우
                        results['stem_A_remains'] += 1
                        detail_remain.append(('A?', qnum, f'old={s_old:.2f} new={s_new:.2f}', db_stem[:60]))
                    else:
                        # 내용 자체가 다름
                        results['stem_Z_real'] += 1
                        detail_remain.append(('Z', qnum, vision[:60], db_stem[:60]))

    file_results[fname] = (ts, results, detail_disappears, detail_remain)
    for k, v in results.items():
        grand[k] += v

# ── 파일별 출력 ──────────────────────────────────────────────────────────
print(f"\n{'파일':<50} {'보기X':>5} {'보기Y':>5} {'사라짐':>7} {'B실제':>6} {'Z실제':>6}")
print("-" * 85)
for fname, (ts, r, dis, rem) in sorted(file_results.items()):
    short = fname[-50:]
    print(f"{short:<50} {r['bogi_X']:>5} {r['bogi_Y']:>5} "
          f"{r['stem_disappears']:>7} {r['stem_B_real']:>6} {r['stem_Z_real']:>6}  [{ts}]")
print("-" * 85)
print(f"{'합계':<50} {grand['bogi_X']:>5} {grand['bogi_Y']:>5} "
      f"{grand['stem_disappears']:>7} {grand['stem_B_real']:>6} {grand['stem_Z_real']:>6}")

total_stem = grand['stem_disappears'] + grand['stem_B_real'] + grand['stem_Z_real'] + grand['stem_A_remains']
total_all  = grand['bogi_X'] + grand['bogi_Y'] + total_stem

print(f"\n  발문불일치 {total_stem}건 중:")
print(f"    정규화 개선으로 해소 (재실행 시 사라짐): {grand['stem_disappears']}건")
print(f"    실제 데이터 문제 (B - img/empty stem):  {grand['stem_B_real']}건")
print(f"    실제 내용 불일치 (Z):                   {grand['stem_Z_real']}건")

print(f"\n  보기불일치 {grand['bogi_X']+grand['bogi_Y']}건 중:")
print(f"    patch_bogi_hwajak 대상 (X):  {grand['bogi_X']}건")
print(f"    기타 (Y - 이미지없음/혼재):  {grand['bogi_Y']}건")

# ── 상세: 사라질 발문 불일치 ──────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4a: HTML 정규화 후 '사라지는' 발문 불일치 상세")
print("=" * 80)
for fname, (ts, r, dis, rem) in sorted(file_results.items()):
    if not dis:
        continue
    print(f"\n  [{fname[-45:]}]")
    for qnum, s_old, s_new, vision, db in dis:
        print(f"    Q{qnum}: old={s_old:.2f} → new={s_new:.2f}  (사라질 오탐)")
        print(f"      vision={repr(vision[:55])}")
        print(f"      db    ={repr(db[:55])}")

# ── 상세: 실제 남는 문제 ─────────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4b: 정규화 후에도 남는 실제 문제")
print("=" * 80)
for fname, (ts, r, dis, rem) in sorted(file_results.items()):
    if not rem:
        continue
    print(f"\n  [{fname[-45:]}]")
    for item in rem:
        if item[0].startswith('B'):
            t, qnum, v, d = item
            print(f"    Q{qnum} [{t}]: db={repr(d[:70])}")
        else:
            t, qnum, v, d = item
            print(f"    Q{qnum} [{t}]:")
            print(f"      vision={repr(v[:60])}")
            print(f"      db    ={repr(d[:60])}")

# ── 보기Y 상세 ───────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4c: 보기Y — 이미지없음 vs 국어()혼재 구분")
print("=" * 80)
for fname in sorted(TARGET_FILES | {f for f in reports if TARGET_PATTERN.search(f)}):
    if fname not in reports:
        continue
    ts, fr = reports[fname]
    q_map = db_segs.get(fname, {})
    y_items = []
    for m in fr.get('mismatches', []):
        qnum = m.get('question_number')
        q_db = q_map.get(qnum, {})
        db_stem = str(q_db.get('stem', '') or '')
        for issue in m.get('issues', []):
            if issue.get('field') == 'bogi':
                if not (STEM_END_NN.search(db_stem) or STEM_END_SP.search(db_stem)):
                    has_bogi_ref = bool(BOGI_IN_STEM.search(db_stem))
                    y_items.append((qnum, has_bogi_ref, db_stem[:80]))
    if not y_items:
        continue
    is_gookeo = TARGET_PATTERN.search(fname)
    print(f"\n  [{fname[-45:]}] {'← 합본파일' if is_gookeo else ''}")
    for qnum, has_ref, d in y_items:
        tag = '보기참조O' if has_ref else '보기참조X'
        print(f"    Q{qnum} [{tag}]: db={repr(d[:70])}")
print()
