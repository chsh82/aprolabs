"""
현재 DB 상태를 저장된 baseline과 비교.
실행: python3 compare_baseline.py [baseline_file]
       baseline_file 생략 시 golden_tests/의 가장 최신 파일 자동 선택

출력:
  - 파일별 경고 변화 (개선 / 퇴보)
  - 새로 생긴 경고 목록 (regression)
  - 사라진 경고 목록 (improvement)
  - bleed-in 변화
"""
import sqlite3, json, re, sys, os, hashlib, glob
from datetime import datetime
from collections import Counter

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
GOLDEN_DIR = '/home/chsh82/aprolabs/golden_tests'

# ── baseline 파일 선택 ────────────────────────────────────────────────────
if len(sys.argv) > 1:
    baseline_path = sys.argv[1]
else:
    candidates = sorted(glob.glob(os.path.join(GOLDEN_DIR, 'baseline_*.json')))
    if not candidates:
        print("ERROR: baseline 파일 없음. save_baseline.py 먼저 실행하세요.")
        sys.exit(1)
    baseline_path = candidates[-1]

print(f"Baseline: {baseline_path}")
with open(baseline_path, encoding='utf-8') as f:
    baseline = json.load(f)

print(f"Baseline 저장일시: {baseline['saved_at']}")
print(f"Baseline 총 경고: {baseline['summary']['total_warnings']}")

# ── bleed-in 탐지 패턴 ───────────────────────────────────────────────────
# choices: <u> 태그 없는 일반 텍스트 → 키워드 기반
HEADER_KW = re.compile(
    r'국어영역|고3|고등학교|홀수형|짝수형|이 문제지에 관한 저작권'
)
# passages: <u> 태그 있는 content → 국어영역+고3 쌍을 1건으로 카운트
HEADER_PAIR = re.compile(
    r'<u>[^<]*국어영역[^<]*</u>\s*<u>[^<]*고3?[^<]*</u>'
    r'|<u>[^<]*고3?[^<]*</u>\s*<u>[^<]*국어영역[^<]*</u>'
)

# ── 카테고리 분류 ────────────────────────────────────────────────────────
def categorize(msg):
    m = msg
    if '<u>' in m and '미확인' in m:
        return '밑줄_JSON→PDF미확인'
    if 'PDF 밑줄 텍스트' in m and ('못' in m or '찾' in m):
        return '밑줄_PDF→JSON못찾음'
    if '<img>' in m and '[그림]' in m:
        return 'img_그림불일치'
    if '이미지' in m and '위치 불일치' in m:
        return 'img_그림불일치'
    if '이미지' in m and '개수' in m:
        return '이미지개수불일치'
    if '텍스트 불일치' in m and re.search(r'\[[A-E]\]', m):
        return 'bracket텍스트불일치'
    if re.search(r'\[[A-E]\]', m) and ('범위 내 텍스트' in m or '텍스트 미확인' in m):
        return 'bracket텍스트불일치'
    if '끝 위치 특정 불가' in m or '시작 위치는 찾았' in m:
        return 'bracket위치특정불가'
    if '선택지' in m and '불일치' in m:
        return '텍스트불일치'
    if '텍스트 불일치' in m:
        return '텍스트불일치'
    if '지문을 PDF에서 찾지 못' in m or '대응하는 PDF 지문' in m:
        return '지문못찾음'
    if '문항을 PDF에서 찾지 못' in m or 'PDF에서 해당 문항을' in m:
        return '문항못찾음'
    if 'SKIPPED' in m:
        return 'SKIPPED'
    return '미분류'

# ── bleed-in 감지 ────────────────────────────────────────────────────────
def count_bleed_in_choices(questions):
    count = 0
    for q in questions:
        choices = q.get('choices') or []
        if isinstance(choices, dict):
            choices = list(choices.values())
        for ch in choices:
            if isinstance(ch, str):
                text = ch
            elif isinstance(ch, dict):
                text = ch.get('text', '')
            else:
                text = str(ch)
            if HEADER_KW.search(text):
                count += 1
    return count

def count_bleed_in_passages(passages):
    count = 0
    for p in passages:
        count += len(HEADER_PAIR.findall(p.get('content', '')))
    return count

# ── fingerprint ──────────────────────────────────────────────────────────
def fingerprint(fname, loc, cat, msg):
    raw = f"{fname}|{loc}|{cat}|{msg[:80]}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()[:12]

# ── 현재 DB 읽기 ──────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("SELECT filename, segments, raw_result FROM pipeline_jobs WHERE raw_result IS NOT NULL")
rows = cur.fetchall()
conn.close()

current = {}
for fname, segments_raw, raw_json in rows:
    if not raw_json:
        continue
    try:
        data = json.loads(raw_json)
    except Exception:
        continue
    if not isinstance(data, dict):
        continue

    seg_passages, seg_questions = [], []
    if segments_raw:
        try:
            seg = json.loads(segments_raw)
            if isinstance(seg, dict):
                seg_passages = seg.get('passages', []) or []
                seg_questions = seg.get('questions', []) or []
        except Exception:
            pass

    bi_choices = count_bleed_in_choices(seg_questions)
    bi_passages = count_bleed_in_passages(seg_passages)

    vc = data.get('verify_corrections', [])
    warnings = []
    cats = Counter()
    for v in vc:
        if v.get('kind', '').lower() != 'warning':
            continue
        msg = v.get('message', '')
        loc = v.get('location', '')
        cat = categorize(msg)
        cats[cat] += 1
        fp = fingerprint(fname, loc, cat, msg)
        warnings.append({
            'fingerprint': fp,
            'location': loc,
            'category': cat,
            'message': msg[:120],
        })

    current[fname] = {
        'total_warnings': len(warnings),
        'categories': dict(cats),
        'bleed_in': {'choices': bi_choices, 'passages': bi_passages},
        'warnings': warnings,
    }

# ── 비교 ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("비교 결과 (baseline → 현재)")
print("=" * 70)

all_files = sorted(set(list(baseline['files'].keys()) + list(current.keys())))

total_improved = 0
total_regressed = 0
total_bi_choice_delta = 0
total_bi_passage_delta = 0

for fname in all_files:
    b = baseline['files'].get(fname)
    c = current.get(fname)
    short = fname[-35:]

    if b is None:
        print(f"\n  [NEW FILE] ...{short}")
        if c:
            print(f"    경고: +{c['total_warnings']}건")
        continue
    if c is None:
        print(f"\n  [REMOVED] ...{short}")
        continue

    b_total = b['total_warnings']
    c_total = c['total_warnings']
    delta = c_total - b_total

    b_fps = {w['fingerprint'] for w in b['warnings']}
    c_fps = {w['fingerprint'] for w in c['warnings']}

    new_warns = [w for w in c['warnings'] if w['fingerprint'] not in b_fps]
    gone_warns = [w for w in b['warnings'] if w['fingerprint'] not in c_fps]

    bi_choice_delta = c['bleed_in']['choices'] - b['bleed_in']['choices']
    bi_passage_delta = c['bleed_in']['passages'] - b['bleed_in']['passages']
    total_bi_choice_delta += bi_choice_delta
    total_bi_passage_delta += bi_passage_delta

    status = '✓ 변화없음' if delta == 0 else (f'▼ -{abs(delta)}건 개선' if delta < 0 else f'▲ +{delta}건 퇴보')
    print(f"\n  ...{short:<37} {b_total}→{c_total} ({status})")

    if gone_warns:
        total_improved += len(gone_warns)
        print(f"    [개선 {len(gone_warns)}건]")
        for w in gone_warns:
            print(f"      - {w['location']} | {w['category']} | {w['message'][:55]}")

    if new_warns:
        total_regressed += len(new_warns)
        print(f"    [퇴보 {len(new_warns)}건] ← REGRESSION")
        for w in new_warns:
            print(f"      + {w['location']} | {w['category']} | {w['message'][:55]}")

    if bi_choice_delta != 0 or bi_passage_delta != 0:
        print(f"    bleed-in 선택지: {b['bleed_in']['choices']}→{c['bleed_in']['choices']} ({bi_choice_delta:+d}), "
              f"지문: {b['bleed_in']['passages']}→{c['bleed_in']['passages']} ({bi_passage_delta:+d})")

# ── 전체 요약 ─────────────────────────────────────────────────────────────
b_total_all = baseline['summary']['total_warnings']
c_total_all = sum(f['total_warnings'] for f in current.values())
delta_all = c_total_all - b_total_all

print("\n" + "=" * 70)
print("전체 요약")
print("=" * 70)
print(f"  경고 총계: {b_total_all} → {c_total_all}  ({delta_all:+d})")
print(f"  개선된 경고: {total_improved}건")
print(f"  퇴보(REGRESSION): {total_regressed}건")
print(f"  bleed-in 선택지 합계: {baseline['summary']['bleed_in']['choices']} → "
      f"{sum(f['bleed_in']['choices'] for f in current.values())}  ({total_bi_choice_delta:+d})")
print(f"  bleed-in 지문 합계:   {baseline['summary']['bleed_in']['passages']} → "
      f"{sum(f['bleed_in']['passages'] for f in current.values())}  ({total_bi_passage_delta:+d})")

# 카테고리별 변화
b_cats = Counter(baseline['summary']['categories'])
c_cats = Counter()
for f in current.values():
    c_cats.update(f['categories'])

all_cats = sorted(set(list(b_cats.keys()) + list(c_cats.keys())))
print("\n  카테고리별 변화:")
for cat in all_cats:
    bc = b_cats.get(cat, 0)
    cc = c_cats.get(cat, 0)
    d = cc - bc
    mark = '' if d == 0 else (' ▼' if d < 0 else ' ▲ REGRESSION')
    print(f"    {cat:<40} {bc}→{cc} ({d:+d}){mark}")

if total_regressed == 0:
    print("\n  ✓ REGRESSION 없음")
else:
    print(f"\n  ✗ REGRESSION {total_regressed}건 발생 — 반드시 확인 필요")
