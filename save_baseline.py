"""
현재 DB 상태를 golden baseline으로 저장.
실행: python3 save_baseline.py
출력: /home/chsh82/aprolabs/golden_tests/baseline_YYYYMMDD.json

캡처 항목:
  - 파일별 경고 카테고리 집계
  - 각 경고의 fingerprint (file+location+category+message 앞 80자)
  - bleed-in 건수 (선택지 / 지문) — HEADER_KW 키워드 기반
"""
import sqlite3, json, re, os, hashlib
from datetime import datetime
from collections import Counter

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
OUTPUT_DIR = '/home/chsh82/aprolabs/golden_tests'
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

# ── 경고 카테고리 분류 ────────────────────────────────────────────────────
def categorize(msg):
    m = msg
    if '<u>' in m and '미확인' in m:
        return '밑줄_JSON→PDF미확인'
    if 'PDF 밑줄 텍스트' in m and ('못' in m or '찾' in m):
        return '밑줄_PDF→JSON못찾음'
    if '<img>' in m and '[그림]' in m:
        return 'img_그림불일치'
    if '이미지' in m and '개수' in m:
        return '이미지개수불일치'
    if '텍스트 불일치' in m and re.search(r'\[[A-E]\]', m):
        return 'bracket텍스트불일치'
    if '끝 위치 특정 불가' in m or '시작 위치는 찾았' in m:
        return 'bracket위치특정불가'
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
    """선택지 텍스트에서 페이지 헤더 키워드 건수."""
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
    """지문 content 내 국어영역+고3 쌍 (실제 헤더 삽입 이벤트 수)."""
    count = 0
    for p in passages:
        count += len(HEADER_PAIR.findall(p.get('content', '')))
    return count

# ── fingerprint ──────────────────────────────────────────────────────────
def fingerprint(fname, loc, cat, msg):
    raw = f"{fname}|{loc}|{cat}|{msg[:80]}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()[:12]

# ── DB 읽기 ──────────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("SELECT filename, segments, raw_result FROM pipeline_jobs WHERE raw_result IS NOT NULL")
rows = cur.fetchall()
conn.close()

baseline = {
    'saved_at': datetime.now().isoformat(timespec='seconds'),
    'db_path': DB_PATH,
    'summary': {},
    'files': {}
}

all_cats = Counter()

for fname, segments_raw, raw_json in rows:
    if not raw_json:
        continue
    try:
        data = json.loads(raw_json)
    except Exception:
        continue
    if not isinstance(data, dict):
        continue

    # segments에서 bleed-in 스캔
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

    # 경고 파싱
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
        all_cats[cat] += 1
        fp = fingerprint(fname, loc, cat, msg)
        warnings.append({
            'fingerprint': fp,
            'location': loc,
            'category': cat,
            'message': msg[:120],
            'passage_number': v.get('passage_number', ''),
            'question_number': v.get('question_number', ''),
        })

    baseline['files'][fname] = {
        'total_warnings': len(warnings),
        'categories': dict(cats),
        'bleed_in': {
            'choices': bi_choices,
            'passages': bi_passages,
        },
        'warnings': warnings,
    }

total = sum(f['total_warnings'] for f in baseline['files'].values())
baseline['summary'] = {
    'total_warnings': total,
    'total_files': len(baseline['files']),
    'categories': dict(all_cats),
    'bleed_in': {
        'choices': sum(f['bleed_in']['choices'] for f in baseline['files'].values()),
        'passages': sum(f['bleed_in']['passages'] for f in baseline['files'].values()),
    }
}

# ── 저장 ─────────────────────────────────────────────────────────────────
date_str = datetime.now().strftime('%Y%m%d')
out_path = os.path.join(OUTPUT_DIR, f'baseline_{date_str}.json')
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(baseline, f, ensure_ascii=False, indent=2)

print(f"Baseline saved → {out_path}")
print(f"총 파일: {baseline['summary']['total_files']}")
print(f"총 경고: {baseline['summary']['total_warnings']}")
print(f"bleed-in 선택지: {baseline['summary']['bleed_in']['choices']}건")
print(f"bleed-in 지문:   {baseline['summary']['bleed_in']['passages']}건")
print("\n카테고리별:")
for cat, cnt in sorted(baseline['summary']['categories'].items(), key=lambda x: -x[1]):
    print(f"  {cat:<40} {cnt}건")

print("\n파일별 bleed-in:")
for fname, fdata in baseline['files'].items():
    bi = fdata['bleed_in']
    if bi['choices'] or bi['passages']:
        print(f"  {fname[-40:]:<42} 선택지:{bi['choices']} 지문:{bi['passages']}")
