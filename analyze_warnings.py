"""
세 가지 확인을 수행하는 분석 스크립트:
  확인1: 카테고리 전수 집계 (합=150 검증, 누락 카테고리 포함)
  확인2: bracket 경고의 실제 PDF vs JSON diff 재추출
  확인3: 밑줄못찾음 distinct 텍스트 카운트
"""
import sqlite3, json, re, sys
from collections import Counter, defaultdict

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("SELECT filename, raw_result FROM pipeline_jobs WHERE raw_result IS NOT NULL")
rows = cur.fetchall()

all_warnings = []  # {file, location, message, detail, passage_number}

for fname, raw_json in rows:
    if not raw_json:
        continue
    try:
        data = json.loads(raw_json)
    except Exception:
        continue
    if not isinstance(data, dict):
        continue
    vc = data.get('verify_corrections', [])
    for v in vc:
        if v.get('kind', '').lower() == 'warning':
            all_warnings.append({
                'file': fname,
                'location': v.get('location', ''),
                'passage_number': v.get('passage_number', ''),
                'question_number': v.get('question_number', ''),
                'message': v.get('message', ''),
                'detail': v.get('detail', ''),
            })

print("=" * 60)
print("총 warning 건수:", len(all_warnings))
print("=" * 60)

# ─── 확인 1: 카테고리 전수 집계 ───────────────────────────────

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
        return '텍스트불일치(문항/지문stem)'
    if '지문을 PDF에서 찾지 못' in m or '대응하는 PDF 지문' in m:
        return '지문못찾음'
    if '문항을 PDF에서 찾지 못' in m or 'PDF에서 해당 문항을' in m:
        return '문항못찾음'
    if 'SKIPPED' in m:
        return 'SKIPPED'
    return '미분류'

cats = Counter(categorize(w['message']) for w in all_warnings)

print("\n[확인 1] 카테고리 전수 집계")
print("-" * 50)
total = 0
for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
    print("  %-40s %d건" % (cat, cnt))
    total += cnt
print("-" * 50)
print("  합계:", total)

# 미분류 상세
uncategorized = [w for w in all_warnings if categorize(w['message']) == '미분류']
if uncategorized:
    print("\n  [미분류 상세]")
    for w in uncategorized:
        print("    %s | %s | %s" % (w['file'][-20:], w['location'], w['message'][:60]))

# ─── 확인 2: bracket 경고 실제 diff 재추출 ────────────────────

print("\n\n[확인 2] bracket 경고 실제 PDF vs JSON diff")
print("-" * 50)

bracket_warns = [w for w in all_warnings if categorize(w['message']) in ('bracket텍스트불일치', 'bracket위치특정불가')]
print("bracket 관련 경고 총:", len(bracket_warns))

# passage content에서 bracket 텍스트 추출
# raw_result.passages[].content에서 [A]...[B] 구간을 자름
def extract_bracket_texts(passages):
    result = {}
    for p in passages:
        pnum = p.get('passage_number') or p.get('id', '')
        content = p.get('content', '')
        # [A]~[E] 위치 찾기
        markers = [(m.start(), m.group()) for m in re.finditer(r'\[([A-E])\]', content)]
        for i, (pos, label) in enumerate(markers):
            end = markers[i+1][0] if i+1 < len(markers) else min(pos+300, len(content))
            snippet = content[pos:end].strip()[:150]
            result[(pnum, label)] = snippet
    return result

# 각 파일의 bracket passage 내용 샘플 출력 (최대 3파일)
shown_files = set()
shown = 0
for fname, raw_json in rows:
    if shown >= 3:
        break
    try:
        data = json.loads(raw_json)
    except Exception:
        continue
    if not isinstance(data, dict):
        continue
    vc = data.get('verify_corrections', [])
    file_bracket_warns = [v for v in vc if v.get('kind','').lower() == 'warning'
                          and '텍스트 불일치' in v.get('message','')
                          and re.search(r'\[[A-E]\]', v.get('message',''))]
    if not file_bracket_warns:
        continue

    passages = data.get('passages', [])
    bracket_texts = extract_bracket_texts(passages)

    short_fname = fname[-30:]
    print("\n  파일: ...%s" % short_fname)
    for v in file_bracket_warns[:3]:
        loc = v.get('location', '')
        msg = v.get('message', '')
        detail = v.get('detail', '')
        m = re.search(r'\[([A-E])\]', msg)
        label = ('[%s]' % m.group(1)) if m else '?'
        # passage number 추출
        pnum_m = re.search(r'지문(\d+)', loc)
        pnum = pnum_m.group(1) if pnum_m else ''

        snippet = bracket_texts.get((pnum, label), bracket_texts.get(('', label), '(못찾음)'))
        print("    경고: %s" % msg)
        print("    detail: %s" % (detail if detail else '(비어있음)'))
        print("    JSON 내용 [%s]: %s" % (label, repr(snippet[:100])))
    shown += 1

# ocr_text에서 bracket 근방 텍스트도 확인
print("\n  [ocr_text 에서 [A]~[E] 근방 텍스트 샘플]")
shown2 = 0
for fname, raw_json in rows:
    if shown2 >= 2:
        break
    try:
        data = json.loads(raw_json)
    except Exception:
        continue
    if not isinstance(data, dict):
        continue
    vc = data.get('verify_corrections', [])
    has_bracket_warn = any(
        v.get('kind','').lower() == 'warning'
        and '텍스트 불일치' in v.get('message','')
        and re.search(r'\[[A-E]\]', v.get('message',''))
        for v in vc
    )
    if not has_bracket_warn:
        continue
    ocr = data.get('ocr_text', '')
    if not ocr:
        continue
    # [A]~[E] 위치 찾기
    for m in re.finditer(r'\[([A-E])\]', ocr):
        label = m.group(0)
        snippet = ocr[m.start():m.start()+100].strip()
        print("    ...%s | %s: %s" % (fname[-20:], label, repr(snippet[:80])))
        break
    shown2 += 1

# ─── 확인 3: 밑줄못찾음 distinct 텍스트 ─────────────────────

print("\n\n[확인 3] 밑줄 경고 distinct 분석")
print("-" * 50)

underline_warns = [w for w in all_warnings if categorize(w['message']) in
                   ('밑줄_JSON→PDF미확인', '밑줄_PDF→JSON못찾음')]

# 텍스트 추출
def extract_underline_text(msg):
    m = re.search(r"['\u2018\u2019](.+?)['\u2018\u2019]", msg)
    if m:
        return m.group(1)
    # "PDF 밑줄 텍스트를 지문에서 찾지 못함: ..." 패턴
    m2 = re.search(r':\s*(.+)$', msg)
    if m2:
        return m2.group(1).strip()
    return msg

texts_by_cat = defaultdict(list)
for w in underline_warns:
    cat = categorize(w['message'])
    txt = extract_underline_text(w['message'])
    texts_by_cat[cat].append((txt, w['file']))

for cat, items in texts_by_cat.items():
    print("\n  카테고리: %s (총 %d건)" % (cat, len(items)))
    texts = [t for t, f in items]
    distinct = set(texts)
    print("  distinct 텍스트 수:", len(distinct))
    # 중복 파악
    dup = Counter(texts)
    dups = [(t, c) for t, c in dup.items() if c > 1]
    if dups:
        print("  중복 텍스트:")
        for t, c in sorted(dups, key=lambda x: -x[1]):
            files = [f[-20:] for t2, f in items if t2 == t]
            print("    (%d회) %s" % (c, repr(t[:60])))
            for ff in files:
                print("      - ...%s" % ff)
    print("  전체 목록:")
    for t in sorted(distinct):
        print("    %s" % repr(t[:70]))

conn.close()
print("\n\n완료.")
