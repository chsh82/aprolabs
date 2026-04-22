"""
analyze_qa_reports.py — QA 리포트 stem 불일치 재분류
- 문항번호 prefix 제거 후 유사도 재계산
- 유형 A (오탐): 정규화 후 sim >= 0.75
- 유형 B (실제 문제): 정규화 후 sim < 0.75
"""
import json, re, difflib

REPORTS = [
    '/home/chsh82/aprolabs/golden_tests/qa_report_20260422_145000.json',
    '/home/chsh82/aprolabs/golden_tests/qa_report_20260422_153430.json',
]

_QNUM_PREFIX_RE = re.compile(r'^\d+\.\s*[\u3000\s]*')

def normalize(text):
    t = re.sub(r'\s+', ' ', str(text or '')).strip()
    t = _QNUM_PREFIX_RE.sub('', t).strip()
    return t

def sim(a, b):
    a, b = normalize(a), normalize(b)
    if not a and not b:
        return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()

for path in REPORTS:
    try:
        data = json.load(open(path, encoding='utf-8'))
    except FileNotFoundError:
        print(f"[없음] {path}\n")
        continue

    label = path.split('qa_report_')[1].replace('.json','')
    stem_issues = []
    bogi_issues = []

    for r in data:
        for m in r.get('mismatches', []):
            qnum = m['question_number']
            for iss in m.get('issues', []):
                if iss.get('field') == 'stem':
                    v = iss.get('vision', '') or ''
                    d = iss.get('db', '') or ''
                    s = sim(v, d)
                    typ = 'A(오탐)' if s >= 0.75 else 'B(실제)'
                    stem_issues.append((qnum, typ, round(s, 2), v[:40], d[:40]))
                elif iss.get('field') == 'bogi':
                    bogi_issues.append(qnum)

    type_a = [x for x in stem_issues if x[1].startswith('A')]
    type_b = [x for x in stem_issues if x[1].startswith('B')]

    print(f"=== {label} ===")
    print(f"bogi 불일치: {len(bogi_issues)}건  Q{sorted(bogi_issues)}")
    print(f"stem 불일치: {len(stem_issues)}건  → 유형A(오탐)={len(type_a)}건  유형B(실제)={len(type_b)}건")

    if type_a:
        print("  [유형A — 정규화 후 일치]")
        for qnum, typ, s, v, d in type_a:
            print(f"    Q{qnum} sim={s}  vision='{v}'  db='{d}'")

    if type_b:
        print("  [유형B — DB stem 실제 문제]")
        for qnum, typ, s, v, d in type_b:
            print(f"    Q{qnum} sim={s}  vision='{v}'  db='{d}'")

    # 보정 후 수치
    real_stem = len(type_b)
    print(f"  → 개선 후 실제 stem 불일치: {real_stem}건  (bogi 불일치 {len(bogi_issues)}건은 별도)")
    print()
