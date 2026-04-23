"""
investigate_q2.py — 2025 7월 언매/화작 Q2 누락 원인 조사
"""
import sqlite3, json, re
import fitz

DB_PATH  = '/home/chsh82/aprolabs/aprolabs.db'
UPLOADS  = '/home/chsh82/aprolabs'

FILES = [
    '2025 7월 학력평가 국어(언매) 문제.pdf',
    '2025 7월 학력평가 국어(화작) 문제.pdf',
]

def load_q(fname, qnum):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT segments, file_path FROM pipeline_jobs WHERE filename=?", (fname,)).fetchone()
    db.close()
    segs = json.loads(row['segments'])
    qs_raw = segs.get('questions', '[]')
    qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
    q = next((q for q in qs if isinstance(q, dict) and str(q.get('number','')) == str(qnum)), None)
    return q, row['file_path']

for fname in FILES:
    print(f"\n{'='*70}")
    print(f"파일: {fname}")

    # Q1 DB 상태
    q1, file_path = load_q(fname, 1)
    if q1:
        stem    = str(q1.get('stem',''))
        bogi    = str(q1.get('bogi',''))
        content = str(q1.get('content',''))
        choices = q1.get('choices', {})
        if isinstance(choices, dict): choices = list(choices.values())
        print(f"\n  [Q1 DB]")
        print(f"    stem ({len(stem)}자): {repr(stem[:120])}")
        print(f"    bogi ({len(bogi)}자): {repr(bogi[:120])}")
        print(f"    content ({len(content)}자): {repr(content[:200])}")
        print(f"    choices: {len(choices)}개")
        for i,c in enumerate(choices):
            print(f"      {i+1}. {repr(str(c)[:80])}")
    else:
        print("  Q1 DB 레코드 없음")

    # PDF p1 전체 텍스트
    pdf_path = f'{UPLOADS}/{file_path}'
    doc = fitz.open(pdf_path)
    p1_text = doc[0].get_text()

    # Q1, Q2, Q3 마커 위치
    print(f"\n  [PDF p1 문항번호 위치]")
    for marker in ['1.', '2.', '3.']:
        idx = p1_text.find(f'\n{marker}')
        if idx >= 0:
            ctx = repr(p1_text[max(0,idx-10):idx+80])
            print(f"    '{marker}' at {idx}: {ctx}")
        else:
            print(f"    '{marker}' 미발견")

    # p1 전체 텍스트 (Q2 흡수 여부 확인)
    print(f"\n  [PDF p1 텍스트 전체]")
    print(repr(p1_text[:1500]))

    # p1 블록 구조
    print(f"\n  [PDF p1 블록 구조]")
    blocks = doc[0].get_text('blocks')
    for b in blocks[:15]:
        x0,y0,x1,y1,text,*_ = b
        print(f"    y={y0:.0f}-{y1:.0f}  {repr(text[:80])}")

    doc.close()
