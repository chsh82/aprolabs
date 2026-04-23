"""
patch_missing_questions.py — segmenter가 누락한 문항을 수동으로 DB에 삽입
========================================================================
사용법:
  python3 patch_missing_questions.py --dry-run   # 내용 확인만
  python3 patch_missing_questions.py             # 실제 적용

현재 하드코딩: 2025 10월 학력평가 국어(언매) 문제.pdf  Q36/Q37
범용성: PATCHES 리스트에 다른 파일/문항 추가 가능
"""
import sqlite3, json, re, argparse
import fitz  # PyMuPDF

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'

CIRCLES = ['①', '②', '③', '④', '⑤']

# ── 파싱 유틸 ─────────────────────────────────────────────────────────────────

def get_page_text(pdf_path: str, page_num: int) -> str:
    doc = fitz.open(pdf_path)
    text = doc[page_num - 1].get_text()
    doc.close()
    return text

def extract_between(text: str, start_marker: str, end_marker: str) -> str:
    s = text.find(start_marker)
    if s < 0:
        return ''
    e = text.find(end_marker, s + len(start_marker))
    return text[s:e] if e > 0 else text[s:]

def clean_inline(text: str) -> str:
    """줄바꿈을 공백으로, 연속공백 정리."""
    return re.sub(r'\s*\n\s*', ' ', text).strip()

def parse_stem(block: str) -> str:
    """첫 줄(발문)만 추출. 번호 prefix 포함 (DB 형식과 동일)."""
    first = block.strip().splitlines()[0].strip()
    return first

def parse_bogi(block: str) -> str:
    """독립 줄의 < 보 기 > 이후 ~ 첫 선택지 직전까지.
    stem 내 '<보기>의 ...' 참조는 무시 (줄 단독 마커만 인식)."""
    m = re.search(r'\n\s*<\s*보\s*기\s*>\s*\n', block)
    if not m:
        return ''
    after = block[m.end():]
    fc = re.search(r'[①②③④⑤]', after)
    bogi_raw = after[:fc.start()] if fc else after
    return clean_inline(bogi_raw)

def parse_choices(block: str) -> list:
    """①~⑤ 파싱 → list[str].
    - 선행 ①② 마커 제거
    - 내부 \\n → 공백
    - 말미 페이지 헤더 제거
    """
    # 각 마커의 위치 수집
    positions = []
    for c in CIRCLES:
        idx = block.find(c)
        if idx >= 0:
            positions.append(idx)
    positions.sort()

    choices = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(block)
        raw = block[start:end]
        # 선행 원문자 제거
        raw = raw[len(CIRCLES[i]):]
        # 줄바꿈 → 공백
        raw = clean_inline(raw)
        # 말미 페이지 헤더 제거 (예: '2025학년도 1', '국어영역')
        raw = re.sub(r'\s*\d{4}학년도.*$', '', raw, flags=re.DOTALL).strip()
        raw = re.sub(r'\s*[가-힣]+영역.*$', '', raw, flags=re.DOTALL).strip()
        choices.append(raw)
    return choices

def build_content(stem: str, bogi: str, choices: list) -> str:
    """Q35/Q38 content 필드 형식 재현:
    stem + ' \u3000' + [< 보 기 > bogi + ' \u3000'] + ①choice1 + ' \u3000②' + ..."""
    parts = [stem]
    if bogi:
        parts.append(f'< 보 기 > {bogi}')
    for i, c in enumerate(choices):
        parts.append(f'{CIRCLES[i]}{c}')
    return ' \u3000'.join(parts)

# ── DB 로드 ───────────────────────────────────────────────────────────────────

def load_job(filename_like: str):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT id, filename, segments FROM pipeline_jobs WHERE filename LIKE ?",
        (filename_like,)
    ).fetchone()
    db.close()
    if not row:
        raise ValueError(f"파일 없음: {filename_like}")
    segs = json.loads(row['segments'])
    qs_raw = segs.get('questions', '[]')
    qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
    return dict(row), segs, qs

def show_db_structure(qs: list, sample_nums):
    """DB 문항 구조 출력."""
    sample_nums_str = {str(n) for n in sample_nums} | {n for n in sample_nums}
    for q in qs:
        if isinstance(q, dict) and q.get('number') in sample_nums_str:
            print(f"\n--- Q{q['number']} DB 구조 ---")
            for k, v in q.items():
                val = repr(str(v)[:100]) if k != 'choices' else f"({type(v).__name__}, {len(v)}개) {repr(str(v)[:80])}"
                print(f"  {k}: {val}")

# ── 패치 빌드 ─────────────────────────────────────────────────────────────────

def build_patches(pdf_path: str) -> list[dict]:
    p13 = get_page_text(pdf_path, 13)
    p14 = get_page_text(pdf_path, 14)
    full = p13 + p14

    raw36 = extract_between(full, '36.', '37.')
    raw37 = extract_between(full, '37.', '38.')

    results = []
    for num, raw in [(36, raw36), (37, raw37)]:
        stem    = parse_stem(raw)
        bogi    = parse_bogi(raw)
        choices = parse_choices(raw)
        content = build_content(stem, bogi, choices)
        results.append({
            'number':  num,
            'stem':    stem,
            'bogi':    bogi,
            'choices': choices,
            'content': content,
            '_raw':    raw,
        })
    return results

# ── 적용 ──────────────────────────────────────────────────────────────────────

def apply_patch(job_row: dict, segs: dict, qs: list, patches: list):
    existing_nums = {str(q.get('number')) for q in qs if isinstance(q, dict)}

    # Q35 구조 참조 (choices 포맷)
    ref_q = next((q for q in qs if str(q.get('number')) == '35'), None)
    choices_as_dict = isinstance(ref_q.get('choices'), dict) if ref_q else True

    new_qs = list(qs)
    for p in patches:
        num_str = str(p['number'])
        if num_str in existing_nums:
            print(f"  Q{num_str} 이미 존재 → skip")
            continue

        choices = p['choices']
        if choices_as_dict:
            choices = {str(i + 1): c for i, c in enumerate(choices)}

        entry = {
            '_id':           f'q{p["number"]}',
            'number':        num_str,
            'stem':          p['stem'],
            'bogi':          p['bogi'],
            'choices':       choices,
            'content':       p['content'],
            'answer':        None,
            'explanation':   None,
            'has_choices':   str(bool(choices)),
            'passage_idx':   None,
            'passage_ref':   None,
            'difficulty':    None,
            'tags':          None,
            'thinking_types': None,
            'topic':         None,
        }
        new_qs.append(entry)
        print(f"  Q{p['number']} 삽입")

    new_qs.sort(key=lambda q: int(q.get('number', 0)) if isinstance(q, dict) else 0)

    segs_copy = dict(segs)
    if isinstance(segs.get('questions'), str):
        segs_copy['questions'] = json.dumps(new_qs, ensure_ascii=False)
    else:
        segs_copy['questions'] = new_qs

    db = sqlite3.connect(DB_PATH)
    db.execute(
        "UPDATE pipeline_jobs SET segments = ? WHERE id = ?",
        (json.dumps(segs_copy, ensure_ascii=False), job_row['id'])
    )
    db.commit()
    db.close()
    print(f"  DB 업데이트 완료 (job_id={job_row['id']})")

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    FILENAME_LIKE = '%10월%언매%문제%'
    PDF_PATH = '/home/chsh82/aprolabs/uploads/suneung/48654b73-a689-43fb-8313-5859611cafc3.pdf'

    print("=" * 60)
    print("patch_missing_questions.py — 2025 10월 언매 Q36/Q37")
    print("=" * 60)

    job_row, segs, qs = load_job(FILENAME_LIKE)
    print(f"\n파일: {job_row['filename']}")
    print(f"현재 문항 수: {len(qs)}개")
    print(f"번호 목록: {sorted(int(q.get('number',0)) for q in qs if isinstance(q, dict))}")

    print("\n[DB 샘플] Q35 / Q38 구조 참조")
    show_db_structure(qs, [35, 38])

    print("\n[PDF 추출] Q36 / Q37")
    patches = build_patches(PDF_PATH)
    for p in patches:
        print(f"\n  Q{p['number']}:")
        print(f"    stem   : {repr(p['stem'])}")
        print(f"    bogi   : {repr(p['bogi'][:80])}")
        print(f"    choices: {len(p['choices'])}개")
        for i, c in enumerate(p['choices']):
            print(f"      {i+1}. {repr(c[:70])}")
        print(f"    content: {repr(p['content'][:100])}")

    if args.dry_run:
        print("\n[dry-run 완료] --dry-run 제거 후 재실행 시 DB에 적용됩니다.")
        return

    print("\n[적용]")
    apply_patch(job_row, segs, qs, patches)

if __name__ == '__main__':
    main()
