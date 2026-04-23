"""
patch_missing_questions.py — segmenter가 누락한 문항을 수동으로 DB에 삽입
========================================================================
사용법:
  python3 patch_missing_questions.py --dry-run   # 내용 확인만
  python3 patch_missing_questions.py             # 실제 적용

현재 하드코딩: 2025 10월 학력평가 국어(언매) 문제.pdf  Q36/Q37
범용성: PATCHES 리스트에 다른 파일/문항 추가 가능
"""
import sqlite3, json, re, argparse, sys
import fitz  # PyMuPDF

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'

# ── 1. PDF에서 Q36/Q37 텍스트 추출 ───────────────────────────────────────────

def extract_between(text: str, start_marker: str, end_marker: str) -> str:
    """start_marker 이후 ~ end_marker 직전 텍스트 반환."""
    s = text.find(start_marker)
    if s < 0:
        return ''
    e = text.find(end_marker, s + len(start_marker))
    if e < 0:
        return text[s:]
    return text[s:e]

def get_page_text(pdf_path: str, page_num: int) -> str:
    """1-based 페이지 번호로 텍스트 추출."""
    doc = fitz.open(pdf_path)
    text = doc[page_num - 1].get_text()
    doc.close()
    return text

def parse_choices(block: str) -> list:
    """①~⑤ 선택지 파싱 → list[str]"""
    circles = ['①', '②', '③', '④', '⑤']
    choices = []
    for i, c in enumerate(circles):
        idx = block.find(c)
        if idx < 0:
            break
        next_c = circles[i + 1] if i + 1 < len(circles) else None
        if next_c:
            end = block.find(next_c, idx + 1)
            choices.append(block[idx:end].strip() if end > 0 else block[idx:].strip())
        else:
            choices.append(block[idx:].strip())
    return choices

def parse_bogi(block: str) -> str:
    """< 보 기 > 또는 <보기> 이후 ~ 선택지 직전 내용 추출."""
    m = re.search(r'<\s*보\s*기\s*>', block)
    if not m:
        return ''
    first_choice = re.search(r'[①②③④⑤]', block[m.end():])
    if first_choice:
        return block[m.end(): m.end() + first_choice.start()].strip()
    return block[m.end():].strip()

# ── 2. 패치 정의 ──────────────────────────────────────────────────────────────

def build_patches(pdf_path: str) -> list[dict]:
    """PDF에서 Q36/Q37 추출 → patch dict 목록 반환."""
    # p13에 Q36/Q37 모두 있음
    p13 = get_page_text(pdf_path, 13)
    p14 = get_page_text(pdf_path, 14)
    full = p13 + p14  # 경계 넘칠 경우 대비

    raw36 = extract_between(full, '36.', '37.')
    raw37 = extract_between(full, '37.', '38.')

    def stem_line(raw: str, num: int) -> str:
        """첫 줄(발문)만 추출, 번호 prefix 제거."""
        lines = raw.strip().splitlines()
        first = lines[0].strip() if lines else ''
        # "36. 윗글을..." → "36. 윗글을..."  (번호 포함 유지: DB 형식 맞춤)
        return first

    patch36 = {
        'number': 36,
        'stem': stem_line(raw36, 36),
        'bogi': parse_bogi(raw36),
        'choices': parse_choices(raw36),
        '_raw': raw36,
    }
    patch37 = {
        'number': 37,
        'stem': stem_line(raw37, 37),
        'bogi': parse_bogi(raw37),
        'choices': parse_choices(raw37),
        '_raw': raw37,
    }
    return [patch36, patch37]

# ── 3. DB 구조 확인 ──────────────────────────────────────────────────────────

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

def show_db_structure(qs: list, sample_nums=(35, 38)):
    """DB 문항 구조 출력 (dry-run 참고용)."""
    for q in qs:
        if isinstance(q, dict) and q.get('number') in sample_nums:
            print(f"\n--- Q{q['number']} DB 구조 ---")
            for k, v in q.items():
                if k == 'choices':
                    print(f"  choices ({type(v).__name__}, {len(v)}개): {repr(str(v)[:80])}")
                else:
                    print(f"  {k}: {repr(str(v)[:80])}")

# ── 4. 적용 ──────────────────────────────────────────────────────────────────

def apply_patch(job_row: dict, segs: dict, qs: list, patches: list):
    """qs에 patch 삽입 후 DB 업데이트."""
    existing_nums = {q.get('number') for q in qs if isinstance(q, dict)}

    # 기존 Q35 구조를 참조해 choices 포맷 맞추기
    ref_q = next((q for q in qs if q.get('number') == 35), None)
    choices_is_dict = isinstance(ref_q.get('choices'), dict) if ref_q else False

    new_qs = list(qs)
    for p in patches:
        num = p['number']
        if num in existing_nums:
            print(f"  Q{num} 이미 존재 → skip")
            continue
        choices = p['choices']
        if choices_is_dict:
            choices = {str(i + 1): c for i, c in enumerate(choices)}
        entry = {
            'number':  num,
            'stem':    p['stem'],
            'bogi':    p['bogi'],
            'choices': choices,
        }
        # ref_q의 다른 필드 키 복사 (값은 기본값)
        if ref_q:
            for k in ref_q:
                if k not in entry:
                    entry[k] = None
        new_qs.append(entry)
        print(f"  Q{num} 삽입 완료")

    # 번호 순 정렬
    new_qs.sort(key=lambda q: q.get('number', 0) if isinstance(q, dict) else 0)

    # segments 업데이트
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
    print(f"\n  DB 업데이트 완료 (job_id={job_row['id']})")

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='내용 출력만, DB 수정 없음')
    args = parser.parse_args()

    FILENAME_LIKE = '%10월%언매%문제%'
    PDF_PATH = '/home/chsh82/aprolabs/uploads/suneung/48654b73-a689-43fb-8313-5859611cafc3.pdf'

    print("=" * 60)
    print("patch_missing_questions.py — 2025 10월 언매 Q36/Q37")
    print("=" * 60)

    # DB 로드
    job_row, segs, qs = load_job(FILENAME_LIKE)
    print(f"\n파일: {job_row['filename']}")
    print(f"현재 문항 수: {len(qs)}개")
    print(f"현재 번호 목록: {sorted(q.get('number') for q in qs if isinstance(q, dict))}")

    # DB 구조 확인
    print("\n[STEP 2] DB 샘플 구조 (Q35/Q38)")
    show_db_structure(qs)

    # PDF 추출
    print("\n[STEP 1] PDF에서 Q36/Q37 추출")
    patches = build_patches(PDF_PATH)
    for p in patches:
        print(f"\n  Q{p['number']}:")
        print(f"    stem   : {repr(p['stem'][:80])}")
        print(f"    bogi   : {repr(p['bogi'][:80])}")
        print(f"    choices: {len(p['choices'])}개")
        for i, c in enumerate(p['choices']):
            print(f"      {i+1}. {repr(c[:60])}")

    if args.dry_run:
        print("\n[dry-run] DB 수정 없음. --dry-run 제거 후 재실행하면 적용됩니다.")
        return

    # 적용
    print("\n[STEP 3] DB 삽입 적용")
    apply_patch(job_row, segs, qs, patches)

if __name__ == '__main__':
    main()
