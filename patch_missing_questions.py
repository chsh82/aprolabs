"""
patch_missing_questions.py — segmenter 누락 문항 수동 복구 (범용)
=================================================================
사용법:
  python3 patch_missing_questions.py --file "<파일명 부분>" --questions "2,8,19" --dry-run
  python3 patch_missing_questions.py --file "<파일명 부분>" --questions "2,8,19" --apply
"""
import sqlite3, json, re, argparse
import fitz

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
UPLOADS = '/home/chsh82/aprolabs'

CIRCLES = ['①', '②', '③', '④', '⑤']

# ── 텍스트 파싱 유틸 ──────────────────────────────────────────────────────────

def get_all_page_text(pdf_path: str) -> dict:
    """페이지번호(1-based) → 텍스트 딕셔너리."""
    doc = fitz.open(pdf_path)
    pages = {i + 1: page.get_text() for i, page in enumerate(doc)}
    doc.close()
    return pages

def find_raw_block(pages: dict, qnum: int, next_qnum: int | None) -> tuple[int, str]:
    """'qnum.' 시작부터 'next_qnum.' 직전까지 원문 블록 반환.
    페이지 경계를 넘을 수 있으므로 연결된 텍스트에서 탐색."""
    # 전체 페이지를 이어붙인 텍스트 (페이지 구분자 삽입)
    full = ''
    page_starts = {}  # 누적 offset → 페이지번호
    offset = 0
    for pnum in sorted(pages):
        page_starts[offset] = pnum
        full += pages[pnum]
        offset += len(pages[pnum])

    # qnum. 패턴 탐색 (줄 시작 또는 앞에 공백/줄바꿈)
    start_pat = re.compile(r'(?:^|\n)' + str(qnum) + r'\.\s')
    m_start = start_pat.search(full)
    if not m_start:
        return -1, ''

    block_start = m_start.start() + (1 if full[m_start.start()] == '\n' else 0)

    if next_qnum:
        end_pat = re.compile(r'(?:^|\n)' + str(next_qnum) + r'\.\s')
        m_end = end_pat.search(full, m_start.end())
        block_end = m_end.start() if m_end else len(full)
    else:
        block_end = len(full)

    # 페이지 번호 추정
    pnum = 1
    for off, p in sorted(page_starts.items()):
        if off <= block_start:
            pnum = p

    return pnum, full[block_start:block_end]

def clean_inline(text: str) -> str:
    """줄바꿈 → 공백, 연속공백 정리."""
    return re.sub(r'\s*\n\s*', ' ', text).strip()

def parse_stem(block: str) -> str:
    """발문: 첫 줄 (문항번호 포함). ①이 나오기 전까지."""
    # 보기 마커나 선택지 직전까지
    for end_pat in [
        re.compile(r'\n\s*<\s*보\s*기\s*>'),   # 독립 보기 마커
        re.compile(r'[①②③④⑤]'),               # 선택지 시작
    ]:
        m = end_pat.search(block)
        if m:
            return clean_inline(block[:m.start()])
    return clean_inline(block.splitlines()[0]) if block.strip() else ''

def parse_bogi(block: str) -> str:
    """독립 줄의 < 보 기 > / <보기> / <학습 활동> 이후 ~ 첫 선택지 직전."""
    bogi_start = re.search(
        r'\n\s*(?:<\s*보\s*기\s*>|<학습\s*활동>|<자료>)\s*\n',
        block
    )
    if not bogi_start:
        return ''
    after = block[bogi_start.end():]
    fc = re.search(r'[①②③④⑤]', after)
    return clean_inline(after[:fc.start()] if fc else after)

def parse_choices(block: str) -> list:
    """①~⑤ 파싱 → list[str]. 마커 제거, 줄바꿈→공백, 헤더 침입 방지."""
    positions = []
    for c in CIRCLES:
        idx = block.find(c)
        if idx >= 0:
            positions.append(idx)
    positions.sort()

    choices = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(block)
        raw = block[start + len(CIRCLES[i]):end]  # 마커 제거
        raw = clean_inline(raw)
        # 다음 페이지 헤더 / 연도 표기 제거
        raw = re.sub(r'\s*\d{4}학년도.*$', '', raw, flags=re.DOTALL).strip()
        raw = re.sub(r'\s*[가-힣]+영역.*$',  '', raw, flags=re.DOTALL).strip()
        choices.append(raw)
    return choices

def build_content(stem: str, bogi: str, choices: list) -> str:
    """DB content 필드 형식 재현."""
    parts = [stem]
    if bogi:
        parts.append(f'< 보 기 > {bogi}')
    for i, c in enumerate(choices):
        parts.append(f'{CIRCLES[i]}{c}')
    return ' \u3000'.join(parts)

# ── DB 조작 ──────────────────────────────────────────────────────────────────

def load_job(fname_like: str):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT id, filename, segments, file_path FROM pipeline_jobs "
        "WHERE filename LIKE ? AND filename NOT LIKE '%정답%' AND filename NOT LIKE '%해설%'",
        (f'%{fname_like}%',)
    ).fetchone()
    db.close()
    if not row:
        raise ValueError(f"파일 없음: {fname_like}")
    segs = json.loads(row['segments'])
    qs_raw = segs.get('questions', '[]')
    qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
    return dict(row), segs, qs

def existing_nums(qs: list) -> set:
    nums = set()
    for q in qs:
        if isinstance(q, dict):
            try:
                nums.add(int(q.get('number', 0)))
            except Exception:
                pass
    return nums

def ref_q(qs: list, fallback_num: int = 1):
    """구조 참조용 기존 문항 (가장 가까운 번호)."""
    candidates = [q for q in qs if isinstance(q, dict)]
    if not candidates:
        return None
    return min(candidates, key=lambda q: abs(int(q.get('number', 0)) - fallback_num))

# ── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file',      required=True, help='파일명 부분 문자열')
    parser.add_argument('--questions', required=True, help='누락 문항번호 (쉼표 구분)')
    parser.add_argument('--dry-run',   action='store_true', default=True)
    parser.add_argument('--apply',     action='store_true', help='실제 DB 적용')
    args = parser.parse_args()

    apply = args.apply
    dry_run = not apply

    target_nums = sorted(int(x.strip()) for x in args.questions.split(',') if x.strip())

    print("=" * 60)
    print(f"patch_missing_questions  ({'apply' if apply else 'dry-run'})")
    print("=" * 60)

    job_row, segs, qs = load_job(args.file)
    fname    = job_row['filename']
    pdf_path = f"{UPLOADS}/{job_row['file_path']}"

    print(f"\n파일    : {fname}")
    print(f"PDF     : {pdf_path}")
    print(f"현재 문항: {sorted(existing_nums(qs))}")
    print(f"복구 대상: {target_nums}")

    already = existing_nums(qs)
    to_patch = [n for n in target_nums if n not in already]
    skip     = [n for n in target_nums if n in already]
    if skip:
        print(f"이미 존재 (skip): {skip}")
    if not to_patch:
        print("복구할 문항 없음.")
        return

    pages = get_all_page_text(pdf_path)

    # 참조 문항 구조 확인
    sample = ref_q(qs, to_patch[0])
    choices_as_dict = isinstance(sample.get('choices'), dict) if sample else True

    patches = []
    for i, qnum in enumerate(to_patch):
        next_qnum = to_patch[i + 1] if i + 1 < len(to_patch) else None
        # next_qnum이 없으면 DB에서 다음 번호 추정
        if not next_qnum:
            all_db = sorted(existing_nums(qs))
            bigger = [n for n in all_db if n > qnum]
            next_qnum = bigger[0] if bigger else None

        pnum, raw = find_raw_block(pages, qnum, next_qnum)
        if not raw:
            print(f"\nQ{qnum}: PDF에서 텍스트 찾지 못함 — 수동 확인 필요")
            continue

        stem    = parse_stem(raw)
        bogi    = parse_bogi(raw)
        choices = parse_choices(raw)
        content = build_content(stem, bogi, choices)

        print(f"\nQ{qnum} (p{pnum}):")
        print(f"  stem   : {repr(stem)}")
        print(f"  bogi   : {repr(bogi[:80])}")
        print(f"  choices: {len(choices)}개")
        for j, c in enumerate(choices):
            print(f"    {j+1}. {repr(c[:70])}")
        print(f"  content: {repr(content[:100])}")

        patches.append({
            'qnum': qnum, 'stem': stem, 'bogi': bogi,
            'choices': choices, 'content': content,
        })

    if dry_run:
        print(f"\n[dry-run 완료] --apply 추가 시 DB에 삽입됩니다.")
        return

    # ── 적용 ──────────────────────────────────────────────────────────────────
    new_qs = list(qs)
    for p in patches:
        c = p['choices']
        if choices_as_dict:
            c = {str(i + 1): v for i, v in enumerate(c)}
        entry = {
            '_id':           f'q{p["qnum"]}',
            'number':        p['qnum'],   # int
            'stem':          p['stem'],
            'bogi':          p['bogi'],
            'choices':       c,
            'content':       p['content'],
            'answer':        None,
            'explanation':   None,
            'has_choices':   bool(c),
            'passage_idx':   None,
            'passage_ref':   None,
            'difficulty':    None,
            'tags':          None,
            'thinking_types': None,
            'topic':         None,
        }
        new_qs.append(entry)
        print(f"  Q{p['qnum']} 삽입")

    new_qs.sort(key=lambda q: int(q.get('number', 0)) if isinstance(q, dict) else 0)

    segs_copy = dict(segs)
    if isinstance(segs.get('questions'), str):
        segs_copy['questions'] = json.dumps(new_qs, ensure_ascii=False)
    else:
        segs_copy['questions'] = new_qs

    db = sqlite3.connect(DB_PATH)
    db.execute(
        "UPDATE pipeline_jobs SET segments=? WHERE id=?",
        (json.dumps(segs_copy, ensure_ascii=False), job_row['id'])
    )
    db.commit()
    db.close()
    print(f"\nDB 업데이트 완료 (job_id={job_row['id']})")


if __name__ == '__main__':
    main()
