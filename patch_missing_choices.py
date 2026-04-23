"""
patch_missing_choices.py — 누락/오염 선택지 PDF에서 재추출·복구
=================================================================
사용법:
  python3 patch_missing_choices.py --file "..." --questions "17,38,41" --dry-run
  python3 patch_missing_choices.py --file "..." --questions "17,38,41" --apply
"""
import sqlite3, json, re, argparse, difflib
import fitz

DB_PATH  = '/home/chsh82/aprolabs/aprolabs.db'
UPLOADS  = '/home/chsh82/aprolabs'

CIRCLES         = ['①', '②', '③', '④', '⑤']
RANGE_MARKER_RE = re.compile(r'\[(?:A|B|C|D):\s*(?:START|END)\]', re.IGNORECASE)

# ── PDF 텍스트 추출 ────────────────────────────────────────────────────────────

def get_all_page_text(pdf_path: str) -> dict:
    doc = fitz.open(pdf_path)
    pages = {i + 1: page.get_text() for i, page in enumerate(doc)}
    doc.close()
    return pages


def find_raw_block(pages: dict, qnum: int) -> tuple:
    """patch_missing_questions.py와 동일 — next_qnum 인자 없이 자력 탐색."""
    full = ''
    page_starts = {}
    offset = 0
    for pnum in sorted(pages):
        page_starts[offset] = pnum
        full += pages[pnum]
        offset += len(pages[pnum])

    def qpat(n):
        return re.compile(r'(?:^|\n)\s*' + str(n) + r'[.．][\s\u3000\u2002\u2003]')

    m_start = qpat(qnum).search(full)
    if not m_start:
        return -1, ''

    block_start = m_start.start() + (1 if full[m_start.start()] == '\n' else 0)
    block_end   = len(full)
    for next_n in range(qnum + 1, qnum + 16):
        m_end = qpat(next_n).search(full, m_start.end())
        if m_end:
            block_end = m_end.start()
            break

    pnum = 1
    for off, p in sorted(page_starts.items()):
        if off <= block_start:
            pnum = p
    return pnum, full[block_start:block_end]

# ── 선택지 파싱 ───────────────────────────────────────────────────────────────

def clean_inline(text: str) -> str:
    return re.sub(r'\s*\n\s*', ' ', text).strip()


def parse_choices_from_block(block: str) -> list:
    """①~⑤ 파싱 → list[str] (PDF 순서 기준, 재번호).

    추가 처리:
    - [A:START/END], [B:START/END] 등 범위 마커 제거
    - 줄바꿈 → 공백
    - 페이지 헤더 침입 방지 (학년도/영역 패턴)
    """
    positions = []
    for c in CIRCLES:
        idx = block.find(c)
        if idx >= 0:
            positions.append(idx)
    positions.sort()

    if not positions:
        return []

    choices = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(block)
        raw = block[start + len(CIRCLES[i]):end]
        raw = RANGE_MARKER_RE.sub('', raw)          # [A:START] 등 제거
        raw = clean_inline(raw)
        raw = re.sub(r'\s*\d{4}학년도.*$', '', raw, flags=re.DOTALL).strip()
        raw = re.sub(r'\s*[가-힣]+영역.*$',  '', raw, flags=re.DOTALL).strip()
        choices.append(raw)
    return choices


def build_content(stem: str, bogi: str, choices: list) -> str:
    parts = [stem]
    if bogi:
        parts.append(f'< 보 기 > {bogi}')
    for i, c in enumerate(choices):
        parts.append(f'{CIRCLES[i]}{c}')
    return ' \u3000'.join(parts)

# ── DB 유틸 ───────────────────────────────────────────────────────────────────

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


def find_question(qs: list, qnum: int):
    for q in qs:
        if isinstance(q, dict):
            try:
                if int(q.get('number', -1)) == qnum:
                    return q
            except (ValueError, TypeError):
                pass
    return None


def choices_to_list(choices_field) -> list:
    """dict 또는 list → list[str] (키 순서 정렬)."""
    if isinstance(choices_field, dict):
        return [choices_field[k] for k in sorted(choices_field.keys(), key=lambda x: int(x))]
    if isinstance(choices_field, list):
        return list(choices_field)
    return []


def list_to_choices(choices_list: list, original_was_dict: bool):
    """PDF 순서 기준으로 재번호 → {'1':..., '2':..., ...} 또는 list."""
    if original_was_dict:
        return {str(i + 1): v for i, v in enumerate(choices_list)}
    return choices_list


def choices_similarity(old_list: list, new_list: list) -> float:
    """기존 choices 전체 텍스트 ↔ 새 choices 전체 텍스트 유사도 (0~1).
    공통 부분 선택지만 비교 (길이 차이 자체는 무관)."""
    old_text = ' '.join(old_list)
    new_text = ' '.join(new_list[:len(old_list)])  # 비교 대상 길이 맞춤
    if not old_text and not new_text:
        return 1.0
    return difflib.SequenceMatcher(None, old_text, new_text).ratio()

# ── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file',      required=True, help='파일명 부분 문자열')
    parser.add_argument('--questions', required=True, help='문항번호 쉼표 구분')
    parser.add_argument('--dry-run',   action='store_true', default=True)
    parser.add_argument('--apply',     action='store_true', help='실제 DB 적용')
    args = parser.parse_args()

    apply       = args.apply
    target_nums = sorted(int(x.strip()) for x in args.questions.split(',') if x.strip())

    print('=' * 60)
    print(f"patch_missing_choices  ({'apply' if apply else 'dry-run'})")
    print('=' * 60)

    job_row, segs, qs = load_job(args.file)
    fname    = job_row['filename']
    pdf_path = f"{UPLOADS}/{job_row['file_path']}"

    print(f"\n파일: {fname}")
    print(f"PDF : {pdf_path}")
    print(f"대상: Q{target_nums}")

    pages = get_all_page_text(pdf_path)

    patches = []
    for qnum in target_nums:
        print(f"\n{'─'*50}")
        q = find_question(qs, qnum)
        if q is None:
            print(f"Q{qnum}: DB에 없음 — patch_missing_questions.py 사용")
            continue

        old_choices    = q.get('choices', {})
        old_is_dict    = isinstance(old_choices, dict)
        old_list       = choices_to_list(old_choices)
        old_keys       = list(old_choices.keys()) if old_is_dict else list(range(len(old_list)))

        # ── 현재 DB 상태 출력 ─────────────────────────────────────────────────
        print(f"Q{qnum}  DB 현재: {len(old_list)}개  keys={old_keys}")
        for j, c in enumerate(old_list):
            print(f"  DB {j+1}. {repr(c[:70])}")

        if len(old_list) >= 5 and sorted(old_keys) == ['1','2','3','4','5']:
            print(f"  → 이미 완전 (5개, 번호 정상) — SKIP")
            continue

        # ── PDF 추출 ──────────────────────────────────────────────────────────
        pnum, raw = find_raw_block(pages, qnum)
        if not raw:
            print(f"  [ERROR] PDF 텍스트 미발견 — 수동 확인 필요")
            continue

        new_list = parse_choices_from_block(raw)

        print(f"  PDF (p{pnum}) 추출: {len(new_list)}개")
        for j, c in enumerate(new_list):
            # 변경점 표시
            if j < len(old_list):
                tag = ' ← 변경' if c != old_list[j] else ''
            else:
                tag = ' ← 신규'
            print(f"  PDF {j+1}. {repr(c[:70])}{tag}")

        # ── 안전장치 ──────────────────────────────────────────────────────────
        skip = False

        if len(new_list) < 5:
            print(f"  [WARNING] PDF 추출 {len(new_list)}개 < 5 — 검토 필요, apply 건너뜀")
            skip = True

        if old_list:
            sim = choices_similarity(old_list, new_list)
            sim_pct = int(sim * 100)
            if sim < 0.70:
                print(f"  [WARNING] 유사도 {sim_pct}% < 70% — 내용 불일치 의심, apply 건너뜀")
                skip = True
            else:
                print(f"  유사도: {sim_pct}%  ({'OK' if sim >= 0.70 else '낮음'})")

        if skip:
            continue

        patches.append({
            'qnum':         qnum,
            'new_list':     new_list,
            'old_is_dict':  old_is_dict,
            'stem':         q.get('stem', ''),
            'bogi':         q.get('bogi', ''),
        })
        print(f"  → {'apply 예정' if apply else 'dry-run OK'}")

    # ── dry-run 종료 ──────────────────────────────────────────────────────────
    if not apply:
        print(f"\n[dry-run 완료] --apply 추가 시 DB에 적용됩니다.")
        return

    if not patches:
        print(f"\n적용할 패치 없음.")
        return

    # ── 적용 ──────────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("DB 적용 중...")
    new_qs = list(qs)
    for p in patches:
        qnum     = p['qnum']
        new_list = p['new_list']
        q = find_question(new_qs, qnum)
        if q is None:
            continue
        q['choices'] = list_to_choices(new_list, p['old_is_dict'])
        q['content'] = build_content(p['stem'], p['bogi'], new_list)
        print(f"  Q{qnum}: choices {len(new_list)}개 + content 재구성")

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
