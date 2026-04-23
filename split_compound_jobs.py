"""
split_compound_jobs.py — 합본 국어() 파일을 화작/언매 두 job으로 분리
=====================================================================
사용법:
  python3 split_compound_jobs.py --investigate   # 현황 조사 (변경 없음)
  python3 split_compound_jobs.py --dry-run       # 분리 제안 출력 (변경 없음)
  python3 split_compound_jobs.py --apply         # 실제 실행 (자동 백업 후)
"""
import sqlite3, json, re, argparse, os, shutil, uuid
from datetime import datetime
import fitz

DB_PATH  = '/home/chsh82/aprolabs/aprolabs.db'
UPLOADS  = '/home/chsh82/aprolabs'

COMBINE_RE = re.compile(r'국어\s*\(\s*\)')
HWAJAK_RE  = re.compile(r'화법과\s*작문')
EONMAE_RE  = re.compile(r'언어와\s*매체')
CIRCLES    = ['①', '②', '③', '④', '⑤']

# ── 페이지 분류 ───────────────────────────────────────────────────────────────

def classify_pages(pdf_path: str) -> dict:
    """각 페이지(1-based)를 'common' | 'hwajak' | 'eonmae'로 분류.
    '화법과 작문' 마커 포함 → hwajak, '언어와 매체' → eonmae, 둘 다 없음 → common."""
    doc = fitz.open(pdf_path)
    result = {}
    for i, page in enumerate(doc, 1):
        text = page.get_text()
        if HWAJAK_RE.search(text):
            result[i] = 'hwajak'
        elif EONMAE_RE.search(text):
            result[i] = 'eonmae'
        else:
            result[i] = 'common'
    doc.close()
    return result


def get_section_pages(page_map: dict) -> dict:
    """page_map → {'common': [...], 'hwajak': [...], 'eonmae': [...]}"""
    return {
        cat: sorted(p for p, c in page_map.items() if c == cat)
        for cat in ('common', 'hwajak', 'eonmae')
    }

# ── PDF 텍스트 파싱 (patch_missing_questions.py 로직 재사용) ─────────────────

def get_pages_text(pdf_path: str, page_nums) -> dict:
    """지정 페이지(1-based, iterable) → {pnum: text}."""
    page_set = set(page_nums)
    doc = fitz.open(pdf_path)
    pages = {i + 1: page.get_text() for i, page in enumerate(doc) if i + 1 in page_set}
    doc.close()
    return pages


def find_raw_block(pages: dict, qnum: int) -> tuple:
    """qnum 문항 블록 추출. next_qnum 없이 qnum+1..qnum+15 자력 탐색."""
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


def clean_inline(text: str) -> str:
    return re.sub(r'\s*\n\s*', ' ', text).strip()


def parse_stem(block: str) -> str:
    for pat in [re.compile(r'\n\s*<\s*보\s*기\s*>'), re.compile(r'[①②③④⑤]')]:
        m = pat.search(block)
        if m:
            return clean_inline(block[:m.start()])
    return clean_inline(block.splitlines()[0]) if block.strip() else ''


def parse_bogi(block: str) -> str:
    m = re.search(r'\n\s*(?:<\s*보\s*기\s*>|<학습\s*활동>|<자료>)\s*\n', block)
    if not m:
        return ''
    after = block[m.end():]
    fc = re.search(r'[①②③④⑤]', after)
    return clean_inline(after[:fc.start()] if fc else after)


def parse_choices(block: str) -> list:
    positions = []
    for c in CIRCLES:
        idx = block.find(c)
        if idx >= 0:
            positions.append(idx)
    positions.sort()

    choices = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(block)
        raw = block[start + len(CIRCLES[i]):end]
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


def extract_selective_questions(pdf_path: str, page_nums, label: str,
                                verbose: bool = False,
                                sample_nums: tuple = ()) -> list:
    """화작 또는 언매 페이지에서 Q35~Q45 추출.

    verbose=True  : stem + bogi + choices 전체 출력 (dry-run용)
    sample_nums   : 이 번호는 verbose와 무관하게 상세 출력 (확인 샘플)
    """
    pages = get_pages_text(pdf_path, page_nums)
    questions = []
    for qnum in range(35, 46):
        pnum, raw = find_raw_block(pages, qnum)
        if not raw:
            print(f"    [{label}] Q{qnum}: 미발견")
            continue
        stem    = parse_stem(raw)
        bogi    = parse_bogi(raw)
        choices = parse_choices(raw)
        content = build_content(stem, bogi, choices)
        questions.append({
            '_id':           f'q{qnum}',
            'number':        qnum,
            'stem':          stem,
            'bogi':          bogi,
            'choices':       {str(i + 1): v for i, v in enumerate(choices)},
            'content':       content,
            'answer':        None,
            'explanation':   None,
            'has_choices':   bool(choices),
            'passage_idx':   None,
            'passage_ref':   None,
            'difficulty':    None,
            'tags':          None,
            'thinking_types': None,
            'topic':         None,
        })

        show_detail = verbose or qnum in sample_nums
        tag = '[SAMPLE] ' if qnum in sample_nums else ''
        print(f"    [{label}] {tag}Q{qnum} (p{pnum}):")
        print(f"      stem   : {repr(stem[:80])}")
        if show_detail:
            print(f"      bogi   : {repr(bogi[:80]) if bogi else '(없음)'}")
            print(f"      choices: {len(choices)}개")
            for j, c in enumerate(choices):
                print(f"        {j+1}. {repr(c[:70])}")
    return questions

# ── DB 유틸 ───────────────────────────────────────────────────────────────────

def load_compound_jobs(db: sqlite3.Connection) -> list:
    """합본 파일(국어() 패턴) job 목록 로드."""
    db.row_factory = sqlite3.Row
    all_rows = db.execute(
        "SELECT * FROM pipeline_jobs WHERE segments IS NOT NULL"
    ).fetchall()
    return [r for r in all_rows if COMBINE_RE.search(r['filename'] or '')]


def parse_segments(job_row) -> tuple:
    """(segs_dict, qs_list, is_questions_str) 반환."""
    segs = json.loads(job_row['segments'])
    qs_raw = segs.get('questions', '[]')
    is_str = isinstance(qs_raw, str)
    qs = json.loads(qs_raw) if is_str else qs_raw
    return segs, qs, is_str


def get_common_questions(qs: list) -> list:
    """Q1~Q34 공통 문항 필터."""
    result = []
    for q in qs:
        if not isinstance(q, dict):
            continue
        try:
            n = int(q.get('number', 0))
        except (ValueError, TypeError):
            continue
        if 1 <= n <= 34:
            result.append(q)
    return result

# ── 모드별 함수 ───────────────────────────────────────────────────────────────

def cmd_investigate():
    db = sqlite3.connect(DB_PATH)
    jobs = load_compound_jobs(db)
    db.close()

    if not jobs:
        print("합본 파일(국어() 패턴) 없음.")
        return

    print(f"\n{'='*70}")
    print(f"합본 파일 {len(jobs)}개 발견")
    print(f"{'='*70}")

    for job in jobs:
        fname    = job['filename']
        pdf_path = f"{UPLOADS}/{job['file_path']}"
        _, qs, _ = parse_segments(job)

        q_nums  = sorted(int(q.get('number', 0)) for q in qs if isinstance(q, dict))
        common  = [n for n in q_nums if n <= 34]
        select  = [n for n in q_nums if n >= 35]

        print(f"\n파일   : {fname}")
        print(f"job_id : {job['id']}")
        print(f"status : {job['status']}")
        print(f"PDF    : {pdf_path}")
        print(f"현재 문항: {len(q_nums)}개  공통(Q1-34)={len(common)}개  선택(Q35-45)={len(select)}개")
        print(f"  공통: {common}")
        print(f"  선택: {select}")

        if not os.path.exists(pdf_path):
            print(f"  [WARNING] PDF 파일 없음!")
            continue

        page_map = classify_pages(pdf_path)
        sections = get_section_pages(page_map)

        print(f"\n  PDF 페이지 분류 ({len(page_map)}페이지):")
        for pnum, cat in sorted(page_map.items()):
            marker = {'hwajak': '← 화법과작문', 'eonmae': '← 언어와매체', 'common': ''}.get(cat, '')
            print(f"    p{pnum:02d}: {cat:8s} {marker}")

        print(f"\n  섹션 요약:")
        print(f"    공통  : {sections['common']}")
        print(f"    화작  : {sections['hwajak']}")
        print(f"    언매  : {sections['eonmae']}")

        if not sections['hwajak']:
            print(f"  [WARNING] 화작 페이지 미탐지 — '화법과 작문' 마커 확인 필요")
        if not sections['eonmae']:
            print(f"  [WARNING] 언매 페이지 미탐지 — '언어와 매체' 마커 확인 필요")

        print(f"\n  분리 제안:")
        for track, pages in [('화작', sections['hwajak']), ('언매', sections['eonmae'])]:
            if not pages:
                continue
            new_fname = COMBINE_RE.sub(f'국어({track})', fname)
            print(f"    → {new_fname}")
            print(f"         Q1-Q34: 기존 DB segments 재사용 ({len(common)}개)")
            print(f"         Q35-Q45: 페이지 {pages}에서 재추출 (11개 예상)")


def cmd_dry_run():
    db = sqlite3.connect(DB_PATH)
    jobs = load_compound_jobs(db)
    db.close()

    if not jobs:
        print("합본 파일 없음.")
        return

    for job in jobs:
        fname    = job['filename']
        pdf_path = f"{UPLOADS}/{job['file_path']}"
        segs, qs, _ = parse_segments(job)
        common_qs   = get_common_questions(qs)

        print(f"\n{'='*70}")
        print(f"[DRY-RUN] {fname}")
        print(f"{'='*70}")

        if not os.path.exists(pdf_path):
            print(f"  [ERROR] PDF 없음: {pdf_path}")
            continue

        page_map = classify_pages(pdf_path)
        sections = get_section_pages(page_map)

        # 파일별 샘플 번호: 2025 언매→Q36, 2026 화작→Q42 확인
        sample_map = {'화작': (42,), '언매': (36,)}

        for track, track_pages in [('화작', sections['hwajak']), ('언매', sections['eonmae'])]:
            if not track_pages:
                print(f"\n  [{track}] 페이지 없음 — skip")
                continue

            new_fname = COMBINE_RE.sub(f'국어({track})', fname)
            print(f"\n  → 새 job: {new_fname}")
            print(f"    공통 문항: {len(common_qs)}개  (Q{min(q['number'] for q in common_qs)}~Q{max(q['number'] for q in common_qs)})")
            print(f"    선택과목 추출 (페이지 {track_pages}):")

            selective_qs = extract_selective_questions(
                pdf_path, track_pages, track,
                verbose=True,
                sample_nums=sample_map.get(track, ()),
            )

            total = len(common_qs) + len(selective_qs)
            missing = [n for n in range(35, 46) if n not in {q['number'] for q in selective_qs}]
            print(f"\n    추출 결과: {len(selective_qs)}개 / 11개 예상", end='')
            if missing:
                print(f"  [WARNING] 미발견: Q{missing}")
            else:
                print()
            print(f"    합계: {total}개 문항 (공통 {len(common_qs)} + 선택 {len(selective_qs)})")

    print(f"\n[DRY-RUN 완료] --apply 추가 시 실제 분리됩니다.")


def cmd_apply():
    db = sqlite3.connect(DB_PATH)
    jobs = load_compound_jobs(db)

    if not jobs:
        print("합본 파일 없음.")
        db.close()
        return

    # ── 자동 백업 ──────────────────────────────────────────────────────────────
    ts      = datetime.now().strftime('%Y%m%d_%H%M%S')
    bak_db  = f"{UPLOADS}/aprolabs.db.bak_before_split_{ts}"
    bak_dir = f"{UPLOADS}/golden_tests"
    bak_json = f"{bak_dir}/backup_compound_jobs_{ts}.json"

    shutil.copy2(DB_PATH, bak_db)
    print(f"DB 백업  : {bak_db}")
    os.makedirs(bak_dir, exist_ok=True)
    backup_data = [dict(r) for r in jobs]
    with open(bak_json, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
    print(f"JSON 백업: {bak_json}")

    # ── 컬럼 목록 확인 ─────────────────────────────────────────────────────────
    col_names = [c[1] for c in db.execute("PRAGMA table_info(pipeline_jobs)").fetchall()]
    has_exam_type = 'exam_type' in col_names

    # ── 분리 실행 ──────────────────────────────────────────────────────────────
    for job in jobs:
        fname    = job['filename']
        pdf_path = f"{UPLOADS}/{job['file_path']}"
        orig_id  = job['id']
        segs, qs, qs_is_str = parse_segments(job)
        common_qs = get_common_questions(qs)

        print(f"\n{'='*70}")
        print(f"분리: {fname}")

        if not os.path.exists(pdf_path):
            print(f"  [ERROR] PDF 없음 — skip")
            continue

        page_map = classify_pages(pdf_path)
        sections = get_section_pages(page_map)

        new_ids = []
        for track, track_pages in [('화작', sections['hwajak']), ('언매', sections['eonmae'])]:
            if not track_pages:
                print(f"\n  [{track}] 페이지 없음 — skip")
                continue

            new_fname = COMBINE_RE.sub(f'국어({track})', fname)
            new_id    = str(uuid.uuid4())
            new_ids.append(new_id)

            print(f"\n  생성: {new_fname}")
            print(f"  id  : {new_id}")
            print(f"  선택과목 추출 (페이지 {track_pages}):")

            selective_qs = extract_selective_questions(pdf_path, track_pages, track)
            all_qs = sorted(
                common_qs + selective_qs,
                key=lambda q: int(q.get('number', 0))
            )

            new_segs = dict(segs)
            new_segs['questions'] = (
                json.dumps(all_qs, ensure_ascii=False) if qs_is_str else all_qs
            )

            # 원본 row 복사 후 필드 교체
            new_row = dict(job)
            new_row['id']       = new_id
            new_row['filename'] = new_fname
            new_row['segments'] = json.dumps(new_segs, ensure_ascii=False)
            if has_exam_type:
                new_row['exam_type'] = track  # '화작' or '언매'

            cols_str     = ', '.join(col_names)
            placeholders = ', '.join('?' for _ in col_names)
            values       = [new_row.get(c) for c in col_names]
            db.execute(
                f"INSERT INTO pipeline_jobs ({cols_str}) VALUES ({placeholders})",
                values
            )
            print(f"  INSERT 완료: {len(all_qs)}개 문항")

        # ── 원본 삭제 ──────────────────────────────────────────────────────────
        api_cnt = db.execute(
            "SELECT COUNT(*) FROM api_usage WHERE job_id=?", (orig_id,)
        ).fetchone()[0]
        if api_cnt:
            db.execute("DELETE FROM api_usage WHERE job_id=?", (orig_id,))
            print(f"\n  api_usage 삭제: {api_cnt}건")
        db.execute("DELETE FROM pipeline_jobs WHERE id=?", (orig_id,))
        print(f"  원본 삭제: {orig_id[:8]}...")

    db.commit()
    db.close()
    print(f"\n{'='*70}")
    print(f"분리 완료.")
    print(f"백업 위치: {bak_db}")

# ── 진입점 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='합본 국어() 파일 화작/언매 분리'
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument('--investigate', action='store_true', help='현황 조사 (변경 없음)')
    g.add_argument('--dry-run',     action='store_true', help='분리 제안 출력 (변경 없음)')
    g.add_argument('--apply',       action='store_true', help='실제 분리 실행 (자동 백업)')
    args = parser.parse_args()

    if args.investigate:
        cmd_investigate()
    elif args.dry_run:
        cmd_dry_run()
    elif args.apply:
        cmd_apply()


if __name__ == '__main__':
    main()
