"""
investigate_missing.py — 문항누락 심층 조사
============================================
사용법:
  python3 investigate_missing.py
"""
import sqlite3, json, re, glob, os
from collections import defaultdict

DB_PATH    = '/home/chsh82/aprolabs/aprolabs.db'
GOLDEN_DIR = '/home/chsh82/aprolabs/golden_tests'
UPLOADS    = '/home/chsh82/aprolabs'

TARGET_FILES = [
    '2025 7월 학력평가 국어(언매) 문제.pdf',
    '2025 7월 학력평가 국어(화작) 문제.pdf',
]

# ── 패턴 ──────────────────────────────────────────────────────────────────────
PUA_RE      = re.compile(r'[\ue000-\uf8ff]')
SPACED_BOGI = re.compile(r'<\s+보\s+기\s+>|<\s*보\s+기\s*>|<\s*보\s*기\s+>')
BOX_LIKE    = re.compile(r'<학습\s*활동>|<자료>|<조건>')
QNUM_RE     = re.compile(r'(?:^|\n)(\d{1,2})\.\s')

# ── 최신 QA 리포트 ────────────────────────────────────────────────────────────

def latest_reports():
    paths = sorted(glob.glob(f'{GOLDEN_DIR}/qa_report_*.json'))
    latest = {}
    for path in paths:
        try:
            ts = os.path.basename(path).replace('qa_report_', '').replace('.json', '')
            for fr in json.load(open(path, encoding='utf-8')):
                fn = fr.get('filename', '')
                if fn not in latest or ts > latest[fn][0]:
                    latest[fn] = (ts, fr)
        except Exception:
            pass
    return latest

# ── DB 로드 ───────────────────────────────────────────────────────────────────

def load_db_nums(fname):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT segments, file_path FROM pipeline_jobs WHERE filename=?", (fname,)
    ).fetchone()
    db.close()
    if not row:
        return set(), None
    segs = json.loads(row['segments'])
    qs_raw = segs.get('questions', '[]')
    qs = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
    nums = set()
    for q in qs:
        if isinstance(q, dict):
            try:
                nums.add(int(q.get('number', 0)))
            except Exception:
                pass
    return nums, row['file_path']

# ── PDF 텍스트 추출 ──────────────────────────────────────────────────────────

def get_all_text(pdf_path):
    try:
        import fitz
        doc = fitz.open(pdf_path)
        pages = {}
        for i, page in enumerate(doc, 1):
            pages[i] = page.get_text()
        doc.close()
        return pages
    except Exception as e:
        return {}

def find_question_context(pages, qnum, window=600):
    """문항번호 주변 텍스트 반환. (페이지번호, 텍스트) 튜플."""
    marker = f'{qnum}.'
    for pnum, text in sorted(pages.items()):
        # 줄 시작에 문항번호가 있는지 확인
        for m in re.finditer(r'(?:^|\n)(' + str(qnum) + r')\.\s', text):
            start = max(0, m.start() - 30)
            end   = min(len(text), m.start() + window)
            return pnum, text[start:end]
    return None, ''

# ── 원인 분류 ─────────────────────────────────────────────────────────────────

def classify_cause(ctx):
    causes = []
    if PUA_RE.search(ctx):
        pua_chars = set(PUA_RE.findall(ctx))
        causes.append(f'A-PUA ({", ".join(repr(c) for c in sorted(pua_chars)[:3])})')
    if SPACED_BOGI.search(ctx):
        causes.append('B-공백보기')
    if BOX_LIKE.search(ctx):
        causes.append('C-박스마커')
    if not causes:
        causes.append('D-기타')
    return causes

# ── 주변 문항번호 확인 ────────────────────────────────────────────────────────

def find_qnums_on_page(text):
    return sorted(set(int(m.group(1)) for m in QNUM_RE.finditer(text)
                      if 1 <= int(m.group(1)) <= 45))

# ── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    reports = latest_reports()

    print("=" * 80)
    print("문항누락 심층 조사 — 2025 7월 언매/화작")
    print("=" * 80)

    all_results = {}

    for fname in TARGET_FILES:
        if fname not in reports:
            print(f"\n[SKIP] 리포트 없음: {fname}")
            continue

        ts, fr = reports[fname]
        db_nums, file_path = load_db_nums(fname)

        # QA 리포트에서 missing 문항 번호 수집
        report_missing = set()
        for m in fr.get('mismatches', []):
            for iss in m.get('issues', []):
                if iss.get('field') == 'missing':
                    try:
                        report_missing.add(int(m.get('question_number', 0)))
                    except Exception:
                        pass

        # 전체 PDF 문항번호 스캔 (1~45 범위)
        pdf_path = os.path.join(UPLOADS, file_path) if file_path else None
        pages = get_all_text(pdf_path) if pdf_path else {}
        all_pdf_nums = set()
        for text in pages.values():
            all_pdf_nums |= set(find_qnums_on_page(text))

        # 누락 = PDF에는 있는데 DB에 없는 것
        missing_nums = sorted(all_pdf_nums - db_nums - {0})
        # + QA 리포트에서 missing으로 표시된 것도 포함
        if report_missing:
            missing_nums = sorted(set(missing_nums) | report_missing)

        print(f"\n{'='*70}")
        print(f"파일: {fname}")
        print(f"  리포트: {ts}")
        print(f"  DB 문항: {sorted(db_nums)}")
        print(f"  PDF 감지 문항: {sorted(all_pdf_nums)}")
        print(f"  QA missing 표시: {sorted(report_missing)}")
        print(f"  → 누락 추정: {missing_nums}")

        if not missing_nums:
            print("  누락 문항 없음 (또는 PDF 스캔 불가)")
            continue

        # 각 누락 문항 조사
        print(f"\n  [STEP 2] 누락 문항 PDF 분석")
        results = []
        for qnum in missing_nums:
            pnum, ctx = find_question_context(pages, qnum)
            if not ctx:
                results.append((qnum, None, '텍스트 미발견', ['D-기타']))
                print(f"\n    Q{qnum}: PDF에서 텍스트 찾지 못함")
                continue

            causes = classify_cause(ctx)
            results.append((qnum, pnum, ctx, causes))

            print(f"\n    Q{qnum} (p{pnum}):")
            print(f"      원인: {', '.join(causes)}")
            # PUA 문자 위치 표시
            for m in PUA_RE.finditer(ctx):
                print(f"      PUA char: {repr(m.group())} at pos {m.start()}")
            # 공백 보기
            for m in SPACED_BOGI.finditer(ctx):
                print(f"      공백보기: {repr(m.group())}")
            print(f"      컨텍스트 (앞 300자):")
            print(f"      {repr(ctx[:300])}")

        all_results[fname] = results

    # ── STEP 3: 분류 표 ────────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("STEP 3: 원인 분류 종합")
    print(f"{'='*80}")
    cause_count = defaultdict(int)
    print(f"\n{'파일':<45} {'Q':>4}  {'원인'}")
    print("-" * 65)
    for fname, results in all_results.items():
        short = fname[-45:]
        for qnum, pnum, ctx, causes in results:
            cause_str = ', '.join(causes)
            print(f"{short:<45} Q{qnum:>2}  {cause_str}")
            for c in causes:
                cause_count[c.split('(')[0].strip()] += 1
        short = ''  # 이후 행은 파일명 생략

    print(f"\n원인별 집계:")
    for cause, cnt in sorted(cause_count.items()):
        print(f"  {cause}: {cnt}건")

    # ── STEP 4: 처리 방안 ──────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("STEP 4: 처리 방안 제안")
    print(f"{'='*80}")

    a_cnt = cause_count.get('A-PUA', 0)
    b_cnt = cause_count.get('B-공백보기', 0)
    c_cnt = cause_count.get('C-박스마커', 0)
    d_cnt = cause_count.get('D-기타', 0)
    total = a_cnt + b_cnt + c_cnt + d_cnt

    if a_cnt + b_cnt >= total * 0.7:
        print(f"""
  → 주요 원인: PUA({a_cnt}건) + 공백보기({b_cnt}건) — Q36/Q37과 동일 패턴
  → 처리: patch_missing_questions.py 확장 (파일명·문항번호 파라미터화)
  → 예상 성공률: 높음 (PyMuPDF로 텍스트 정상 추출됨)
""")
    if c_cnt > 0:
        print(f"""
  → 박스마커({c_cnt}건): <학습 활동>/<자료> 형태 문항
  → 처리: 별도 파싱 로직 (박스 내부 텍스트 추출)
  → 예상 성공률: 중간 (레이아웃 복잡도에 따라 다름)
""")
    if d_cnt > 0:
        print(f"""
  → 기타({d_cnt}건): 개별 PDF 확인 필요
  → 처리: 수동 조사 후 개별 패치
""")


if __name__ == '__main__':
    main()
