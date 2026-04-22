"""
fix_bogi_split.py — 보기 분리 자동 수정
------------------------------------------
QA 리포트에서 bogi 불일치 케이스를 분석하고,
stem에 `< 보 기 >` 구분자가 포함된 경우 bogi를 자동 분리.

사용법:
  python3 fix_bogi_split.py [--dry-run] [--report REPORT_PATH] [--file FILENAME]
"""
import os, sys, json, sqlite3, re, argparse, glob
from datetime import datetime

DB_PATH     = '/home/chsh82/aprolabs/aprolabs.db'
REPORT_DIR  = '/home/chsh82/aprolabs/golden_tests'

def _latest_report():
    files = sorted(glob.glob(f'{REPORT_DIR}/qa_report_*.json'))
    return files[-1] if files else None

# < 보 기 > (spaces between chars) → section separator
BOGI_SEPARATOR_RE = re.compile(r'<\s+보\s+기\s+>')
# 문항 앞 번호 패턴: "15. " or "15.\u3000"
QNUM_PREFIX_RE = re.compile(r'^\d+\.\s*[\u3000\s]*')

def clean_stem(stem: str) -> str:
    """stem에서 bogi 분리자 이전 텍스트만 추출하고 번호 앞 공백 정리."""
    m = BOGI_SEPARATOR_RE.search(stem)
    if m:
        stem = stem[:m.start()]
    return stem.rstrip()

def extract_bogi(stem: str) -> str | None:
    """stem에서 bogi 분리자 이후 텍스트 추출."""
    m = BOGI_SEPARATOR_RE.search(stem)
    if m:
        content = stem[m.end():].strip()
        return content if content else None
    return None

def load_report(path: str):
    with open(path, encoding='utf-8') as f:
        return json.load(f)

def get_bogi_mismatch_cases(report):
    """리포트에서 보기 유무 불일치 케이스 수집."""
    cases = {}  # (filename, qnum) → True
    for r in report:
        for m in r.get('mismatches', []):
            for issue in m.get('issues', []):
                if issue['field'] == 'bogi' and issue.get('vision') is True and issue.get('db') is False:
                    key = (r['filename'], m['question_number'])
                    cases[key] = True
    return cases

def run(dry_run: bool, report_path: str = None, file_filter: str = None):
    report = load_report(report_path or _latest_report())
    mismatch_cases = get_bogi_mismatch_cases(report)
    if file_filter:
        mismatch_cases = {k: v for k, v in mismatch_cases.items() if file_filter in k[0]}
    print(f"QA 리포트 bogi 불일치 (vision=True, db=False): {len(mismatch_cases)}건\n")

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    cur.execute("SELECT id, filename, segments FROM pipeline_jobs WHERE segments IS NOT NULL")
    jobs = cur.fetchall()

    fixed = []
    skipped = []
    examples = []

    for job in jobs:
        fname = job['filename']
        raw = job['segments']
        if not raw:
            continue
        segs = json.loads(raw)
        if not segs or not isinstance(segs, dict):
            continue
        qs_raw = segs.get('questions', '[]')
        questions = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw

        modified = False
        for q in questions:
            qnum = q.get('number')
            if (fname, qnum) not in mismatch_cases:
                continue

            stem = str(q.get('stem', ''))
            current_bogi = q.get('bogi')

            # 조건: stem에 < 보 기 > 구분자가 있고, bogi가 null/empty
            if not BOGI_SEPARATOR_RE.search(stem):
                skipped.append((fname, qnum, 'no_separator'))
                continue
            if current_bogi and str(current_bogi).strip() and str(current_bogi) != 'None':
                skipped.append((fname, qnum, 'bogi_already_set'))
                continue

            new_stem = clean_stem(stem)
            new_bogi = extract_bogi(stem)

            if new_bogi is None:
                skipped.append((fname, qnum, 'bogi_empty_after_split'))
                continue

            # 예시 수집 (최초 3건)
            if len(examples) < 3:
                examples.append({
                    'file': fname,
                    'qnum': qnum,
                    'before_stem': stem[:200],
                    'before_bogi': repr(current_bogi),
                    'after_stem': new_stem[:200],
                    'after_bogi': new_bogi[:200],
                })

            q['stem'] = new_stem
            q['bogi'] = new_bogi
            fixed.append((fname, qnum))
            modified = True

        if modified and not dry_run:
            segs['questions'] = json.dumps(questions, ensure_ascii=False)
            cur.execute(
                "UPDATE pipeline_jobs SET segments = ?, updated_at = ? WHERE id = ?",
                (json.dumps(segs, ensure_ascii=False), datetime.utcnow().isoformat(), job['id'])
            )

    if not dry_run:
        db.commit()
    db.close()

    # ── 결과 출력 ──────────────────────────────────────────────────────────────
    mode = "[DRY-RUN]" if dry_run else "[APPLIED]"
    print(f"{mode} 수정 완료: {len(fixed)}건 / 건너뜀: {len(skipped)}건\n")

    print("=== 수정된 문항 목록 ===")
    for fname, qnum in fixed:
        print(f"  {fname[-45:]} Q{qnum}")

    if skipped:
        print("\n=== 건너뜀 ===")
        for fname, qnum, reason in skipped:
            print(f"  {fname[-40:]} Q{qnum} ({reason})")

    if examples:
        print("\n=== Before / After 예시 (최대 3건) ===")
        for ex in examples:
            print(f"\n  [{ex['file'][-35:]} Q{ex['qnum']}]")
            print(f"  BEFORE stem : {repr(ex['before_stem'])}")
            print(f"  BEFORE bogi : {ex['before_bogi']}")
            print(f"  AFTER  stem : {repr(ex['after_stem'])}")
            print(f"  AFTER  bogi : {repr(ex['after_bogi'])}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='DB 수정 없이 결과 미리보기')
    parser.add_argument('--report', default=None, help='QA 리포트 경로 (기본: 최신 리포트)')
    parser.add_argument('--file', default=None, help='파일명 필터 (부분 일치)')
    args = parser.parse_args()

    report_path = args.report or _latest_report()
    if not report_path:
        print("QA 리포트 없음")
        sys.exit(1)
    print(f"리포트: {report_path}")
    run(dry_run=args.dry_run, report_path=report_path, file_filter=args.file)
