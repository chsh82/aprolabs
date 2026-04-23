"""
final_summary.py — QA 리포트 최종 현황 분석
============================================
사용법:
  python3 final_summary.py              # 최신 리포트 기반
  python3 final_summary.py --baseline   # 오늘 baseline과 비교
"""
import json, glob, os, re, argparse
from collections import defaultdict
from datetime import datetime

GOLDEN_DIR  = '/home/chsh82/aprolabs/golden_tests'
QNUM_PREFIX = re.compile(r'^\d+\.\s*[\u3000\s]*')
HTML_TAG    = re.compile(r'</?(u|b|i|em|strong|span|s)\b[^>]*>', re.IGNORECASE)
STEM_END_NN = re.compile(r'것은\?(?:\s*\[\d점\])?\s*\n\n')
STEM_END_SP = re.compile(r'것은\?(?:\s*\[\d점\])?\s+[\u3000<]')
BOGI_MARK   = re.compile(r'<보기>')

# 정답해설 / 합본 제외
SKIP_RE = re.compile(r'정답|해설|국어\(\)')


def latest_reports():
    """파일명 기준 최신 QA 리포트 1개씩."""
    paths = sorted(glob.glob(f'{GOLDEN_DIR}/qa_report_*.json'))
    latest = {}
    for path in paths:
        try:
            ts = os.path.basename(path).replace('qa_report_', '').replace('.json', '')
            for fr in json.load(open(path, encoding='utf-8')):
                fn = fr.get('filename', '')
                if SKIP_RE.search(fn):
                    continue
                if fn not in latest or ts > latest[fn][0]:
                    latest[fn] = (ts, fr)
        except Exception:
            pass
    return latest


def classify_issue(field, vision, db_val, db_stem=''):
    """이슈 유형 분류."""
    if field == 'bogi':
        if STEM_END_NN.search(db_stem) or STEM_END_SP.search(db_stem):
            return 'bogi_X'   # patch_bogi_hwajak 대상
        return 'bogi_Y'       # 기타

    if field == 'stem':
        db_stripped = QNUM_PREFIX.sub('', db_val)
        db_stripped = HTML_TAG.sub('', db_stripped).strip()
        if len(db_stripped) < 5:
            return 'stem_B_empty'
        if '\n\n<img' in db_val or '\n<img' in db_val:
            return 'stem_B_img'
        if not vision or vision in ('None', ''):
            return 'stem_A_vision_empty'
        return 'stem_Z'

    if field == 'image_location':
        return 'image'
    if field == 'missing':
        return 'missing'
    return field


def summarize_file(fr):
    counts = defaultdict(int)
    details = []
    for m in fr.get('mismatches', []):
        q = m.get('question_number')
        for iss in m.get('issues', []):
            field  = iss.get('field', '')
            vision = str(iss.get('vision', '') or '')
            db_val = str(iss.get('db',     '') or '')
            # db_stem: 보기 분류에 필요
            db_stem = db_val if field == 'bogi' else ''
            kind = classify_issue(field, vision, db_val, db_stem)
            counts[kind] += 1
            details.append((q, field, kind, vision[:50], db_val[:50]))
    return dict(counts), details


def bucket(total):
    if total == 0:    return '✅ 완벽'
    if total <= 2:    return '🟢 1-2건'
    if total <= 8:    return '🟡 3-8건'
    return               '🔴 9건+'


def load_baseline():
    """오늘 날짜 qa_baseline JSON 로드."""
    today = datetime.now().strftime('%Y%m%d')
    path  = f'{GOLDEN_DIR}/qa_baseline_{today}.json'
    if not os.path.isfile(path):
        # 가장 최근 것
        cands = sorted(glob.glob(f'{GOLDEN_DIR}/qa_baseline_*.json'))
        path  = cands[-1] if cands else None
    if not path:
        return None, None
    data = json.load(open(path, encoding='utf-8'))
    mapping = {}
    for f in data.get('files', []):
        s = f.get('summary', {})
        mapping[f['filename']] = {
            'bogi':    s.get('bogi_mismatch', 0),
            'image':   s.get('image_missing',  0),
            'stem':    s.get('stem_mismatch',  0),
            'missing': s.get('question_missing', 0),
        }
    return mapping, path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', action='store_true', help='baseline과 비교')
    args = parser.parse_args()

    reports = latest_reports()
    baseline_map, baseline_path = load_baseline() if args.baseline else (None, None)

    print("=" * 80)
    print("파이프라인 QA 최종 현황")
    print(f"집계 기준: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if baseline_path:
        print(f"비교 baseline: {os.path.basename(baseline_path)}")
    print("=" * 80)

    # ── 파일별 요약 표 ────────────────────────────────────────────────────────
    col = f"{'파일':<50} {'보기':>4} {'이미지':>5} {'발문':>5} {'누락':>5} {'합계':>5} {'판정':>8}"
    print(f"\n{col}")
    print("-" * 82)

    file_data = {}
    grand = defaultdict(int)
    bucket_cnt = defaultdict(int)

    for fname, (ts, fr) in sorted(reports.items()):
        counts, details = summarize_file(fr)

        bogi   = counts.get('bogi_X', 0) + counts.get('bogi_Y', 0)
        image  = counts.get('image', 0)
        stem   = sum(v for k, v in counts.items() if k.startswith('stem_'))
        miss   = counts.get('missing', 0)
        total  = bogi + image + stem + miss

        grand['bogi']  += bogi
        grand['image'] += image
        grand['stem']  += stem
        grand['miss']  += miss
        grand['total'] += total

        b = bucket(total)
        bucket_cnt[b] += 1
        file_data[fname] = (ts, counts, details, bogi, image, stem, miss, total)

        short = fname[-50:]
        diff_str = ''
        if baseline_map and fname in baseline_map:
            bl = baseline_map[fname]
            bl_total = sum(bl.values())
            diff = total - bl_total
            diff_str = f'  ({diff:+d})'

        print(f"{short:<50} {bogi:>4} {image:>5} {stem:>5} {miss:>5} {total:>5} {b:>8}{diff_str}  [{ts}]")

    print("-" * 82)
    print(f"{'합계':<50} {grand['bogi']:>4} {grand['image']:>5} {grand['stem']:>5} {grand['miss']:>5} {grand['total']:>5}")

    # ── 통계 ──────────────────────────────────────────────────────────────────
    print(f"\n{'='*40}")
    print("파일 판정 분포")
    print(f"{'='*40}")
    for label in ['✅ 완벽', '🟢 1-2건', '🟡 3-8건', '🔴 9건+']:
        cnt = bucket_cnt.get(label, 0)
        bar = '█' * cnt
        print(f"  {label:10s} {cnt:3d}개  {bar}")
    print(f"  총 {len(reports)}개 파일")

    # ── 이슈 유형별 분류 상세 ────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("이슈 유형별 전체 집계")
    print(f"{'='*80}")
    type_total = defaultdict(int)
    for fname, (ts, counts, details, *_) in file_data.items():
        for k, v in counts.items():
            type_total[k] += v

    labels = {
        'bogi_X':          'bogi_X  — patch_bogi_hwajak 대상 (것은?\\n\\n 패턴)',
        'bogi_Y':          'bogi_Y  — 기타 보기불일치',
        'stem_A_vision_empty': 'stem_A  — vision 빈 값 (Gemini 오탐)',
        'stem_B_empty':    'stem_B  — DB stem 빈값/번호만',
        'stem_B_img':      'stem_B  — DB stem에 <img> 포함',
        'stem_Z':          'stem_Z  — 실제 내용 불일치',
        'image':           'image   — 이미지 위치/누락',
        'missing':         'missing — 문항 누락',
    }
    for key, label in labels.items():
        n = type_total.get(key, 0)
        if n:
            print(f"  {label:<55} {n:>4}건")

    # ── 파일별 상세 (이슈 있는 파일만) ─────────────────────────────────────
    print(f"\n{'='*80}")
    print("파일별 이슈 상세 (0건 파일 제외)")
    print(f"{'='*80}")
    for fname, (ts, counts, details, bogi, image, stem, miss, total) in sorted(file_data.items()):
        if total == 0:
            continue
        print(f"\n  [{fname[-55:]}]  합계={total}")
        # 유형별 소계
        type_summary = ', '.join(f'{k}={v}' for k, v in sorted(counts.items()) if v)
        print(f"    유형: {type_summary}")
        # Q별 목록
        by_q = defaultdict(list)
        for q, field, kind, vis, db in details:
            by_q[q].append(f'{kind}')
        for q in sorted(by_q.keys(), key=lambda x: (x is None, x)):
            print(f"    Q{q}: {', '.join(by_q[q])}")

    # ── baseline 비교 ─────────────────────────────────────────────────────────
    if baseline_map:
        print(f"\n{'='*80}")
        print("Baseline 대비 변화 (오늘 아침 → 현재)")
        print(f"{'='*80}")
        improved = worsened = unchanged = 0
        for fname, (ts, counts, details, bogi, image, stem, miss, total) in sorted(file_data.items()):
            if fname not in baseline_map:
                continue
            bl = baseline_map[fname]
            bl_total = sum(bl.values())
            diff = total - bl_total
            if diff < 0:
                print(f"  ↓ {fname[-50:]:50s}  {bl_total} → {total}  ({diff:+d})")
                improved += 1
            elif diff > 0:
                print(f"  ↑ {fname[-50:]:50s}  {bl_total} → {total}  ({diff:+d})")
                worsened += 1
            else:
                unchanged += 1
        print(f"\n  개선: {improved}개  악화: {worsened}개  동일: {unchanged}개")


if __name__ == '__main__':
    main()
