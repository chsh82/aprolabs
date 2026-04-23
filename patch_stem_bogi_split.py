"""
patch_stem_bogi_split.py — stem에 혼입된 bogi 분리 복구
=========================================================
사용법:
  python3 patch_stem_bogi_split.py --file "..." --questions "3,37" --dry-run
  python3 patch_stem_bogi_split.py --file "..." --questions "3,37" --apply
"""
import sqlite3, json, re, argparse

DB_PATH = '/home/chsh82/aprolabs/aprolabs.db'
UPLOADS = '/home/chsh82/aprolabs'

CIRCLES     = ['①', '②', '③', '④', '⑤']
HTML_TAG_RE = re.compile(r'</?(u|b|i|em|strong|s|span)\b[^>]*>', re.IGNORECASE)

# ── 분리 규칙 ─────────────────────────────────────────────────────────────────

# 1순위: 점수 태그
_SCORE_RE = re.compile(r'\[[23]점\]')

# 2순위: 마지막 의문문 종결 '?'
_QMARK_RE = re.compile(r'[?？]')

# 3순위: 지시문 종결 (이다./였다. 뒤 비공백)
_STMT_RE  = re.compile(r'(?:이다|였다)[.。](?=[\s\u3000]+\S)')


def find_stem_end(text: str) -> int:
    """stem 종결 인덱스 반환 (bogi 시작 직전 위치). 분리 불가 시 -1."""

    # 1순위: [3점] / [2점]
    m = _SCORE_RE.search(text)
    if m and text[m.end():].strip():
        return m.end()

    # 2순위: 마지막 '?' 직후 — 뒤에 실질 내용이 있을 때만
    positions = [m.start() for m in _QMARK_RE.finditer(text)]
    if positions:
        last_q = positions[-1]
        if text[last_q + 1:].strip():
            return last_q + 1

    # 3순위: '이다.' / '였다.' 뒤 공백 + 비공백
    m = _STMT_RE.search(text)
    if m and text[m.end():].strip():
        return m.end()

    return -1


def split_stem_bogi(old_stem: str) -> tuple:
    """(new_stem, new_bogi) 반환. 분리 불가 시 (old_stem, '')."""
    idx = find_stem_end(old_stem)
    if idx < 0:
        return old_stem, ''

    raw_stem = old_stem[:idx].strip()
    raw_bogi = re.sub(r'^[\s\u3000]+', '', old_stem[idx:]).strip()

    new_stem = HTML_TAG_RE.sub('', raw_stem).strip()
    return new_stem, raw_bogi

# ── content 재조립 ────────────────────────────────────────────────────────────

def choices_to_list(choices) -> list:
    if isinstance(choices, dict):
        return [choices[k] for k in sorted(choices.keys(), key=lambda x: int(x))]
    if isinstance(choices, list):
        return list(choices)
    return []


def build_content(stem: str, bogi: str, choices) -> str:
    cl = choices_to_list(choices)
    parts = [stem]
    if bogi:
        parts.append(f'< 보 기 > {bogi}')
    for i, c in enumerate(cl):
        if i < len(CIRCLES):
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
    print(f"patch_stem_bogi_split  ({'apply' if apply else 'dry-run'})")
    print('=' * 60)

    job_row, segs, qs = load_job(args.file)
    fname = job_row['filename']
    print(f"\n파일: {fname}")
    print(f"대상: Q{target_nums}")

    patches = []
    for qnum in target_nums:
        print(f"\n{'─'*50}")
        q = find_question(qs, qnum)
        if q is None:
            print(f"Q{qnum}: DB에 없음")
            continue

        old_stem  = q.get('stem', '') or ''
        old_bogi  = q.get('bogi', '') or ''
        choices   = q.get('choices', {})

        # ── 현재 상태 출력 ────────────────────────────────────────────────────
        print(f"Q{qnum}  현재 stem ({len(old_stem)}자):")
        print(f"  {repr(old_stem[:150])}")
        print(f"Q{qnum}  현재 bogi ({len(old_bogi)}자):")
        print(f"  {repr(old_bogi[:80])}")

        # 안전장치: bogi 이미 존재
        if old_bogi.strip():
            print(f"  [SKIP] bogi 이미 존재 — 덮어쓰기 금지")
            continue

        # ── 분리 시도 ─────────────────────────────────────────────────────────
        new_stem, new_bogi = split_stem_bogi(old_stem)

        print(f"\n  ▶ 분리 결과:")
        print(f"    new_stem ({len(new_stem)}자): {repr(new_stem[:150])}")
        print(f"    new_bogi ({len(new_bogi)}자): {repr(new_bogi[:150])}")

        # ── 안전장치 ──────────────────────────────────────────────────────────
        skip = False

        if new_stem == old_stem:
            print(f"  [SKIP] 분리 위치 미발견")
            skip = True
        elif len(new_bogi) < 20:
            print(f"  [SKIP] 추출 bogi {len(new_bogi)}자 < 20자 — 노이즈 가능성")
            skip = True
        elif len(new_stem) < 10:
            print(f"  [SKIP] 분리 후 stem {len(new_stem)}자 < 10자")
            skip = True

        if skip:
            continue

        new_content = build_content(new_stem, new_bogi, choices)
        print(f"    new_content (앞100): {repr(new_content[:100])}")
        print(f"  → {'apply 예정' if apply else 'dry-run OK'}")

        patches.append({
            'qnum':        qnum,
            'new_stem':    new_stem,
            'new_bogi':    new_bogi,
            'new_content': new_content,
        })

    # ── dry-run 종료 ──────────────────────────────────────────────────────────
    if not apply:
        print(f"\n[dry-run 완료] 총 {len(patches)}건 적용 가능. --apply 추가 시 DB에 반영됩니다.")
        return

    if not patches:
        print(f"\n적용할 패치 없음.")
        return

    # ── 적용 ──────────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("DB 적용 중...")
    new_qs = list(qs)
    for p in patches:
        q = find_question(new_qs, p['qnum'])
        if q is None:
            continue
        q['stem']    = p['new_stem']
        q['bogi']    = p['new_bogi']
        q['content'] = p['new_content']
        print(f"  Q{p['qnum']}: stem/bogi/content 업데이트")

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
