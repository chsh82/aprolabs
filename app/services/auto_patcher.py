"""
G-2: 자동 패치 — QA 이슈 자동 복구
"""
import re
import json
import difflib

CIRCLES = ["①", "②", "③", "④", "⑤"]
RANGE_MARKER_RE = re.compile(r'\[(?:A|B|C|D):\s*(?:START|END)\]', re.IGNORECASE)

_REVERSE_DETECT_RE = re.compile(r'[·]{3,}[①②③④⑤]')
_REVERSE_SPLIT_RE = re.compile(r'[·\s]{2,}([①②③④⑤])')
_CHOICE_BULLET_RE = re.compile(r'(?:^|\n)\s*[◦•∙]\s*\S')
_CIRCLE_ORDER = {c: i for i, c in enumerate(CIRCLES)}
_BULLET_RE = re.compile(r'^[◦•◆◇▪▫・\-]\s*')
_CIRCLE_SET = set(CIRCLES)

_SCORE_RE = re.compile(r'\[[23]점\]')
_KO_QEND_RE = re.compile(
    r'(?:것은|것인가|엇인가|무엇인가|있는가|없는가|옳은가|맞는가|알맞은가|고르면)[?？]'
)
_QMARK_RE = re.compile(r'[?？]')
_STMT_RE = re.compile(r'(?:이다|였다)[.。](?=[\s　]+\S)')
_HTML_TAG_RE = re.compile(r'</?(u|b|i|em|strong|s|span)\b[^>]*>', re.IGNORECASE)


# ── PDF 유틸 ──────────────────────────────────────────────────────────────────

def _get_all_page_text(pdf_path: str) -> dict:
    import fitz
    doc = fitz.open(pdf_path)
    pages = {i + 1: page.get_text() for i, page in enumerate(doc)}
    doc.close()
    return pages


def _find_raw_block(pages: dict, qnum: int) -> tuple:
    full = ""
    page_starts = {}
    offset = 0
    for pnum in sorted(pages):
        page_starts[offset] = pnum
        full += pages[pnum]
        offset += len(pages[pnum])

    def qpat(n):
        return re.compile(r'(?:^|\n)\s*' + str(n) + r'[.．][\s　  ]')

    m_start = qpat(qnum).search(full)
    if not m_start:
        return -1, ""

    block_start = m_start.start() + (1 if full[m_start.start()] == "\n" else 0)
    block_end = len(full)
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


# ── 선택지 파싱 ────────────────────────────────────────────────────────────────

def _clean_inline(text: str) -> str:
    return re.sub(r'\s*\n\s*', ' ', text).strip()


def _cleanup_choice(raw: str) -> str:
    raw = RANGE_MARKER_RE.sub('', raw)
    raw = _clean_inline(raw)
    raw = re.sub(r'\s*\d{4}학년도.*$', '', raw, flags=re.DOTALL).strip()
    raw = re.sub(r'\s*[가-힣]+영역.*$', '', raw, flags=re.DOTALL).strip()
    raw = re.sub(r'\s*\[\d+\s*[~～]\s*\d+\].*$', '', raw, flags=re.DOTALL).strip()
    raw = re.sub(r'\s*고\d+\s*$', '', raw).strip()
    raw = re.sub(r'(\s*\[[A-Z]\])+\s*$', '', raw).strip()
    return raw


def _parse_standard(block: str) -> list:
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
        choices.append(_cleanup_choice(raw))
    return choices


def _parse_reverse(block: str) -> list:
    parts = _REVERSE_SPLIT_RE.split(block)
    if len(parts) < 3:
        return []
    choices_raw = []
    for i in range(1, len(parts), 2):
        if i >= len(parts):
            break
        circle = parts[i]
        if circle not in _CIRCLE_SET:
            continue
        seg = parts[i - 1]
        if i == 1:
            bullet_matches = list(_CHOICE_BULLET_RE.finditer(seg))
            if bullet_matches:
                text = seg[bullet_matches[-1].start():].lstrip('\n').strip()
            else:
                text = seg.rsplit('\n', 1)[-1].strip()
        else:
            text = seg.lstrip('\n').strip()
        text = _BULLET_RE.sub('', text)
        choices_raw.append((circle, text))
    if len(choices_raw) < 3:
        return []
    choices_raw.sort(key=lambda x: _CIRCLE_ORDER.get(x[0], 99))
    return [_cleanup_choice(text) for _, text in choices_raw]


def _parse_choices_from_block(block: str) -> list:
    if _REVERSE_DETECT_RE.search(block):
        choices = _parse_reverse(block)
        if len(choices) >= 3:
            return choices
    return _parse_standard(block)


def _choices_to_list(choices) -> list:
    if isinstance(choices, dict):
        return [choices[k] for k in sorted(choices.keys(), key=lambda x: int(x))]
    if isinstance(choices, list):
        return list(choices)
    return []


def _list_to_choices(choices_list: list, original_was_dict: bool):
    if original_was_dict:
        return {str(i + 1): v for i, v in enumerate(choices_list)}
    return choices_list


def _choices_similarity(old_list: list, new_list: list) -> float:
    if not old_list:
        return 1.0
    old_text = ' '.join(old_list)
    new_text = ' '.join(new_list) if len(new_list) > len(old_list) else ' '.join(new_list[:len(old_list)])
    if not old_text and not new_text:
        return 1.0
    return difflib.SequenceMatcher(None, old_text, new_text).ratio()


# ── stem/bogi 분리 ─────────────────────────────────────────────────────────────

def _find_stem_end(text: str) -> int:
    m = _SCORE_RE.search(text)
    if m and text[m.end():].strip():
        return m.end()
    ko_matches = list(_KO_QEND_RE.finditer(text))
    if ko_matches:
        last_m = ko_matches[-1]
        if text[last_m.end():].strip():
            return last_m.end()
    positions = [m.start() for m in _QMARK_RE.finditer(text)]
    if positions:
        last_q = positions[-1]
        if text[last_q + 1:].strip():
            return last_q + 1
    m = _STMT_RE.search(text)
    if m and text[m.end():].strip():
        return m.end()
    return -1


def _split_stem_bogi(old_stem: str) -> tuple:
    idx = _find_stem_end(old_stem)
    if idx < 0:
        return old_stem, ''
    raw_stem = old_stem[:idx].strip()
    raw_bogi = re.sub(r'^[\s　]+', '', old_stem[idx:]).strip()
    new_stem = _HTML_TAG_RE.sub('', raw_stem).strip()
    return new_stem, raw_bogi


def _build_content(stem: str, bogi: str, choices) -> str:
    cl = _choices_to_list(choices)
    parts = [stem]
    if bogi:
        parts.append(f'< 보 기 > {bogi}')
    for i, c in enumerate(cl):
        if i < len(CIRCLES):
            parts.append(f'{CIRCLES[i]}{c}')
    return ' 　'.join(parts)


# ── 패치 핸들러 ────────────────────────────────────────────────────────────────

def _find_question(questions: list, qnum: int) -> dict | None:
    for q in questions:
        if isinstance(q, dict):
            try:
                if int(q.get("number", -1)) == qnum:
                    return q
            except (ValueError, TypeError):
                pass
    return None


def _patch_choices(questions: list, issue: dict, pages: dict) -> tuple:
    qnum = issue.get("question_number")
    if qnum is None:
        return False, "question_number 없음"

    q = _find_question(questions, qnum)
    if q is None:
        return False, f"Q{qnum} 세그먼트 미발견"

    _, raw = _find_raw_block(pages, qnum)
    if not raw:
        return False, f"Q{qnum} PDF 블록 미발견"

    new_list = _parse_choices_from_block(raw)
    if len(new_list) < 5:
        return False, f"PDF 추출 {len(new_list)}개 < 5"

    old_choices = q.get("choices") or {}
    old_list = _choices_to_list(old_choices)
    if old_list:
        sim = _choices_similarity(old_list, new_list)
        if sim < 0.70:
            return False, f"유사도 {int(sim * 100)}% < 70%"

    original_was_dict = isinstance(old_choices, dict)
    q["choices"] = _list_to_choices(new_list, original_was_dict)
    q["content"] = _build_content(q.get("stem", ""), q.get("bogi", ""), q["choices"])
    return True, f"choices {len(new_list)}개 복구"


def _patch_stem_bogi(questions: list, issue: dict) -> tuple:
    qnum = issue.get("question_number")
    if qnum is None:
        return False, "question_number 없음"

    q = _find_question(questions, qnum)
    if q is None:
        return False, f"Q{qnum} 세그먼트 미발견"

    old_stem = (q.get("stem") or "").strip()
    if (q.get("bogi") or "").strip():
        return False, "bogi 이미 존재"

    new_stem, new_bogi = _split_stem_bogi(old_stem)
    if new_stem == old_stem or len(new_bogi) < 20:
        return False, f"분리 실패 (bogi {len(new_bogi)}자)"
    if len(new_stem) < 10:
        return False, f"분리 후 stem {len(new_stem)}자 < 10"

    q["stem"] = new_stem
    q["bogi"] = new_bogi
    q["content"] = _build_content(new_stem, new_bogi, q.get("choices", {}))
    return True, f"stem {len(old_stem)}자 → stem {len(new_stem)}자 + bogi {len(new_bogi)}자"


def _patch_missing(questions: list, issue: dict, pages: dict) -> tuple:
    qnum = issue.get("question_number")
    if qnum is None:
        return False, "question_number 없음"

    _, raw = _find_raw_block(pages, qnum)
    if not raw:
        return False, f"Q{qnum} PDF 블록 미발견"

    new_list = _parse_choices_from_block(raw)
    stem_end = raw.find(CIRCLES[0]) if new_list else len(raw)
    stem_text = raw[:stem_end].strip()
    if len(stem_text) < 5:
        return False, "stem 추출 실패"

    choices = {str(i + 1): v for i, v in enumerate(new_list)}
    new_q = {
        "number": qnum,
        "stem": stem_text,
        "bogi": "",
        "choices": choices,
        "content": _build_content(stem_text, "", choices),
        "_auto_patched": True,
    }
    questions.append(new_q)
    questions.sort(key=lambda x: x.get("number") or 0)
    return True, f"Q{qnum} PDF에서 복구 (stem {len(stem_text)}자, choices {len(new_list)}개)"


def _patch_empty_stem(questions: list, issue: dict) -> tuple:
    qnum = issue.get("question_number")
    if qnum is None:
        return False, "question_number 없음"

    q = _find_question(questions, qnum)
    if q is None:
        return False, f"Q{qnum} 세그먼트 미발견"

    bogi = (q.get("bogi") or "").strip()
    if len(bogi) < 5:
        return False, "bogi도 비어 있음"

    q["stem"] = bogi
    q["bogi"] = ""
    q["content"] = _build_content(bogi, "", q.get("choices", {}))
    return True, f"stem <- bogi ({len(bogi)}자)"


# ── 메인 API ──────────────────────────────────────────────────────────────────

def auto_patch(segments: dict, issues: list, pdf_path: str = None) -> dict:
    """
    QA 이슈 자동 복구.
    반환: {patched: int, skipped: int, segments: dict}
    """
    questions_raw = segments.get("questions", [])
    if isinstance(questions_raw, str):
        questions = json.loads(questions_raw)
    else:
        questions = list(questions_raw)

    pages = None
    if pdf_path:
        try:
            pages = _get_all_page_text(pdf_path)
        except Exception:
            pages = None

    patched_count = 0
    skipped_count = 0
    patch_log = []

    for issue in issues:
        issue_type = issue.get("type")
        qnum = issue.get("question_number")
        ok, reason = False, "처리 불가"

        if issue_type == "choices_incomplete":
            if pages:
                ok, reason = _patch_choices(questions, issue, pages)
            else:
                reason = "PDF 없음 — choices 패치 불가"

        elif issue_type == "stem_bogi_merged":
            ok, reason = _patch_stem_bogi(questions, issue)

        elif issue_type == "missing_question":
            if pages:
                ok, reason = _patch_missing(questions, issue, pages)
            else:
                reason = "PDF 없음 — 누락 문항 복구 불가"

        elif issue_type == "empty_stem":
            ok, reason = _patch_empty_stem(questions, issue)

        if ok:
            patched_count += 1
        else:
            skipped_count += 1

        patch_log.append({
            "type": issue_type,
            "question_number": qnum,
            "patched": ok,
            "reason": reason,
        })

    new_segments = dict(segments)
    if isinstance(segments.get("questions"), str):
        new_segments["questions"] = json.dumps(questions, ensure_ascii=False)
    else:
        new_segments["questions"] = questions
    new_segments["_patch_log"] = patch_log

    return {
        "patched": patched_count,
        "skipped": skipped_count,
        "segments": new_segments,
    }
