"""
Stage 2: 텍스트 구조 파싱 (순수 Regex)
- plain text(+[[IMG:url]] 마커)로 문항번호 탐색
- 추출 후 [[IMG:url]] → <img> HTML 변환
"""
import re

IMG_MARKER_RE = re.compile(r'\[\[IMG:([^\]]+)\]\]')


# ─────────────────────────────────────────
# 메인 진입점
# ─────────────────────────────────────────

def segment_text(ocr_text: str, job_id: str = None, question_hints: list = None) -> dict:
    """추출된 텍스트(plain + [[IMG:url]] 마커)를 지문과 문항으로 분리
    question_hints: Gemini Vision이 탐지한 [{number, first_words}, ...] — 누락 문항 보완용
    """
    # 페이지 구분자 제거
    clean = re.sub(r"--- 페이지 \d+ ---\n?", "", ocr_text)
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    clean = _normalize_whitespace(clean)

    # 문항 위치 탐색
    q_positions = _find_question_positions(clean, question_hints or [])
    sorted_nums = sorted(q_positions.keys())

    if not sorted_nums:
        return {"passages": [], "questions": []}

    questions = _extract_questions(clean, q_positions, sorted_nums)
    passages = _extract_passages(clean, q_positions, sorted_nums)

    # [[IMG:url]] → <img> 변환
    _convert_img_markers(passages, questions)

    return {"passages": passages, "questions": questions}


# ─────────────────────────────────────────
# 문항번호 위치 탐색
# ─────────────────────────────────────────

def _find_question_positions(text: str, hints: list = None) -> dict:
    """수능 문항번호(1~45) 위치 탐색 + Gemini 힌트로 누락 보완"""
    positions = {}
    for num in range(1, 46):
        pos = _find_single_question(text, num)
        if pos is not None:
            positions[num] = pos

    # Gemini 힌트로 누락 문항 보완
    if hints:
        for hint in hints:
            num = hint.get("number")
            if not num or num in positions:
                continue
            first_words = (hint.get("first_words") or "").strip()
            if first_words:
                pos = _find_by_first_words(text, num, first_words)
                if pos is not None:
                    positions[num] = pos

    return _validate_sequence(positions)


def _find_by_first_words(text: str, num: int, first_words: str) -> int | None:
    """Gemini 제공 발문 첫 단어로 문항번호 위치 탐지"""
    try:
        escaped = re.escape(first_words[:20].strip())
        m = re.search(escaped, text)
        if not m:
            return None
        # 발견 위치 직전 100자 내에서 문항번호 탐색
        search_start = max(0, m.start() - 120)
        before = text[search_start:m.start()]
        num_m = re.search(rf'^[ \t]*{num}[.．。]', before, re.MULTILINE)
        if num_m:
            return search_start + num_m.start()
    except re.error:
        pass
    return None


def _find_single_question(text: str, num: int) -> int | None:
    patterns = [
        (rf'^[ \t]*{num}[.．。][ \t]',   re.MULTILINE),  # 줄 시작 + 공백
        (rf'^[ \t]*{num}[.．。]\n',       re.MULTILINE),  # 줄 시작 + 줄바꿈
        (rf'\n[ \t]*{num}[.．。][ \t]',   0),             # 줄바꿈 후 + 공백
    ]
    for pat, flags in patterns:
        for m in re.finditer(pat, text, flags):
            line = _get_line(text, m.start())
            if any(skip in line for skip in ['페이지', '교시', '수험번호', '성명']):
                continue
            line_stripped = line.strip()
            if re.match(rf'^{num}[.．。]', line_stripped):
                return m.start()
    return None


def _get_line(text: str, pos: int) -> str:
    start = text.rfind('\n', 0, pos) + 1
    end = text.find('\n', pos)
    return text[start:end] if end >= 0 else text[start:]


def _validate_sequence(positions: dict) -> dict:
    """
    연속된 문항 시퀀스 유지 (1개 gap 허용).
    수능 선택과목 구조(예: 1~11 공통 + 12~17 선택 + 23~45 공통처럼
    중간에 큰 gap이 있는 비연속 구간)도 올바르게 처리.
    """
    if len(positions) <= 3:
        return positions

    nums = sorted(positions.keys())

    # 연속 구간 분리 (gap ≤ 2 이면 같은 구간)
    sequences = []
    cur = [nums[0]]
    for n in nums[1:]:
        if n <= cur[-1] + 2:
            cur.append(n)
        else:
            sequences.append(cur)
            cur = [n]
    sequences.append(cur)

    # 구간이 1개 → 그대로 사용
    if len(sequences) == 1:
        longest = sequences[0]
        if len(longest) < len(nums) * 0.5:
            return positions
        min_n, max_n = min(longest), max(longest)
        return {n: positions[n] for n in nums if min_n <= n <= max_n}

    # 구간이 여러 개 → 수능 선택과목 패턴 판별
    # 패턴: 구간들의 합이 전체의 70% 이상이면 모두 합쳐서 사용
    total_in_seqs = sum(len(s) for s in sequences)
    if total_in_seqs >= len(nums) * 0.70:
        # 모든 구간을 합쳐서 반환 (비연속 구간 전체 허용)
        valid_nums = set()
        for seq in sequences:
            if len(seq) >= 2:  # 단독 1개짜리 오탐 제거
                valid_nums.update(seq)
        if valid_nums:
            return {n: positions[n] for n in nums if n in valid_nums}

    # 폴백: 최장 단일 시퀀스 사용
    longest = max(sequences, key=len)
    if len(longest) < len(nums) * 0.5:
        return positions
    min_n, max_n = min(longest), max(longest)
    return {n: positions[n] for n in nums if min_n <= n <= max_n}


# ─────────────────────────────────────────
# 문항 텍스트 추출
# ─────────────────────────────────────────

def _extract_questions(text: str, positions: dict, sorted_nums: list) -> list:
    # 핵심: 텍스트 내 실제 위치 순서로 slice (번호 순서 ≠ 텍스트 위치 순서)
    # 예) 2단 레이아웃: Q2(pos=66), Q3(pos=134), Q1(pos=170) → 번호순 slice 시 Q1 소실
    pos_sorted = sorted(positions.items(), key=lambda kv: kv[1])

    questions = []
    for i, (num, start) in enumerate(pos_sorted):
        end = pos_sorted[i + 1][1] if i + 1 < len(pos_sorted) else len(text)

        q_text = text[start:end].strip()
        if len(q_text) < 5:
            continue

        q_text = _trim_trailing_passage(q_text)

        choices = _parse_choices(q_text)
        stem = _extract_stem(q_text)
        bogi = _extract_bogi(q_text)

        questions.append({
            "number": num,
            "passage_ref": None,
            "passage_idx": None,
            "stem": stem,
            "bogi": bogi,
            "choices": choices,
            "content": q_text,
            "has_choices": choices is not None,
            "answer": None,
            "explanation": None,
        })

    # 최종 결과는 문항 번호 순서로 정렬
    questions.sort(key=lambda q: q["number"])
    return questions


def _trim_trailing_passage(q_text: str) -> str:
    """
    문항 끝에 붙은 다음 지문 제거.
    - 마지막 ⑤ 이후 빈 줄 + 긴 텍스트 = 다음 지문 시작
    - 임계값을 100 → 60자로 완화 (짧은 지문도 잘 제거)
    """
    last_choice = max(
        (q_text.rfind(c) for c in "①②③④⑤"),
        default=-1
    )
    if last_choice < 0:
        return q_text
    after = q_text[last_choice:]
    m = re.search(r'\n\n\S.{60,}', after, re.DOTALL)
    if m:
        return q_text[:last_choice + m.start()].strip()
    return q_text


# ─────────────────────────────────────────
# 지문 추출
# ─────────────────────────────────────────

def _extract_passages(text: str, positions: dict, sorted_nums: list) -> list:
    passages = []

    # 텍스트 위치 순서로 정렬 (번호 순서 아님)
    pos_sorted = sorted(positions.items(), key=lambda kv: kv[1])

    # 텍스트에서 가장 먼저 등장하는 문항 이전이 지문
    first_pos = pos_sorted[0][1]
    pre = _strip_passage_intro(text[:first_pos].strip())
    if len(pre) > 30:
        passages.append({
            "id": "p1",
            "question_range": None,
            "content": _clean(pre),
        })

    # 각 문항 블록(텍스트 위치 순) 끝에 붙은 다음 지문
    # pos_sorted[-2]까지만 순회: 마지막 문항(45번 등) 이후 텍스트는 지문이 아님
    # (저작권 안내, 페이지 여백 등 오탐 방지)
    for i, (num, pos) in enumerate(pos_sorted[:-1]):
        next_pos = pos_sorted[i + 1][1]
        block = text[pos:next_pos]
        chunk = _extract_passage_from_block(block)
        if chunk and len(chunk) > 30:
            cleaned = _strip_passage_intro(chunk)
            if len(cleaned) < 30:
                continue
            passages.append({
                "id": f"p{len(passages) + 1}",
                "question_range": None,
                "content": _clean(cleaned),
                "short": len(cleaned) <= 150,
            })

    # 수능 국어 최대 지문 수: 12개 초과 시 짧은 지문부터 제거 (오탐 방지)
    if len(passages) > 12:
        passages = sorted(passages, key=lambda p: len(p.get("content", "")), reverse=True)[:12]
        for i, p in enumerate(passages):
            p["id"] = f"p{i+1}"

    return passages


def _extract_passage_from_block(block: str) -> str | None:
    """문항 블록 끝의 지문 텍스트 추출"""
    last_choice = max(
        (block.rfind(c) for c in "①②③④⑤"),
        default=-1
    )
    if last_choice < 0:
        return None
    after = block[last_choice:]
    m = re.search(r'\n\n(\S.{80,})', after, re.DOTALL)
    if m:
        candidate = m.group(1).strip()
        # 단일 블록 텍스트 조각 방지: \n\n이 1개 이상 있어야 passage로 인정
        if "\n\n" not in candidate:
            return None
        return candidate
    return None


# ─────────────────────────────────────────
# 선택지 / 발문 / 보기 파싱
# ─────────────────────────────────────────

def _parse_choices(text: str) -> dict | None:
    """
    ①②③④⑤ 선택지 파싱.
    - 범위 마커([A:START] 등) 제거 후 파싱
    - 끝 경계: 다음 문항번호 패턴 또는 \n\n + 긴 텍스트 중 먼저
    - 위치 기반 슬라이싱 (re 캡처 대신)으로 역방향 레이아웃도 대응
    - 각 선택지 최대 250자 제한
    """
    circle_map = {"①": "1", "②": "2", "③": "3", "④": "4", "⑤": "5"}

    # 첫 선택지 마커 위치
    first_pos = min(
        (text.find(c) for c in circle_map if text.find(c) >= 0),
        default=-1,
    )
    if first_pos < 0:
        return None

    choices_text = text[first_pos:]

    # Step 1: 범위 마커 제거 (선택지 파싱 방해 방지)
    choices_text = re.sub(r'\[[A-Z]:(START|END)\]\n?', '', choices_text)

    # Step 2: 끝 경계 — 다음 문항번호 또는 \n\n + 긴 텍스트 중 먼저
    end_pos = len(choices_text)

    next_q_m = re.search(r'\n[ \t]*\d{1,2}[.．。][ \t\n]', choices_text)
    if next_q_m:
        end_pos = min(end_pos, next_q_m.start())

    last_c_pos = max((choices_text.rfind(c) for c in "①②③④⑤"), default=0)
    after_last = choices_text[last_c_pos:]
    cut_m = re.search(r'\n\n(?=\S.{60,})', after_last, re.DOTALL)
    if cut_m:
        end_pos = min(end_pos, last_c_pos + cut_m.start())

    choices_text = choices_text[:end_pos]

    # Step 3: 위치 기반 슬라이싱
    positions = sorted(
        [(c, choices_text.find(c)) for c in circle_map if choices_text.find(c) >= 0],
        key=lambda x: x[1],
    )
    if not positions:
        return None

    choices = {}
    for i, (circle, pos) in enumerate(positions):
        next_pos = positions[i + 1][1] if i + 1 < len(positions) else len(choices_text)
        val = choices_text[pos + len(circle):next_pos].strip()
        if len(val) > 250:
            cut = val.find('\n\n')
            val = val[:cut].strip() if 0 < cut < 250 else val[:250].strip()
        val = re.sub(r'\n+', ' ', val).strip()
        choices[circle_map[circle]] = val

    return choices if choices else None


# 발문 앞에 나타날 수 있는 박스 마커 (발문 범위 경계)
# <보기>는 인라인 참조(발문 문장 중간)와 독립 박스를 구분하므로 여기에 포함하지 않음
_STEM_BOX_MARKERS = [
    "①", "<조건>", "<작성 조건>",
    "<답변 전략>", "<표현 전략>", "<작문 계획>",
]

# <보기> 독립 박스: 줄 시작 위치에 등장 (앞이 줄바꿈이거나 텍스트 시작)
# 인라인 참조:      문장 중간에 등장 (예: "바탕으로 <보기>에 대한...")
# OCR 오류: "<보기>" → "보 기" (꺾쇠 탈락) 도 허용
_BOGI_STANDALONE_RE = re.compile(
    r'(?:(?<=\n)|^)[ \t]*(?:<보기>|보\s+기)[ \t]*\n',
    re.MULTILINE
)

# <보기> 공백 변형 (OCR 오류: "< 보 기 >", "< 보기>")
_BOGI_VARIANT_RE = re.compile(r'<\s*보\s*기\s*>')

# 발문 종결 패턴 — 이 패턴 이후 20자+ 텍스트가 있으면 bogi
_STEM_TERMINATION_RE = re.compile(
    r'(?:것은|것인가|않은\s*것은|옳은\s*것은|맞는\s*것은|틀린\s*것은)\s*\?'
    r'|고르시오\s*\.'
    r'|\[[23]점\]',
)


def _extract_stem(text: str) -> str:
    """
    발문 추출.
    ①, <조건> 등 박스 마커 이전까지만 잡음.

    개선 (F-2):
    - [A:START] 이후 종결 패턴이 있으면 [A:START]를 stem 경계로 쓰지 않음
      (F-4 이전 잘못 삽입된 마커 대응)
    - earliest >= 400 (bogi가 있을 가능성) 일 때만 종결 패턴 적용:
      종결 패턴 + 첫 \\n\\n + 실질 텍스트 50자+ → 빈 줄 직전을 stem 경계로
      (bogi가 마커 없이 이어지는 케이스 대응, 정상 문항 오탐 방지)
    """
    earliest = len(text)
    for marker in _STEM_BOX_MARKERS:
        idx = text.find(marker)
        if 0 < idx < earliest:
            earliest = idx

    # <보기> 독립 박스 여부 판별
    m = _BOGI_STANDALONE_RE.search(text)
    if m and 0 < m.start() < earliest:
        earliest = m.start()

    # [A:START] 글상자 경계
    # — [A:START] 이후 종결 패턴이 있으면 잘못 삽입된 것 → skip
    a_start = re.search(r'\[A:START\]', text)
    if a_start and 0 < a_start.start() < earliest:
        if not _STEM_TERMINATION_RE.search(text[a_start.end():]):
            earliest = a_start.start()

    # 인라인 <보기> 참조 + [[IMG:...]] 마커 → 이미지 위치가 stem 경계
    bogi_ref = _BOGI_VARIANT_RE.search(text[:earliest])
    if bogi_ref:
        after = text[bogi_ref.end():earliest]
        img_m = re.search(r'\[\[IMG:[^\]]+\]\]', after)
        if img_m:
            img_abs = bogi_ref.end() + img_m.start()
            if 0 < img_abs < earliest:
                earliest = img_abs

    # 종결 패턴 적용: earliest >= 400 (bogi 있는 구조)일 때만
    # 종결 직후 \n\n + 실질 텍스트 50자+ → 빈 줄 직전을 stem 경계로
    if earliest >= 400:
        last_split = 0
        for m in _STEM_TERMINATION_RE.finditer(text[:earliest]):
            after_term = text[m.end():earliest]
            nn_m = re.search(r'\n\n', after_term)
            if nn_m:
                text_after = after_term[nn_m.end():]
                text_only = re.sub(r'\[\[IMG:[^\]]+\]\]', '', text_after).strip()
                if len(text_only) >= 50:
                    split_pos = m.end() + nn_m.start()
                    if split_pos > last_split:
                        last_split = split_pos
        if last_split > 0:
            earliest = last_split

    return text[:earliest].strip() if earliest < len(text) else text[:300].strip()


def _extract_bogi(text: str) -> str | None:
    """
    <보기> 내용 추출.

    패턴 1 (독립 박스): 줄 시작 <보기> 직후 ~ 첫 선택지 전
    패턴 2 (인라인 참조 + 이미지/[A]박스): <보기>가 발문 중간에 있고
        선택지 전에 [A:START]...[A:END] 블록 또는 이미지 마커가 있는 경우
    """
    # 첫 선택지 위치
    first_choice = min(
        (text.find(c) for c in '①②③④⑤' if text.find(c) >= 0),
        default=len(text),
    )
    pre = text[:first_choice]

    # 패턴 1: 줄 시작 독립 박스
    m = _BOGI_STANDALONE_RE.search(pre)
    if m:
        return pre[m.end():].strip()

    # 패턴 2: [A:START]...[A:END] 박스 (pre 내에 완결)
    bracket = re.search(r'\[A:START\](.*?)\[A:END\]', pre, re.DOTALL)
    if bracket:
        return bracket.group(1).strip()

    # 패턴 2b: [A:START]가 pre에 있지만 [A:END]가 선택지 이후에 잘못 배치된 경우
    # (Gemini가 [A:END]를 선택지 텍스트 중간에 삽입한 오류)
    # → pre 내 [A:START] 이후의 이미지/내용만 추출
    a_start_m = re.search(r'\[A:START\]', pre)
    if a_start_m and '[A:END]' not in pre:
        after_start = pre[a_start_m.end():]
        img_m = re.search(r'\[\[IMG:[^\]]+\]\]', after_start)
        if img_m:
            return after_start[img_m.start():].strip()

    # 패턴 3: 인라인 참조 이후 이미지 마커만 있는 경우
    # (PDF에서 보기 내용이 이미지로 처리된 경우)
    bogi_ref = _BOGI_VARIANT_RE.search(pre)
    if bogi_ref:
        after_bogi = pre[bogi_ref.end():]
        img_m = re.search(r'\[\[IMG:[^\]]+\]\]', after_bogi)
        if img_m:
            return after_bogi[img_m.start():].strip()

    # 패턴 4: 종결 패턴 이후 ~ ① 사이 텍스트 (마커 없는 bogi)
    last_term_end = 0
    for m in _STEM_TERMINATION_RE.finditer(pre):
        if m.end() > last_term_end:
            last_term_end = m.end()
    if last_term_end > 0:
        candidate = pre[last_term_end:].strip()
        if len(candidate) >= 20 or '[[IMG:' in candidate:
            return candidate

    return None


def _clean(text: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", text).strip()


# ─────────────────────────────────────────
# 제시문 앞 안내문 + 소개 이미지 제거
# ─────────────────────────────────────────

_INTRO_RE = re.compile(
    r'^\s*\[?\s*\d+\s*[～~∼\-]\s*\d+\s*\]?\s*다음.*?(?:물음에\s*)?답하시오[.．。]?\s*'
    r'|^\s*다음\s+글을\s+읽고\s+(?:물음에\s+)?답하시오[.．。]?\s*',
    re.DOTALL,
)
_GAP_IMG_RE = re.compile(r'\[\[IMG:[^\]]*(?:gapR|gapL)[^\]]*\]\]\s*')


def _strip_passage_intro(text: str) -> str:
    """제시문 앞 안내문 및 소개용 gap 이미지 제거."""
    text = _INTRO_RE.sub('', text).strip()
    # 앞 200자 이내 gapR/gapL 이미지만 제거 (본문 중간 이미지는 유지)
    prefix = _GAP_IMG_RE.sub('', text[:200])
    return (prefix + text[200:]).strip()


def _normalize_whitespace(text: str) -> str:
    """PDF 추출 시 발생하는 불필요한 다중 공백 정규화.

    - [[IMG:...]] / <img ...> 마커 보존
    - 줄바꿈(\\n)은 그대로 유지 (PDF 단락 구조 보존)
    - 각 줄 내의 다중 공백(2개 이상) → 단일 공백
    """
    # 보호할 마커를 플레이스홀더로 치환
    protected: list[str] = []

    def protect(m: re.Match) -> str:
        protected.append(m.group(0))
        return f"\x00PROT{len(protected)-1}\x00"

    text = re.sub(r'\[\[IMG:[^\]]+\]\]', protect, text)
    text = re.sub(r'<img\b[^>]*>', protect, text, flags=re.IGNORECASE)

    # <img> 태그 내 줄바꿈 정리 (속성 사이 개행 → 공백)
    text = re.sub(r'<img\b[\s\S]*?>', lambda m: m.group(0).replace('\n', ' '), text, flags=re.IGNORECASE)

    # 각 줄 내의 다중 공백만 단일 공백으로 정규화 (줄바꿈은 보존)
    lines = text.split('\n')
    lines = [re.sub(r'[ \t]{2,}', ' ', ln) for ln in lines]
    text = '\n'.join(lines)

    # 마커 복원
    text = re.sub(r'\x00PROT(\d+)\x00', lambda m: protected[int(m.group(1))], text)

    return text


# ─────────────────────────────────────────
# [[IMG:url]] → <img> HTML 변환
# ─────────────────────────────────────────

def _convert_img_markers(passages: list, questions: list):
    """[[IMG:url]] 마커를 <img> HTML 태그로 변환"""
    def convert(s: str) -> str:
        if not s:
            return s
        return IMG_MARKER_RE.sub(
            r'<img src="\1" style="max-width:100%;display:block;margin:8px 0">',
            s
        )

    for p in passages:
        if p.get("content"):
            p["content"] = convert(p["content"])

    for q in questions:
        for field in ("content", "stem", "bogi"):
            if q.get(field):
                q[field] = convert(q[field])
        if q.get("choices"):
            for k, v in q["choices"].items():
                q["choices"][k] = convert(v)


# ─────────────────────────────────────────
# passage_ref → passage_idx 매핑
# ─────────────────────────────────────────

def attach_passage_idx(passages: list, questions: list):
    if not passages or not questions:
        return
    if len(passages) == 1:
        for q in questions:
            if q.get("passage_idx") is None:
                q["passage_idx"] = 0
        return

    passage_map = {p["id"]: i for i, p in enumerate(passages) if p.get("id")}
    for q in questions:
        ref = q.get("passage_ref")
        if ref and ref in passage_map:
            q["passage_idx"] = passage_map[ref]

    for i, p in enumerate(passages):
        rng = p.get("question_range") or ""
        m = re.search(r"(\d+)\s*[~～∼]\s*(\d+)", rng)
        if m:
            s, e = int(m.group(1)), int(m.group(2))
            for q in questions:
                n = q.get("number")
                if n and s <= n <= e and q.get("passage_idx") is None:
                    q["passage_idx"] = i
