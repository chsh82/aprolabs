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
    """연속된 최장 문항 시퀀스 유지 (1개 gap 허용)"""
    if len(positions) <= 3:
        return positions

    nums = sorted(positions.keys())
    # 1개 건너뜀 허용: n+1 또는 n+2 모두 연속으로 처리
    sequences = []
    cur = [nums[0]]
    for n in nums[1:]:
        if n <= cur[-1] + 2:
            cur.append(n)
        else:
            sequences.append(cur)
            cur = [n]
    sequences.append(cur)

    longest = max(sequences, key=len)

    # 최장 시퀀스가 전체의 50% 미만이면 전체 사용 (부분 시험지)
    if len(longest) < len(nums) * 0.5:
        return positions

    # 최장 시퀀스 min~max 범위 내 모든 positions 포함
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
    """문항 끝에 붙은 다음 지문 제거 (마지막 ⑤ 이후 긴 텍스트)"""
    last_choice = max(
        (q_text.rfind(c) for c in "①②③④⑤"),
        default=-1
    )
    if last_choice < 0:
        return q_text
    after = q_text[last_choice:]
    m = re.search(r'\n\n\S.{100,}', after, re.DOTALL)
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
    pre = text[:first_pos].strip()
    if len(pre) > 30:
        passages.append({
            "id": "p1",
            "question_range": None,
            "content": _clean(pre),
        })

    # 각 문항 블록(텍스트 위치 순) 끝에 붙은 다음 지문
    for i, (num, pos) in enumerate(pos_sorted[:-1]):
        next_pos = pos_sorted[i + 1][1]
        block = text[pos:next_pos]
        chunk = _extract_passage_from_block(block)
        if chunk and len(chunk) > 150:
            passages.append({
                "id": f"p{len(passages) + 1}",
                "question_range": None,
                "content": _clean(chunk),
            })

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
    """①②③④⑤ 선택지 파싱"""
    circle_map = {"①": "1", "②": "2", "③": "3", "④": "4", "⑤": "5"}
    choices = {}
    for circle, num in circle_map.items():
        m = re.search(rf'{circle}(.+?)(?=[①②③④⑤]|$)', text, re.DOTALL)
        if m:
            choices[num] = m.group(1).strip()
    return choices if choices else None


def _extract_stem(text: str) -> str:
    """발문 추출 (첫 ① 또는 <보기> 이전)"""
    for marker in ["①", "<보기>"]:
        idx = text.find(marker)
        if idx > 0:
            return text[:idx].strip()
    return text[:300].strip()


def _extract_bogi(text: str) -> str | None:
    """<보기> 내용 추출"""
    m = re.search(r"<보기>(.*?)(?:①|$)", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    return None


def _clean(text: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", text).strip()


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
