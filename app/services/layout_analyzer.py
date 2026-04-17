"""
Stage 1: PDF 텍스트 추출
- 블록 내부 줄바꿈 → 공백 (편집상 줄바꿈 제거)
- 블록 간 \\n\\n (원문 문단 구분 보존)
- 밑줄: get_drawings() + get_text("words") 워드 매칭 → <u> 태그
- 이미지: 래스터 블록 + 갭 감지 → [[IMG:url]] 마커
"""
import os
import re
import fitz  # PyMuPDF

MIN_CHARS_PER_PAGE = 80
MIN_GAP_PX = 45

# ── 제외 키워드 ───────────────────────────────
SKIP_KEYWORDS = ['수험번호', '성명란', '제한시간', '홀수형', '짝수형',
                 '감독관', '확인란', '※ 답안지']

# ── 하단 푸터 제외 키워드 (저작권, 인쇄소 등) ──
FOOTER_KEYWORDS = [
    '저작권', '한국교육과정평가원', '교육과정평가원',
    '무단 전재', '무단전재', '복제를 금합니다', '복제금지',
    '인쇄하지 마십시오', '이 문제지에 관한',
]

# ── 구간 전환 안내문 제외 (P9 등 선택과목 경계 안내) ──
INTER_SECTION_RE = re.compile(
    r'^\*?\s*확인\s*사항'
    r'|◦\s*답안지'
    r'|◦\s*이어서'
    r'|선택과목.{0,20}문제가\s*제시'
    r'|자신이\s*선택한\s*과목인지'
)

# ── 시험지 상단 제목 패턴 ─────────────────────
TITLE_PATTERNS = [
    re.compile(r'\d{4}학년도'),
    re.compile(r'대학수학능력시험'),
    re.compile(r'[가-힣]+\s*영역\s*문제지'),
    re.compile(r'^[가-힣]+\s*영역$'),
    re.compile(r'학력평가'),
    re.compile(r'모의고사'),
    re.compile(r'성명\s*수험번호'),
]

# ── 페이지 번호 패턴 ──
PAGE_NUM_RE = re.compile(r'^\d+\s*/\s*\d+$|^-\s*\d+\s*-$')

# ── 폰트 인코딩 보정: ｢｣ 등 코너 괄호 정규화 ──
# 일부 PDF 폰트에서 halfwidth 코너 괄호가 다른 문자로 추출되는 경우 보정
_CHAR_NORM = str.maketrans({
    # 수능 PDF 폰트 인코딩 오류 (ETX/EOT 계열)
    '\x03':   '「',   # U+0003 ETX → 「
    '\x04':   '」',   # U+0004 EOT → 」
    '\x05':   ''',   # U+0005 ENQ → 왼쪽 작은따옴표 (일부 PDF)
    '\x06':   ''',   # U+0006 ACK → 오른쪽 작은따옴표
    '\x07':   '"',   # U+0007 BEL → 왼쪽 큰따옴표
    '\x08':   '"',   # U+0008 BS  → 오른쪽 큰따옴표
    '\x0e':   '…',   # U+000E SO  → 줄임표
    '\x0f':   '·',   # U+000F SI  → 가운뎃점
    # halfwidth 코너 괄호
    '\uff62': '「',   # ｢ (halfwidth) → 「
    '\uff63': '」',   # ｣ (halfwidth) → 」
    '\u300c': '「',   # 「 유지
    '\u300d': '」',   # 」 유지
    # 기타 인코딩 혼용 따옴표
    '\u2018': ''',   # ' → 정규화
    '\u2019': ''',   # ' → 정규화
    '\u201c': '"',   # " → 정규화
    '\u201d': '"',   # " → 정규화
    # 중간점·물결 혼용
    '\u00b7': '·',   # · (가운뎃점)
    '\u2027': '·',   # ‧ (하이픈 포인트)
    '\u223c': '~',   # ∼ → ~
    '\uff5e': '~',   # ～ → ~
})


def _normalize_text(text: str) -> str:
    return text.translate(_CHAR_NORM)


def _should_skip(text: str) -> bool:
    plain = re.sub(r'<[^>]+>', '', text).strip()
    if any(kw in plain for kw in SKIP_KEYWORDS):
        return True
    if any(kw in plain for kw in FOOTER_KEYWORDS):
        return True
    if INTER_SECTION_RE.search(plain):
        return True
    if PAGE_NUM_RE.match(plain):
        return True
    if plain.isdigit() and len(plain) <= 3:
        return True
    for pat in TITLE_PATTERNS:
        if pat.search(plain):
            return True
    return False


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────

def extract_pdf_text(pdf_path: str,
                     img_save_dir: str = None) -> tuple[str, int, dict]:
    """
    반환: (text_with_markers, num_pages, manifest)
    - text 내부: [[IMG:url]] 이미지 마커, <u>...</u> 밑줄, \\n\\n 문단 구분
    - manifest: {page_num: {"underlines": [...]}}
    """
    doc = fitz.open(pdf_path)
    num_pages = doc.page_count
    pages_text = []
    manifest = {}

    if img_save_dir:
        os.makedirs(img_save_dir, exist_ok=True)

    for page_num, page in enumerate(doc):
        pn = page_num + 1
        text = _extract_page_columns(page, pn, img_save_dir)
        if text.strip():
            pages_text.append(f"--- 페이지 {pn} ---\n{text}")

        underlines = _collect_underline_rects(page)
        if underlines:
            manifest[pn] = {"underlines": underlines}

    doc.close()
    return "\n\n".join(pages_text), num_pages, manifest


# ─────────────────────────────────────────
# 페이지 추출 (핵심)
# ─────────────────────────────────────────

def _extract_page_columns(page, page_num: int = None, img_save_dir: str = None) -> str:
    blocks = page.get_text("blocks")
    page_width = page.rect.width
    mid_x = page_width * 0.5

    # 밑줄 데이터
    underline_rects = _collect_underline_rects(page)
    words_by_block, underlined_keys = {}, set()
    if underline_rects:
        words_by_block, underlined_keys = _map_underlined_words(page, underline_rects)

    # [A]/[B] 세로 브라켓 + 박스형 브라켓 탐지 (블록 루프 전에)
    brackets, bracket_label_bnos, bracket_boxes = _find_labeled_brackets(page)

    # 들여쓰기 블록 탐지
    block_indents = _detect_block_indents(page)

    page_height = page.rect.height
    y_top_limit = page_height * 0.05
    y_bot_limit = page_height * 0.90

    all_items = []  # (x0, y0, x1, y1, text)

    for idx, b in enumerate(blocks):
        x0, y0, x1, y1 = b[0], b[1], b[2], b[3]

        if b[6] == 1:  # 래스터 이미지
            if y0 < y_top_limit or y1 > y_bot_limit:
                continue
            if img_save_dir and page_num:
                url = _save_crop(page, page_num, f"img{idx}", x0, y0, x1, y1, img_save_dir)
                if url:
                    all_items.append((x0, y0, x1, y1, f"[[IMG:{url}]]"))

        elif b[6] == 0:  # 텍스트
            if y1 > y_bot_limit:
                continue
            if idx in bracket_label_bnos:
                continue  # [A]/[B] 레이블 블록은 마커로 대체
            has_ul = idx in words_by_block and any(
                (idx, l, w) in underlined_keys for l, w, _ in words_by_block[idx]
            )
            if has_ul:
                text = _build_text_with_underlines(idx, words_by_block[idx], underlined_keys)
            else:
                raw = b[4].strip()
                text = ' '.join(line.strip() for line in raw.split('\n') if line.strip())

            text = _normalize_text(text)

            if not text:
                continue
            if _should_skip(text):
                continue

            # 들여쓰기 적용
            if idx in block_indents:
                text = '\u3000' + text  # 전각 공백 = 한 글자 들여쓰기

            all_items.append((x0, y0, x1, y1, text))

    # 갭 기반 이미지 캡처 (벡터 그래픽)
    if img_save_dir and page_num:
        gap_items = _capture_gaps(page, page_num, all_items, page_width, img_save_dir)
        all_items.extend(gap_items)
        # <보기> 섹션 이미지화
        all_items = _capture_bogi_as_images(page, page_num, all_items, mid_x, img_save_dir)
        # 레이블 없는 콘텐츠 박스 이미지화 (Q15, Q36 등)
        content_boxes = _collect_content_boxes(page, bracket_boxes)
        if content_boxes:
            all_items = _capture_boxes_as_images(
                page, page_num, all_items, mid_x, content_boxes, img_save_dir
            )

    # 2단 레이아웃 — (x0, y0, x1, y1, text) 5-튜플
    full_width, left_col, right_col = [], [], []
    for x0, y0, x1, y1, text in all_items:
        block_width = x1 - x0
        if block_width > page_width * 0.55:
            full_width.append((x0, y0, x1, y1, text))
        elif x0 < mid_x:
            left_col.append((x0, y0, x1, y1, text))
        else:
            right_col.append((x0, y0, x1, y1, text))

    full_width.sort(key=lambda b: b[1])
    left_col.sort(key=lambda b: b[1])
    right_col.sort(key=lambda b: b[1])

    parts = []
    for _, _, _, _, t in full_width:
        parts.append(t)
    if left_col:
        parts.append(_smart_join_col(left_col, brackets))
    if right_col:
        parts.append(_smart_join_col(right_col, brackets))

    return "\n\n".join(parts)


# ─────────────────────────────────────────
# 들여쓰기 탐지
# ─────────────────────────────────────────

def _detect_block_indents(page) -> set:
    """
    블록의 x0가 해당 컬럼의 기준 x0보다 6px 이상 오른쪽이면 들여쓰기로 처리.
    기준 x0: 컬럼 내 5자 이상 텍스트 블록의 x0 하위 10 퍼센타일
    (문항번호 "1.", 페이지번호 등 짧은 블록 오염 방지)
    """
    indented = set()
    blocks = page.get_text("blocks")
    if not blocks:
        return indented

    page_width = page.rect.width
    page_height = page.rect.height
    mid_x = page_width * 0.5
    y_top = page_height * 0.05
    y_bot = page_height * 0.92

    text_blocks = [
        (b[0], b[1], b[2], b[3], idx)
        for idx, b in enumerate(blocks)
        if b[6] == 0 and b[1] >= y_top and b[3] <= y_bot
    ]

    def _col_baseline(xs: list) -> float:
        if not xs:
            return 0.0
        s = sorted(xs)
        return s[max(0, len(s) // 10)]  # 하위 10 퍼센타일

    # 5자 이상 블록만 기준선 계산에 사용
    left_xs = [
        bx0 for bx0, by0, bx1, by1, bno in text_blocks
        if (bx0 + bx1) / 2 < mid_x
        and len(blocks[bno][4].strip()) >= 5
    ]
    right_xs = [
        bx0 for bx0, by0, bx1, by1, bno in text_blocks
        if (bx0 + bx1) / 2 >= mid_x
        and len(blocks[bno][4].strip()) >= 5
    ]

    left_base = _col_baseline(left_xs)
    right_base = _col_baseline(right_xs)

    for bx0, by0, bx1, by1, bno in text_blocks:
        is_left = (bx0 + bx1) / 2 < mid_x
        base = left_base if is_left else right_base
        if bx0 - base > 6:
            indented.add(bno)

    return indented


# ─────────────────────────────────────────
# 세로 브라켓 탐지 ([A], [B] 등)
# ─────────────────────────────────────────

_BRACKET_LABEL_RE = re.compile(r'^\[([A-Z])\]$')


def _find_labeled_brackets(page) -> tuple[list, set, list]:
    """
    세로선/박스 드로잉 + [A]/[B] 레이블 텍스트를 매칭하여 범위 반환.

    반환: (brackets, label_block_nos, bracket_box_rects)
      brackets: [{"label": "A", "y0": float, "y1": float}, ...]
      label_block_nos: 제거할 [A] 텍스트 블록 번호 집합
      bracket_box_rects: [A] 범위로 사용된 박스 rect 목록 (이미지 캡처에서 제외)

    개선 사항:
    - 코너 피스(｢｣ 형태 짧은 수평선)로 세로선 y 범위 정확화
    - 레이블-선 거리 허용 80px (오른쪽 위치 [A] 지원)
    - 박스형 [A] 지원 (세로선 없이 사각형 박스로 표시된 경우)
    """
    page_width = page.rect.width

    # ── 1. 세로선 + 코너 피스 수집 ──────────────────────────
    vertical_lines = []  # (x_mid, y0, y1)

    for d in page.get_drawings():
        items = d.get("items", [])
        verts = []    # (x_mid, y0, y1)
        horiz_ys = [] # 짧은 수평선의 y 좌표 (코너 피스)

        for item in items:
            if item[0] == "l":
                p1, p2 = item[1], item[2]
                dx = abs(p1.x - p2.x)
                dy = abs(p1.y - p2.y)
                if dx < 5 and dy > 25:  # 세로선
                    verts.append(((p1.x + p2.x) / 2, min(p1.y, p2.y), max(p1.y, p2.y)))
                elif dy < 4 and 3 < dx < 30:  # 짧은 수평선 = ｢｣ 코너 피스
                    horiz_ys.append((p1.y + p2.y) / 2)
            elif item[0] == "re":
                r = item[1]
                if r.width < 5 and r.height > 25:
                    verts.append(((r.x0 + r.x1) / 2, r.y0, r.y1))

        for vx, vy0, vy1 in verts:
            # 코너 피스가 있으면 y 범위 정밀화
            tops = [y for y in horiz_ys if abs(y - vy0) < 12]
            bots = [y for y in horiz_ys if abs(y - vy1) < 12]
            actual_y0 = min(tops) if tops else vy0
            actual_y1 = max(bots) if bots else vy1
            vertical_lines.append((vx, actual_y0, actual_y1))

    # ── 2. 박스형 브라켓 후보 수집 (세로+가로선 모두 있는 완전한 직사각형) ──
    box_candidates = []  # fitz.Rect
    for d in page.get_drawings():
        items = d.get("items", [])
        has_horiz = has_vert = False
        for item in items:
            if item[0] == "l":
                dx = abs(item[1].x - item[2].x)
                dy = abs(item[1].y - item[2].y)
                if dx > 50 and dy < 5:
                    has_horiz = True
                if dy > 50 and dx < 5:
                    has_vert = True
            elif item[0] == "re":
                r = item[1]
                if r.width > 50 and r.height > 50:
                    has_horiz = True
                    has_vert = True
        if has_horiz and has_vert:
            r = d.get("rect")
            if r and r.width > 50 and r.height > 50 and r.width < page_width * 0.85:
                box_candidates.append(r)

    # ── 3. 레이블 후보 수집 ──────────────────────────────────
    label_candidates = []  # [(label, x_mid, y_mid, block_no)]

    for word in page.get_text("words"):
        wx0, wy0, wx1, wy1 = word[0], word[1], word[2], word[3]
        wtext, bno = word[4], int(word[5])
        m = _BRACKET_LABEL_RE.match(wtext.strip())
        if m:
            label_candidates.append((m.group(1), (wx0 + wx1) / 2, (wy0 + wy1) / 2, bno))

    _LABEL_IN_BLOCK = re.compile(r'^\s*\[([A-Z])\]\s*$')
    for b in page.get_text("blocks"):
        if b[6] != 0:
            continue
        m = _LABEL_IN_BLOCK.match(b[4])
        if m:
            label = m.group(1)
            bx_mid = (b[0] + b[2]) / 2
            by_mid = (b[1] + b[3]) / 2
            already = any(abs(c[1] - bx_mid) < 5 and abs(c[2] - by_mid) < 5
                          for c in label_candidates)
            if not already:
                label_candidates.append((label, bx_mid, by_mid, -1))

    # ── 4. 레이블 ↔ 세로선/박스 매칭 ────────────────────────
    brackets = []
    label_block_nos = set()
    bracket_box_rects = []  # [A] 범위로 사용된 박스

    for label, lx, ly, bno in label_candidates:
        matched = False

        # 4-a. 세로선 매칭: y-containment 우선, x-거리 최소 선 선택 (#4-A)
        matching_vlines = [
            (vx, vy0, vy1) for vx, vy0, vy1 in vertical_lines
            if vy0 - 10 <= ly <= vy1 + 10
        ]
        if matching_vlines:
            vx, vy0, vy1 = min(matching_vlines, key=lambda v: abs(v[0] - lx))
            brackets.append({"label": label, "y0": vy0, "y1": vy1})
            if bno >= 0:
                label_block_nos.add(bno)
            matched = True

        # 4-b. 박스형 매칭 (레이블이 박스 가장자리 근처에 있는 경우)
        if not matched:
            for r in box_candidates:
                near_left = abs(lx - r.x0) < 40
                near_right = abs(lx - r.x1) < 40
                in_y = r.y0 - 10 <= ly <= r.y1 + 10
                if (near_left or near_right) and in_y:
                    brackets.append({"label": label, "y0": r.y0, "y1": r.y1})
                    if bno >= 0:
                        label_block_nos.add(bno)
                    bracket_box_rects.append(r)
                    matched = True
                    break

        # 4-c. 폴백: 레이블 위치 기준 ±80px 추정
        if not matched:
            brackets.append({"label": label, "y0": ly - 80, "y1": ly + 80})
            if bno >= 0:
                label_block_nos.add(bno)

    return brackets, label_block_nos, bracket_box_rects


# ── 스마트 이어쓰기 ────────────────────────────────────
_SENT_END_RE = re.compile(r'[.。!?…]["\'」』)）]?$')
_QUESTION_START_RE = re.compile(r'^[ \t]*\d{1,2}[.．。][ \t]')


def _smart_join_col(col: list, brackets: list = None) -> str:
    """
    col: [(x0, y0, x1, y1, text), ...] y0 순 정렬
    brackets: [{"label", "y0", "y1"}, ...]

    연결 규칙:
      문항번호 시작            → \\n\\n
      gap > 15px             → \\n\\n (문단 구분)
      짧은 줄(칼럼 80% 미만)  → \\n   (시/시조 의도적 줄바꿈)
      문장 끝(.?! 등)         → \\n
      그 외                   → ' '  (이어쓰기)

    [A:START]/[A:END] 마커는 여분의 빈 줄 없이 삽입.
    """
    brackets = brackets or []
    if not col:
        return ""

    col_right = max(item[2] for item in col)

    def _bracket_label(y0, y1):
        # 중점이 아닌 overlap 기반: 블록이 브라켓 범위와 조금이라도 겹치면 포함
        for b in brackets:
            if b["y0"] - 5 <= y1 and b["y1"] + 5 >= y0:
                return b["label"]
        return None

    def _sep(prev_item, curr_y0, curr_text):
        px0, py0, px1, py1, ptxt = prev_item
        gap = curr_y0 - py1
        if _QUESTION_START_RE.match(curr_text):
            return "\n\n"
        if gap > 15:
            return "\n\n"
        if col_right > 0 and px1 < col_right * 0.80:
            return "\n"
        if _SENT_END_RE.search(ptxt.rstrip()):
            return "\n"
        return " "

    x0, y0, x1, y1, text = col[0]
    prev_label = _bracket_label(y0, y1)
    result = (f"[{prev_label}:START]\n" if prev_label else "") + text

    for i in range(1, len(col)):
        item = col[i]
        x0, y0, x1, y1, text = item
        curr_label = _bracket_label(y0, y1)
        sep = _sep(col[i - 1], y0, text)

        if prev_label is not None and curr_label != prev_label:
            result += f"[{prev_label}:END]"

        if curr_label is not None and curr_label != prev_label:
            result += sep + f"[{curr_label}:START]\n" + text
        else:
            result += sep + text

        prev_label = curr_label

    if prev_label is not None:
        result += f"[{prev_label}:END]"

    return result


# ─────────────────────────────────────────
# 밑줄 처리
# ─────────────────────────────────────────

def _collect_underline_rects(page) -> list:
    """
    수평선(밑줄) rect 수집.
    박스 경계선은 제외: (1) 같은 드로잉 내 세로선 동반, 또는
    (2) 다른 드로잉의 세로선이 수평선 양 끝점과 교차 (#7-B 교차-드로잉 검사).
    """
    # 페이지 전체 세로 선분 사전 수집 (드로잉 경계 무관)
    all_vert_segs = []  # (x, y0, y1)
    try:
        for d in page.get_drawings():
            for item in d.get("items", []):
                if item[0] == "l":
                    p1, p2 = item[1], item[2]
                    if abs(p1.x - p2.x) < 5 and abs(p1.y - p2.y) > 10:
                        x = (p1.x + p2.x) / 2
                        all_vert_segs.append((x, min(p1.y, p2.y), max(p1.y, p2.y)))
                elif item[0] == "re":
                    r = item[1]
                    if r.height > 10:
                        all_vert_segs.append((r.x0, r.y0, r.y1))
                        all_vert_segs.append((r.x1, r.y0, r.y1))
    except Exception:
        pass

    def _is_box_border(hx0: float, hx1: float, hy: float) -> bool:
        """수평선이 박스 경계선인지 판별: 좌우 양쪽 끝 모두에 세로선이 있어야 박스 경계.
        한쪽에만 있으면 브라켓 코너 피스일 수 있으므로 밑줄로 허용."""
        left  = any(abs(vx - hx0) < 8 and vy0 - 5 <= hy <= vy1 + 5
                    for vx, vy0, vy1 in all_vert_segs)
        right = any(abs(vx - hx1) < 8 and vy0 - 5 <= hy <= vy1 + 5
                    for vx, vy0, vy1 in all_vert_segs)
        return left and right

    rects = []
    seen = set()
    try:
        for d in page.get_drawings():
            items = d.get("items", [])

            # 같은 드로잉 내 유의미한 세로 선분
            has_vert = any(
                (item[0] == "l"
                 and abs(item[1].x - item[2].x) < 5
                 and abs(item[1].y - item[2].y) > 20)
                or (item[0] == "re" and item[1].height > 20)
                for item in items
            )

            for item in items:
                if item[0] == "l":
                    p1, p2 = item[1], item[2]
                    dx = abs(p1.x - p2.x)
                    dy = abs(p1.y - p2.y)
                    if dx > 10 and dy < 3:  # 수평선
                        if has_vert:
                            continue  # 같은 드로잉 내 세로선 — 박스 경계
                        hx0, hx1 = min(p1.x, p2.x), max(p1.x, p2.x)
                        hy = (p1.y + p2.y) / 2
                        if _is_box_border(hx0, hx1, hy):
                            continue  # 다른 드로잉 세로선과 교차 — 박스 경계
                        key = (round(hx0), round(min(p1.y, p2.y)))
                        if key not in seen:
                            seen.add(key)
                            rects.append(fitz.Rect(hx0, min(p1.y, p2.y), hx1, max(p1.y, p2.y) + 1))
                elif item[0] == "re":
                    r = item[1]
                    if r.width > 10 and r.height < 3:
                        if has_vert:
                            continue
                        if _is_box_border(r.x0, r.x1, (r.y0 + r.y1) / 2):
                            continue
                        key = (round(r.x0), round(r.y0))
                        if key not in seen:
                            seen.add(key)
                            rects.append(r)

            # bounding rect 폴백 (세로선 없는 경우만)
            if not has_vert:
                r = d.get("rect") or d.get("border_rect")
                if r and r.width > 10 and r.height < 3:
                    if not _is_box_border(r.x0, r.x1, (r.y0 + r.y1) / 2):
                        key = (round(r.x0), round(r.y0))
                        if key not in seen:
                            seen.add(key)
                            rects.append(r)
    except Exception:
        pass
    return rects


def _map_underlined_words(page, underline_rects) -> tuple[dict, set]:
    words_by_block = {}
    underlined_keys = set()

    all_words = []
    for w in page.get_text("words"):
        wx0, wy0, wx1, wy1, wtext = w[0], w[1], w[2], w[3], w[4]
        bno, lno, wno = int(w[5]), int(w[6]), int(w[7])
        words_by_block.setdefault(bno, []).append((lno, wno, wtext))
        all_words.append((wx0, wy0, wx1, wy1, wtext, bno, lno, wno))

    # 줄 단위 최대 wy1 (㉠ 등 키 큰 기호 보정)
    line_max_y1: dict[tuple, float] = {}
    for wx0, wy0, wx1, wy1, wtext, bno, lno, wno in all_words:
        key = (bno, lno)
        if key not in line_max_y1 or wy1 > line_max_y1[key]:
            line_max_y1[key] = wy1

    for wx0, wy0, wx1, wy1, wtext, bno, lno, wno in all_words:
        ly1 = line_max_y1.get((bno, lno), wy1)
        for ur in underline_rects:
            if ur.x0 <= wx1 and ur.x1 >= wx0 and -4 <= ur.y0 - ly1 < 16:
                underlined_keys.add((bno, lno, wno))
                break

    return words_by_block, underlined_keys


def _build_text_with_underlines(block_no: int, words: list, underlined_keys: set) -> str:
    lines: dict[int, list] = {}
    for lno, wno, wtext in words:
        lines.setdefault(lno, []).append((wno, wtext, (block_no, lno, wno) in underlined_keys))

    result_lines = []
    for lno in sorted(lines.keys()):
        sorted_words = sorted(lines[lno], key=lambda x: x[0])
        parts = []
        span = []
        current_ul = None
        for _, wtext, is_ul in sorted_words:
            if is_ul != current_ul:
                if span:
                    joined = " ".join(span)
                    parts.append(f"<u>{joined}</u>" if current_ul else joined)
                    span = []
                current_ul = is_ul
            span.append(wtext)
        if span:
            joined = " ".join(span)
            parts.append(f"<u>{joined}</u>" if current_ul else joined)
        result_lines.append(" ".join(parts))

    return " ".join(result_lines)


# ─────────────────────────────────────────
# 이미지 저장 / 갭 캡처
# ─────────────────────────────────────────

def _save_crop(page, page_num: int, tag, x0, y0, x1, y1, img_save_dir: str) -> str | None:
    try:
        clip = fitz.Rect(x0, y0, x1, y1)
        if clip.is_empty or clip.width < 5 or clip.height < 5:
            return None
        pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), clip=clip)
        fname = f"p{page_num}_{tag}.png"
        fpath = os.path.join(img_save_dir, fname)
        if os.path.exists(fpath):
            os.remove(fpath)
        pix.save(fpath)
        rel = img_save_dir.replace("\\", "/").lstrip("/")
        return f"/{rel}/{fname}"
    except Exception:
        return None


def _find_containing_box_bottom(page, x0: float, y0: float, x1: float, y1: float) -> float | None:
    """
    <보기> 영역(x0,y0,x1,y1)을 감싸는 박스의 하단 y1 반환.
    조건: 박스 x범위가 bogi x범위를 포함, y범위가 bogi 상단을 포함하고 하단 이상 연장.
    여유 허용치를 충분히 크게 설정하여 실제 PDF 레이아웃 변동 흡수.
    """
    def _is_enclosing(r) -> bool:
        return (r.x0 <= x0 + 30 and r.x1 >= x1 - 30 and
                r.y0 <= y0 + 60 and r.y1 >= y1 - 20 and
                r.width > 50 and r.height > 30)

    try:
        for d in page.get_drawings():
            items = d.get("items", [])
            # "re" 아이템 직접 검사
            for item in items:
                if item[0] == "re" and _is_enclosing(item[1]):
                    return item[1].y1
            # path bounding rect — 가로/세로선 모두 있는 닫힌 패스
            r = d.get("rect")
            if r and _is_enclosing(r):
                has_h = has_v = False
                for item in items:
                    if item[0] == "l":
                        dx = abs(item[1].x - item[2].x)
                        dy = abs(item[1].y - item[2].y)
                        if dx > 30 and dy < 5:
                            has_h = True
                        if dy > 30 and dx < 5:
                            has_v = True
                if has_h and has_v:
                    return r.y1
    except Exception:
        pass
    return None


def _find_nearest_hline_below(page, y_ref: float, x0: float, x1: float,
                               max_dist: float = 35) -> float | None:
    """
    y_ref 아래 max_dist 이내에 있는 수평선 중 가장 가까운 것의 y 좌표.
    <보기> 박스 하단선을 정확히 포함하기 위해 사용.
    """
    best = None
    try:
        for d in page.get_drawings():
            for item in d.get("items", []):
                if item[0] == "l":
                    p1, p2 = item[1], item[2]
                    if abs(p1.x - p2.x) > 30 and abs(p1.y - p2.y) < 3:
                        ly = (p1.y + p2.y) / 2
                        lx0 = min(p1.x, p2.x)
                        lx1 = max(p1.x, p2.x)
                        if lx0 <= x1 and lx1 >= x0:  # x 겹침
                            if 0 < ly - y_ref <= max_dist:
                                if best is None or ly < best:
                                    best = ly
    except Exception:
        pass
    return best


_BOGI_HDR = re.compile(
    r'[<〈《\(（]보\s*기[>〉》\)）]'
    r'|^[\s─━\-~=*]+보\s*기[\s─━\-~=*]*$'
    r'|^\s*보\s*기\s*$',
    re.MULTILINE,
)


def _capture_bogi_as_images(page, page_num: int, all_items: list, mid_x: float,
                              img_save_dir: str) -> list:
    """<보기> 섹션을 이미지로 캡처. 박스 하단선까지 정확히 포함."""
    consumed = set()
    result = []

    for i, (x0, y0, x1, y1, text) in enumerate(all_items):
        if i in consumed:
            continue
        if not _BOGI_HDR.search(text):
            result.append(all_items[i])
            continue
        if _QUESTION_START_RE.match(text.strip()):
            result.append(all_items[i])
            continue

        is_left = (x0 + x1) / 2 < mid_x
        bx0, by0, bx1, by1 = x0, y0, x1, y1
        consumed.add(i)

        same_col_after = sorted(
            [(j, it) for j, it in enumerate(all_items)
             if j > i and j not in consumed
             and (((it[0] + it[2]) / 2 < mid_x) == is_left)
             and it[1] >= y0],
            key=lambda kv: kv[1][1]
        )

        for j, (jx0, jy0, jx1, jy1, jtext) in same_col_after:
            if re.match(r'^①', jtext.strip()) or _QUESTION_START_RE.match(jtext):
                break
            if jy0 - by1 > 100:
                break
            consumed.add(j)
            bx0 = min(bx0, jx0)
            bx1 = max(bx1, jx1)
            by1 = max(by1, jy1)

        # 박스 하단선 탐지: 감싸는 박스 우선, 없으면 가장 가까운 수평선, 없으면 +20px
        box_bot = _find_containing_box_bottom(page, bx0, by0, bx1, by1)
        if box_bot is None:
            box_bot = _find_nearest_hline_below(page, by1, bx0, bx1, max_dist=35)
            crop_bot = (box_bot + 3) if box_bot else (by1 + 20)
        else:
            crop_bot = box_bot + 3

        url = _save_crop(page, page_num, f"bogi{int(by0)}",
                         bx0 - 15, by0 - 8, bx1 + 15, crop_bot, img_save_dir)
        if url:
            result.append((bx0, by0, bx1, by1, f"[[IMG:{url}]]"))
        else:
            result.append(all_items[i])

    return result


# ─────────────────────────────────────────
# 레이블 없는 콘텐츠 박스 이미지화 (Q15, Q36 등)
# ─────────────────────────────────────────

def _collect_content_boxes(page, bracket_boxes: list) -> list:
    """
    완전한 사각형 박스(가로+세로선 모두 있음) 수집.
    [A]/[B] 범위로 이미 사용된 박스(bracket_boxes)는 제외.
    얇은 선, 전체 폭 박스, 너무 작은 박스는 제외.
    """
    page_width = page.rect.width
    boxes = []

    for d in page.get_drawings():
        items = d.get("items", [])
        has_horiz = has_vert = False
        re_rects = []

        for item in items:
            if item[0] == "l":
                dx = abs(item[1].x - item[2].x)
                dy = abs(item[1].y - item[2].y)
                if dx > 30 and dy < 5:
                    has_horiz = True
                if dy > 30 and dx < 5:
                    has_vert = True
            elif item[0] == "re":
                ri = item[1]
                if ri.width > 50 and ri.height > 30:
                    has_horiz = True
                    has_vert = True
                    re_rects.append(ri)

        if not (has_horiz and has_vert):
            continue

        # "re" 아이템은 d.get("rect")와 별개로 항상 직접 검사 (#9/#10-A)
        for ri in re_rects:
            if ri.width >= 50 and ri.height >= 30 and ri.width < page_width * 0.85:
                is_bracket = any(
                    abs(ri.x0 - bb.x0) < 10 and abs(ri.y0 - bb.y0) < 10
                    for bb in bracket_boxes
                )
                already = any(
                    abs(ri.x0 - b.x0) < 5 and abs(ri.y0 - b.y0) < 5
                    for b in boxes
                )
                if not is_bracket and not already:
                    boxes.append(ri)

        r = d.get("rect")
        if r is None:
            continue
        if r.width < 50 or r.height < 30:
            continue
        if r.width > page_width * 0.85:
            continue  # 전체 폭 박스 = 섹션 구분선

        # [A]/[B] 브라켓으로 이미 사용된 박스 제외
        is_bracket = any(
            abs(r.x0 - bb.x0) < 10 and abs(r.y0 - bb.y0) < 10
            for bb in bracket_boxes
        )
        if is_bracket:
            continue

        # re_rects로 이미 추가된 경우 중복 제외
        already = any(
            abs(r.x0 - b.x0) < 5 and abs(r.y0 - b.y0) < 5
            for b in boxes
        )
        if already:
            continue

        boxes.append(r)

    return boxes


def _capture_boxes_as_images(page, page_num: int, all_items: list, mid_x: float,
                               content_boxes: list, img_save_dir: str) -> list:
    """
    레이블 없는 콘텐츠 박스 안의 텍스트를 이미지로 캡처.
    이미 [[IMG:...]] 로 교체된 아이템은 건너뜀 (중복 캡처 방지).
    """
    if not content_boxes:
        return all_items

    consumed = set()
    captured_box_idxs = set()
    result = []

    for i, item in enumerate(all_items):
        if i in consumed:
            continue

        x0, y0, x1, y1, text = item

        # 이미 이미지 처리된 아이템
        if text.startswith('[[IMG:'):
            result.append(item)
            continue

        # 이 아이템이 속하는 콘텐츠 박스 탐색
        target_box = None
        target_bi = -1
        item_cx = (x0 + x1) / 2
        item_cy = (y0 + y1) / 2

        for bi, box in enumerate(content_boxes):
            if bi in captured_box_idxs:
                continue
            if (box.x0 - 10 <= item_cx <= box.x1 + 10 and
                    box.y0 - 10 <= item_cy <= box.y1 + 10):
                target_box = box
                target_bi = bi
                break

        if target_box is None:
            result.append(item)
            continue

        # 같은 박스 안의 모든 아이템 consumed 처리
        for j, jitem in enumerate(all_items):
            if j in consumed:
                continue
            jx0, jy0, jx1, jy1, jtext = jitem
            jcx = (jx0 + jx1) / 2
            jcy = (jy0 + jy1) / 2
            if (target_box.x0 - 10 <= jcx <= target_box.x1 + 10 and
                    target_box.y0 - 10 <= jcy <= target_box.y1 + 10):
                consumed.add(j)

        captured_box_idxs.add(target_bi)

        url = _save_crop(page, page_num, f"box{int(target_box.y0)}",
                         target_box.x0 - 5, target_box.y0 - 5,
                         target_box.x1 + 5, target_box.y1 + 5, img_save_dir)
        if url:
            result.append((target_box.x0, target_box.y0, target_box.x1, target_box.y1,
                           f"[[IMG:{url}]]"))
        else:
            result.append(item)

    return result


def _capture_gaps(page, page_num: int, items: list, page_width: float, img_save_dir: str) -> list:
    if not items:
        return []

    page_height = page.rect.height
    y_top_limit = page_height * 0.05
    y_bot_limit = page_height * 0.90

    gap_items = []
    for col_tag, x_min, x_max in [
        ("L", 10, page_width * 0.50 - 5),
        ("R", page_width * 0.50 + 5, page_width - 10),
    ]:
        col_items = [it for it in items if it[0] >= x_min - 20 and it[2] <= x_max + 20]
        if not col_items:
            continue
        col_items.sort(key=lambda it: it[1])
        prev_y1 = col_items[0][3]
        for it in col_items[1:]:
            y0_next = it[1]
            gap = y0_next - prev_y1
            if gap >= MIN_GAP_PX:
                gy0, gy1 = prev_y1 + 2, y0_next - 2
                if gy0 < y_top_limit or gy1 > y_bot_limit:
                    prev_y1 = max(prev_y1, it[3])
                    continue
                url = _save_crop(page, page_num, f"gap{col_tag}{int(gy0)}",
                                 x_min, gy0, x_max, gy1, img_save_dir)
                if url:
                    gap_items.append((x_min, (gy0 + gy1) / 2, x_max, gy1,
                                      f"[[IMG:{url}]]"))
            prev_y1 = max(prev_y1, it[3])
    return gap_items


# ─────────────────────────────────────────
# 디지털 PDF 판별
# ─────────────────────────────────────────

def is_digital_pdf(text: str, num_pages: int) -> bool:
    if not text or num_pages == 0:
        return False
    clean = re.sub(r'\[\[IMG:[^\]]+\]\]', '', text)
    chars = len(re.sub(r'\s', '', clean))
    return (chars / num_pages) >= MIN_CHARS_PER_PAGE


# ─────────────────────────────────────────
# 스캔 PDF Fallback: Gemini OCR
# ─────────────────────────────────────────

OCR_PROMPT = """이 시험지 페이지의 텍스트를 그대로 옮겨 적으세요.

규칙:
- 2단 구성이면 왼쪽 단 전체를 먼저, 그 다음 오른쪽 단
- 문항 번호, 지문, 발문, 선택지(①②③④⑤), <보기> 모두 포함
- 수식은 텍스트로 표현 (예: x²+2x+1)
- 페이지 번호, 홀수형, 제N교시, 수험번호, 성명, 시험 제목 등은 제외
- 텍스트만 출력하고 설명은 하지 마세요

[원문자 구분 — 반드시 정확히 구별할 것]
- ㄱㄴㄷ 계열 원문자: ㉠㉡㉢㉣㉤ (자음 기반, 주로 <보기> 항목 라벨)
- 가나다 계열 원문자: ㉮㉯㉰㉱㉲ (음절 기반, 주로 지문 내 구분 기호)
- 두 계열은 외형이 유사하므로 문맥으로 판단:
  · <보기>의 항목 앞에 붙은 경우 → ㉠㉡㉢ (ㄱ계열)
  · 지문 본문에서 (가)(나)(다) 대신 쓰인 경우 → ㉮㉯㉰ (가계열)
  · 영문 소문자 원문자: ⓐⓑⓒⓓⓔⓕⓖ — 알파벳 a~g가 원 안에 있는 형태, 영어 지문이나 어휘 문제에서 사용
  · 선택지 ①②③④⑤ 와 혼동하지 말 것"""


def ocr_all_pages(image_paths: list, job_id: str = None) -> tuple[str, dict]:
    from google import genai
    from PIL import Image
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))
    total = len(image_paths)
    pages_text = []
    for i, path in enumerate(image_paths):
        try:
            img = Image.open(path)
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=[OCR_PROMPT, img],
            )
            usage = getattr(response, "usage_metadata", None)
            if usage and job_id:
                _log_ocr(job_id,
                         getattr(usage, "prompt_token_count", 0),
                         getattr(usage, "candidates_token_count", 0))
            text = response.text.strip()
            if text:
                pages_text.append(f"--- 페이지 {i + 1} ---\n{text}")
        except Exception:
            pass
        if job_id:
            _update_progress(job_id, i + 1, total)
    return "\n\n".join(pages_text), {}


def _log_ocr(job_id, input_tokens, output_tokens):
    try:
        from app.database import SessionLocal
        from app.models.api_usage import ApiUsage, calc_cost
        db = SessionLocal()
        db.add(ApiUsage(
            service="gemini", model="gemini-2.0-flash", purpose="ocr",
            job_id=job_id, input_tokens=input_tokens, output_tokens=output_tokens,
            cost_usd=calc_cost("gemini-2.0-flash", input_tokens, output_tokens),
        ))
        db.commit()
        db.close()
    except Exception:
        pass


def _update_progress(job_id: str, current: int, total: int):
    try:
        from app.database import SessionLocal
        from app.models.passage import PipelineJob
        db = SessionLocal()
        job = db.get(PipelineJob, job_id)
        if job:
            job.status = f"analyzing ({current}/{total})"
            db.commit()
        db.close()
    except Exception:
        pass
