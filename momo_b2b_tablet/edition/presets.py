"""④ 검수 화면 프리셋 버튼(SPEC_프롬프트_편집_기능.md 1단계, 2026-09-27 사용자 지시).

LLM을 거치지 않는 고정 패치만 계산한다 - "빠르고 정확한 것이 우선"이라는
지시대로, 실제 문구를 새로 짓지 않고 구조(레이아웃 플래그, 자리표시자)만
바꾼다. 각 프리셋 함수는 (layout, page_idx, params) -> PresetResult를 돌려주며,
적용할 수 없으면 PresetNotApplicable(사람이 읽을 이유)을 던진다.

흐름(edition/api.py): POST .../preset-preview로 ops+summary만 먼저 계산해
검수자가 토글 미리보기로 확인하게 하고, 실제 적용은 POST .../preset-apply가
같은 ops를 edition/store.patch_edition에 kind="prompt_edit"로 넘긴다."""
from __future__ import annotations

from dataclasses import dataclass


class PresetNotApplicable(Exception):
    pass


@dataclass
class PresetResult:
    ops: list[dict]
    summary: str


def _page(layout: dict, idx: int) -> dict:
    pages = layout.get("pages", [])
    if idx < 0 or idx >= len(pages):
        raise PresetNotApplicable(f"페이지 {idx}가 없음")
    return pages[idx]


# ---------- 1. 좌우 바꾸기 ----------
_MIRROR_TYPES = {"qa", "qaband", "qaref", "solo"}


def preset_mirror(layout: dict, idx: int, params: dict) -> PresetResult:
    p = _page(layout, idx)
    if p.get("type") not in _MIRROR_TYPES:
        raise PresetNotApplicable(f"'{p.get('type')}' 페이지는 좌우 바꾸기를 지원하지 않음")
    cur = bool(p.get("mirror"))
    ops = [{"op": "add", "path": f"/pages/{idx}/mirror", "value": not cur}]
    summary = "좌우 배치를 반전합니다" if not cur else "좌우 반전을 원래대로 되돌립니다"
    return PresetResult(ops, summary)


# ---------- 2. 제목 중앙 상단 ----------
_TITLE_TOP_TYPES = {"qa", "qaband", "qaref", "solo"}


def preset_title_top(layout: dict, idx: int, params: dict) -> PresetResult:
    p = _page(layout, idx)
    if p.get("type") not in _TITLE_TOP_TYPES:
        raise PresetNotApplicable(f"'{p.get('type')}' 페이지는 제목 중앙 상단을 지원하지 않음")
    if not (p.get("q") or {}).get("t"):
        raise PresetNotApplicable("이 페이지에 옮길 문항 제목이 없음")
    cur = bool(p.get("titleTop"))
    ops = [{"op": "add", "path": f"/pages/{idx}/titleTop", "value": not cur}]
    summary = "문항 제목을 페이지 중앙 상단(STEP3 방식)으로 옮깁니다" if not cur else "문항 제목을 원래 자리로 되돌립니다"
    return PresetResult(ops, summary)


# ---------- 3. 3층 구조 ----------
_THREE_TIER_TYPES = {"qa", "qaband", "qaref"}


def preset_three_tier(layout: dict, idx: int, params: dict) -> PresetResult:
    p = _page(layout, idx)
    if p.get("type") not in _THREE_TIER_TYPES:
        raise PresetNotApplicable(f"'{p.get('type')}' 페이지는 3층 구조를 지원하지 않음")
    cur = bool(p.get("wide"))
    ops = [{"op": "add", "path": f"/pages/{idx}/wide", "value": not cur}]
    summary = ("제시문 -> 문항 -> 답란 3층 구조(전체 폭)로 바꿉니다" if not cur
               else "3층 구조를 해제하고 2단 배치로 되돌립니다")
    return PresetResult(ops, summary)


# ---------- 4. 답란 늘리기/줄이기 ----------
_LINES_BOOST_MIN, _LINES_BOOST_MAX = -3, 4


def _preset_lines(layout: dict, idx: int, delta: int) -> PresetResult:
    p = _page(layout, idx)
    cur = p.get("linesBoost", 0)
    new = max(_LINES_BOOST_MIN, min(_LINES_BOOST_MAX, cur + delta))
    if new == cur:
        limit = "늘릴" if delta > 0 else "줄일"
        raise PresetNotApplicable(f"더 이상 {limit} 수 없음(한계에 도달)")
    ops = [{"op": "add", "path": f"/pages/{idx}/linesBoost", "value": new}]
    verb = "늘립니다" if delta > 0 else "줄입니다"
    summary = f"이 페이지 답란 줄 수를 {verb}(보정값 {cur:+d} → {new:+d})"
    return PresetResult(ops, summary)


def preset_lines_more(layout: dict, idx: int, params: dict) -> PresetResult:
    return _preset_lines(layout, idx, +1)


def preset_lines_less(layout: dict, idx: int, params: dict) -> PresetResult:
    return _preset_lines(layout, idx, -1)


# ---------- 5. 이미지 자리 추가 ----------
def preset_add_image_slot(layout: dict, idx: int, params: dict) -> PresetResult:
    p = _page(layout, idx)
    if p.get("slot") is not None:
        raise PresetNotApplicable("이미 이미지 자리가 있음(원본 이미지로 바꾸기에서 이미지를 고르세요)")
    ops = [{"op": "add", "path": f"/pages/{idx}/slot",
            "value": {"scene": "", "avoid": "질문이 요구하는 답을 암시하지 않는다."}}]
    return PresetResult(ops, "이미지 자리를 추가합니다(원본 이미지를 고르거나 생성 지시문을 채워야 함)")


# ---------- 6. 표 머리칸 넣기 ----------
def preset_table_header(layout: dict, idx: int, params: dict) -> PresetResult:
    p = _page(layout, idx)
    q = p.get("q") or {}
    ops: list[dict] = []
    if p.get("type") == "qaref" and not (p.get("ref") or {}).get("title"):
        ops.append({"op": "add", "path": f"/pages/{idx}/ref/title", "value": "참고"})
    if q.get("form") == "compare":
        for ci, c in enumerate(q.get("cards") or []):
            if not (c.get("title") or "").strip():
                ops.append({"op": "add", "path": f"/pages/{idx}/q/cards/{ci}/title", "value": f"구분 {ci + 1}"})
    if q.get("form") == "table":
        for ri, r in enumerate(q.get("rows") or []):
            if not (r.get("label") or "").strip():
                ops.append({"op": "add", "path": f"/pages/{idx}/q/rows/{ri}/label", "value": f"항목 {ri + 1}"})
    if not ops:
        raise PresetNotApplicable("채울 빈 머리칸이 없음(이미 다 있거나, 표·비교형·참고표가 아님)")
    return PresetResult(ops, f"빈 머리칸 {len(ops)}곳에 자리표시자 제목을 넣습니다(실제 제목은 검수에서 직접 입력)")


# ---------- 7. 페이지 합치기/나누기 ----------
def preset_merge_pages(layout: dict, idx: int, params: dict) -> PresetResult:
    """제시문 전용(excerpt) 페이지끼리, 또는 긴 제시문 자동 분리로 생긴 이어짐
    쌍만 지원한다(2026-09-26 excerpt 분리 로직의 역방향) - 문항이 있는 페이지끼리
    합치는 건 어느 문항/답란을 남길지 판단이 필요해 LLM 없는 고정 패치로
    안전하게 정의할 수 없다."""
    # 2026-09-27 정정: excerpt.text는 이 코드베이스 전체에서 항상 원소 1개짜리
    # 리스트다(문단 여러 개는 그 안의 문자열이 "\n"으로 이어붙여진다 - layout/
    # step2.py의 _split_excerpt_into_pages가 만드는 모양 그대로). 그래서 합칠
    # 때도 리스트를 이어붙이는 게 아니라 "\n"으로 이어붙인 문자열 하나를 다시
    # 1개짜리 리스트에 담아야 한다.
    pages = layout.get("pages", [])
    if idx < 0 or idx >= len(pages):
        raise PresetNotApplicable(f"페이지 {idx}가 없음")
    p = pages[idx]

    if p.get("type") == "excerpt" and idx + 1 < len(pages) and pages[idx + 1].get("type") == "excerpt":
        # 제시문 전용 페이지는 항상 continues=true(다음 쪽에 더 있다는 뜻)만
        # 갖는다(step2.py의 분리 로직 참고) - 합친 뒤에도 그대로 유지하면 된다.
        merged = f"{_excerpt_text(p)}\n{_excerpt_text(pages[idx + 1])}"
        ops = [
            {"op": "add", "path": f"/pages/{idx}/excerpt/text", "value": [merged]},
            {"op": "remove", "path": f"/pages/{idx + 1}"},
        ]
        return PresetResult(ops, "다음 제시문 전용 페이지를 이 페이지로 합칩니다")

    if p.get("type") == "excerpt" and p.get("continues") and idx + 1 < len(pages):
        nxt = pages[idx + 1]
        if (nxt.get("excerpt") or {}).get("continued"):
            merged = f"{_excerpt_text(p)}\n{_excerpt_text(nxt)}"
            ops = [
                {"op": "add", "path": f"/pages/{idx + 1}/excerpt/text", "value": [merged]},
                {"op": "remove", "path": f"/pages/{idx + 1}/excerpt/continued"},
                {"op": "remove", "path": f"/pages/{idx}"},
            ]
            return PresetResult(ops, "자동으로 나뉜 이어짐 제시문을 다시 한 페이지로 합칩니다")

    raise PresetNotApplicable("합칠 수 있는 인접한 제시문 페이지가 없음"
                               "(제시문 전용 페이지끼리, 또는 자동 분리된 이어짐 쌍만 지원)")


def _excerpt_text(page: dict) -> str:
    texts = (page.get("excerpt") or {}).get("text") or []
    return texts[0] if texts else ""


def preset_split_page(layout: dict, idx: int, params: dict) -> PresetResult:
    """긴 제시문을 문단("\\n" 경계) 절반 지점에서 둘로 나눈다 - layout/step2.py의
    _split_excerpt_into_pages와 같은 모양(continues/continued, text는 1개짜리
    리스트)으로 만들어 합치기 프리셋이 그대로 되돌릴 수 있게 한다."""
    p = _page(layout, idx)
    excerpt = p.get("excerpt")
    if not excerpt:
        raise PresetNotApplicable("이 페이지엔 나눌 제시문이 없음")
    paragraphs = [para for para in _excerpt_text(p).split("\n") if para.strip()]
    if len(paragraphs) < 2:
        raise PresetNotApplicable("문단이 2개 이상인 제시문이 있어야 나눌 수 있음")

    mid = (len(paragraphs) + 1) // 2
    first_text = "\n".join(paragraphs[:mid])
    rest_text = "\n".join(paragraphs[mid:])
    new_page = {
        "type": "excerpt", "step": p.get("step", "STEP 2"), "title": p.get("title", "함께 들여다보기"),
        "guide": p.get("guide", {}), "excerpt": {"p": excerpt.get("p"), "text": [first_text]},
        "continues": True,
    }
    ops = [
        {"op": "add", "path": f"/pages/{idx}", "value": new_page},
        {"op": "add", "path": f"/pages/{idx + 1}/excerpt/text", "value": [rest_text]},
        {"op": "add", "path": f"/pages/{idx + 1}/excerpt/continued", "value": True},
    ]
    return PresetResult(ops, f"제시문을 두 쪽으로 나눕니다({mid}문단 / {len(paragraphs) - mid}문단)")


PRESETS = {
    "mirror": preset_mirror,
    "title_top": preset_title_top,
    "three_tier": preset_three_tier,
    "lines_more": preset_lines_more,
    "lines_less": preset_lines_less,
    "add_image_slot": preset_add_image_slot,
    "table_header": preset_table_header,
    "merge_pages": preset_merge_pages,
    "split_page": preset_split_page,
}

PRESET_LABELS = {
    "mirror": "좌우 바꾸기",
    "title_top": "제목 중앙 상단",
    "three_tier": "3층 구조",
    "lines_more": "답란 늘리기",
    "lines_less": "답란 줄이기",
    "add_image_slot": "이미지 자리 추가",
    "table_header": "표 머리칸 넣기",
    "merge_pages": "페이지 합치기",
    "split_page": "페이지 나누기",
}


def compute(layout: dict, page_idx: int, preset: str, params: dict | None = None) -> PresetResult:
    fn = PRESETS.get(preset)
    if fn is None:
        raise PresetNotApplicable(f"알 수 없는 프리셋: {preset}")
    return fn(layout, page_idx, params or {})
