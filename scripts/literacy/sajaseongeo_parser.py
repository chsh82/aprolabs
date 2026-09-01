"""raw/sajaseongeo/의 두 PDF에서 사자성어를 파싱하고 병합한다.

- PDF1 "(평가원+EBS)사자성어 정리(2011)[4].pdf": 회차별(6월/9월/수능 등) 목록이
  이어붙여진 문서라 번호가 여러 번 1부터 다시 시작하고, 표제어가 내부적으로도
  중복된다(같은 성어가 여러 회차 목록에 반복 등장). 글자별 훈음 분해가 있는게
  장점(예: "苛가혹할 가"). 뒷부분(15~18페이지)의 속담·관용어구 47개는 한자가
  없고 사자성어가 아니라 다루지 않는다(사용자 결정 - "사자성어만").
- PDF2 "수능_언어영역_성어.pdf": 1994년부터의 기출을 "성어"/"속담" 태그로 깨끗하게
  구분해 정리한 문서. 항목마다 실제 출제 연도·회차 목록이 있어 PDF1의 단순
  "N회" 카운트보다 정보가 많다. 다만 글자별 훈음 분해는 없다. "성어" 태그만
  쓰고 "속담" 태그(70건)는 다루지 않는다.

두 파일을 표제어로 병합한다(merge_sources) - 정의/한자/출제정보는 PDF2를
우선하고(더 정리돼 있음), 글자별 훈음 분해는 PDF1에만 있으므로 PDF1에서
가져온다. PDF2에 없는 표제어(PDF1에만 있는 것)는 PDF1의 정의를 그대로 쓴다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import fitz

RAW_DIR = Path(__file__).resolve().parents[2] / "raw" / "sajaseongeo"
PDF1_PATH = RAW_DIR / "(평가원+EBS)사자성어 정리(2011)[4].pdf"
PDF2_PATH = RAW_DIR / "수능_언어영역_성어.pdf"

_HEADER_RE = re.compile(r"^혜정샘이 정리한기출 관용어구$")
_HEADWORD_RE = re.compile(r"^([가-힣]+)\(([一-鿿]+)\)$")
_FREQ_RE = re.compile(r"^(\d+)\s*회$")
_SENSE_START_RE = re.compile(r"^([1-9])\s*(.*)$")
_SYNONYM_INLINE_RE = re.compile(r"[=≒]\s*(.+)$")
_SYNONYM_LINE_RE = re.compile(r"^(?:비슷한\s*말)\s*[:：]?\s*(.+)$")

_DATE_TOKEN = (
    r"\d{4}학년도\s*예비\s*시행(?:\s*[A-Z]형(?:\s*,\s*[A-Z]형)*)?"
    r"|\d{4}\.(?:수능|\d{1,2})(?:\s*[A-Z]형(?:\s*,\s*[A-Z]형)*)?"
)
_DATE_LEAD_RE = re.compile(r"^\s*(" + _DATE_TOKEN + r")\s*(?:/\s*)?")


@dataclass
class CharBreakdown:
    character: str  # 한자 1글자
    meaning_reading: str  # "가혹할 가"


@dataclass
class Pdf1Entry:
    order_no: int
    headword: str
    hanja: str  # 4글자 한자 전체(예: 苛斂誅求)
    chars: list[CharBreakdown]
    definitions: list[str]  # 다의어면 여러 개
    freq_count: int
    related: list[str] = field(default_factory=list)  # 유사어/비슷한 말


@dataclass
class Pdf2Entry:
    headword: str
    hanja: str
    definition: str
    dates: list[str]  # 출제 연도·회차(예: "2013.수능")


@dataclass
class MergedEntry:
    headword: str
    hanja: str
    definitions: list[str]
    chars: list[CharBreakdown]  # PDF1에 없으면 빈 리스트
    related: list[str]
    note: str  # 출제 정보(사람이 읽는 문자열)


# --------------------------------------------------------------------------
# PDF1
# --------------------------------------------------------------------------

def _pdf1_lines() -> list[str]:
    """헤더 줄과, 그 바로 다음에 오는 페이지 번호 줄을 함께 제거한다."""
    doc = fitz.open(PDF1_PATH)
    lines: list[str] = []
    for page in doc:
        page_lines = [ln.strip() for ln in page.get_text().splitlines() if ln.strip()]
        i = 0
        while i < len(page_lines):
            if _HEADER_RE.match(page_lines[i]):
                i += 2  # 헤더 + 페이지 번호 둘 다 건너뜀
                continue
            lines.append(page_lines[i])
            i += 1
    doc.close()
    return lines


def _parse_breakdown(raw: str) -> list[CharBreakdown]:
    parts = [p.strip() for p in raw.split(",")]
    result = []
    for p in parts:
        if not p:
            continue
        char, reading = p[0], p[1:].strip()
        result.append(CharBreakdown(character=char, meaning_reading=reading))
    return result


def _split_synonyms(text: str) -> tuple[str, list[str]]:
    """줄 안에 '=' 또는 '≒'가 있으면 그 뒤를 유사어로 뽑고, 앞부분만 돌려준다."""
    m = _SYNONYM_INLINE_RE.search(text)
    if not m:
        return text, []
    before = text[:m.start()].strip()
    after = m.group(1).strip()
    related = [s.strip() for s in re.split(r"[,\n]", after) if s.strip()]
    return before, related


def iter_pdf1_entries() -> list[Pdf1Entry]:
    """PDF1 전체를 파싱한다. 회차별 목록이 이어붙여져 있어 표제어가 내부적으로
    중복된다(같은 성어가 여러 번 나옴) - 여기서는 원본 그대로 다 돌려주고,
    중복 제거는 merge_sources()에서 한다."""
    lines = _pdf1_lines()

    entries: list[Pdf1Entry] = []
    i = 0
    n = len(lines)
    while i < n:
        if not lines[i].isdigit():
            i += 1
            continue
        order_no = int(lines[i])

        # 사자성어 구간이 끝나고 속담·관용어구 구간(번호가 다시 1부터 시작)이
        # 시작되면 멈춘다 - 표제어 줄에 한자가 없으면(=headword 패턴 불일치)
        # 그 지점이다.
        if i + 1 >= n:
            break
        headword_match = _HEADWORD_RE.match(lines[i + 1])
        if headword_match is None:
            break

        headword, hanja = headword_match.group(1), headword_match.group(2)
        i += 2

        breakdown_text = ""
        while i < n:
            breakdown_text += " " + lines[i]
            has_paren = ")" in lines[i]
            i += 1
            if has_paren:
                break
        bm = re.search(r"\(([^)]+)\)", breakdown_text)
        chars = _parse_breakdown(bm.group(1)) if bm else []

        def_lines: list[str] = []
        while i < n and not _FREQ_RE.match(lines[i]):
            def_lines.append(lines[i])
            i += 1
        if i >= n:
            break
        freq_count = int(_FREQ_RE.match(lines[i]).group(1))
        i += 1

        definitions: list[str] = []
        related: list[str] = []
        current = ""
        for dl in def_lines:
            dl_clean = dl.lstrip(":： ").strip()
            sm = _SENSE_START_RE.match(dl_clean)
            syn_line = _SYNONYM_LINE_RE.match(dl_clean)
            if syn_line:
                related.extend(s.strip() for s in re.split(r"[,\n]", syn_line.group(1)) if s.strip())
                continue
            if sm and current:
                text, syn = _split_synonyms(current)
                definitions.append(text)
                related.extend(syn)
                current = sm.group(2)
            elif sm and not current:
                current = sm.group(2)
            else:
                current = (current + " " + dl_clean).strip() if current else dl_clean
        if current:
            text, syn = _split_synonyms(current)
            definitions.append(text)
            related.extend(syn)

        entries.append(Pdf1Entry(
            order_no=order_no, headword=headword, hanja=hanja, chars=chars,
            definitions=[d for d in definitions if d], freq_count=freq_count,
            related=related,
        ))

    return entries


# --------------------------------------------------------------------------
# PDF2
# --------------------------------------------------------------------------

def iter_pdf2_entries() -> list[Pdf2Entry]:
    """PDF2에서 "성어" 태그가 붙은 항목만 파싱한다("속담" 70건은 제외)."""
    doc = fitz.open(PDF2_PATH)
    lines: list[str] = []
    for page in doc:
        for ln in page.get_text().splitlines():
            ln = ln.strip()
            if ln:
                lines.append(ln)
    doc.close()
    if lines and "수능 언어영역" in lines[0]:
        lines = lines[1:]

    entries: list[Pdf2Entry] = []
    i, n = 0, len(lines)
    while i < n:
        if lines[i] not in ("성어", "속담"):
            i += 1
            continue
        category = lines[i]
        i += 1
        if i >= n:
            break
        headword_line = lines[i]
        i += 1
        block: list[str] = []
        while i < n and lines[i] not in ("성어", "속담"):
            block.append(lines[i])
            i += 1

        if category != "성어":
            continue

        idx = next((j for j, ch in enumerate(headword_line) if "一" <= ch <= "鿿"), None)
        if idx is None:
            continue  # 한자를 못 찾으면 이번 파싱에서는 건너뛴다(보고 대상)
        headword, hanja = headword_line[:idx], headword_line[idx:]

        joined = " ".join(block)
        dates: list[str] = []
        while True:
            m = _DATE_LEAD_RE.match(joined)
            if not m:
                break
            dates.append(m.group(1))
            joined = joined[m.end():]
        definition = joined.strip()

        entries.append(Pdf2Entry(headword=headword, hanja=hanja, definition=definition, dates=dates))

    return entries


# --------------------------------------------------------------------------
# 병합
# --------------------------------------------------------------------------

def merge_sources() -> tuple[list[MergedEntry], dict]:
    """PDF1 + PDF2를 표제어 기준으로 병합한다.

    - 정의/한자/출제정보는 PDF2 우선(더 정리돼 있음). PDF2에 없으면 PDF1.
    - 글자별 훈음 분해(chars)는 PDF1에만 있어서 PDF1에서 가져온다 -
      PDF2에만 있는 표제어는 chars가 빈 리스트가 된다.
    - PDF1 내부 중복(같은 성어가 여러 회차 목록에 반복)은 첫 번째 등장을
      대표로 쓴다(정의 내용은 거의 동일하고, 이 프로젝트에서 사자성어는
      진짜 동음이의어 충돌 위험이 거의 없다고 판단).

    (병합 결과, 통계 dict)를 반환한다.
    """
    pdf1_entries = iter_pdf1_entries()
    pdf1_by_headword: dict[str, Pdf1Entry] = {}
    for e in pdf1_entries:
        pdf1_by_headword.setdefault(e.headword, e)  # 첫 등장만 유지

    pdf2_entries = iter_pdf2_entries()
    pdf2_by_headword: dict[str, Pdf2Entry] = {}
    for e in pdf2_entries:
        pdf2_by_headword.setdefault(e.headword, e)

    all_headwords = sorted(set(pdf1_by_headword) | set(pdf2_by_headword))

    merged: list[MergedEntry] = []
    for hw in all_headwords:
        p1 = pdf1_by_headword.get(hw)
        p2 = pdf2_by_headword.get(hw)

        if p2:
            hanja = p2.hanja
            definitions = [p2.definition] if p2.definition else []
            note = "출제: " + " / ".join(p2.dates) if p2.dates else ""
        else:
            hanja = p1.hanja
            definitions = list(p1.definitions)
            note = f"기출 {p1.freq_count}회"

        chars = list(p1.chars) if p1 else []
        related = list(p1.related) if p1 else []

        merged.append(MergedEntry(
            headword=hw, hanja=hanja, definitions=[d for d in definitions if d],
            chars=chars, related=related, note=note,
        ))

    stats = {
        "pdf1_raw": len(pdf1_entries),
        "pdf1_unique": len(pdf1_by_headword),
        "pdf2_unique": len(pdf2_by_headword),
        "both": len(set(pdf1_by_headword) & set(pdf2_by_headword)),
        "pdf1_only": len(set(pdf1_by_headword) - set(pdf2_by_headword)),
        "pdf2_only": len(set(pdf2_by_headword) - set(pdf1_by_headword)),
        "merged_total": len(merged),
    }
    return merged, stats
