"""
수능 국어 PDF × JSON 검증·교정 에이전트
=========================================
Gemini 1.5 Pro 로 PDF 페이지 이미지를 분석하여
파이프라인이 추출한 segments JSON(지문·보기·문항)의 오류를 찾고
자동으로 교정한 뒤 Pydantic 모델로 반환합니다.

교정 범위
- 지문: 텍스트 오인식, [A]~[E] 범위 마커, <u> 밑줄
- 보기: <보기> 박스 감지·내용 정확도
- 문항: 발문 텍스트, 선택지 ①~⑤ 내용 정확도

사용법 (CLI)
    python verify_agent.py <pdf> <segments.json> [corrected.json]

사용법 (API)
    from verify_agent import run
    result = run("exam.pdf", segments_dict)
    print(result.model_dump_json(indent=2))
"""

from __future__ import annotations

import base64
import concurrent.futures
import difflib
import io
import time
import json
import os
import re
import sys
import textwrap
from enum import Enum
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
load_dotenv()

from google import genai
from google.genai import types
from pydantic import BaseModel, Field, field_validator

try:
    import fitz as _fitz  # PyMuPDF
    _FITZ_AVAILABLE = True
except ImportError:
    _fitz = None
    _FITZ_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic 데이터 모델
# ─────────────────────────────────────────────────────────────────────────────

class CorrectionKind(str, Enum):
    FIXED   = "fixed"    # 자동 교정 완료
    WARNING = "warning"  # 불일치 발견, 수동 확인 필요
    SKIPPED = "skipped"  # 신뢰도 부족 → 교정 건너뜀
    ERROR   = "error"    # 에이전트 내부 오류


class Correction(BaseModel):
    kind:     CorrectionKind
    location: str   # 예: "지문1", "문항3", "지문2.보기"
    field:    str   # 예: "content", "stem", "choices.2", "bogi"
    message:  str
    original: str = ""   # 교정 전 값
    corrected: str = ""  # 교정 후 값


# ── 지문·문항 내부 구조 ──────────────────────────────────────────────────────

class BracketExtract(BaseModel):
    label:      str   # A~E
    inner_text: str = ""   # 범위 내 텍스트 (전체)

    @field_validator('inner_text', mode='before')
    @classmethod
    def _none_to_empty(cls, v: Any) -> str:
        return v if v is not None else ""


class PagePassageExtract(BaseModel):
    """Gemini 가 한 페이지에서 추출한 지문 하나"""
    question_range: str = ""         # "1~3", "4~7" 등
    text:           str = ""         # 지문 본문 (줄바꿈 포함)
    brackets:       list[BracketExtract] = Field(default_factory=list)
    underlines:     list[str] = Field(default_factory=list)  # 밑줄 텍스트들
    bogi_text:      str = ""         # <보기> 박스 내용 (있을 경우)

    @field_validator('question_range', 'text', 'bogi_text', mode='before')
    @classmethod
    def _none_to_empty(cls, v: Any) -> str:
        return v if v is not None else ""


class PageQuestionExtract(BaseModel):
    """Gemini 가 한 페이지에서 추출한 문항 하나"""
    number:   int
    stem:     str = ""               # 발문
    choices:  dict[str, str] = Field(default_factory=dict)  # {"1":"①...", ...}
    bogi_text: str = ""              # 문항에 딸린 <보기>

    @field_validator('stem', 'bogi_text', mode='before')
    @classmethod
    def _none_to_empty(cls, v: Any) -> str:
        return v if v is not None else ""

    @field_validator('choices', mode='before')
    @classmethod
    def _clean_choices(cls, v: Any) -> dict:
        if not isinstance(v, dict):
            return {}
        return {k: (val if val is not None else "") for k, val in v.items()}


class PageExtract(BaseModel):
    """PDF 페이지 한 장에서 Gemini 가 추출한 전체 내용"""
    page:      int
    passages:  list[PagePassageExtract]  = Field(default_factory=list)
    questions: list[PageQuestionExtract] = Field(default_factory=list)


# ── segments JSON 구조 ────────────────────────────────────────────────────────

class Passage(BaseModel):
    content:        str = ""
    question_range: str | None = None
    model_config = {"extra": "allow"}


class Question(BaseModel):
    number:     int | None = None
    stem:       str | None = None
    choices:    dict[str, str] | None = None
    bogi:       str | None = None
    answer:     str | None = None
    explanation: str | None = None
    passage_idx: int | None = None
    model_config = {"extra": "allow"}


class Segments(BaseModel):
    passages:  list[Passage]  = Field(default_factory=list)
    questions: list[Question] = Field(default_factory=list)


class CorrectionResult(BaseModel):
    segments:    Segments
    corrections: list[Correction] = Field(default_factory=list)

    @property
    def fixed(self)   -> list[Correction]:
        return [c for c in self.corrections if c.kind == CorrectionKind.FIXED]

    @property
    def warnings(self) -> list[Correction]:
        return [c for c in self.corrections if c.kind == CorrectionKind.WARNING]


# ─────────────────────────────────────────────────────────────────────────────
# Gemini 프롬프트
# ─────────────────────────────────────────────────────────────────────────────

_PAGE_EXTRACT_PROMPT = """\
당신은 수능 국어 문제지를 분석하는 전문가입니다.
아래 이미지는 수능 국어 문제지의 한 페이지입니다.

이 페이지에 있는 모든 내용을 정확하게 추출하여 JSON으로 반환하세요.

【추출 규칙】
1. passages (지문)
   - question_range: 문항 범위 (예: "1~3", "4~7")
   - text: 지문 본문 전체를 정확하게 추출 (줄바꿈 보존, 맞춤법·띄어쓰기 원문 그대로)
     * 지문 내에 그림·사진·그래프·표·수식 이미지가 있으면 해당 위치에 [그림] 마커를 삽입
     * 예: "...앞 문장\n[그림]\n뒷 문장..." (이미지가 나타나는 정확한 위치에)
   - brackets: 페이지 옆 세로 대괄호 [A]~[E] 범위
     * label: 레이블 문자 (A, B, C, D, E)
     * inner_text: 그 범위 안에 해당하는 본문 텍스트
   - underlines: 밑줄 처리된 텍스트를 모두 추출 (문자 단위로 정확하게)
   - bogi_text: 지문에 딸린 <보기> 박스 내용 (없으면 빈 문자열)

2. questions (문항)
   - number: 문항 번호 (정수)
   - stem: 발문 텍스트 전체 (예: "윗글의 내용과 일치하지 않는 것은?")
   - choices: 선택지 {"1":"①...", "2":"②...", "3":"③...", "4":"④...", "5":"⑤..."}
     * ① ~ ⑤ 원문자 포함하여 텍스트 전체 추출
     * 선택지 내 그림·그래프가 있으면 해당 위치에 [그림] 삽입
   - bogi_text: 문항에 딸린 <보기> 박스 내용 (없으면 빈 문자열)
     * <보기> 내 그림·그래프가 있으면 해당 위치에 [그림] 삽입

【중요】
- 이미지에 보이는 텍스트를 100% 그대로 추출합니다. 내용을 요약하거나 변경하지 마세요.
- 줄바꿈, 들여쓰기 등 원문 형식을 가능한 한 보존하세요.
- 특수문자 (「」『』㉠㉡㉢ 등)도 정확하게 추출하세요.
- 이 페이지에 없는 항목은 빈 배열로 반환하세요.
- 그림/이미지가 전혀 없는 페이지에서는 [그림]을 삽입하지 마세요.

【문항 번호 누락 방지 - 매우 중요】
- 페이지가 좌/우 2단 구성이거나 (가)/(나) 지문이 병렬로 배치된 경우, 각 단을 독립적으로 스캔하여 문항 번호를 빠짐없이 추출하세요.
- 문항 번호는 일반적으로 굵은 숫자(예: 12, 13, 14, 15)로 표시됩니다.
- 지문 아래 바로 이어지는 문항들도 빠짐없이 추출하세요. 특히 마지막 문항까지 확인하세요.
- 선택지(① ~ ⑤)가 있으면 반드시 해당 문항 번호를 함께 추출해야 합니다.
- "다음 중 ...", "윗글의 ...", "㉠~㉤ 중 ..." 등의 패턴이 보이면 문항 발문입니다.

반환 형식 (JSON만, 설명 없이):
{
  "passages": [
    {
      "question_range": "1~3",
      "text": "지문 본문...",
      "brackets": [
        {"label": "A", "inner_text": "범위 내 텍스트..."}
      ],
      "underlines": ["밑줄1", "밑줄2"],
      "bogi_text": ""
    }
  ],
  "questions": [
    {
      "number": 1,
      "stem": "발문...",
      "choices": {"1": "①...", "2": "②...", "3": "③...", "4": "④...", "5": "⑤..."},
      "bogi_text": ""
    }
  ]
}
"""

_DIFF_CORRECTION_PROMPT = """\
당신은 수능 국어 문제지 텍스트 교정 전문가입니다.

【PDF 원본 텍스트】
{pdf_text}

【추출된 텍스트】
{extracted_text}

위 두 텍스트를 비교하여 불일치 부분을 모두 찾아 JSON으로 반환하세요.

규칙:
- 오탈자, 누락 문자, 잘못된 특수문자 등을 모두 찾으세요.
- 의미가 동일한 단순 공백·줄바꿈 차이는 무시하세요.
- 반환 형식: [{"original": "추출된 잘못된 텍스트", "corrected": "PDF 원본 정확한 텍스트"}, ...]
- 차이가 없으면 빈 배열 []을 반환하세요.

JSON만 반환하세요 (설명 없이).
"""


# ─────────────────────────────────────────────────────────────────────────────
# 핵심 에이전트
# ─────────────────────────────────────────────────────────────────────────────

class VerifyAgent:
    def __init__(self, api_key: str, model_name: str = "gemini-2.0-flash"):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name

    # ── 공개 인터페이스 ────────────────────────────────────────────────────

    def run(
        self,
        pdf_path: str | Path,
        segments: dict[str, Any],
        dpi: int = 150,
        verbose: bool = True,
    ) -> CorrectionResult:
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            raise FileNotFoundError(pdf_path)

        _log(verbose, f"[1/4] PDF → 이미지 변환: {pdf_path.name}")
        if not _FITZ_AVAILABLE:
            raise RuntimeError("PyMuPDF(fitz)가 설치되지 않았습니다. pip install pymupdf")
        _pdf_doc = _fitz.open(str(pdf_path))
        mat = _fitz.Matrix(dpi / 72, dpi / 72)
        images = []
        for _page in _pdf_doc:
            pix = _page.get_pixmap(matrix=mat, alpha=False)
            from PIL import Image
            import io
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            images.append(img)
        _pdf_doc.close()
        _log(verbose, f"      {len(images)}페이지")

        # PyMuPDF로 그래픽 밑줄 + bracket 좌표 텍스트 직접 추출 (Gemini 독립적)
        if _FITZ_AVAILABLE:
            _log(verbose, "      (PyMuPDF 밑줄/bracket 추출 중...)", end="\r")
            pdf_underlines_by_page = _extract_pdf_underlines(pdf_path)
            pdf_bracket_texts_by_page = _extract_pdf_bracket_texts(pdf_path)
            ul_count = sum(len(v) for v in pdf_underlines_by_page.values())
            br_count = sum(len(v) for v in pdf_bracket_texts_by_page.values())
            _log(verbose, f"      PyMuPDF 밑줄 {ul_count}건, bracket 레이블 {br_count}건 추출 완료        ")
        else:
            pdf_underlines_by_page: dict[int, list[str]] = {}
            pdf_bracket_texts_by_page: dict[int, dict[str, str]] = {}

        # 페이지별 Gemini 추출 (캐시 우선)
        _log(verbose, "[2/4] 페이지 내용 추출 (Gemini)")
        cached = _load_cache(pdf_path)
        if cached is not None:
            _log(verbose, f"      캐시 로드: {len(cached)}페이지")
            extracts = [PageExtract(**ex) for ex in cached]
        else:
            extracts: list[PageExtract] = []
            for i, img in enumerate(images, 1):
                _log(verbose, f"      {i}/{len(images)}페이지...", end="\r")
                ex = self._extract_page(img, i)
                # 지문이 없으면 최대 2회 재시도 (지문은 핵심 데이터)
                for retry in range(2):
                    if ex.passages:
                        break
                    _log(verbose, f"\n      [{i}페이지 재시도 {retry+1}]", end="\r")
                    time.sleep(2)
                    ex = self._extract_page(img, i)
                extracts.append(ex)
                if i < len(images):
                    time.sleep(1.5)  # API 속도 제한 방지
            _save_cache(pdf_path, extracts)
            _log(verbose, "")

        # PyMuPDF 밑줄을 Gemini 추출 결과에 병합
        # 해당 지문 텍스트에 실제로 존재하는 밑줄만 추가 (노이즈·타지문 밑줄 배제)
        for page_idx, page_ul in pdf_underlines_by_page.items():
            if page_idx < len(extracts):
                for passage in extracts[page_idx].passages:
                    existing = set(passage.underlines)
                    p_text = passage.text  # Gemini가 추출한 지문 텍스트
                    for u in page_ul:
                        if u in existing:
                            continue
                        # 지문 텍스트에 있는 밑줄만 추가
                        if u in p_text or (len(u) >= 8 and u[:15] in p_text):
                            passage.underlines.append(u)
                            existing.add(u)

        # segments 파싱
        passages  = [Passage(**p)  for p in segments.get("passages",  [])]
        questions = [Question(**q) for q in segments.get("questions", [])]

        # 매핑 (추출 내용 → JSON segments)
        _log(verbose, "[3/4] PDF ↔ JSON 비교 및 교정")
        corrections: list[Correction] = []

        corrections += self._verify_passages(extracts, passages, pdf_bracket_texts_by_page, verbose)
        corrections += self._verify_questions(extracts, questions, verbose)

        _log(verbose, f"[4/4] 완료: 수정 {sum(1 for c in corrections if c.kind==CorrectionKind.FIXED)}건"
             f" / 경고 {sum(1 for c in corrections if c.kind==CorrectionKind.WARNING)}건"
             f" / 건너뜀 {sum(1 for c in corrections if c.kind==CorrectionKind.SKIPPED)}건")

        return CorrectionResult(
            segments=Segments(passages=passages, questions=questions),
            corrections=corrections,
        )

    # ── 페이지 추출 ────────────────────────────────────────────────────────

    def _extract_page(self, img, page_num: int) -> PageExtract:
        """PIL Image 한 장을 Gemini로 분석"""
        b64 = _img_to_b64(img)
        resp = None
        try:
            def _call():
                return self.client.models.generate_content(
                    model=self.model_name,
                    contents=[
                        types.Part.from_bytes(data=base64.b64decode(b64), mime_type="image/png"),
                        _PAGE_EXTRACT_PROMPT,
                    ],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        max_output_tokens=8192,
                    ),
                )
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _ex:
                _future = _ex.submit(_call)
                try:
                    resp = _future.result(timeout=90)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(f"페이지{page_num} Gemini 응답 90초 초과")
            data = _parse_json(resp.text)
            passages  = [PagePassageExtract(**p)  for p in data.get("passages",  [])]
            questions = [PageQuestionExtract(**q) for q in data.get("questions", [])]
            _log(True, f"\n      [페이지{page_num}] 지문 {len(passages)}건, 문항 {len(questions)}건 추출")
            return PageExtract(page=page_num, passages=passages, questions=questions)
        except Exception as e:
            _log(True, f"\n      [페이지{page_num} 오류] {type(e).__name__}: {e}")
            return PageExtract(page=page_num)

    # ── 지문 검증 ─────────────────────────────────────────────────────────

    def _verify_passages(
        self,
        extracts: list[PageExtract],
        passages: list[Passage],
        pdf_bracket_texts_by_page: dict[int, dict[str, str]],
        verbose: bool,
    ) -> list[Correction]:
        corrections: list[Correction] = []

        # 같은 question_range를 가진 지문(여러 페이지에 걸친 경우) 병합
        # ─ qr 정규화: 물결표 변형 통일(~, ～, ∼ → ～) 후 키로 사용
        # ─ 빈 qr 지문: Gemini가 qr을 인식 못 한 연속 지문 조각 → 직전 지문에 병합
        merged_map: dict[str, PagePassageExtract] = {}
        pdf_passages: list[PagePassageExtract] = []
        last_named: PagePassageExtract | None = None  # 마지막으로 확인된 qr이 있는 지문
        last_named_page: int = -1                     # 해당 지문이 속한 페이지 번호

        for ex in extracts:
            for pex in ex.passages:
                qr = _norm_qr(pex.question_range)
                if qr:
                    if qr in merged_map:
                        existing = merged_map[qr]
                        if pex.text and pex.text not in existing.text:
                            existing.text = existing.text + "\n" + pex.text
                        existing.brackets.extend(pex.brackets)
                        existing.underlines.extend(pex.underlines)
                        if pex.bogi_text and not existing.bogi_text:
                            existing.bogi_text = pex.bogi_text
                    else:
                        merged_map[qr] = pex
                        pdf_passages.append(pex)
                    last_named = merged_map[qr]
                    last_named_page = ex.page
                else:
                    # 빈 qr → 같은 페이지 직전 지문의 연속 조각인 경우에만 병합
                    # (Gemini가 같은 페이지에서 지문을 2개로 쪼개 반환한 경우)
                    # 다음 페이지의 빈 qr 지문은 독립된 새 지문으로 처리
                    if (last_named is not None
                            and ex.page == last_named_page
                            and pex.text
                            and pex.text not in last_named.text):
                        last_named.text = last_named.text + "\n" + pex.text
                        last_named.underlines.extend(pex.underlines)
                        last_named.brackets.extend(pex.brackets)
                    else:
                        pdf_passages.append(pex)

        for i, passage in enumerate(passages):
            loc = f"지문{i+1}"
            qr  = passage.question_range or ""

            # 가장 유사한 PDF 지문 찾기
            best = _best_match_passage(passage.content, pdf_passages, qr)
            if best is None:
                corrections.append(Correction(
                    kind=CorrectionKind.WARNING, location=loc, field="content",
                    message="대응하는 PDF 지문을 찾지 못했습니다. 수동 확인 필요",
                ))
                continue

            # 교정 전 원본 유사도 계산 (underline skip 여부 판단용)
            _pre_ratio = difflib.SequenceMatcher(
                None,
                _strip_markers(passage.content),
                _strip_markers(best.text),
            ).ratio()

            # ① 중복 bracket 레이블 사전 수정 (예: [B]가 2개 → [A][B]로 재할당)
            fixed_content, dup_desc = _fix_duplicate_bracket_labels(passage.content)
            if dup_desc:
                passage.content = fixed_content
                corrections.append(Correction(
                    kind=CorrectionKind.FIXED, location=loc, field="brackets",
                    message=f"중복 bracket 레이블 재할당: {dup_desc}",
                    original=passage.content[:40], corrected=fixed_content[:40],
                ))

            # ② 텍스트 정확도 교정 (지문 content는 길이가 길어 threshold 완화)
            text_corrs = self._correct_text(
                loc, "content", passage.content, best.text, threshold=0.70
            )
            for tc in text_corrs:
                if tc.kind == CorrectionKind.FIXED:
                    passage.content = tc.corrected
            corrections.extend(text_corrs)

            # ③ [A]~[E] 브래킷 마커 교정
            # PyMuPDF 좌표 기반 bracket 텍스트 조회 (Gemini inner_text='' 보충용)
            p_flat = re.sub(r'\s+', ' ', _strip_markers(passage.content))
            bracket_texts: dict[str, str] = {}
            for page_bts in pdf_bracket_texts_by_page.values():
                for lbl, txt in page_bts.items():
                    if lbl not in bracket_texts:
                        txt_head = re.sub(r'\s+', ' ', txt[:25]).strip()
                        if txt_head and txt_head in p_flat:
                            bracket_texts[lbl] = txt
            corrections.extend(
                self._correct_brackets(loc, passage, best.brackets, bracket_texts or None)
            )

            # ④ 밑줄 <u> 교정 — 교정 전 유사도가 낮으면 경고 신뢰 불가 → 생략
            corrections.extend(
                self._correct_underlines(loc, passage, best.underlines, skip_warnings=_pre_ratio < 0.75)
            )

            # ⑤ <보기> 교정
            if best.bogi_text:
                corrections.extend(
                    self._correct_bogi_in_passage(loc, passage, best.bogi_text)
                )

            # ⑥ 이미지 위치 검증 — JSON의 <img>와 Gemini의 [그림] 위치 일치 확인
            corrections.extend(
                self._verify_images(loc, passage, best.text)
            )

        return corrections

    # ── 문항 검증 ─────────────────────────────────────────────────────────

    def _verify_questions(
        self,
        extracts: list[PageExtract],
        questions: list[Question],
        verbose: bool,
    ) -> list[Correction]:
        corrections: list[Correction] = []

        # 모든 페이지에서 추출된 문항 평탄화 & 번호별 병합 (여러 페이지에 걸친 경우)
        pdf_q_map: dict[int, PageQuestionExtract] = {}
        for ex in extracts:
            for pq in ex.questions:
                if pq.number in pdf_q_map:
                    existing = pdf_q_map[pq.number]
                    # stem이 더 길면 교체
                    if len(pq.stem) > len(existing.stem):
                        existing.stem = pq.stem
                    # choices 병합: 더 긴 텍스트(더 완전한 추출) 우선
                    for k, v in pq.choices.items():
                        if v and (k not in existing.choices or len(v) > len(existing.choices.get(k, ""))):
                            existing.choices[k] = v
                    # bogi 병합
                    if pq.bogi_text and not existing.bogi_text:
                        existing.bogi_text = pq.bogi_text
                else:
                    pdf_q_map[pq.number] = pq

        # 전체 추출 stem 목록 (폴백 유사도 매칭용)
        all_pdf_questions = list(pdf_q_map.values())

        for i, question in enumerate(questions):
            num = question.number
            loc = f"문항{num}" if num else f"문항[idx={i}]"
            pdf_q = pdf_q_map.get(num)

            if pdf_q is None:
                # 폴백: 발문(stem) 유사도로 PDF 추출 문항과 매칭 시도
                q_stem = (question.stem or "").strip()
                best_match: PageQuestionExtract | None = None
                best_ratio = 0.0
                if q_stem and all_pdf_questions:
                    for candidate in all_pdf_questions:
                        if not candidate.stem:
                            continue
                        r = difflib.SequenceMatcher(None, q_stem, candidate.stem).ratio()
                        if r > best_ratio:
                            best_ratio = r
                            best_match = candidate

                FALLBACK_THRESHOLD = 0.55  # 55% 이상이면 같은 문항으로 간주
                if best_match is not None and best_ratio >= FALLBACK_THRESHOLD:
                    _log(verbose,
                         f"  [폴백매칭] {loc} → PDF문항{best_match.number} "
                         f"(유사도={best_ratio:.2f})")
                    pdf_q = best_match
                else:
                    corrections.append(Correction(
                        kind=CorrectionKind.WARNING, location=loc, field="stem",
                        message="PDF에서 해당 문항을 찾지 못했습니다. 수동 확인 필요",
                    ))
                    continue

            # ① 발문(stem) 교정
            if pdf_q.stem:
                stem_corrs = self._correct_text(
                    loc, "stem", question.stem or "", pdf_q.stem, threshold=0.80
                )
                for tc in stem_corrs:
                    if tc.kind == CorrectionKind.FIXED:
                        question.stem = tc.corrected
                corrections.extend(stem_corrs)

            # ② 선택지(choices) 교정
            if pdf_q.choices:
                corrections.extend(
                    self._correct_choices(loc, question, pdf_q.choices)
                )

            # ③ 문항 <보기> 교정
            if pdf_q.bogi_text:
                corrections.extend(
                    self._correct_bogi_in_question(loc, question, pdf_q.bogi_text)
                )

        return corrections

    # ── 텍스트 교정 ────────────────────────────────────────────────────────

    def _correct_text(
        self,
        loc: str,
        field: str,
        extracted: str,
        pdf_text: str,
        threshold: float = 0.85,
    ) -> list[Correction]:
        """
        difflib 로 두 텍스트를 비교, 유사도가 충분하면 자동 교정.
        구조 마커([A:START] 등)가 있는 경우 마커를 보존하면서 텍스트만 교정.
        지문 헤더(다음 글을 읽고 물음에 답하시오 등)를 제거한 버전도 시도.
        """
        if not pdf_text or not extracted:
            return []

        # 마커를 보존한 채 순수 텍스트만 비교
        clean_ext = _strip_markers(extracted)
        clean_pdf = _strip_markers(pdf_text)

        if clean_ext == clean_pdf:
            return []

        ratio = difflib.SequenceMatcher(None, clean_ext, clean_pdf).ratio()

        # 헤더 제거 후 재시도 — Gemini는 지문 헤더를 추출하지 않으므로 인위적으로 유사도 낮아짐
        use_stripped = False
        stripped_ext = _strip_passage_header(clean_ext)
        stripped_pdf = _strip_passage_header(clean_pdf)
        if stripped_ext != clean_ext or stripped_pdf != clean_pdf:
            stripped_ratio = difflib.SequenceMatcher(None, stripped_ext, stripped_pdf).ratio()
            if stripped_ratio > ratio:
                ratio = stripped_ratio
                use_stripped = True

        # 비교 정규화 버전으로도 시도 (『』↔「」 등 OCR 혼용 문자 통일)
        base_ext = stripped_ext if use_stripped else clean_ext
        base_pdf = stripped_pdf if use_stripped else clean_pdf
        norm_ext = _normalize_for_comparison(base_ext)
        norm_pdf = _normalize_for_comparison(base_pdf)
        if norm_ext != base_ext or norm_pdf != base_pdf:
            norm_ratio = difflib.SequenceMatcher(None, norm_ext, norm_pdf).ratio()
            if norm_ratio > ratio:
                ratio = norm_ratio

        if ratio >= threshold:
            # 신뢰도 충분 → 부분 교정 적용
            if use_stripped:
                # 원본에서도 헤더를 제거한 버전으로 패치 적용
                base_for_patch = _strip_passage_header(extracted)
                corrected = _apply_text_patches(base_for_patch, stripped_ext, stripped_pdf)
            else:
                corrected = _apply_text_patches(extracted, clean_ext, clean_pdf)
            if corrected == extracted:
                return []
            # diff_summary는 실제 적용된 변경사항만 표시 (OCR 보호로 차단된 패치 제외)
            actual_before = _strip_markers(base_for_patch if use_stripped else extracted)
            actual_after = _strip_markers(corrected)
            return [Correction(
                kind=CorrectionKind.FIXED, location=loc, field=field,
                message=f"텍스트 교정 (유사도 {ratio:.0%}): "
                        f"{_diff_summary(actual_before, actual_after)}",
                original=extracted, corrected=corrected,
            )]
        elif ratio >= 0.5:
            return [Correction(
                kind=CorrectionKind.WARNING, location=loc, field=field,
                message=f"텍스트 불일치 (유사도 {ratio:.0%}) — 수동 확인 필요: "
                        f"{_diff_summary(clean_ext, clean_pdf)}",
                original=extracted, corrected=pdf_text,
            )]
        else:
            return [Correction(
                kind=CorrectionKind.SKIPPED, location=loc, field=field,
                message=f"유사도 낮음({ratio:.0%}) — 자동 교정 건너뜀 (다른 지문과 혼동 가능성)",
            )]

    # ── 브래킷 마커 교정 ───────────────────────────────────────────────────

    def _correct_brackets(
        self,
        loc: str,
        passage: Passage,
        pdf_brackets: list[BracketExtract],
        pdf_bracket_texts: dict[str, str] | None = None,
    ) -> list[Correction]:
        """
        pdf_bracket_texts: PyMuPDF 좌표 기반으로 추출한 레이블별 텍스트.
                           Gemini inner_text가 빈 경우 보충용으로 사용.
        """
        corrections: list[Correction] = []
        content = passage.content

        # ── 앵커 탐색 헬퍼 ────────────────────────────────────────────────
        def _find_anchor(text: str, c: str) -> tuple[int, str]:
            """앵커 텍스트를 지문에서 탐색.
            직접 → 공백정규화 → 인용부호정규화 → 공백완전제거 순으로 시도."""
            t = text.strip()
            i = c.find(t)
            if i >= 0:
                return i, t
            t_flat = re.sub(r'\s+', ' ', t)
            t_flat = t_flat.replace('｣', '」').replace('｢', '「').replace('『', '「').replace('』', '」')
            pat = re.escape(t_flat).replace(r'\ ', r'\s+')
            m = re.search(pat, c)
            if m:
                return m.start(), c[m.start():m.end()]
            t_nolead = re.sub(r'^[「『]\s*', '', t_flat)
            if t_nolead != t_flat:
                pat2 = re.escape(t_nolead).replace(r'\ ', r'\s+')
                m = re.search(pat2, c)
                if m:
                    return m.start(), c[m.start():m.end()]
            t_nospace = re.sub(r'\s', '', t_flat)
            if len(t_nospace) >= 6:
                pat3 = r'\s*'.join(re.escape(ch) for ch in t_nospace[:20])
                m = re.search(pat3, c)
                if m:
                    return m.start(), c[m.start():m.end()]
            return -1, t

        # ── 삽입 공통 로직 ────────────────────────────────────────────────
        def _try_insert(lbl: str, inner: str, c: str) -> str | None:
            """inner_text 기반으로 마커 삽입 시도. 성공 시 새 content 반환, 실패 시 None."""
            s_tag, e_tag = f"[{lbl}:START]", f"[{lbl}:END]"
            idx, _ = _find_anchor(inner[:30], c)
            if idx < 0:
                idx, _ = _find_anchor(inner[:15], c)
            if idx < 0:
                return None
            end_raw = inner[-30:].strip() if len(inner) > 30 else inner.strip()
            end_s, end_anch = _find_anchor(end_raw, c[idx:])
            end_pos = (idx + end_s + len(end_anch)) if end_s >= 0 else -1
            if end_pos > idx:
                return c[:idx] + f"{s_tag}\n" + c[idx:end_pos] + f"\n{e_tag}" + c[end_pos:]
            return None

        _LABEL_NORM = {
            'Ⓐ': 'A', 'Ⓑ': 'B', 'Ⓒ': 'C', 'Ⓓ': 'D', 'Ⓔ': 'E',
            '@': 'A',
        }

        for br in pdf_brackets:
            label = br.label.strip()
            label = _LABEL_NORM.get(label, label.upper())
            if label not in ('A', 'B', 'C', 'D', 'E'):
                continue

            inner = br.inner_text.strip()
            # Gemini inner_text가 비어 있으면 PyMuPDF 좌표 기반 텍스트로 보충
            if not inner and pdf_bracket_texts:
                inner = (pdf_bracket_texts or {}).get(label, '')

            start_tag = f"[{label}:START]"
            end_tag   = f"[{label}:END]"
            has_start = start_tag in content
            has_end   = end_tag   in content

            if has_start and has_end:
                # 마커 존재 → inner_text와 현재 마커 범위 비교
                m = re.search(
                    re.escape(start_tag) + r'(.*?)' + re.escape(end_tag),
                    content, re.DOTALL
                )
                if m:
                    current_inner = _strip_markers(m.group(1)).strip()
                    if inner and current_inner:
                        ratio = difflib.SequenceMatcher(None, current_inner, inner).ratio()
                        if ratio < 0.7:
                            # 위치 재조정 시도: 기존 마커 제거 후 Gemini 기준으로 재삽입
                            stripped = (content
                                        .replace(f"{start_tag}\n", '')
                                        .replace(f"\n{end_tag}", '')
                                        .replace(start_tag, '')
                                        .replace(end_tag, ''))
                            new_c = _try_insert(label, inner, stripped)
                            if new_c:
                                passage.content = new_c
                                content = new_c
                                corrections.append(Correction(
                                    kind=CorrectionKind.FIXED, location=loc,
                                    field=f"brackets.{label}",
                                    message=f"[{label}] 범위 마커 위치 재조정 (기존 유사도 {ratio:.0%})",
                                    original=current_inner[:80], corrected=inner[:80],
                                ))
                            else:
                                corrections.append(Correction(
                                    kind=CorrectionKind.WARNING, location=loc,
                                    field=f"brackets.{label}",
                                    message=f"[{label}] 범위 내 텍스트 불일치 — 수동 확인 필요",
                                    original=current_inner[:80], corrected=inner[:80],
                                ))
                continue

            # 마커가 없는 경우
            if not inner:
                corrections.append(Correction(
                    kind=CorrectionKind.WARNING, location=loc, field=f"brackets.{label}",
                    message=f"PDF에 [{label}] 범위 존재하나 범위 내 텍스트 미확인 — 수동 삽입 필요",
                ))
                continue

            # inner_text 기반 신규 삽입
            new_c = _try_insert(label, inner, content)
            if new_c:
                passage.content = new_c
                content = new_c
                corrections.append(Correction(
                    kind=CorrectionKind.FIXED, location=loc, field=f"brackets.{label}",
                    message=f"[{label}] 범위 마커 자동 삽입",
                    original="(없음)", corrected=f"{start_tag}...{end_tag}",
                ))
            else:
                idx, _ = _find_anchor(inner[:30], content)
                if idx < 0:
                    idx, _ = _find_anchor(inner[:15], content)
                if idx >= 0:
                    corrections.append(Correction(
                        kind=CorrectionKind.WARNING, location=loc, field=f"brackets.{label}",
                        message=f"[{label}] 시작 위치는 찾았으나 끝 위치 특정 불가 — 수동 삽입 필요",
                    ))
                else:
                    corrections.append(Correction(
                        kind=CorrectionKind.WARNING, location=loc, field=f"brackets.{label}",
                        message=f"[{label}] 범위 내 텍스트를 지문에서 찾지 못했습니다 — 수동 확인 필요",
                        original="", corrected=inner[:30],
                    ))

        return corrections

    # ── 밑줄 교정 ─────────────────────────────────────────────────────────

    def _correct_underlines(
        self,
        loc: str,
        passage: Passage,
        pdf_underlines: list[str],
        skip_warnings: bool = False,
    ) -> list[Correction]:
        corrections: list[Correction] = []
        content = passage.content

        for u_text in pdf_underlines:
            u_text = u_text.strip()
            if not u_text:
                continue
            # PDF에서 추출된 단일 기호·짧은 잡음은 무시
            if _is_noise_underline(u_text) or _is_noise_pdf_underline(u_text):
                continue

            # 짧고 지문에서 여러 번 등장하는 텍스트는 위치 특정 불가 → 건너뜀
            # 예: '높다.' (4자) 처럼 지문 전체에 걸쳐 중복 출현하는 단편
            u_no_ws = re.sub(r'\s', '', u_text)
            if len(u_no_ws) <= 4 and content.count(u_text) > 1:
                continue

            # 이미 <u> 태그로 감싸져 있는지 확인 (다중 방식)
            u_head = u_text[:20]
            # 방법 1: <u>가 바로 앞에 있는 경우
            already = bool(re.search(r'<u>' + re.escape(u_head), content))
            if not already:
                # 방법 2: u_text가 기존 <u>...</u> 내부에 있는 경우 (더 긴 밑줄의 일부)
                #  → u_head 위치 앞의 열린 <u> 태그 수 계산
                idx_head = content.find(u_head)
                if idx_head < 0:
                    # 공백/｣ 정규화 후 재탐색
                    u_head_flat = re.sub(r'\s+', ' ', u_head).replace('｣', '」').replace('｢', '「')
                    m_hd = re.search(re.escape(u_head_flat).replace(r'\ ', r'\s+'), content)
                    idx_head = m_hd.start() if m_hd else -1
                if idx_head >= 0:
                    before = content[:idx_head]
                    if before.count('<u>') > before.count('</u>'):
                        already = True  # 열린 <u> 태그 안에 위치함
            if already:
                continue

            # 지문 내 위치 탐색
            idx = content.find(u_text)
            if idx < 0:
                # ① 공백/줄바꿈 차이 무시 (PyMuPDF는 공백, JSON은 줄바꿈일 수 있음)
                u_flat = re.sub(r'\s+', ' ', u_text).strip()
                # ｣·」 등 특수 닫는 괄호 정규화
                u_flat = u_flat.replace('｣', '」').replace('｢', '「')
                pat = re.escape(u_flat).replace(r'\ ', r'\s+')
                m = re.search(pat, content)
                if m:
                    idx = m.start()
                    u_text = content[m.start():m.end()]  # 원본 텍스트로 교체
            if idx < 0:
                # ② 앞 20자만으로 위치 추정
                idx = content.find(u_text[:20])
                if idx < 0:
                    u_flat20 = re.sub(r'\s+', ' ', u_text[:20])
                    pat20 = re.escape(u_flat20).replace(r'\ ', r'\s+')
                    m20 = re.search(pat20, content)
                    idx = m20.start() if m20 else -1
                if idx < 0:
                    # 5자 이하 짧은 밑줄: 노이즈일 가능성 높고 위치 특정 불가 → 경고 없이 건너뜀
                    if len(re.sub(r'\s', '', u_text)) <= 5:
                        continue
                    corrections.append(Correction(
                        kind=CorrectionKind.WARNING, location=loc, field="underlines",
                        message=f"PDF 밑줄 텍스트를 지문에서 찾지 못함: '{u_text[:40]}'",
                    ))
                    continue
                u_text = content[idx:idx + len(u_text)]  # 실제 길이 보정

            # <u> 태그 삽입
            new_content = content[:idx] + f"<u>{u_text}</u>" + content[idx + len(u_text):]
            passage.content = new_content
            content = new_content
            corrections.append(Correction(
                kind=CorrectionKind.FIXED, location=loc, field="underlines",
                message=f"밑줄 <u> 태그 자동 삽입: '{u_text[:40]}'",
                original=u_text, corrected=f"<u>{u_text}</u>",
            ))

        # JSON에 <u>가 있는데 PDF에 없는 경우 (퍼지 매칭으로 확인)
        # PDF 밑줄 추출이 아예 없거나 지문 유사도 낮으면 경고 생략
        if not pdf_underlines or skip_warnings:
            return corrections
        json_underlines = re.findall(r'<u>(.*?)</u>', content, re.DOTALL)
        for ju in json_underlines:
            ju_clean = _strip_markers(ju).strip()
            if not ju_clean or len(ju_clean) < 2:
                continue
            # 6자 미만 짧은 조각: Gemini가 단편으로 잘못 추출한 경우가 많아 경고 생략
            if len(re.sub(r'\s', '', ju_clean)) < 6:
                continue
            # 명백히 밑줄이 아닌 텍스트(챕터 헤더, 각주 등) 무시
            if _is_noise_underline(ju_clean):
                continue
            if not _underline_confirmed(ju_clean, pdf_underlines):
                corrections.append(Correction(
                    kind=CorrectionKind.WARNING, location=loc, field="underlines",
                    message=f"JSON의 <u> 태그가 PDF에서 미확인: '{ju_clean[:40]}' — 수동 확인 필요",
                ))

        return corrections

    # ── 보기 교정 (지문) ───────────────────────────────────────────────────

    def _correct_bogi_in_passage(
        self,
        loc: str,
        passage: Passage,
        pdf_bogi: str,
    ) -> list[Correction]:
        content = passage.content
        if "<보기>" not in content:
            new_content = content + f"\n<보기>\n{pdf_bogi}"
            passage.content = new_content
            return [Correction(
                kind=CorrectionKind.FIXED, location=loc, field="bogi",
                message="지문 내 <보기> 자동 추가",
                original="(없음)", corrected=pdf_bogi[:60],
            )]

        # 이미 있으면 내용 비교
        m = re.search(r'<보기>\s*(.*?)(?=\n\d+\.|$)', content, re.DOTALL)
        if m:
            current = m.group(1).strip()
            ratio = difflib.SequenceMatcher(None, current, pdf_bogi.strip()).ratio()
            if ratio < 0.8:
                return [Correction(
                    kind=CorrectionKind.WARNING, location=loc, field="bogi",
                    message=f"<보기> 내용 불일치 (유사도 {ratio:.0%}) — 수동 확인",
                    original=current[:80], corrected=pdf_bogi[:80],
                )]
        return []

    # ── 보기 교정 (문항) ───────────────────────────────────────────────────

    def _correct_bogi_in_question(
        self,
        loc: str,
        question: Question,
        pdf_bogi: str,
    ) -> list[Correction]:
        current = (question.bogi or "").strip()
        pdf_bogi = pdf_bogi.strip()
        if not current:
            question.bogi = pdf_bogi
            return [Correction(
                kind=CorrectionKind.FIXED, location=loc, field="bogi",
                message="문항 <보기> 자동 추가",
                original="(없음)", corrected=pdf_bogi[:60],
            )]
        ratio = difflib.SequenceMatcher(None, current, pdf_bogi).ratio()
        if ratio < 0.85:
            text_corrs = self._correct_text(loc, "bogi", current, pdf_bogi)
            for tc in text_corrs:
                if tc.kind == CorrectionKind.FIXED:
                    question.bogi = tc.corrected
            return text_corrs
        return []

    # ── 이미지 위치 검증 ───────────────────────────────────────────────────

    def _verify_images(
        self,
        loc: str,
        passage: Passage,
        pdf_text: str,
    ) -> list[Correction]:
        """
        JSON의 <img> 태그와 Gemini 추출의 [그림] 마커 위치를 비교.
        - JSON에 <img>가 있는데 Gemini에 [그림]이 없으면: 이미지 누락 경고
        - Gemini에 [그림]이 있는데 JSON에 <img>가 없으면: 이미지 미삽입 경고
        - 양쪽 모두 있으면 앞뒤 문맥 텍스트로 위치 일치 확인
        """
        corrections: list[Correction] = []
        content = passage.content

        json_imgs = list(re.finditer(r'<img\b[^>]*/?\s*>', content))
        pdf_imgs  = list(re.finditer(r'\[그림\]', pdf_text))

        json_count = len(json_imgs)
        pdf_count  = len(pdf_imgs)

        if json_count == 0 and pdf_count == 0:
            return []  # 양쪽 모두 이미지 없음 → 정상

        if json_count == 0 and pdf_count > 0:
            corrections.append(Correction(
                kind=CorrectionKind.WARNING, location=loc, field="images",
                message=f"Gemini가 [그림] {pdf_count}개를 감지했으나 JSON에 <img> 없음 — 이미지 삽입 누락 확인 필요",
            ))
            return corrections

        if json_count > 0 and pdf_count == 0:
            # 캐시된 Gemini 결과는 구 프롬프트로 추출됐을 수 있으므로 INFO만 기록
            corrections.append(Correction(
                kind=CorrectionKind.WARNING, location=loc, field="images",
                message=f"JSON에 <img> {json_count}개 존재하나 Gemini가 [그림]을 감지하지 못함 — 재추출 시 확인",
            ))
            return corrections

        # 양쪽 모두 이미지 있음 → 개수 및 위치 일치 확인
        if json_count != pdf_count:
            corrections.append(Correction(
                kind=CorrectionKind.WARNING, location=loc, field="images",
                message=f"이미지 개수 불일치: JSON {json_count}개 vs PDF {pdf_count}개 — 수동 확인 필요",
            ))
            return corrections

        # 개수 일치 → 각 이미지의 앞뒤 문맥 텍스트로 위치 검증
        clean_content = _strip_markers(content)
        clean_pdf     = _strip_markers(pdf_text)

        for i, (jm, pm) in enumerate(zip(json_imgs, pdf_imgs)):
            # 이미지 앞 20자 문맥
            j_before = re.sub(r'\s+', ' ', clean_content[max(0, jm.start()-20):jm.start()]).strip()[-15:]
            p_before = re.sub(r'\s+', ' ', clean_pdf[max(0, pm.start()-20):pm.start()]).strip()[-15:]
            if j_before and p_before:
                ratio = difflib.SequenceMatcher(None, j_before, p_before).ratio()
                if ratio < 0.5:
                    corrections.append(Correction(
                        kind=CorrectionKind.WARNING, location=loc, field="images",
                        message=f"이미지 {i+1}번 위치 불일치 — JSON: '...{j_before}[img]', PDF: '...{p_before}[그림]'",
                    ))

        return corrections

    # ── 선택지 교정 ────────────────────────────────────────────────────────

    def _correct_choices(
        self,
        loc: str,
        question: Question,
        pdf_choices: dict[str, str],
    ) -> list[Correction]:
        corrections: list[Correction] = []
        if not question.choices:
            question.choices = pdf_choices
            return [Correction(
                kind=CorrectionKind.FIXED, location=loc, field="choices",
                message="선택지 자동 추가 (JSON에 없었음)",
            )]

        for k, pdf_val in pdf_choices.items():
            json_val = question.choices.get(k, "")
            if not json_val:
                question.choices[k] = pdf_val
                corrections.append(Correction(
                    kind=CorrectionKind.FIXED, location=loc, field=f"choices.{k}",
                    message=f"선택지 {k}번 추가",
                    original="(없음)", corrected=pdf_val[:60],
                ))
                continue

            # 비교 전 trailing <img>, 마커 등 제거 후 순수 텍스트만 비교
            json_clean = _clean_choice_text(json_val)
            pdf_clean  = _clean_choice_text(pdf_val)
            ratio = difflib.SequenceMatcher(None, json_clean, pdf_clean).ratio()

            if ratio < 0.85:
                if ratio >= 0.6:
                    # JSON의 후행 쓰레기는 제거하되 PDF 원문으로 교정
                    question.choices[k] = pdf_val
                    corrections.append(Correction(
                        kind=CorrectionKind.FIXED, location=loc, field=f"choices.{k}",
                        message=f"선택지 {k}번 교정 (유사도 {ratio:.0%})",
                        original=json_clean[:60], corrected=pdf_clean[:60],
                    ))
                elif ratio >= 0.4 and json_clean and _choices_trailing_junk(json_val):
                    # 텍스트 자체는 비슷한데 JSON에 후행 쓰레기가 있는 경우 자동 정리
                    question.choices[k] = json_clean  # 쓰레기만 제거
                    corrections.append(Correction(
                        kind=CorrectionKind.FIXED, location=loc, field=f"choices.{k}",
                        message=f"선택지 {k}번 후행 내용 제거 (유사도 {ratio:.0%})",
                        original=json_val[:60], corrected=json_clean[:60],
                    ))
                else:
                    corrections.append(Correction(
                        kind=CorrectionKind.WARNING, location=loc, field=f"choices.{k}",
                        message=f"선택지 {k}번 불일치 (유사도 {ratio:.0%}) — 수동 확인",
                        original=json_clean[:80], corrected=pdf_clean[:80],
                    ))

        return corrections


# ─────────────────────────────────────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────────────────────────────────────

# 명백히 밑줄이 아닌 패턴 (챕터 헤더, 각주, 출처 등)
_NOISE_UNDERLINE_PATTERNS = [
    r'^\[[\d～~]+\]',              # [1～3] 형태의 챕터 헤더
    r'다음 글을 읽고 물음에 답하시오',
    r'다음은.*물음에 답하시오',
    r'물음에 답하시오',            # 단독 등장하는 경우도 제거
    r'^\*\s',                      # * 각주
    r'^[-―]\s*.+[,，「].+[-―]?\s*$',  # - 작자 미상, 「...」 - 형태
    r'확인 사항',
    r'선택한 과목인지 확인',
    r'이어서.*문제가 제시',
    r'답안지의 해당란',
    r'이를 바탕으로.*초고이다',    # 작문 지문 도입부
    r'^\d+\.\s',                   # 숫자. 으로 시작하는 문항 번호
]


def _is_noise_underline(text: str) -> bool:
    """챕터 헤더, 각주 등 밑줄과 무관한 텍스트인지 판별"""
    for pat in _NOISE_UNDERLINE_PATTERNS:
        if re.search(pat, text.strip()):
            return True
    return False


def _is_noise_pdf_underline(text: str) -> bool:
    """PDF 추출 밑줄 중 단일 기호/숫자처럼 명백히 잡음인 것 판별"""
    t = text.strip()
    # 1~2글자 짧은 기호 (단일 원문자, 특수문자 등)
    if len(t) <= 2:
        return True
    # [A]~[E] 브래킷 마커 (수능 범위 표시, 밑줄 아님)
    if re.fullmatch(r'\[[A-E]\]', t):
        return True
    # 수능 시험지 폼 텍스트 (홀수형, 짝수형)
    if t in ('홀수형', '짝수형'):
        return True
    # 순수 기호/숫자로만 구성 (@ ! # 등)
    if re.fullmatch(r'[①-⑳ⓐ-ⓩ㉠-㉿\u2460-\u2473\d@#!%&*\s]+', t):
        return True
    return False


def _underline_confirmed(ju: str, pdf_underlines: list[str]) -> bool:
    """JSON의 <u> 내용이 PDF 밑줄 목록과 매칭되는지 퍼지 검사"""
    ju_core = ju.strip()
    if len(ju_core) < 2:
        return True

    # 공백/줄바꿈 정규화 (여러 줄 밑줄은 줄바꿈이 다를 수 있음)
    def _norm(s: str) -> str:
        return re.sub(r'\s+', ' ', s).strip()

    # 앞쪽 기호 문자(ⓐ~ⓩ, ①~⑳, ㉠~㉿) 제거한 버전도 비교
    ju_stripped = re.sub(r'^[①-⑳ⓐ-ⓩ㉠-㉿\u2460-\u2473]+\s*', '', ju_core).strip()
    candidates = list(dict.fromkeys([_norm(ju_core), _norm(ju_stripped)]))  # 중복 제거 순서 유지

    _PREFIX_RE = re.compile(r'^[①-⑳ⓐ-ⓩ㉠-㉿\u2460-\u2473]+\s*')

    for cand in candidates:
        if not cand:
            continue
        for pu in pdf_underlines:
            pu_norm = _norm(pu)
            # PDF 밑줄에도 prefix 제거 버전 생성 (ⓐ/ⓑ 등으로 시작하는 경우)
            pu_stripped = _norm(_PREFIX_RE.sub('', pu))
            pu_variants = list(dict.fromkeys([pu_norm, pu_stripped]))
            for pv in pu_variants:
                if not pv:
                    continue
                # 직접 부분 포함 검사 (양방향)
                if cand[:20] in pv or pv[:20] in cand:
                    return True
                # 긴 밑줄이 여러 조각으로 분할 추출된 경우 대응
                if len(cand) >= 5 and cand[:10] in pv:
                    return True
                if len(pv) >= 5 and pv[:10] in cand:
                    return True
                # 퍼지 유사도 — 긴 텍스트일수록 더 관대하게
                cmp_len = min(len(cand), len(pv), 50)
                if cmp_len >= 3:
                    threshold = 0.60 if cmp_len >= 20 else 0.70
                    ratio = difflib.SequenceMatcher(None, cand[:cmp_len], pv[:cmp_len]).ratio()
                    if ratio >= threshold:
                        return True
        # 여러 줄에 걸친 밑줄: 각 라인이 개별 pdf_underline과 매칭되는지 확인
        if '\n' in ju_core:
            lines = [l.strip() for l in ju_core.split('\n') if len(l.strip()) >= 4]
            matched = sum(1 for line in lines if any(
                _norm(line)[:10] in _norm(pu) or _norm(pu)[:10] in _norm(line)
                for pu in pdf_underlines
            ))
            if lines and matched >= len(lines) // 2 + 1:
                return True
    return False


def _clean_choice_text(text: str) -> str:
    """선택지 비교용: 앞쪽 원문자·번호, 뒷쪽 <img>/<보기>/마커 제거"""
    # 뒷쪽 이미지 태그 및 구조 마커 이후 내용 제거
    text = re.sub(r'\n{1,2}<img\b[^>]*>.*', '', text, flags=re.DOTALL)
    text = re.sub(r'\n{1,2}\[[A-E]:(?:START|END)\].*', '', text, flags=re.DOTALL)
    text = re.sub(r'\n{1,2}<보기>.*', '', text, flags=re.DOTALL)
    # 앞쪽 원문자 (①~⑤) 제거
    text = re.sub(r'^[①②③④⑤⑥⑦⑧⑨⑩]\s*', '', text.strip())
    return text.strip()


def _choices_trailing_junk(text: str) -> bool:
    """선택지에 <img> 또는 구조 마커 같은 후행 쓰레기가 있는지 확인"""
    return bool(re.search(r'\n{1,2}(<img\b|\[[A-E]:(?:START|END)\]|<보기>)', text))


def _img_to_b64(img) -> str:
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def _parse_json(text: str) -> dict:
    text = text.strip()
    # 마크다운 코드 블록 제거
    if "```" in text:
        m = re.search(r'```(?:json)?\s*([\s\S]*?)```', text)
        if m:
            text = m.group(1).strip()

    # 1차: 직접 파싱
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2차: 문자열 내 비이스케이프 제어문자 수정
    try:
        return json.loads(_fix_json_strings(text))
    except json.JSONDecodeError:
        pass

    # 3차: 빈 결과 반환
    return {"passages": [], "questions": []}


def _fix_json_strings(text: str) -> str:
    """JSON 문자열 안의 이스케이프되지 않은 개행·탭 등을 수정"""
    result: list[str] = []
    in_string = False
    escape = False
    for ch in text:
        if escape:
            result.append(ch)
            escape = False
        elif ch == '\\':
            result.append(ch)
            escape = True
        elif ch == '"':
            result.append(ch)
            in_string = not in_string
        elif in_string and ch == '\n':
            result.append('\\n')
        elif in_string and ch == '\r':
            result.append('\\r')
        elif in_string and ch == '\t':
            result.append('\\t')
        else:
            result.append(ch)
    return ''.join(result)


def _strip_markers(text: str) -> str:
    """구조 마커를 제거하여 순수 텍스트만 반환"""
    text = re.sub(r'\[[A-E]:(?:START|END)\]\n?', '', text)
    text = re.sub(r'</?u>', '', text)
    text = re.sub(r'<보기>', '', text)
    # <img> 태그와 [그림] 마커를 동일하게 제거 (비교 시 이미지 영역 제외)
    text = re.sub(r'<img\b[^>]*/?\s*>\n?', '', text)
    text = re.sub(r'\[그림\]\n?', '', text)
    return text


_COMPARISON_NORM_TABLE = str.maketrans({
    '『': '「', '』': '」',  # double angle brackets → single (OCR 혼용 보정)
    '\u2018': "'", '\u2019': "'",  # typographic single quotes
    '\u201c': '"', '\u201d': '"',  # typographic double quotes
    '\u00b7': '·',               # middle dot variants
    '\uff08': '(', '\uff09': ')', # fullwidth parentheses
    '～': '~',                    # 전각 물결표 → 반각
    '\uff5e': '~',               # FULLWIDTH TILDE → 반각
})


def _normalize_for_comparison(text: str) -> str:
    """유사도 비교 전용 정규화 — OCR 혼용 문자·공백·줄바꿈 차이를 통일.
    실제 내용은 변경하지 않으며, 유사도 계산에만 사용한다."""
    text = text.translate(_COMPARISON_NORM_TABLE)
    # 별표 + 따옴표 패턴: *' → ' (지문7 '*'' 오인식 보정)
    text = re.sub(r'\*(?=[\'\'\"\"\'\"『「])', '', text)
    # (가)/(나)/(다)/(라)/(마) 소단락 구분자 제거 — 지문10 등 구조적 차이 완화
    text = re.sub(r'(?<!\S)\((?:가|나|다|라|마)\)(?!\S)', '', text)
    # <img> 태그와 [그림] 마커를 동일한 플레이스홀더로 통일 → 유사도 계산 시 동등 취급
    text = re.sub(r'<img\b[^>]*/?\s*>', '[그림]', text)
    text = re.sub(r'\s+', ' ', text)  # 줄바꿈·연속 공백 → 단일 공백
    return text.strip()


def _extract_pdf_bracket_texts(pdf_path: Path) -> dict[int, dict[str, str]]:
    """
    PyMuPDF로 PDF 각 페이지에서 [A]~[E] 레이블 위치를 찾아 해당 Y 범위의 텍스트를 추출.
    Gemini가 inner_text를 비워서 반환한 경우의 대체 수단.

    Returns:
        {page_idx(0-based): {'A': text, 'B': text, ...}}
    """
    if not _FITZ_AVAILABLE:
        return {}
    result: dict[int, dict[str, str]] = {}
    try:
        doc = _fitz.open(str(pdf_path))
        for page_idx, page in enumerate(doc):
            bracket_y: dict[str, float] = {}
            page_dict = page.get_text("dict")
            for block in page_dict.get("blocks", []):
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        txt = span["text"].strip()
                        if re.fullmatch(r'\[[A-E]\]', txt):
                            lbl = txt[1]
                            y = span["bbox"][1]  # top y
                            if lbl not in bracket_y:
                                bracket_y[lbl] = y

            if not bracket_y:
                continue

            # 레이블을 Y 좌표 순으로 정렬 → 각 범위의 텍스트 추출
            page_h = page.rect.height
            sorted_labels = sorted(bracket_y.items(), key=lambda x: x[1])
            page_brackets: dict[str, str] = {}

            for i, (lbl, y_start) in enumerate(sorted_labels):
                y_end = sorted_labels[i + 1][1] if i + 1 < len(sorted_labels) else page_h
                # 여백(좌측 bracket 영역)을 제외한 본문 영역
                rect = _fitz.Rect(55, y_start - 2, page.rect.width - 15, y_end - 2)
                txt = page.get_text("text", clip=rect).strip()
                if txt and len(txt) >= 5:
                    page_brackets[lbl] = txt

            if page_brackets:
                result[page_idx] = page_brackets
        doc.close()
    except Exception:
        pass
    return result


def _extract_pdf_underlines(pdf_path: Path) -> dict[int, list[str]]:
    """
    PyMuPDF로 PDF 각 페이지에서 그래픽 밑줄(수평 선) 아래의 텍스트를 직접 추출.
    annotation이 아닌 path/drawing으로 그려진 밑줄 처리.

    Returns:
        {page_index(0-based): [밑줄텍스트, ...]}
    """
    if not _FITZ_AVAILABLE:
        return {}
    result: dict[int, list[str]] = {}
    try:
        doc = _fitz.open(str(pdf_path))
        for page_idx, page in enumerate(doc):
            texts: list[str] = []
            for d in page.get_drawings():
                w = d.get("width") or 1.0  # None 방어
                if w >= 1.0:          # 굵은 선 = 구분선, 제외
                    continue
                for item in d.get("items", []):
                    if item[0] != "l":  # line 타입만
                        continue
                    p1, p2 = item[1], item[2]
                    dy = abs(p2.y - p1.y)
                    dx = abs(p2.x - p1.x)
                    if dy >= 2 or dx < 20 or dx > 500:
                        continue
                    # 선 위 텍스트 영역 추출 (선 y 기준 -14pt ~ +2pt)
                    rect = _fitz.Rect(
                        min(p1.x, p2.x), p1.y - 14,
                        max(p1.x, p2.x), p1.y + 2,
                    )
                    txt = page.get_text("text", clip=rect).strip()
                    if (txt and len(txt) >= 3
                            and not _is_noise_pdf_underline(txt)
                            and not re.fullmatch(r'\d+\n\d+', txt)):  # 페이지번호 제외
                        texts.append(txt)
            # 중복 제거 (순서 유지)
            seen: set[str] = set()
            unique = [t for t in texts if t not in seen and not seen.add(t)]  # type: ignore[func-returns-value]
            if unique:
                result[page_idx] = unique
        doc.close()
    except Exception:
        pass
    return result


def _cache_path(pdf_path: Path) -> Path:
    """PDF별 Gemini 추출 캐시 파일 경로"""
    return pdf_path.parent / f".verify_cache_{pdf_path.stem}.json"


def _load_cache(pdf_path: Path) -> list[dict] | None:
    """캐시 파일 로드. 없거나 PDF보다 오래됐으면 None 반환."""
    cp = _cache_path(pdf_path)
    if not cp.exists():
        return None
    # PDF 수정 시각보다 캐시가 오래됐으면 무효
    if cp.stat().st_mtime < pdf_path.stat().st_mtime:
        return None
    try:
        return json.loads(cp.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cache(pdf_path: Path, extracts: list) -> None:
    """PageExtract 목록을 JSON으로 캐싱."""
    cp = _cache_path(pdf_path)
    try:
        data = [ex.model_dump() for ex in extracts]
        cp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _strip_passage_header(text: str) -> str:
    """
    수능 지문 앞의 헤더와 이미지·확인사항 등 Gemini가 추출하지 않는 부분 제거.
    Gemini는 순수 지문 본문만 추출하므로, 이를 제거해야 유사도가 정확해진다.
    """
    t = text.strip()
    # <img ...> 태그 제거
    t = re.sub(r'<img\b[^>]*>\s*', '', t)
    # 물결표 변형 통일 (~, ～, ∼ 모두 동일하게 처리)
    _TILDE_PAT = r'[~～∼\u223C\uFF5E]'
    # [XX～XX] 다음 글을 읽고 물음에 답하시오. / 다음은 ... 물음에 답하시오.
    t = re.sub(rf'^\[?\d+{_TILDE_PAT}\d+\]?\s*다음[^\n]*물음에 답하시오\.?\s*\n+', '', t, flags=re.MULTILINE)
    t = re.sub(r'^다음[^\n]*물음에 답하시오\.?\s*\n+', '', t, flags=re.MULTILINE)
    # [XX～XX] 으로 시작하는 모든 헤더 줄 제거 — 수능 지문 번호 범위 표시 헤더
    # 예: "[38～42] (가)는 '전통 문화 연구 동아리'...", "[12～17] 다음 글을 읽고..."
    _TILDE_PAT2 = r'[~～∼\u223C\uFF5E]'
    t = re.sub(rf'^\[?\d+{_TILDE_PAT2}\d+\]?[^\n]*\n+', '', t, flags=re.MULTILINE)
    # 다음은 ... 초고이다. / 다음은 ... 대화이다. 형태의 화법·작문 지문 도입부 (범위 표시 없는 경우)
    t = re.sub(r'^다음은[^\n]+(?:이다|이다\.)\s*\n+', '', t, flags=re.MULTILINE)
    # 확인 사항 / 선택과목 안내 등 페이지 하단 안내문
    t = re.sub(r'\*\s*확인 사항.*', '', t, flags=re.DOTALL)
    t = re.sub(r'◦\s*답안지의 해당란.*', '', t, flags=re.DOTALL)
    # (화법과 작문) / (언어와 매체) 과목 표시
    t = re.sub(r'^\((?:화법과 작문|언어와 매체)\)\s*\n+', '', t, flags=re.MULTILINE)
    return t.strip()


def _apply_text_patches(
    original: str, clean_orig: str, clean_pdf: str
) -> str:
    """
    구조 마커를 보존하면서 텍스트 오류만 교정.
    difflib opcodes 를 이용해 replace 구간을 찾아 원본 텍스트에 반영.
    """
    sm = difflib.SequenceMatcher(None, clean_orig, clean_pdf)
    result = original
    offset = 0  # 마커 길이 차이로 인한 오프셋

    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == "equal":
            continue
        old_frag = clean_orig[i1:i2]
        new_frag = clean_pdf[j1:j2]
        if not old_frag:
            continue
        # 원본 텍스트에서 해당 조각 위치 탐색
        search_start = max(0, i1 + offset - 5)
        idx = result.find(old_frag, search_start)
        if idx >= 0:
            # Gemini OCR 오류 패치 방지:
            # 1) @ 가 한국어/라틴 원문자를 대체하는 경우
            if '@' in new_frag and re.search(r'[ⓐ-ⓩ㉠-㉿\u2460-\u2473]', old_frag):
                continue
            # 2) ①~⑳(숫자 원문자)가 ㉠~㉿(한국어/가나 원문자)를 대체하는 경우
            if (re.search(r'[①-⑳]', new_frag)
                    and re.search(r'[㉠-㉿]', old_frag)
                    and not re.search(r'[①-⑳]', old_frag)):
                continue
            # 3) 『』↔「」 교환 — Gemini OCR 혼용이므로 JSON 원본 유지
            if (re.search(r'[『』]', old_frag) and re.search(r'[「」]', new_frag)
                    or re.search(r'[「」]', old_frag) and re.search(r'[『』]', new_frag)):
                continue
            # 4) <img> 태그 영역 보호 — 이미지 태그 앞뒤 10자 범위는 패치 건너뜀
            #    Gemini는 [그림]으로 추출하므로 이 영역 텍스트를 덮어쓰면 태그 손상
            img_zone = re.search(r'<img\b[^>]*/?\s*>', result[max(0, idx-10):idx+len(old_frag)+10])
            if img_zone:
                continue
            result = result[:idx] + new_frag + result[idx + len(old_frag):]
            offset += len(new_frag) - len(old_frag)

    return result


def _diff_summary(a: str, b: str, max_len: int = 80) -> str:
    """두 텍스트의 차이를 짧은 문자열로 요약"""
    sm = difflib.SequenceMatcher(None, a, b)
    diffs = []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == "equal":
            continue
        old = a[i1:i2]
        new = b[j1:j2]
        diffs.append(f"'{old}' → '{new}'" if old else f"삽입 '{new}'")
        if sum(len(d) for d in diffs) > max_len:
            diffs.append("…")
            break
    return " | ".join(diffs) if diffs else "(동일)"


def _norm_qr(qr: str) -> str:
    """question_range 비교용: 물결표 변형 통일 (~, ∼, ～ 모두 동일하게 처리)"""
    return re.sub(r'[~∼\u223C\uFF5E～]', '～', qr).strip()


def _fix_duplicate_bracket_labels(content: str) -> tuple[str, str]:
    """JSON 지문 내 중복된 bracket 레이블을 순서 기반으로 재할당.
    예: [B:START]...[B:END] + [B:START]...[C:END] → [A:START]...[A:END] + [B:START]...[C:END]

    Returns:
        (fixed_content, description)   description이 빈 문자열이면 변경 없음.
    """
    label_order = ['A', 'B', 'C', 'D', 'E']

    # START 마커를 출현 순서대로 수집
    starts = list(re.finditer(r'\[([A-E]):START\]', content))
    labels_found = [m.group(1) for m in starts]

    if len(labels_found) == len(set(labels_found)):
        return content, ''  # 중복 없음

    # 순서대로 A, B, C... 재할당 — 기존과 다른 경우만 교체
    result = content
    fixes: list[str] = []
    offset = 0

    for i, (m, old_label) in enumerate(zip(starts, labels_found)):
        if i >= len(label_order):
            break
        new_label = label_order[i]
        if new_label == old_label:
            continue  # 변경 불필요

        # START 태그 교체
        old_s = f"[{old_label}:START]"
        new_s = f"[{new_label}:START]"
        pos_s = m.start() + offset
        result = result[:pos_s] + new_s + result[pos_s + len(old_s):]
        offset += len(new_s) - len(old_s)

        # 이 START 이후 첫 번째 [old_label:END] → [new_label:END] 교체
        old_e = f"[{old_label}:END]"
        new_e = f"[{new_label}:END]"
        end_search = pos_s + len(new_s)
        pos_e = result.find(old_e, end_search)
        if pos_e >= 0:
            result = result[:pos_e] + new_e + result[pos_e + len(old_e):]
            offset += len(new_e) - len(old_e)

        fixes.append(f"[{old_label}]→[{new_label}]")

    return result, ', '.join(fixes)


def _best_match_passage(
    json_text: str,
    pdf_passages: list[PagePassageExtract],
    question_range: str = "",
) -> PagePassageExtract | None:
    if not pdf_passages:
        return None

    # question_range 로 먼저 매칭 (물결표 변형 정규화 후 비교)
    if question_range:
        norm_qr = _norm_qr(question_range)
        for pp in pdf_passages:
            if _norm_qr(pp.question_range) == norm_qr:
                return pp

    # 텍스트 유사도로 매칭
    clean_json = _strip_markers(json_text)
    best, best_ratio = None, 0.0
    for pp in pdf_passages:
        ratio = difflib.SequenceMatcher(
            None, clean_json[:500], pp.text[:500]
        ).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best = pp

    return best if best_ratio > 0.3 else None


def _log(verbose: bool, msg: str, end: str = "\n") -> None:
    if verbose:
        print(msg, end=end, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# 편의 함수
# ─────────────────────────────────────────────────────────────────────────────

def run(
    pdf_path: str | Path,
    segments_json: str | dict[str, Any],
    api_key: str | None = None,
    dpi: int = 150,
    verbose: bool = True,
) -> CorrectionResult:
    """
    메인 진입점.

    Args:
        pdf_path:      PDF 경로
        segments_json: segments dict 또는 JSON 문자열
        api_key:       Gemini API 키 (없으면 GEMINI_API_KEY 환경변수)
        dpi:           이미지 해상도 (높을수록 정확, 느림)
        verbose:       진행 상황 출력

    Returns:
        CorrectionResult (수정된 segments + 교정 내역)
    """
    if isinstance(segments_json, str):
        segments_json = json.loads(segments_json)

    key = api_key or os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise ValueError(
            "Gemini API 키 필요: api_key 인자 또는 GEMINI_API_KEY 환경변수 설정"
        )

    return VerifyAgent(api_key=key).run(pdf_path, segments_json, dpi=dpi, verbose=verbose)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Windows cp949 콘솔에서 한글·이모지 출력 깨짐 방지
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if len(sys.argv) < 3:
        print("사용법: python verify_agent.py <pdf> <segments.json> [출력.json]")
        sys.exit(1)

    result = run(sys.argv[1], Path(sys.argv[2]).read_text(encoding="utf-8"))

    print(f"\n{'─'*60}")
    if result.fixed:
        print(f"✅ 자동 교정 {len(result.fixed)}건")
        for c in result.fixed:
            print(f"   [{c.location}] {c.field}: {c.message}")
    if result.warnings:
        print(f"⚠  경고 {len(result.warnings)}건 (수동 확인 필요)")
        for c in result.warnings:
            print(f"   [{c.location}] {c.field}: {c.message}")

    out = sys.argv[3] if len(sys.argv) > 3 else sys.argv[2].replace(".json", "_corrected.json")
    Path(out).write_text(
        json.dumps(result.segments.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n💾 저장: {out}")
