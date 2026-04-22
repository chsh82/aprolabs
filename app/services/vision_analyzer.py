"""
Stage 1b: Gemini Vision 구조 분석
- 페이지 이미지에서 문항 번호, [A]/[B] 범위, 밑줄 어구를 JSON으로 추출
- 텍스트 추출(Stage 1)과 독립적으로 실행하여 구조 정보 보완
"""
import os
import re
import json


STRUCTURE_PROMPT = """이 수능 문제지 페이지를 분석하여 구조 정보를 JSON으로 반환하세요.

출력 형식 (JSON만, 다른 설명 없이):
{
  "questions": [
    {"number": 2, "first_words": "다음 중 가장 적절한"}
  ],
  "labeled_ranges": [
    {"label": "A", "start_words": "초인지는 글을 읽기", "end_words": "방법을 사용할 수 있다"}
  ],
  "underlined_phrases": ["특정 단어", "다른 밑줄 어구"]
}

규칙:
- questions: 이 페이지에서 보이는 문항 번호(숫자)와 발문 첫 4~6단어 (문항번호 다음 텍스트)

- labeled_ranges: 아래 두 종류를 모두 포함하라:
  ① 명시적 레이블: [A], [B] 등 세로 괄호({, |, [ 형태) 또는 세로선으로 표시된 범위
  ② 시각적 박스: 테두리·선·음영으로 주변 텍스트와 시각적으로 구분된 모든 영역
     - <보기> 라벨 유무와 무관 — 박스·틀로 시각적 구분이 있으면 무조건 포함
     - 대상 유형: <보기>, 조건, 메모, 계획표, 초고, 학습활동지, 대화 상자, 도표, 표, 기사·인터뷰 발췌 등
     - 이미지·그림만 있는 박스도 포함 (start_words/end_words는 빈 문자열 허용)
  레이블:
  - ①는 페이지에 적힌 레이블 그대로 (A, B, C...)
  - ②는 ①에서 사용되지 않은 알파벳으로 순서대로 부여
  - start_words: 박스 내 첫 텍스트 4단어
  - end_words: 박스 내 마지막 텍스트 4단어

- underlined_phrases: 밑줄이 그어진 어구 (정확히, 있는 그대로). 없으면 []
- 해당 요소가 없으면 빈 배열 []
- JSON 외 다른 텍스트 없이 JSON만 출력

[원문자 구분 유의]
- ㉠㉡㉢㉣㉤ (ㄱ계열): <보기> 항목 앞 라벨
- ㉮㉯㉰㉱㉲ (가계열): 지문 본문 구분 기호
- ⓐⓑⓒⓓⓔⓕⓖ (영문 소문자 원문자): 영어 지문/어휘 문제 항목 기호
- ①②③④⑤: 선택지 전용 — 위 세 계열과 혼동 금지"""


def analyze_page_structure(image_path: str, job_id: str = None) -> dict:
    """페이지 이미지 한 장을 Gemini Vision으로 분석"""
    empty = {"questions": [], "labeled_ranges": [], "underlined_phrases": []}
    try:
        from google import genai
        from PIL import Image

        client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))
        img = Image.open(image_path)

        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[STRUCTURE_PROMPT, img],
        )

        text = response.text.strip()
        # 마크다운 코드블록 제거
        text = re.sub(r'^```[a-z]*\n?', '', text, flags=re.MULTILINE)
        text = re.sub(r'\n?```$', '', text, flags=re.MULTILINE)
        text = text.strip()

        result = json.loads(text)

        usage = getattr(response, "usage_metadata", None)
        if usage and job_id:
            _log_usage(job_id,
                       getattr(usage, "prompt_token_count", 0),
                       getattr(usage, "candidates_token_count", 0))

        return {
            "questions": result.get("questions", []),
            "labeled_ranges": result.get("labeled_ranges", []),
            "underlined_phrases": result.get("underlined_phrases", []),
        }
    except Exception:
        return empty


def analyze_all_pages(image_paths: list, job_id: str = None) -> dict:
    """
    모든 페이지 구조 분석 후 병합.
    반환: {questions, labeled_ranges, underlined_phrases}
    """
    all_questions = []
    all_ranges = []
    all_underlines = []
    seen_q_nums = set()

    total = len(image_paths)
    for i, path in enumerate(image_paths):
        result = analyze_page_structure(path, job_id)

        for q in result.get("questions", []):
            n = q.get("number")
            if n and n not in seen_q_nums:
                seen_q_nums.add(n)
                all_questions.append(q)

        all_ranges.extend(result.get("labeled_ranges", []))
        all_underlines.extend(result.get("underlined_phrases", []))

        if job_id:
            _update_progress(job_id, i + 1, total)

    return {
        "questions": sorted(all_questions, key=lambda x: x.get("number", 0)),
        "labeled_ranges": all_ranges,
        "underlined_phrases": list(dict.fromkeys(all_underlines)),  # 순서 보존 중복 제거
    }


def _flexible_pattern(phrase: str, max_chars: int = 25) -> str:
    """공백/줄바꿈을 \\s+ 로 치환하여 유연한 매칭 패턴 생성"""
    words = phrase.strip()[:max_chars].split()
    return r'\s+'.join(re.escape(w) for w in words if w)


def apply_structure_to_text(raw_text: str, structure: dict) -> str:
    """
    Gemini Vision이 탐지한 구조 정보를 텍스트에 적용.
    - underlined_phrases → <u> 태그 추가 (이미 있는 것은 스킵)
    - labeled_ranges → [A:START]...[A:END] 마커 추가 (이미 있는 것은 스킵)
    """
    text = raw_text

    # 1. 밑줄 어구 적용 (PyMuPDF가 못 잡은 것만)
    for phrase in structure.get("underlined_phrases", []):
        phrase = phrase.strip()
        if not phrase or len(phrase) < 2:
            continue
        # 이미 <u> 태그 안에 있는지 확인
        if re.search(r'<u>[^<]*' + re.escape(phrase[:10]) + r'[^<]*</u>', text):
            continue
        try:
            pat = _flexible_pattern(phrase)
            m = re.search(pat, text)
            if m:
                matched_str = m.group(0)
                text = text[:m.start()] + f'<u>{matched_str}</u>' + text[m.end():]
        except re.error:
            continue

    # 2. [A]/[B] 범위 마커 적용
    # 이미 PyMuPDF geometric 탐지로 적용된 레이블 수집
    used_labels = set(re.findall(r'\[([A-Z]):START\]', text))

    def _next_free_label(used: set) -> str | None:
        for c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            if c not in used:
                return c
        return None

    for rng in structure.get("labeled_ranges", []):
        proposed = (rng.get("label") or "A").strip().upper()[:1]
        if not proposed:
            proposed = "A"

        if proposed in used_labels:
            # 충돌: 다음 빈 레이블로 재할당
            actual_label = _next_free_label(used_labels)
            if actual_label is None:
                continue
        else:
            actual_label = proposed

        start_words = (rng.get("start_words") or "").strip()
        end_words = (rng.get("end_words") or "").strip()
        if not start_words or not end_words:
            continue

        try:
            start_pat = _flexible_pattern(start_words, max_chars=20)
            end_pat = _flexible_pattern(end_words[-20:], max_chars=20)
            start_m = re.search(start_pat, text)
            end_m = re.search(end_pat, text)

            if start_m and end_m and start_m.start() < end_m.end():
                sp, ep = start_m.start(), end_m.end()
                text = (text[:sp]
                        + f"[{actual_label}:START]\n"
                        + text[sp:ep]
                        + f"\n[{actual_label}:END]"
                        + text[ep:])
                used_labels.add(actual_label)
        except re.error:
            continue

    return text


def _log_usage(job_id, input_tokens, output_tokens):
    try:
        from app.database import SessionLocal
        from app.models.api_usage import ApiUsage, calc_cost
        db = SessionLocal()
        db.add(ApiUsage(
            service="gemini", model="gemini-2.0-flash", purpose="structure_analysis",
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
            job.status = f"vision ({current}/{total})"
            db.commit()
        db.close()
    except Exception:
        pass
