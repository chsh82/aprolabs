"""
answer_pipeline.py — 수능 국어 정답/해설 PDF 파싱 파이프라인

사용법:
  python3 answer_pipeline.py <PDF_PATH> [--exam-type 수능|모의평가|학력평가]
                             [--year 2026] [--subject 국어] [--dry-run] [--save]

입력:  정답해설 PDF (정답표만 있는 1페이지짜리도 처리)
출력:  JSON (stdout) + DB 저장 (--save 시)
"""
import os, re, sys, json, argparse, sqlite3
from dataclasses import dataclass, field, asdict
from typing import Optional

try:
    import fitz        # PyMuPDF
except ImportError:
    print("pip install pymupdf 필요", file=sys.stderr)
    sys.exit(1)

DB_PATH = os.path.join(os.path.dirname(__file__), "aprolabs.db")

# ─── 원문자 변환 ────────────────────────────────────────────
CIRCLE_MAP = {'①': 1, '②': 2, '③': 3, '④': 4, '⑤': 5,
              '⑥': 6, '⑦': 7, '⑧': 8, '⑨': 9, '⑩': 10}
CIRCLE_RE  = re.compile(r'[①②③④⑤⑥⑦⑧⑨⑩]')


def _circle(c: str) -> int:
    return CIRCLE_MAP.get(c, 0)


def _circle_str(c: str) -> str:
    """'①' → '1'"""
    return str(_circle(c)) if c in CIRCLE_MAP else c


# ─── 데이터 구조 ─────────────────────────────────────────────
@dataclass
class AnswerItem:
    number: int
    answer: int          # 1~5
    score:  int          # 배점
    subject: str = ""    # 공통 | 화법과작문 | 언어와매체


@dataclass
class PassageExplanation:
    question_range: str          # "1~3"
    range_start:    int
    range_end:      int
    domain:         str          # "독서" | "문학" | "화법" | "언어" | "매체"
    sub_domain:     str = ""     # "독서 이론" | "주제 통합" | "과학기술" | ...
    title:          str = ""     # '제목' 부분
    passage_summary: str = ""    # 지문해설 전문
    topic:          str = ""     # [주제] 뒤 텍스트


@dataclass
class QuestionExplanation:
    number:          int
    question_type:   str = ""    # "세부 내용 파악" 등
    correct_answer:  int = 0     # 1~5
    explanation:     str = ""    # 정답해설 전문
    wrong_answers:   dict = field(default_factory=dict)  # {1: "...", 2: "..."}
    score:           int = 0


@dataclass
class ParseResult:
    source_year:   int
    exam_type:     str
    subject:       str
    answer_table:  list[AnswerItem]        = field(default_factory=list)
    passages:      list[PassageExplanation] = field(default_factory=list)
    questions:     list[QuestionExplanation] = field(default_factory=list)
    raw_text:      str = ""


# ─── 1. PDF 텍스트 추출 ──────────────────────────────────────
def extract_text(pdf_path: str) -> tuple[str, int]:
    """전체 텍스트와 페이지 수 반환."""
    doc = fitz.open(pdf_path)
    pages = [doc[i].get_text() for i in range(len(doc))]
    return "\n".join(pages), len(doc)


# ─── 2. 정답표 파싱 ──────────────────────────────────────────
# 형식 A (2026~): ■ [공통: 독서·문학]\n01. ③\n...
# 형식 B (2025~): 1\n②\n2\n③\n... (■ 없음, 배점 없음)
# 형식 C (단답형): 공통 과목 헤더 + 4열 표 텍스트 (1페이지)
_ANS_SECTION_RE = re.compile(
    r'■\s*\[(?:공통|선택)\s*:\s*([^\]]+)\]'
)
_ANS_ROW_RE = re.compile(r'(\d{1,2})\.\s+([①②③④⑤])')
_ANS_ALTROW_RE = re.compile(r'(?:^|\n)(\d{1,2})\n([①②③④⑤])(?=\n|$)')


def _label_to_subj(label: str) -> str:
    if '화법' in label: return '화법과작문'
    if '언어' in label: return '언어와매체'
    return '공통'


def parse_answer_table(text: str) -> list[AnswerItem]:
    """
    공통/선택과목 정답표 파싱.
    형식 A (■ 마커): ■ [공통: 독서·문학]\n01. ③\n...
    형식 B (줄바꿈): 1\n②\n2\n③\n...  (■ 없음)
    """
    items = []

    # ── 형식 A: ■ 섹션 헤더 ─────────────────────────────────
    section_matches = list(_ANS_SECTION_RE.finditer(text))
    if section_matches:
        for i, sm in enumerate(section_matches):
            raw_label = sm.group(1).strip()
            subj = _label_to_subj(raw_label)
            block_start = sm.end()
            block_end   = section_matches[i + 1].start() if i + 1 < len(section_matches) else len(text)
            block       = text[block_start:block_end]
            for rm in _ANS_ROW_RE.finditer(block):
                num = int(rm.group(1))
                ans = _circle(rm.group(2))
                if 1 <= num <= 45:
                    items.append(AnswerItem(number=num, answer=ans, score=0, subject=subj))
        return sorted(items, key=lambda x: (x.subject, x.number))

    # ── 형식 B: 교대 줄 (번호\n원문자) ─────────────────────
    # 섹션 경계: '화법과 작문' / '언어와 매체' 텍스트로 판별
    sec_positions: dict[str, int] = {}
    for label, pat in [('화법과작문', r'화법과\s*작문'), ('언어와매체', r'언어와\s*매체')]:
        m = re.search(pat, text)
        if m:
            sec_positions[label] = m.start()

    for m in _ANS_ALTROW_RE.finditer(text):
        num = int(m.group(1))
        ans = _circle(m.group(2))
        if not (1 <= num <= 45):
            continue
        pos = m.start()
        subj = '공통'
        best_dist = float('inf')
        for sec, spos in sec_positions.items():
            if spos <= pos and (pos - spos) < best_dist:
                best_dist = pos - spos
                subj = sec
        items.append(AnswerItem(number=num, answer=ans, score=0, subject=subj))

    # 중복 제거 (같은 번호·과목)
    seen: set = set()
    unique = []
    for it in items:
        k = (it.number, it.subject)
        if k not in seen:
            seen.add(k)
            unique.append(it)
    return sorted(unique, key=lambda x: (x.subject, x.number))


# ─── 3. 지문 범위 블록 파싱 ──────────────────────────────────
_RANGE_RE = re.compile(
    r'\[(\d{1,2})\s*[~～]\s*(\d{1,2})\]\s+'
    r'(독서|문학|화법|작문|언어|매체|화법과 작문|언어와 매체)'
    r'[^\n]*\n'
    r'\[(\d{1,2})\s*[~～]\s*(\d{1,2})\]\s+([^\n]{5,120})\n',
    re.MULTILINE,
)

_DOMAIN_LINE_RE = re.compile(
    r'\[(\d{1,2})\s*[~～]\s*(\d{1,2})\]\s+'
    r'(?:(?:독서|문학|화법|작문|언어|매체)[^\n]*)?\n'
    r'?\[(\d{1,2})[~～](\d{1,2})\]\s+([^\n]{5,150})',
    re.MULTILINE,
)

_PASSAGE_SUMMARY_RE = re.compile(
    r'지문해설\s*:\s*(.*?)(?=\[주제\]|\n\d{1,2}\.|$)',
    re.DOTALL,
)

_TOPIC_RE  = re.compile(r'\[주제\]\s*([^\n]{5,200})')
_TITLE_RE  = re.compile(r"['\u2018\u2019](.{3,80})['\u2019]")


def _parse_sub_domain_and_title(detail_line: str) -> tuple[str, str, str]:
    """
    '[4~9] 주제 통합, \'(가) ...\'' 형식에서 (domain, sub_domain, title) 추출
    """
    domain = ""
    for d in ("독서 이론", "주제 통합", "과학기술", "인문", "예술", "기술",
              "사회", "고전 산문", "현대소설", "고전시가", "현대시",
              "복합", "화법", "언어", "매체"):
        if d in detail_line:
            domain = d
            break

    title_m = _TITLE_RE.search(detail_line)
    title   = title_m.group(1) if title_m else ""

    return domain, title


def parse_passages(text: str) -> list[PassageExplanation]:
    """지문 블록을 파싱하여 PassageExplanation 리스트 반환."""
    passages = []

    # 지문 헤더 패턴: [N~M] 도메인\n[N~M] 세부정보, '제목'
    # 두 줄이 같은 범위를 가리키는 구조
    header_re = re.compile(
        r'\[(\d{1,2})\s*[~～]\s*(\d{1,2})\]\s*(독서|문학|화법|작문|언어|매체)[^\n]*\n'
        r'\[(\d{1,2})\s*[~～]\s*(\d{1,2})\]\s*([^\n]{5,200})',
        re.MULTILINE,
    )

    headers = list(header_re.finditer(text))
    if not headers:
        # 단일 줄 형식 폴백
        headers = list(re.finditer(
            r'\[(\d{1,2})\s*[~～]\s*(\d{1,2})\]\s*(독서|문학|화법)[^\n]+\n',
            text, re.MULTILINE,
        ))

    for i, m in enumerate(headers):
        # 범위 끝은 다음 헤더 또는 EOF
        block_start = m.end()
        block_end   = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        block       = text[block_start:block_end]

        # 그룹 수에 따라 파싱
        try:
            start = int(m.group(1))
            end   = int(m.group(2))
            top_domain = m.group(3).strip()
            detail_line = m.group(6).strip() if m.lastindex >= 6 else ""
        except Exception:
            continue

        sub_domain, title = _parse_sub_domain_and_title(detail_line)
        if not sub_domain:
            sub_domain = top_domain

        # 지문해설 추출
        summary_m = _PASSAGE_SUMMARY_RE.search(block)
        summary   = summary_m.group(1).strip() if summary_m else ""

        # 주제 추출
        topic_m   = _TOPIC_RE.search(block)
        topic     = topic_m.group(1).strip() if topic_m else ""

        passages.append(PassageExplanation(
            question_range = f"{start}~{end}",
            range_start    = start,
            range_end      = end,
            domain         = top_domain,
            sub_domain     = sub_domain,
            title          = title,
            passage_summary = summary,
            topic          = topic,
        ))

    return passages


# ─── 4. 문항별 해설 파싱 ─────────────────────────────────────
_Q_HEADER_RE = re.compile(
    r'\n(\d{1,2})\.\s+([^\n]{2,60})\n',
)
_CORRECT_ANS_RE  = re.compile(r'정답\s+([①②③④⑤])')
_EXPLANATION_RE  = re.compile(
    r'정답해설\s*:\s*(.*?)(?=정답\s+[①②③④⑤]|\Z)',
    re.DOTALL,
)
_WRONG_BLOCK_RE  = re.compile(
    r'\[오답피하기\]|\[오답풀이\]'
)
_WRONG_ITEM_RE   = re.compile(
    r'([①②③④⑤])\s+([^①②③④⑤\n]{10,})',
)
# 선택과목 섹션 구분 (■ [선택: 화법과 작문] / ■ [선택: 언어와 매체])
_SELECT_SECTION_RE = re.compile(
    r'■\s*\[선택\s*:\s*([^\]]+)\]'
)
_SUBJ_NORM = {
    '화법과작문': '화법과작문', '화법과 작문': '화법과작문',
    '언어와매체': '언어와매체', '언어와 매체': '언어와매체',
}


def _subject_to_select(subject: str) -> str:
    """paper subject 문자열 → 선택과목 키 ('화법과작문'|'언어와매체'|'')"""
    if '화작' in subject or '화법' in subject: return '화법과작문'
    if '언매' in subject or '언어' in subject: return '언어와매체'
    return ''


def parse_questions(text: str) -> list[QuestionExplanation]:
    """
    문항별 해설 파싱.
    전체 텍스트를 파싱하며 동일 번호 중복이 발생할 수 있음.
    중복 제거는 save_to_db() 단계에서 정답표 기준으로 처리.
    """
    questions = []
    q_headers = list(_Q_HEADER_RE.finditer(text))
    for i, m in enumerate(q_headers):
        num       = int(m.group(1))
        q_type    = m.group(2).strip()
        block_end = q_headers[i + 1].start() if i + 1 < len(q_headers) else len(text)
        block     = text[m.end():block_end]

        ans_m   = _CORRECT_ANS_RE.search(block)
        correct = _circle(ans_m.group(1)) if ans_m else 0

        exp_m = _EXPLANATION_RE.search(block)
        expl  = exp_m.group(1).strip() if exp_m else ""

        wrong_answers: dict[int, str] = {}
        wrong_split = _WRONG_BLOCK_RE.split(block)
        if len(wrong_split) > 1:
            for wm in _WRONG_ITEM_RE.finditer(wrong_split[1]):
                wrong_answers[_circle(wm.group(1))] = wm.group(2).strip()

        questions.append(QuestionExplanation(
            number=num, question_type=q_type,
            correct_answer=correct, explanation=expl,
            wrong_answers=wrong_answers,
        ))
    return questions


def _dedup_questions(
    questions: list[QuestionExplanation],
    ans_map: dict[int, int],
) -> list[QuestionExplanation]:
    """
    35~45번 중복 제거: 정답표 답과 일치하는 것 우선 선택.
    일치 없으면 마지막 파싱 항목(선택과목 섹션이 뒤에 위치).
    """
    from collections import defaultdict
    by_num: dict[int, list[QuestionExplanation]] = defaultdict(list)
    for q in questions:
        by_num[q.number].append(q)

    result = []
    for num in sorted(by_num.keys()):
        qs = by_num[num]
        if len(qs) == 1:
            result.append(qs[0])
            continue
        # 정답표와 일치하는 것 우선
        target_ans = ans_map.get(num)
        if target_ans:
            matched = [q for q in qs if q.correct_answer == target_ans]
            if matched:
                result.append(matched[0])
                continue
        # 폴백: 마지막 항목 (선택과목 섹션이 PDF 뒤쪽에 위치)
        result.append(qs[-1])
    return result


# ─── 5. DB 스키마 생성 ──────────────────────────────────────
SCHEMA_SQL = """
-- 정답표
CREATE TABLE IF NOT EXISTS answer_explanations (
    id              TEXT PRIMARY KEY,
    paper_code      TEXT NOT NULL,     -- "{year}-{exam_type}-{subject}"
    source_year     INTEGER,
    exam_type       TEXT,
    subject         TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 지문 해설
CREATE TABLE IF NOT EXISTS passage_explanations (
    id              TEXT PRIMARY KEY,
    paper_code      TEXT NOT NULL,
    question_range  TEXT,              -- "1~3"
    range_start     INTEGER,
    range_end       INTEGER,
    domain          TEXT,              -- 독서|문학|화법|언어|매체
    sub_domain      TEXT,
    title           TEXT,
    passage_summary TEXT,
    topic           TEXT
);

-- 문항별 해설
CREATE TABLE IF NOT EXISTS question_explanations (
    id              TEXT PRIMARY KEY,
    paper_code      TEXT NOT NULL,
    question_number INTEGER,
    question_type   TEXT,
    correct_answer  INTEGER,
    score           INTEGER,
    explanation     TEXT,
    wrong_answers   TEXT,              -- JSON
    question_id     TEXT               -- questions 테이블 FK (매칭 후 설정)
);
"""


def ensure_schema(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    conn.close()


# ─── 6. DB 저장 ─────────────────────────────────────────────
def save_to_db(result: ParseResult, db_path: str = DB_PATH):
    import uuid
    ensure_schema(db_path)
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    paper_code = f"{result.source_year}-{result.exam_type}-{result.subject}"

    # 기존 데이터 삭제 (재실행 안전)
    cur.execute("DELETE FROM passage_explanations  WHERE paper_code = ?", (paper_code,))
    cur.execute("DELETE FROM question_explanations WHERE paper_code = ?", (paper_code,))

    # 지문 해설 저장
    for p in result.passages:
        cur.execute("""
            INSERT INTO passage_explanations
              (id, paper_code, question_range, range_start, range_end,
               domain, sub_domain, title, passage_summary, topic)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            str(uuid.uuid4()), paper_code,
            p.question_range, p.range_start, p.range_end,
            p.domain, p.sub_domain, p.title, p.passage_summary, p.topic,
        ))

    # 정답표에서 배점 + 정답 추출
    score_map: dict[int, int] = {a.number: a.score for a in result.answer_table}

    # correct_answer 보완용 정답표 맵 (선택과목 구분)
    select_subj = _subject_to_select(result.subject)
    ans_map: dict[int, int] = {}
    for a in result.answer_table:
        if a.number <= 34 and a.subject == '공통':
            ans_map[a.number] = a.answer
        elif a.number >= 35:
            if select_subj and a.subject == select_subj:
                ans_map[a.number] = a.answer
            elif not select_subj:
                ans_map.setdefault(a.number, a.answer)

    # 기존 questions 테이블에서 question_id 매핑
    cur.execute(
        "SELECT id, question_number FROM questions WHERE paper_code LIKE ?",
        (f"{result.source_year}%",)
    )
    q_id_map: dict[int, str] = {row[1]: row[0] for row in cur.fetchall()}

    # 문항 해설 저장 (중복 제거 후)
    deduped = _dedup_questions(result.questions, ans_map)
    for q in deduped:
        # 수정 2: correct_answer=0이면 정답표에서 보완
        ca = q.correct_answer or ans_map.get(q.number, 0)
        cur.execute("""
            INSERT INTO question_explanations
              (id, paper_code, question_number, question_type,
               correct_answer, score, explanation, wrong_answers, question_id)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            str(uuid.uuid4()), paper_code,
            q.number, q.question_type,
            ca, score_map.get(q.number, 0),
            q.explanation,
            json.dumps(q.wrong_answers, ensure_ascii=False),
            q_id_map.get(q.number),
        ))

    conn.commit()
    conn.close()
    return paper_code


# ─── 7. 메인 ─────────────────────────────────────────────────
def parse(pdf_path: str, source_year: int = 0,
          exam_type: str = "수능", subject: str = "국어") -> ParseResult:
    """전체 파이프라인 실행."""
    text, n_pages = extract_text(pdf_path)

    result = ParseResult(
        source_year = source_year,
        exam_type   = exam_type,
        subject     = subject,
        raw_text    = text,
    )

    # 정답표 파싱
    result.answer_table = parse_answer_table(text)

    # 해설 페이지가 있는 경우만 파싱 (1페이지 = 정답표만)
    if n_pages > 1:
        result.passages  = parse_passages(text)
        result.questions = parse_questions(text)

    return result


def main():
    ap = argparse.ArgumentParser(description="수능 국어 정답/해설 PDF 파싱")
    ap.add_argument("pdf",           help="정답해설 PDF 경로")
    ap.add_argument("--year",        type=int, default=0, help="학년도")
    ap.add_argument("--exam-type",   default="수능", help="수능|6월모의평가|9월모의평가|학력평가")
    ap.add_argument("--subject",     default="국어")
    ap.add_argument("--dry-run",     action="store_true", help="DB 저장 안 함")
    ap.add_argument("--save",        action="store_true", help="DB 저장")
    ap.add_argument("--json",        action="store_true", help="JSON 출력")
    args = ap.parse_args()

    if not os.path.exists(args.pdf):
        print(f"❌ 파일 없음: {args.pdf}", file=sys.stderr)
        sys.exit(1)

    print(f"📄 파싱 중: {args.pdf}")
    result = parse(args.pdf, args.year, args.exam_type, args.subject)

    print(f"\n=== 파싱 결과 ===")
    print(f"  정답표:    {len(result.answer_table)}건")
    print(f"  지문 해설: {len(result.passages)}블록")
    print(f"  문항 해설: {len(result.questions)}건")

    if result.answer_table:
        print("\n  [정답표 샘플 — 처음 5건]")
        for a in result.answer_table[:5]:
            print(f"    {a.number:2d}번  정답={a.answer}  배점={a.score}  과목={a.subject}")

    if result.passages:
        print("\n  [지문 해설 블록]")
        for p in result.passages:
            print(f"    [{p.question_range}] {p.domain} / {p.sub_domain} / '{p.title[:30]}'")
            if p.topic:
                print(f"      주제: {p.topic[:70]}")

    if result.questions:
        print("\n  [문항 해설 샘플 — 처음 3건]")
        for q in result.questions[:3]:
            print(f"    {q.number}번  정답={q.correct_answer}  유형={q.question_type}")
            print(f"      해설: {q.explanation[:80]}...")
            if q.wrong_answers:
                print(f"      오답피하기: {len(q.wrong_answers)}항목")

    if args.json:
        d = asdict(result)
        d.pop("raw_text", None)
        print("\n" + json.dumps(d, ensure_ascii=False, indent=2))

    if args.save and not args.dry_run:
        paper_code = save_to_db(result)
        print(f"\n✅ DB 저장 완료: paper_code={paper_code}")
    elif not args.save:
        print("\n(--save 옵션으로 DB에 저장 가능)")


if __name__ == "__main__":
    main()
