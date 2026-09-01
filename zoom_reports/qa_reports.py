"""학부모 리포트 초안(report.status='draft')의 사실관계·금칙표현 검수 배치.

CLAUDE.md 개인정보 요구사항 그대로 - 이 배치도 LLM을 호출하므로 다른 학생
실명을 프롬프트에 넣기 전에 코드로 마스킹한다. 마스킹 로직은 새로 만들지
않고 generate_reports.py/correct_reports.py가 이미 쓰는 함수를 그대로
가져다 쓴다(같은 보안 로직이 여러 파일에 따로 존재하면 안 된다는 그 전례
그대로).

report 테이블은 절대 건드리지 않는다(body_md/status 등 전부 읽기만).
결과는 report_qa / report_qa_flag 두 테이블에만 쌓는다 - 검토 화면
(app/routers/zoom_summaries.py)에서 report와 나란히 보여줄 뿐, 이 배치가
report.status를 바꾸지는 않는다.

1단계(코드, LLM 호출 없음) - 아래 둘 중 하나라도 걸리면 실패 기록하고
2단계로 보내지 않는다:
  (a) 마스킹 스캔 재사용 - 이번엔 "새로 마스킹"이 아니라 "이미 새는 게
      없는지 확인"이 핵심이다. draft 본문은 검토 화면에서 강사가 직접
      편집했을 수 있어서(correct_reports.py의 안전장치와 같은 이유),
      본문에 다른 학생 실명이 이미 들어있으면(find_leaked_names) 실패.
      원본 요약 쪽은 mask_other_students로 마스킹하되 실패하면(MaskingError)
      역시 실패.
  (b) 본문 길이가 같은 class_meeting 내 다른 리포트들 길이의 중앙값 기준
      절반 미만이거나 두 배 초과면 실패(사용자 결정 - 반 전체가 아니라
      "해당 회차 내에서" 비교한다. 수업마다 분량이 다르니 전체 기준으로
      섞으면 안 됨).

2단계(LLM) - 1단계를 통과한 것만. masking된 원본 요약과 리포트 본문을
같이 주고 문장 단위로 판정시킨다: 근거 있음/근거 없음/원문과 모순.
"근거 없음"과 "모순"만 해당 문장을 인용해서 돌려받는다. 금칙 표현(다른
학생과의 비교, 부정적 단정)도 같은 호출에서 같이 지적하게 한다.

실행:
    python qa_reports.py                              # draft 중 QA 미실행 전부 자동 탐색
    python qa_reports.py <class_meeting_id> [...]      # 특정 회차만
    python qa_reports.py --force ...                   # 이미 QA한 report도 다시 검사
    python qa_reports.py --verbose ...                  # 로그에 학생 실명까지 출력

.env에 ANTHROPIC_API_KEY 필요(zoom_reports 자체 .env).
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from pathlib import Path

from generate_reports import (
    MODEL,
    MaskingError,
    build_raw_text,
    call_claude,
    load_class_students,
    load_meeting_segments,
    mask_other_students,
)
from correct_reports import find_leaked_names
from verify_zoom_auth import load_env
from zoom_pipeline_core import init_db

LENGTH_RATIO_MIN = 0.5
LENGTH_RATIO_MAX = 2.0
VALID_FLAG_TYPES = {"근거없음", "모순", "금칙_비교", "금칙_단정"}


def find_class_meetings_with_pending_qa(conn) -> list[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT r.class_meeting_id
        FROM report r
        LEFT JOIN report_qa q ON q.report_id = r.id
        WHERE r.status = 'draft' AND q.id IS NULL
        ORDER BY r.class_meeting_id
        """
    ).fetchall()
    return [r[0] for r in rows]


def load_class_meeting_reports(conn, class_meeting_id: int) -> list[tuple[int, int, str, str]]:
    """(report_id, student_id, student_name, body_md) - class_meeting 전체(상태 무관,
    길이 중앙값을 안정적으로 구하려고 draft만이 아니라 전부 포함)."""
    return conn.execute(
        """
        SELECT r.id, r.student_id, s.name, r.body_md, r.status
        FROM report r
        JOIN student s ON r.student_id = s.id
        WHERE r.class_meeting_id = ?
        ORDER BY s.name
        """,
        (class_meeting_id,),
    ).fetchall()


def already_qa_checked(conn, report_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM report_qa WHERE report_id = ?", (report_id,)).fetchone()
    return row is not None


def clear_previous_qa(conn, report_id: int) -> None:
    row = conn.execute("SELECT id FROM report_qa WHERE report_id = ?", (report_id,)).fetchone()
    if row:
        conn.execute("DELETE FROM report_qa_flag WHERE report_qa_id = ?", (row[0],))
        conn.execute("DELETE FROM report_qa WHERE id = ?", (row[0],))


def build_qa_prompt(class_name: str, meeting_date: str, target_name: str,
                     masked_source: str, body_md: str) -> str:
    return f"""당신은 학부모에게 발송될 리포트 초안을 검수합니다.

반: {class_name}
수업 날짜: {meeting_date}
대상 학생: {target_name}

아래 "원본 수업 요약"은 이번 수업의 Zoom AI 요약이고(다른 학생 실명은 이미
익명 라벨 "다른 학생 A/B/..."로 치환돼 있습니다), "리포트 본문"은 이 요약을
바탕으로 작성된 학부모 발송용 초안입니다.

--- 원본 수업 요약 ---
{masked_source}
--- 원본 끝 ---

--- 리포트 본문 ---
{body_md}
--- 본문 끝 ---

작업 지침:
1. 리포트 본문을 문장 단위로 나누어, 각 문장이 원본 수업 요약으로 근거가
   뒷받침되는지 판정하세요: "근거있음" / "근거없음"(원본에 없는 내용을
   지어냄) / "모순"(원본과 반대되거나 다른 사실을 말함).
2. "근거없음"과 "모순"으로 판정한 문장만 그대로 인용해서 돌려주세요.
   "근거있음" 문장은 결과에 포함하지 마세요.
3. 판정과 무관하게, 아래 두 종류의 금칙 표현이 있는 문장도 전부 찾아서
   인용하세요(근거있음이어도 포함):
   - "금칙_비교": 대상 학생을 다른 학생(익명 라벨 포함)과 비교하는 표현
   - "금칙_단정": 학생에 대한 부정적인 단정(예: "느리다", "부족하다", 성향/
     능력을 낙인찍듯 규정하는 표현)
4. 다른 학생의 익명 라벨("다른 학생 A" 등)을 실명으로 추측하거나 복원하지
   마세요.
5. 문제 되는 부분이 전혀 없으면 flags를 빈 배열로 반환하세요.

출력은 다른 설명 없이 아래 JSON 형식만 반환하세요(코드블록 없이 순수 JSON):
{{"flags": [{{"type": "근거없음|모순|금칙_비교|금칙_단정", "sentence": "인용한 문장 그대로"}}]}}"""


def _extract_json(text: str) -> dict:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if m:
        text = m.group(1).strip()
    return json.loads(text)


def parse_qa_response(raw_text: str) -> list[dict]:
    parsed = _extract_json(raw_text)
    flags = parsed.get("flags", [])
    if not isinstance(flags, list):
        raise ValueError("flags가 배열이 아님")
    result = []
    for f in flags:
        ftype = f.get("type")
        sentence = f.get("sentence")
        if ftype not in VALID_FLAG_TYPES:
            raise ValueError(f"알 수 없는 flag type: {ftype}")
        if not sentence or not str(sentence).strip():
            raise ValueError("빈 sentence")
        result.append({"type": ftype, "sentence": str(sentence).strip()})
    return result


def save_qa_result(conn, report_id: int, status: str, fail_reason: str | None,
                    body_length: int | None, median_length: float | None,
                    model: str | None, raw_response: str | None,
                    flags: list[dict] | None = None) -> None:
    cur = conn.execute(
        """
        INSERT INTO report_qa
            (report_id, status, fail_reason, body_length, median_length, model, raw_response)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (report_id, status, fail_reason, body_length, median_length, model, raw_response),
    )
    qa_id = cur.lastrowid
    for f in (flags or []):
        conn.execute(
            "INSERT INTO report_qa_flag (report_qa_id, flag_type, sentence) VALUES (?, ?, ?)",
            (qa_id, f["type"], f["sentence"]),
        )
    conn.commit()


def process_class_meeting(conn, api_key: str, class_meeting_id: int,
                           force: bool = False, verbose: bool = False) -> dict:
    cm_row = conn.execute(
        """
        SELECT cm.class_id, cm.meeting_date, c.name, c.class_code, i.name
        FROM class_meeting cm
        JOIN class c ON cm.class_id = c.id
        LEFT JOIN instructor i ON c.instructor_id = i.id
        WHERE cm.id = ?
        """,
        (class_meeting_id,),
    ).fetchone()
    if not cm_row:
        print(f"  class_meeting_id={class_meeting_id}: 존재하지 않음 - 건너뜀", file=sys.stderr)
        return {"passed": 0, "flagged": 0, "failed_stage1": 0, "skipped": 0, "errors": []}

    class_id, meeting_date, class_name, class_code, instructor_name = cm_row

    all_reports = load_class_meeting_reports(conn, class_meeting_id)
    if not all_reports:
        return {"passed": 0, "flagged": 0, "failed_stage1": 0, "skipped": 0, "errors": []}

    lengths = [len(body_md or "") for _, _, _, body_md, _ in all_reports]
    median_length = statistics.median(lengths) if len(lengths) >= 2 else None

    targets = [(rid, sid, name, body) for rid, sid, name, body, status in all_reports if status == "draft"]
    if not targets:
        return {"passed": 0, "flagged": 0, "failed_stage1": 0, "skipped": 0, "errors": []}

    segments = load_meeting_segments(conn, class_meeting_id)
    raw_text = build_raw_text(segments) if segments else ""

    students = load_class_students(conn, class_id)

    passed = flagged = failed_stage1 = skipped = 0
    errors: list[tuple[int, str, str]] = []

    for report_id, student_id, student_name, body_md in targets:
        who = student_name if verbose else f"report_id={report_id}"

        if already_qa_checked(conn, report_id):
            if not force:
                skipped += 1
                continue
            clear_previous_qa(conn, report_id)

        body_length = len(body_md or "")
        other_names = sorted(n for sid, n in students if sid != student_id)

        # 1-a. 본문에 다른 학생 실명이 이미 있는지(강사가 검토 화면에서 편집했을 수 있음)
        leaked = find_leaked_names(body_md or "", other_names)
        if leaked:
            save_qa_result(conn, report_id, "fail_masking",
                            "리포트 본문에 다른 학생 실명이 이미 있음",
                            body_length, median_length, None, None)
            failed_stage1 += 1
            print(f"  1단계 실패(마스킹) - class_meeting_id={class_meeting_id} / {who}", file=sys.stderr)
            continue

        # 1-a. 원본 요약 마스킹
        try:
            masked_source = mask_other_students(raw_text, student_name, other_names)
        except MaskingError as e:
            save_qa_result(conn, report_id, "fail_masking",
                            str(e) if verbose else "원본 요약 마스킹 실패",
                            body_length, median_length, None, None)
            failed_stage1 += 1
            print(f"  1단계 실패(마스킹) - class_meeting_id={class_meeting_id} / {who}: "
                  f"{e if verbose else '마스킹 실패'}", file=sys.stderr)
            continue

        # 1-b. 길이 이상치(같은 class_meeting 내 중앙값 기준)
        if median_length and median_length > 0:
            ratio = body_length / median_length
            if ratio < LENGTH_RATIO_MIN or ratio > LENGTH_RATIO_MAX:
                save_qa_result(conn, report_id, "fail_length",
                                f"본문 길이 {body_length}자, 회차 내 중앙값 {median_length:.0f}자 "
                                f"(비율 {ratio:.2f})",
                                body_length, median_length, None, None)
                failed_stage1 += 1
                print(f"  1단계 실패(길이) - class_meeting_id={class_meeting_id} / {who}: "
                      f"{body_length}자 vs 중앙값 {median_length:.0f}자", file=sys.stderr)
                continue

        # 2단계 - LLM
        prompt = build_qa_prompt(class_name, meeting_date, student_name, masked_source, body_md or "")
        try:
            raw_response = call_claude(api_key, prompt)
        except Exception as e:
            errors.append((report_id, student_name, f"API 호출 실패: {e}"))
            print(f"  API 호출 실패 - class_meeting_id={class_meeting_id} / {who}: {e}", file=sys.stderr)
            continue

        response_leaked = find_leaked_names(raw_response, other_names)
        if response_leaked:
            errors.append((report_id, student_name, "LLM 응답에 다른 학생 실명이 나타나 저장하지 않음"))
            print(f"  실패 - class_meeting_id={class_meeting_id} / {who}: "
                  f"LLM 응답에 실명 유출 (건수 {len(response_leaked)})", file=sys.stderr)
            continue

        try:
            parsed_flags = parse_qa_response(raw_response)
        except (ValueError, json.JSONDecodeError) as e:
            errors.append((report_id, student_name, f"응답 파싱 실패: {e}"))
            print(f"  실패 - class_meeting_id={class_meeting_id} / {who}: 응답 파싱 실패: {e}", file=sys.stderr)
            continue

        status = "pass_with_flags" if parsed_flags else "pass_clean"
        save_qa_result(conn, report_id, status, None, body_length, median_length,
                        MODEL, raw_response, parsed_flags)
        if parsed_flags:
            flagged += 1
        else:
            passed += 1
        if verbose:
            print(f"  QA 완료 - {class_code} {meeting_date} / {student_name}: {status} "
                  f"(flag {len(parsed_flags)}건)")

    return {"passed": passed, "flagged": flagged, "failed_stage1": failed_stage1,
            "skipped": skipped, "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser(description="학부모 리포트 초안 QA 배치(사실관계·금칙표현)")
    parser.add_argument("class_meeting_ids", type=int, nargs="*",
                       help="처리할 class_meeting id (생략하면 QA 미실행 draft가 있는 회차를 전부 자동 탐색)")
    parser.add_argument("--force", action="store_true",
                       help="이미 report_qa가 있는 report도 지우고 다시 검사")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="로그에 학생 실명을 출력한다(기본은 report_id만)")
    args = parser.parse_args()

    env = load_env(Path(__file__).resolve().parent / ".env")
    api_key = env.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("치명적 오류: .env에 ANTHROPIC_API_KEY가 없습니다.", file=sys.stderr)
        return 1

    conn = init_db()

    if args.class_meeting_ids:
        class_meeting_ids = args.class_meeting_ids
    else:
        class_meeting_ids = find_class_meetings_with_pending_qa(conn)
        if args.force:
            all_draft_meetings = [r[0] for r in conn.execute(
                "SELECT DISTINCT class_meeting_id FROM report WHERE status='draft'"
            ).fetchall()]
            class_meeting_ids = sorted(set(class_meeting_ids) | set(all_draft_meetings))

    if not class_meeting_ids:
        print("QA할 draft가 없습니다.")
        conn.close()
        return 0

    total_passed = total_flagged = total_failed1 = total_skipped = 0
    total_errors: list[tuple[int, int, str, str]] = []
    for cmid in class_meeting_ids:
        print(f"class_meeting_id={cmid} 처리 중...")
        result = process_class_meeting(conn, api_key, cmid, force=args.force, verbose=args.verbose)
        total_passed += result["passed"]
        total_flagged += result["flagged"]
        total_failed1 += result["failed_stage1"]
        total_skipped += result["skipped"]
        for report_id, name, reason in result["errors"]:
            total_errors.append((cmid, report_id, name, reason))

    conn.close()

    print(f"\nQA 완료 - 통과(깨끗) {total_passed}건, 통과(플래그 있음) {total_flagged}건, "
          f"1단계 실패 {total_failed1}건, 이미 QA함(건너뜀) {total_skipped}건, "
          f"오류(재시도 필요) {len(total_errors)}건")
    if total_errors:
        print("오류 내역:")
        for cmid, report_id, name, reason in total_errors:
            who = name if args.verbose else f"report_id={report_id}"
            print(f"  class_meeting_id={cmid} / {who}: {reason}")

    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
