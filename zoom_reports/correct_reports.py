"""
class_meeting 단위로 학생별 리포트 초안의 고유명사를 교정하는 배치.

CLAUDE.md 로드맵 "변환 (마스킹 → LLM 초안 → 고유명사 교정)"의 세 번째
단계 - generate_reports.py로 만든 draft를 다듬는 후속 배치. Zoom AI
Companion 요약은 음성 인식 기반이라 인명·지명·책 제목·역사적 사건/인물명
같은 고유명사가 잘못 표기되는 경우가 있고, 그 오류가 LLM 초안에도 그대로
넘어올 수 있다. 이 배치는 초안을 원본 수업 요약(마스킹된 상태)과 대조해서
고유명사만 교정하고, 그 외 내용·구조·어조는 바꾸지 않는다.

마스킹 규칙은 generate_reports.py와 완전히 같다(같은 함수를 그대로
가져다 쓴다 - 이 로직은 복제하지 않고 반드시 하나로 유지해야 한다):
대상 학생이 아닌 다른 학생 실명은 API 호출 전에 "다른 학생 A/B/..."
익명 라벨로 치환하고, 치환이 안전하게 끝나지 않으면 API를 호출하지 않고
실패로 기록한다.

이 배치에는 generate_reports.py에 없는 안전장치가 하나 더 있다: 교정
대상 draft 본문(body_md)은 검토 화면(app/routers/zoom_summaries.py)에서
운영자가 이미 편집했을 수 있으므로, API에 보내기 *전에* 현재 draft
본문에 다른 학생 실명이 이미 들어있지 않은지도 검사한다. 그리고 API
응답(교정 결과)에도 다른 학생 실명이 새로 나타나지 않았는지 저장 전에
다시 검사한다(모델이 익명 라벨을 실명으로 "복원"할 가능성 대비) - 입력과
출력 양쪽 다 코드로 강제한다.

대상: report.status='draft'이고 corrected_at이 아직 없는 것만.
review/published(승인 후)는 검토 화면에서 "편집 불가"로 잠기는 것과
같은 원칙으로 이 배치도 건드리지 않는다. corrected_at은 교정이 실제로
성공해서 저장될 때만 찍힌다 - 이미 교정된 draft를 스케줄러가 4시간마다
또 교정 API에 태우는 낭비를 막는다(운영자가 검토 화면에서 본문을 직접
고친 뒤에도 corrected_at은 그대로 남아 재교정 대상이 되지 않는다 -
사람이 손댄 걸 배치가 다시 덮어쓰지 않는다).

실행:
    python correct_reports.py                              # draft 중 미교정 전부 자동 탐색
    python correct_reports.py <class_meeting_id> [...]      # 특정 회차만
    python correct_reports.py --verbose ...                 # 로그에 학생 실명까지 출력

기본 실행은 로그에 학생 실명을 안 찍는다(집계 수치·class_meeting_id만) -
이 배치는 스케줄러로 무인 실행되고 출력이 journald 등 서버 로그로 그대로
들어가므로, 로그 자체가 새로운 PII 유출 지점이 되는 걸 피한다. 실패한
학생을 실제로 찾아 고쳐야 할 때는 --verbose로 로컬에서 다시 돌려서 본다.

.env에 ANTHROPIC_API_KEY 필요(zoom_reports 자체 .env - generate_reports.py와 동일).
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from generate_reports import (
    MaskingError,
    build_raw_text,
    call_claude,
    load_class_students,
    load_meeting_segments,
    mask_other_students,
)
from verify_zoom_auth import load_env
from zoom_pipeline_core import init_db


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def ensure_corrected_at_column(conn: sqlite3.Connection) -> None:
    """오래된 momo_zoom.db에는 corrected_at이 없을 수 있다 - 있으면 건너뜀(재실행 안전)."""
    if _has_column(conn, "report", "corrected_at"):
        return
    conn.execute("ALTER TABLE report ADD COLUMN corrected_at TEXT")
    conn.commit()


def find_leaked_names(text: str, other_names: list[str]) -> list[str]:
    return [name for name in other_names if name and name in (text or "")]


def build_correction_prompt(class_name: str, meeting_date: str, instructor_name: str,
                             target_name: str, masked_source: str, draft_body_md: str) -> str:
    return f"""당신은 학부모 리포트 초안의 고유명사를 교정합니다.

반: {class_name}
수업 날짜: {meeting_date}
담당 강사: {instructor_name}
대상 학생: {target_name}

아래 "원본 수업 요약"은 Zoom AI가 자동 생성한 요약이고(다른 학생 실명은
이미 익명 라벨로 치환돼 있음), "리포트 초안"은 이 요약을 바탕으로 이미
작성된 학부모 리포트 초안입니다. Zoom은 음성 인식 기반이라 이 초안에는
인명·지명·책 제목·역사적 사건/인물명 같은 고유명사가 잘못 표기됐을 수
있습니다.

--- 원본 수업 요약 ---
{masked_source}
--- 원본 끝 ---

--- 리포트 초안 ---
{draft_body_md}
--- 초안 끝 ---

작업 지침:
- 리포트 초안에서 원본 수업 요약과 다르게 표기된 고유명사(인명·지명·
  책 제목·역사적 사건/인물명 등)만 원본에 맞게 고치세요.
- 원본에도 근거가 없는 이름은 추측해서 바꾸지 마세요 - 확실하지 않으면
  그대로 두세요.
- 익명 라벨("다른 학생 A" 등)은 절대 실명으로 바꾸지 마세요.
- 고유명사 교정 외에는 문장, 구조, 어조, 내용을 바꾸지 마세요.
- 고칠 부분이 없으면 초안을 그대로 반환하세요.
- 교정된 리포트 전체를 마크다운으로 반환하세요. 교정 내역 설명이나
  목록은 출력하지 말고 교정된 본문만 출력하세요."""


def load_draft_reports(conn, class_meeting_id: int) -> list[tuple[int, int, str, str]]:
    """(report_id, student_id, student_name, body_md) - status='draft'이고 아직 안 교정된 것만."""
    return conn.execute(
        """
        SELECT r.id, r.student_id, s.name, r.body_md
        FROM report r
        JOIN student s ON r.student_id = s.id
        WHERE r.class_meeting_id = ? AND r.status = 'draft' AND r.corrected_at IS NULL
        ORDER BY s.name
        """,
        (class_meeting_id,),
    ).fetchall()


def find_class_meetings_with_pending_correction(conn) -> list[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT class_meeting_id FROM report
        WHERE status = 'draft' AND corrected_at IS NULL
        ORDER BY class_meeting_id
        """
    ).fetchall()
    return [r[0] for r in rows]


def process_class_meeting(conn, api_key: str, class_meeting_id: int, verbose: bool = False) -> dict:
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
        return {"corrected": 0, "locked": 0, "failed": []}

    class_id, meeting_date, class_name, class_code, instructor_name = cm_row
    instructor_name = instructor_name or "(미등록)"

    locked = conn.execute(
        "SELECT COUNT(*) FROM report WHERE class_meeting_id = ? AND status != 'draft'",
        (class_meeting_id,),
    ).fetchone()[0]

    drafts = load_draft_reports(conn, class_meeting_id)
    if not drafts:
        if verbose:
            print(f"  {class_code} {meeting_date}: 교정할 draft 없음 - 건너뜀")
        return {"corrected": 0, "locked": locked, "failed": []}

    segments = load_meeting_segments(conn, class_meeting_id)
    if not segments:
        print(f"  class_meeting_id={class_meeting_id}: 연결된 mapped 세션 없음 - 건너뜀")
        return {"corrected": 0, "locked": locked, "failed": []}
    raw_text = build_raw_text(segments)

    students = load_class_students(conn, class_id)

    corrected, failed = 0, []
    for report_id, student_id, student_name, body_md in drafts:
        who = student_name if verbose else f"student_id={student_id}"
        other_names = sorted(n for sid, n in students if sid != student_id)

        leaked_before = find_leaked_names(body_md, other_names)
        if leaked_before:
            failed.append((report_id, student_name,
                           "현재 draft 본문에 다른 학생 실명이 이미 있어 API를 호출하지 않음"))
            print(f"  실패 - class_meeting_id={class_meeting_id} report_id={report_id} / {who}: "
                  f"기존 본문에 실명 유출 (건수 {len(leaked_before)})", file=sys.stderr)
            continue

        try:
            masked_source = mask_other_students(raw_text, student_name, other_names)
        except MaskingError as e:
            failed.append((report_id, student_name, str(e) if verbose else "마스킹 실패"))
            print(f"  실패 - class_meeting_id={class_meeting_id} report_id={report_id} / {who}: "
                  f"{e if verbose else '마스킹 실패'}", file=sys.stderr)
            continue

        prompt = build_correction_prompt(class_name, meeting_date, instructor_name,
                                          student_name, masked_source, body_md)
        try:
            corrected_body = call_claude(api_key, prompt)
        except Exception as e:
            failed.append((report_id, student_name, f"API 호출 실패: {e}"))
            print(f"  실패 - class_meeting_id={class_meeting_id} report_id={report_id} / {who}: "
                  f"API 호출 실패: {e}", file=sys.stderr)
            continue

        leaked_after = find_leaked_names(corrected_body, other_names)
        if leaked_after:
            failed.append((report_id, student_name, "교정 결과에 다른 학생 실명이 나타나 저장하지 않음"))
            print(f"  실패 - class_meeting_id={class_meeting_id} report_id={report_id} / {who}: "
                  f"교정 결과에 실명 유출 (건수 {len(leaked_after)})", file=sys.stderr)
            continue

        conn.execute(
            "UPDATE report SET body_md = ?, corrected_at = datetime('now') WHERE id = ?",
            (corrected_body, report_id),
        )
        conn.commit()
        corrected += 1
        if verbose:
            print(f"  교정 완료 - {class_code} {meeting_date} / {student_name}")

    return {"corrected": corrected, "locked": locked, "failed": failed}


def main() -> int:
    parser = argparse.ArgumentParser(description="class_meeting 단위 리포트 초안 고유명사 교정 배치")
    parser.add_argument("class_meeting_ids", type=int, nargs="*",
                       help="처리할 class_meeting id (생략하면 미교정 draft가 있는 회차를 전부 자동 탐색)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="로그에 학생 실명을 출력한다(기본은 student_id만 - 서버 로그에 실명이 안 남게)")
    args = parser.parse_args()

    env = load_env(Path(__file__).resolve().parent / ".env")
    api_key = env.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("치명적 오류: .env에 ANTHROPIC_API_KEY가 없습니다.", file=sys.stderr)
        return 1

    conn = init_db()
    ensure_corrected_at_column(conn)

    class_meeting_ids = args.class_meeting_ids or find_class_meetings_with_pending_correction(conn)
    if not class_meeting_ids:
        print("교정할 draft가 없습니다.")
        conn.close()
        return 0

    total_corrected = total_locked = 0
    total_failed: list[tuple[int, int, str, str]] = []
    for cmid in class_meeting_ids:
        print(f"class_meeting_id={cmid} 처리 중...")
        result = process_class_meeting(conn, api_key, cmid, verbose=args.verbose)
        total_corrected += result["corrected"]
        total_locked += result["locked"]
        for report_id, name, reason in result["failed"]:
            total_failed.append((cmid, report_id, name, reason))

    conn.close()

    print(f"\n교정 완료 {total_corrected}건, 승인돼서 건너뜀 {total_locked}건, 실패 {len(total_failed)}건")
    if total_failed:
        print("실패 내역:")
        for cmid, report_id, name, reason in total_failed:
            who = name if args.verbose else f"report_id={report_id}"
            print(f"  class_meeting_id={cmid} / {who}: {reason}")

    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
