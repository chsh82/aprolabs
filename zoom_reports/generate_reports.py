"""
class_meeting 단위로 학생별 학부모 리포트 초안을 생성하는 변환 배치.

CLAUDE.md 개인정보 요구사항 1번(1:4 수업 정보 분리) - 대상 학생이 아닌
다른 학생의 실명은 **API 호출 전에 코드로** 익명 라벨("다른 학생 A",
"다른 학생 B", ...)로 치환한다. 삭제가 아니라 치환이다 - 라벨이 남아야
토론 흐름(누가 무슨 말을 했는지)을 모델이 이해할 수 있다. 모델에게
"언급하지 말라"고 프롬프트로 지시하는 방식은 쓰지 않는다.

치환 후에도 다른 학생의 원본 이름이 하나라도 텍스트에 남아 있으면(치환이
안전하게 끝나지 않았다는 뜻) API를 호출하지 않고 그 학생 건을 실패로
기록한다 - 조용히 넘기지 않는다. 강사 이름은 치환하지 않는다.

대상: class_meeting에 연결된 session.status='mapped' 세션만(raw는 그대로,
가공은 여기서 읽기만 한다). report는 (class_meeting_id, student_id)
UNIQUE라 이미 생성된 건 API를 다시 부르지 않고 건너뛴다(재실행 안전 +
비용 절약).

실행:
    python generate_reports.py                              # 전체 class_meeting 자동 탐색
    python generate_reports.py <class_meeting_id> [...]      # 특정 회차만
    python generate_reports.py --verbose ...                 # 로그에 학생 실명까지 출력

기본 실행은 로그에 학생 실명을 안 찍는다(집계 수치·class_meeting_id·
student_id만) - 스케줄러로 무인 실행되면 출력이 journald 등 서버 로그로
그대로 들어가므로, 로그 자체가 새로운 PII 유출 지점이 되는 걸 피한다.
실패한 학생을 실제로 찾아 고쳐야 할 때는 --verbose로 로컬에서 다시
돌려서 본다.

.env에 ANTHROPIC_API_KEY 필요(zoom_reports가 독립 앱이라 자체 .env에 둠 -
aprolabs/.env와 별개).
"""
from __future__ import annotations

import argparse
import json
import string
import sys
from datetime import datetime, timedelta
from pathlib import Path

from verify_zoom_auth import load_env
from zoom_pipeline_core import init_db

MODEL = "claude-sonnet-4-6"


class MaskingError(Exception):
    """마스킹이 안전하게 끝나지 않아 API를 호출하면 안 되는 상태."""


def to_kst(dt_str: str) -> datetime:
    """Zoom의 UTC 'YYYY-MM-DDTHH:MM:SSZ'를 KST naive datetime으로 변환(map_sessions.to_kst와 동일 규칙)."""
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=9)


def load_meeting_segments(conn, class_meeting_id: int) -> list[tuple[datetime | None, str]]:
    """이 회차에 연결된 mapped 세션들의 (KST 시각, 요약 본문)을 시각순으로."""
    rows = conn.execute(
        """
        SELECT meeting_uuid, started_at
        FROM session
        WHERE class_meeting_id = ? AND status = 'mapped'
        ORDER BY started_at ASC
        """,
        (class_meeting_id,),
    ).fetchall()

    segments = []
    for meeting_uuid, started_at in rows:
        raw = conn.execute(
            "SELECT payload_json FROM zoom_summary_raw WHERE meeting_uuid = ?", (meeting_uuid,)
        ).fetchone()
        if not raw:
            continue
        payload = json.loads(raw[0])
        content = payload.get("summary_content") or payload.get("summary_overview") or ""
        kst = to_kst(started_at) if started_at else None
        segments.append((kst, content))
    return segments


def build_raw_text(segments: list[tuple[datetime | None, str]]) -> str:
    parts = []
    for kst, content in segments:
        header = f"[{kst.strftime('%Y-%m-%d %H:%M')} KST]" if kst else "[시각 미확인]"
        parts.append(f"{header}\n{content}")
    return "\n\n".join(parts)


def load_class_students(conn, class_id: int) -> list[tuple[int, str]]:
    return conn.execute(
        "SELECT id, name FROM student WHERE class_id = ? ORDER BY name", (class_id,)
    ).fetchall()


def report_exists(conn, class_meeting_id: int, student_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM report WHERE class_meeting_id = ? AND student_id = ?",
        (class_meeting_id, student_id),
    ).fetchone()
    return row is not None


def build_label_map(other_names: list[str]) -> dict[str, str]:
    letters = string.ascii_uppercase
    if len(other_names) > len(letters):
        raise MaskingError("다른 학생 수가 26명을 넘어 익명 라벨을 만들 수 없음")
    return {name: f"다른 학생 {letters[i]}" for i, name in enumerate(other_names)}


def mask_other_students(text: str, target_name: str, other_names: list[str]) -> str:
    """target_name을 뺀 나머지 학생 이름을 익명 라벨로 치환한다.

    - other_name이 target_name의 부분 문자열이면(예: "민수"가 "김민수"의
      일부) 안전하게 치환할 수 없다 - target 학생 이름 자체가 잘못 잘려
      나갈 위험이 있어 이 경우 바로 실패시킨다.
    - 이름이 긴 것부터 치환해야 짧은 이름이 긴 이름의 일부를 먼저 깨물지
      않는다(다른 학생들 사이의 부분 문자열 겹침 대비).
    - 치환 후에도 원본 이름이 하나라도 남아 있으면 실패시킨다 - 이게
      "프롬프트가 아니라 코드로 강제"의 핵심 안전장치.
    """
    for other in other_names:
        if other and other in target_name:
            raise MaskingError(
                f"다른 학생 이름 '{other}'가 대상 학생 이름 '{target_name}'의 "
                "부분 문자열이라 안전하게 치환할 수 없음"
            )

    label_map = build_label_map(other_names)
    masked = text
    for name in sorted(label_map, key=len, reverse=True):
        masked = masked.replace(name, label_map[name])

    leftover = [name for name in other_names if name and name in masked]
    if leftover:
        raise MaskingError(f"치환 후에도 원본 이름이 남아 있음: {leftover}")

    return masked


def build_prompt(class_name: str, meeting_date: str, instructor_name: str,
                  target_name: str, masked_text: str) -> str:
    return f"""당신은 학원 강사를 도와 학부모에게 보낼 수업 리포트 초안을 작성합니다.

반: {class_name}
수업 날짜: {meeting_date}
담당 강사: {instructor_name}
이 리포트의 대상 학생: {target_name}

아래는 이번 수업의 Zoom AI 요약입니다. 대상 학생이 아닌 다른 학생의 실명은
이미 "다른 학생 A", "다른 학생 B" 같은 익명 라벨로 바뀌어 있습니다. 이
라벨이 누구인지 추측하거나 실명을 복원하려 하지 마세요.

--- 수업 요약 ---
{masked_text}
--- 요약 끝 ---

위 내용을 바탕으로 {target_name} 학생의 학부모에게 보낼 리포트 초안을
한국어로 작성하세요. 조건:
- {target_name} 학생의 수업 참여·발언·학습 내용을 중심으로 서술
- 다른 학생이 언급될 때는 위 익명 라벨을 그대로 사용 (실명 추측 금지)
- 요약에 없는 내용은 지어내지 말 것
- 담당 강사 이름은 실명 그대로 사용해도 됨
- 담당 강사가 검토하기 전 초안이므로 완성된 어조보다는 사실 위주로 간결하게
- 마크다운 형식, 3~5문단 정도"""


def call_claude(api_key: str, prompt: str) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model=MODEL,
        max_tokens=1536,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def find_all_class_meetings(conn) -> list[int]:
    return [r[0] for r in conn.execute("SELECT id FROM class_meeting ORDER BY id").fetchall()]


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
        return {"generated": 0, "skipped": 0, "failed": []}

    class_id, meeting_date, class_name, class_code, instructor_name = cm_row
    instructor_name = instructor_name or "(미등록)"

    segments = load_meeting_segments(conn, class_meeting_id)
    if not segments:
        print(f"  {class_code} {meeting_date}: 연결된 mapped 세션 없음 - 건너뜀")
        return {"generated": 0, "skipped": 0, "failed": []}
    raw_text = build_raw_text(segments)

    students = load_class_students(conn, class_id)
    if not students:
        print(f"  {class_code} {meeting_date}: 등록 학생 없음 - 건너뜀")
        return {"generated": 0, "skipped": 0, "failed": []}

    generated, skipped, failed = 0, 0, []
    for student_id, student_name in students:
        if report_exists(conn, class_meeting_id, student_id):
            skipped += 1
            continue

        who = student_name if verbose else f"student_id={student_id}"
        other_names = sorted(n for sid, n in students if sid != student_id)
        try:
            masked_text = mask_other_students(raw_text, student_name, other_names)
        except MaskingError as e:
            failed.append((student_id, student_name, str(e) if verbose else "마스킹 실패"))
            print(f"  실패 - class_meeting_id={class_meeting_id} / {who}: "
                  f"{e if verbose else '마스킹 실패'}", file=sys.stderr)
            continue

        prompt = build_prompt(class_name, meeting_date, instructor_name, student_name, masked_text)
        try:
            body_md = call_claude(api_key, prompt)
        except Exception as e:
            failed.append((student_id, student_name, f"API 호출 실패: {e}"))
            print(f"  실패 - class_meeting_id={class_meeting_id} / {who}: API 호출 실패: {e}", file=sys.stderr)
            continue

        leaked = [name for name in other_names if name and name in body_md]
        if leaked:
            # 입력을 마스킹해도 모델이 라벨을 실명으로 "복원"해 출력할 가능성은 남아 있다 -
            # 저장 전에 출력도 검사한다. 입력 검사(mask_other_students)와 같은 원칙.
            failed.append((student_id, student_name, "LLM 출력에 다른 학생 실명이 나타나 저장하지 않음"))
            print(f"  실패 - class_meeting_id={class_meeting_id} / {who}: 출력에 실명 유출 (건수 {len(leaked)})",
                  file=sys.stderr)
            continue

        conn.execute(
            """
            INSERT INTO report (class_meeting_id, student_id, body_md, status)
            VALUES (?, ?, ?, 'draft')
            ON CONFLICT (class_meeting_id, student_id) DO NOTHING
            """,
            (class_meeting_id, student_id, body_md),
        )
        conn.commit()  # 학생 1명씩 즉시 커밋 - 중간에 실패해도 이미 만든 초안은 지킨다
        generated += 1
        if verbose:
            print(f"  생성 완료 - {class_code} {meeting_date} / {student_name}")

    return {"generated": generated, "skipped": skipped, "failed": failed}


def main() -> int:
    parser = argparse.ArgumentParser(description="class_meeting 단위 학생별 리포트 초안 생성 배치")
    parser.add_argument("class_meeting_ids", type=int, nargs="*",
                       help="처리할 class_meeting id (생략하면 전체 class_meeting을 자동 탐색 - "
                            "학생별로 이미 있으면 건너뛰니 안전)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="로그에 학생 실명을 출력한다(기본은 student_id만 - 서버 로그에 실명이 안 남게)")
    args = parser.parse_args()

    env = load_env(Path(__file__).resolve().parent / ".env")
    api_key = env.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("치명적 오류: .env에 ANTHROPIC_API_KEY가 없습니다.", file=sys.stderr)
        return 1

    conn = init_db()

    class_meeting_ids = args.class_meeting_ids or find_all_class_meetings(conn)
    if not class_meeting_ids:
        print("처리할 class_meeting이 없습니다.")
        conn.close()
        return 0

    total_generated = total_skipped = 0
    total_failed: list[tuple[int, int, str, str]] = []
    for cmid in class_meeting_ids:
        print(f"class_meeting_id={cmid} 처리 중...")
        result = process_class_meeting(conn, api_key, cmid, verbose=args.verbose)
        total_generated += result["generated"]
        total_skipped += result["skipped"]
        for student_id, name, reason in result["failed"]:
            total_failed.append((cmid, student_id, name, reason))

    conn.close()

    print(f"\n초안 생성 {total_generated}건, 이미 있어서 건너뜀 {total_skipped}건, 실패 {len(total_failed)}건")
    if total_failed:
        print("실패 내역:")
        for cmid, student_id, name, reason in total_failed:
            who = name if args.verbose else f"student_id={student_id}"
            print(f"  class_meeting_id={cmid} / {who}: {reason}")

    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
