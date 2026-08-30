"""
derive_class_codes.py가 뽑아낸 하크니스 5개 반 후보를 class / class_key에 입력한다.

derive_class_codes.py는 출력만 하고 아무것도 쓰지 않는다(설계 의도) - 이
스크립트가 그 결과를 받아 실제로 적재하는 쪽이다. CLAUDE.md 7절의
"처음 보는 키 조합은 자동으로 class를 만들지 않는다"는 이후 수집기가
새로운 (강사,요일,시각)을 만났을 때 적용되는 규칙이고, 이번처럼 이미
운영자가 검토를 마친 후보 목록을 한 번에 입력하는 것과는 별개다.

재실행해도 안전하다:
  - class: class_code UNIQUE 제약으로 UPSERT
  - class_key: (instructor_id, weekday, start_time) UNIQUE 제약으로 UPSERT
    (class_id가 바뀌면 갱신됨 - 반이 재발급된 경우를 대비)

실행: python load_class_keys.py
"""

from __future__ import annotations

import sys

from derive_class_codes import (  # noqa: F401 - importing this also sets up UTF-8 stdout on win32
    ACTIVE_HARKNESS_COURSES,
    WEEKDAY_INT,
    build_class_code_candidate,
    parse_course_name,
)
from zoom_pipeline_core import init_db

COURSE_TYPE = "하크니스"


def main() -> int:
    conn = init_db()
    failures: list[str] = []
    inserted_classes = 0
    inserted_keys = 0

    for course_code, course_name in ACTIVE_HARKNESS_COURSES:
        parsed = parse_course_name(course_name)
        if not parsed.ok:
            failures.append(f"{course_code}: 파싱 실패 - {'; '.join(parsed.errors)}")
            continue

        class_code = build_class_code_candidate(parsed)
        if class_code is None:
            failures.append(f"{course_code}: class_code 생성 실패")
            continue

        instr_row = conn.execute(
            "SELECT id FROM instructor WHERE name = ?", (parsed.teacher,)
        ).fetchone()
        if instr_row is None:
            failures.append(f"{course_code}: instructor '{parsed.teacher}'를 찾을 수 없음")
            continue
        instructor_id = instr_row[0]

        # class UPSERT
        conn.execute(
            """
            INSERT INTO class (class_code, name, course_type, instructor_id, active)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(class_code) DO UPDATE SET
                name = excluded.name,
                course_type = excluded.course_type,
                instructor_id = excluded.instructor_id,
                active = 1
            """,
            (class_code, course_name, COURSE_TYPE, instructor_id),
        )
        inserted_classes += 1

        class_row = conn.execute(
            "SELECT id FROM class WHERE class_code = ?", (class_code,)
        ).fetchone()
        class_id = class_row[0]

        weekday_int = WEEKDAY_INT[parsed.weekday_kr]
        start_time = f"{parsed.time_hhmm[:2]}:{parsed.time_hhmm[2:]}"

        # class_key UPSERT - (instructor_id, weekday, start_time)이 유니크 키
        conn.execute(
            """
            INSERT INTO class_key (class_id, instructor_id, weekday, start_time)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(instructor_id, weekday, start_time) DO UPDATE SET
                class_id = excluded.class_id
            """,
            (class_id, instructor_id, weekday_int, start_time),
        )
        inserted_keys += 1

        print(f"{course_code} -> class_code={class_code} "
              f"(instructor={parsed.teacher}, weekday={weekday_int}, start_time={start_time})")

    conn.commit()

    if failures:
        print("\n실패:")
        for f in failures:
            print(f"  {f}")

    print(f"\nclass: {inserted_classes}건 입력/갱신, class_key: {inserted_keys}건 입력/갱신, 실패 {len(failures)}건")

    print("\n현재 DB 상태:")
    for table in ("instructor", "class", "class_key", "student"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}건")

    conn.close()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
