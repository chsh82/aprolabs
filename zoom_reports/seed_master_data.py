"""
마스터 데이터(instructor / class / student) 입력 스크립트.

CLAUDE.md "다음 작업 1번" - 사용자가 채팅으로 불러준 명단을 그대로 옮겨
DB에 넣는다. 여러 번 실행해도 안전하도록(재실행 시 중복 삽입 안 되도록)
INSERT OR IGNORE / UPSERT를 쓴다.

실행: python seed_master_data.py
DB 경로: zoom_pipeline_core.init_db()의 기본값(momo_zoom.db, 이 파일과 같은 폴더)
"""

from __future__ import annotations

import io
import sys

from zoom_pipeline_core import init_db

# 이미 utf-8로 래핑돼 있으면 다시 감싸지 않는다 - 다른 스크립트가 이 모듈을
# import할 때 이중 래핑으로 "I/O operation on closed file"이 나는 걸 방지.
if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# (zoom_email, name) - /v2/users 조회 결과와 대조해 확인된 9명 (2026-08-31)
INSTRUCTORS: list[tuple[str, str]] = [
    ("aproacademy01@gmail.com", "윤영기"),
    ("aproacademy11@gmail.com", "이정기"),
    ("aproacademy06@gmail.com", "송경선"),
    ("aproacademy12@gmail.com", "박소영"),
    ("aproacademy26@gmail.com", "이선연"),
    ("aproacademy08@gmail.com", "김별하"),
    ("aproacademy25@gmail.com", "박선진"),
    ("aproacademy28@gmail.com", "박은영"),
    ("aproacademy13@gmail.com", "김지수"),
]

# (class_code, name, instructor_zoom_email) - 아직 미입력, 데이터 도착하면 채운다
CLASSES: list[tuple[str, str | None, str]] = []

# (student_name, class_code) - 아직 미입력
STUDENTS: list[tuple[str, str]] = []


def seed_instructors(conn) -> None:
    for zoom_email, name in INSTRUCTORS:
        conn.execute(
            """
            INSERT INTO instructor (name, zoom_email, active) VALUES (?, ?, 1)
            ON CONFLICT (zoom_email) DO UPDATE SET name = excluded.name, active = 1
            """,
            (name, zoom_email),
        )
    conn.commit()
    print(f"instructor {len(INSTRUCTORS)}명 입력 완료")


def seed_classes(conn) -> None:
    if not CLASSES:
        print("class 데이터 없음 - 건너뜀")
        return
    for class_code, name, instructor_email in CLASSES:
        row = conn.execute(
            "SELECT id FROM instructor WHERE zoom_email = ?", (instructor_email,)
        ).fetchone()
        if row is None:
            print(f"  경고: {class_code} 담당 강사({instructor_email})가 instructor 테이블에 없음 - 건너뜀")
            continue
        instructor_id = row[0]
        conn.execute(
            """
            INSERT INTO class (class_code, name, instructor_id) VALUES (?, ?, ?)
            ON CONFLICT (class_code) DO UPDATE SET name = excluded.name, instructor_id = excluded.instructor_id
            """,
            (class_code, name, instructor_id),
        )
    conn.commit()
    print(f"class {len(CLASSES)}건 입력 완료")


def seed_students(conn) -> None:
    if not STUDENTS:
        print("student 데이터 없음 - 건너뜀")
        return
    inserted = 0
    for student_name, class_code in STUDENTS:
        row = conn.execute("SELECT id FROM class WHERE class_code = ?", (class_code,)).fetchone()
        if row is None:
            print(f"  경고: {student_name}의 반({class_code})이 class 테이블에 없음 - 건너뜀")
            continue
        class_id = row[0]
        existing = conn.execute(
            "SELECT id FROM student WHERE name = ? AND class_id = ?", (student_name, class_id)
        ).fetchone()
        if existing:
            continue
        conn.execute("INSERT INTO student (name, class_id) VALUES (?, ?)", (student_name, class_id))
        inserted += 1
    conn.commit()
    print(f"student {inserted}명 신규 입력 완료")


def main() -> None:
    conn = init_db()
    seed_instructors(conn)
    seed_classes(conn)
    seed_students(conn)

    print("\n현재 DB 상태:")
    for table in ("instructor", "class", "student"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}건")
    conn.close()


if __name__ == "__main__":
    main()
