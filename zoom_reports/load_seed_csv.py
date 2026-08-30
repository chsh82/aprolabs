"""
seed/classes.csv, seed/students.csv를 읽어 class / student 테이블에 넣는 로더.

CSV 형식 (encoding='utf-8-sig' - 엑셀에서 저장한 BOM 붙은 UTF-8 대응):
    seed/classes.csv:  class_code,class_name,instructor_name
        - class_name(반 이름)은 비어있어도 됨
    seed/students.csv: student_name,class_code

instructor_name은 instructor.name으로 조회해 instructor_id로 변환한다.
매칭 안 되는 행(강사 이름 불일치, 반 코드 불일치, 이름 모호 등)은 삽입하지
않고 실패 목록에 남긴다 - 조용히 버리지 않는다(CLAUDE.md 코딩 규약).

재실행해도 안전하다:
  - class: class_code UNIQUE 제약으로 UPSERT
  - student: (name, class_id) 조합이 이미 있으면 건너뜀

실행: python load_seed_csv.py
seed/ 디렉터리는 학생 개인정보를 담으므로 .gitignore에 포함돼 있다.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
from pathlib import Path

from zoom_pipeline_core import init_db

# 이미 utf-8로 래핑돼 있으면 다시 감싸지 않는다 - 다른 스크립트가 이 모듈을
# import할 때 이중 래핑으로 "I/O operation on closed file"이 나는 걸 방지.
if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

SEED_DIR = Path(__file__).resolve().parent / "seed"
CLASSES_CSV = SEED_DIR / "classes.csv"
STUDENTS_CSV = SEED_DIR / "students.csv"

Failure = tuple[str, int, dict, str]  # (source, line_no, row, reason)


def _read_rows(csv_path: Path, required_columns: set[str]) -> list[dict] | None:
    if not csv_path.exists():
        print(f"경고: {csv_path} 없음 - 건너뜀")
        return None
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = required_columns - fieldnames
        if missing:
            raise ValueError(f"{csv_path.name} 헤더에 {missing} 컬럼이 없음 (실제: {sorted(fieldnames)})")
        return list(reader)


def load_classes(conn: sqlite3.Connection) -> list[Failure]:
    failures: list[Failure] = []
    rows = _read_rows(CLASSES_CSV, {"class_code", "class_name", "instructor_name"})
    if rows is None:
        return failures

    inserted = 0
    for line_no, row in enumerate(rows, start=2):  # 헤더가 1행이므로 데이터는 2행부터
        class_code = (row.get("class_code") or "").strip()
        name = (row.get("class_name") or "").strip() or None
        instructor_name = (row.get("instructor_name") or "").strip()

        if not class_code or not instructor_name:
            failures.append(("classes.csv", line_no, row, "class_code 또는 instructor_name이 비어있음"))
            continue

        matches = conn.execute(
            "SELECT id FROM instructor WHERE name = ?", (instructor_name,)
        ).fetchall()
        if not matches:
            failures.append(("classes.csv", line_no, row, f"instructor '{instructor_name}'을 찾을 수 없음"))
            continue
        if len(matches) > 1:
            failures.append(("classes.csv", line_no, row, f"instructor '{instructor_name}' 이름이 여러 명과 일치(모호함)"))
            continue

        conn.execute(
            """
            INSERT INTO class (class_code, name, instructor_id) VALUES (?, ?, ?)
            ON CONFLICT(class_code) DO UPDATE SET name = excluded.name, instructor_id = excluded.instructor_id
            """,
            (class_code, name, matches[0][0]),
        )
        inserted += 1

    conn.commit()
    print(f"class: {inserted}건 입력/갱신, 실패 {len(failures)}건")
    return failures


def load_students(conn: sqlite3.Connection) -> list[Failure]:
    failures: list[Failure] = []
    rows = _read_rows(STUDENTS_CSV, {"student_name", "class_code"})
    if rows is None:
        return failures

    inserted = 0
    for line_no, row in enumerate(rows, start=2):
        student_name = (row.get("student_name") or "").strip()
        class_code = (row.get("class_code") or "").strip()

        if not student_name or not class_code:
            failures.append(("students.csv", line_no, row, "name 또는 class_code가 비어있음"))
            continue

        cls = conn.execute("SELECT id FROM class WHERE class_code = ?", (class_code,)).fetchone()
        if cls is None:
            failures.append(("students.csv", line_no, row, f"class '{class_code}'를 찾을 수 없음"))
            continue
        class_id = cls[0]

        existing = conn.execute(
            "SELECT id FROM student WHERE name = ? AND class_id = ?", (student_name, class_id)
        ).fetchone()
        if existing:
            continue  # 이미 적재됨 - 재실행 시 중복 방지

        conn.execute("INSERT INTO student (name, class_id) VALUES (?, ?)", (student_name, class_id))
        inserted += 1

    conn.commit()
    print(f"student: {inserted}명 신규 입력, 실패 {len(failures)}건")
    return failures


def main() -> int:
    conn = init_db()
    failures: list[Failure] = []
    # class를 먼저 넣어야 student가 class_code를 찾을 수 있다.
    failures += load_classes(conn)
    failures += load_students(conn)

    if failures:
        print("\n실패한 행:")
        for source, line_no, row, reason in failures:
            print(f"  [{source}:{line_no}] {reason} - {row}")

    print("\n현재 DB 상태:")
    for table in ("instructor", "class", "student"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}건")

    conn.close()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
