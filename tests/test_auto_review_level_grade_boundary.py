"""`scripts/literacy/auto_review_level.py` L5/L6 학년 경계 갱신(L5=고1/L6=고2~3)
및 근거부족 판정 보류(review_status='보류') 회귀 테스트.

배경(`reports/schema_reading_phase10_73drafts_crosscheck_20260924.md` 3절,
`docs/literacy/07-학년경계정책-L5L6.md`): `LEVEL_TABLE`이 실제 Gemini
프롬프트에 그대로 주입되는데 옛 경계("고1~2"/"고3")를 담고 있어 지금도 새
판정을 옛 기준으로 계속 만들어내고 있었다. 이번 수정으로 L5=고1/L6=고2~3로
통일하고, AI가 학년 근거 없이 레벨만 추측한 경우(`grounded=false`)는
`review_status='검수완료'`로 바로 넘어가지 않고 `'보류'`로 남기게 했다.

이 테스트는 **읽기 전용/격리 전용**이다 - `data/literacy.db`는 전혀 열지
않는다. `save_result()` 검증은 `tempfile`로 만든 임시 sqlite DB에 최소
스키마(`terms` 테이블의 `save_result`가 실제로 쓰는 컬럼만)를 직접 만들어
쓰고 테스트 종료 시 삭제한다 - 리포지토리 DB에는 어떤 영향도 주지 않는다.

실행:
    python tests/test_auto_review_level_grade_boundary.py
"""
from __future__ import annotations

import io
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))

import auto_review_level as arl  # noqa: E402

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    _results.append((name, condition, detail))
    print(f"{_PASS if condition else _FAIL} {name}" + (f" - {detail}" if detail and not condition else ""))


# ---------------------------------------------------------------------------
# 1) LEVEL_TABLE / 프롬프트 텍스트에 새 경계가 실제로 들어 있는지
# ---------------------------------------------------------------------------

def test_level_table_boundary() -> None:
    print("\n=== 1) LEVEL_TABLE 새 경계 확인 ===")

    check("LEVEL_TABLE에 '5 | 고1 |' (고1 단독) 포함",
          "| 5 | 고1 |" in arl.LEVEL_TABLE, f"LEVEL_TABLE={arl.LEVEL_TABLE!r}")
    check("LEVEL_TABLE에 '6 | 고2~3 |' 포함",
          "| 6 | 고2~3 |" in arl.LEVEL_TABLE, f"LEVEL_TABLE={arl.LEVEL_TABLE!r}")
    check("LEVEL_TABLE에 옛 경계 '고1~2'가 더 이상 없음",
          "고1~2" not in arl.LEVEL_TABLE)
    check("LEVEL_TABLE에 옛 경계 '고3'(단독, level6)이 더 이상 없음",
          "| 6 | 고3 |" not in arl.LEVEL_TABLE)
    check("LEVEL_TABLE에 중3=level4 경계 그대로 보존",
          "| 4 | 중3 |" in arl.LEVEL_TABLE)


def test_prompt_contains_new_boundary_and_grounded() -> None:
    print("\n=== 2) build_prompt() 실제 텍스트에 새 경계·grounded 지시 포함 확인 ===")

    batch = [
        {"id": 1, "category": "속담", "headword": "테스트 속담", "definition": "테스트 정의"},
    ]
    prompt = arl.build_prompt(batch)

    check("프롬프트 본문에 '고1' 경계 포함", "고1" in prompt)
    check("프롬프트 본문에 '고2~3' 경계 포함", "고2~3" in prompt)
    check("프롬프트 본문에 옛 경계 '고1~2' 없음", "고1~2" not in prompt)
    check("프롬프트 본문에 옛 경계 '고3'(단독 레벨6 표기) 없음",
          "| 6 | 고3 |" not in prompt)
    check("프롬프트가 grounded 필드를 요구함", "grounded" in prompt)
    check("프롬프트에 grounded=false 지침(근거 부족 시 표시) 포함",
          "grounded=false" in prompt or "grounded는 반드시 false" in prompt)
    check("프롬프트 출력 JSON 예시에 grounded 키가 실제로 들어감",
          '"grounded"' in prompt)


# ---------------------------------------------------------------------------
# 2) 경계값 매핑 확인 (중3→L4, 고1→L5, 고2·3→L6)
# ---------------------------------------------------------------------------

def test_boundary_value_mapping() -> None:
    print("\n=== 3) 경계값 학년→레벨 매핑 확인 ===")

    # LEVEL_TABLE 문자열을 그대로 파싱해서 "학년 텍스트 -> level" 매핑을 만들고,
    # 사용자가 확정한 기준(중3->L4, 고1->L5, 고2/고3->L6)과 정확히 일치하는지 검증한다.
    rows = [line for line in arl.LEVEL_TABLE.splitlines() if line.startswith("| ") and "level" not in line and "---" not in line]
    mapping: dict[str, int] = {}
    for row in rows:
        cells = [c.strip() for c in row.strip("|").split("|")]
        level_str, grade_str = cells[0], cells[1]
        mapping[grade_str] = int(level_str)

    check("중3 -> level 4", mapping.get("중3") == 4, f"mapping={mapping}")
    check("고1 -> level 5", mapping.get("고1") == 5, f"mapping={mapping}")
    check("고2~3 -> level 6", mapping.get("고2~3") == 6, f"mapping={mapping}")
    check("고1이 level 6으로 잘못 매핑되지 않음", mapping.get("고1") != 6)
    check("고2~3이 level 5로 잘못 매핑되지 않음", mapping.get("고2~3") != 5)


# ---------------------------------------------------------------------------
# 3) save_result() — 근거 없는 판정이 실제로 보류(review_status='보류')로 남는지
# ---------------------------------------------------------------------------

def _make_temp_terms_db() -> tuple[sqlite3.Connection, Path]:
    tmp = Path(tempfile.mkstemp(suffix=".db", prefix="test_auto_review_level_")[1])
    conn = sqlite3.connect(tmp)
    conn.execute(
        """
        CREATE TABLE terms (
            id INTEGER PRIMARY KEY,
            headword TEXT,
            category TEXT,
            level INTEGER,
            grade_source TEXT,
            review_status TEXT,
            reviewed_at TEXT,
            note TEXT
        )
        """
    )
    for i in range(1, 5):
        conn.execute(
            "INSERT INTO terms (id, headword, category, level, grade_source, review_status, note) "
            "VALUES (?, ?, '속담', NULL, 'manual', '검수전', NULL)",
            (i, f"테스트항목{i}"),
        )
    conn.commit()
    return conn, tmp


def test_save_result_holds_ungrounded() -> None:
    print("\n=== 4) save_result() 보류 처리 확인 ===")

    conn, tmp_path = _make_temp_terms_db()
    try:
        now = "2026-09-24 00:00:00"

        # (a) grounded=True (기존 경로, 회귀 없음) -> 즉시 검수완료
        arl.save_result(conn, 1, 5, "근거 충분한 이유", now, grounded=True)
        row = conn.execute("SELECT level, review_status, grade_source FROM terms WHERE id=1").fetchone()
        check("grounded=True면 review_status='검수완료'로 즉시 확정",
              row == (5, "검수완료", "auto"), f"got={row}")

        # (b) grounded=False (근거 부족) -> 검수완료로 바로 안 넘어가고 '보류'
        arl.save_result(conn, 2, 6, "뜻으로 대략 짐작한 근거 부족 사례", now, grounded=False)
        row = conn.execute("SELECT level, review_status, grade_source FROM terms WHERE id=2").fetchone()
        check("grounded=False면 level은 저장되지만 review_status='검수완료'가 아님",
              row is not None and row[0] == 6 and row[1] != "검수완료", f"got={row}")
        check("grounded=False면 review_status가 정확히 '보류'",
              row == (6, "보류", "auto"), f"got={row}")
        note = conn.execute("SELECT note FROM terms WHERE id=2").fetchone()[0]
        check("보류 사유가 note에 남음('근거부족' 텍스트 포함)",
              "근거부족" in note, f"note={note!r}")

        # (c) grounded 인자 기본값(생략 시)은 True - 기존 호출부(레거시 스타일) 회귀 없음
        arl.save_result(conn, 3, 4, "기본값 확인", now)
        row = conn.execute("SELECT level, review_status FROM terms WHERE id=3").fetchone()
        check("grounded 인자를 생략하면 기존과 동일하게 '검수완료'(하위호환)",
              row == (4, "검수완료"), f"got={row}")

        # (d) level=None(해당없음)은 grounded 값과 무관하게 항상 '제외' (회귀 없음)
        arl.save_result(conn, 4, None, "교육과정에 부적합", now, grounded=False)
        row = conn.execute("SELECT level, review_status FROM terms WHERE id=4").fetchone()
        check("level=None이면 grounded 값과 무관하게 review_status='제외'",
              row == (None, "제외"), f"got={row}")
    finally:
        conn.close()
        try:
            tmp_path.unlink(missing_ok=True)
        except PermissionError:
            # Windows에서 방금 닫은 sqlite 파일이 잠깐 잠겨 있을 수 있다 - 임시
            # 파일이라 정리 실패해도 테스트 결과나 리포지토리에는 영향 없다.
            pass


def test_main_loop_defaults_missing_grounded_field_to_hold() -> None:
    print("\n=== 5) 메인 루프: AI 응답에 grounded 필드가 없을 때 안전 기본값(보류) ===")

    # main()의 grounded 파싱 로직을 그대로 재현해서 검증한다(실제 Gemini 호출 없이
    # 순수 로직만 검사 - 네트워크/키 필요 없음).
    def parse_grounded(result: dict) -> bool:
        level = result.get("level")
        return bool(result.get("grounded", False)) if level is not None else True

    check("grounded 필드가 있고 True면 그대로 True",
          parse_grounded({"level": 5, "grounded": True}) is True)
    check("grounded 필드가 있고 False면 그대로 False",
          parse_grounded({"level": 5, "grounded": False}) is False)
    check("grounded 필드가 아예 없고 level이 있으면 안전 기본값 False(보류)",
          parse_grounded({"level": 5}) is False)
    check("level=None(해당없음)이면 grounded 필드 유무와 무관하게 True(제외 경로, 보류 아님)",
          parse_grounded({"level": None}) is True)


def main() -> int:
    test_level_table_boundary()
    test_prompt_contains_new_boundary_and_grounded()
    test_boundary_value_mapping()
    test_save_result_holds_ungrounded()
    test_main_loop_defaults_missing_grounded_field_to_hold()

    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_fail = len(_results) - n_pass
    print(f"\n{n_pass}/{len(_results)} passed" + (f", {n_fail} FAILED" if n_fail else ""))
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
