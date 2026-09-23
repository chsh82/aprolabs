"""어휘 레벨(자동 후보) 적재 파이프라인 회귀 테스트 - 로컬 R&D 전용.

tests/test_multiformat_quiz_import.py와 같은 방식(pytest 없음, 독립 스크립트,
PASS/FAIL 출력). validate()의 각 HARD 검사를 단위로 확인하고, 환경·경로
가드/트랜잭션 롤백/멱등 재실행/기존 테이블 불변/idiom.db 체크섬 불변까지
end-to-end로 검증한다.

실행:
    python tests/test_vocabulary_level_import.py
"""
from __future__ import annotations

import io
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


VERSION = "level_policy_v0.1"


def _level_row(content_id="LC1", vocab_level="0", level_status="PROVISIONAL_AUTO", **overrides):
    row = {
        "content_id": content_id, "sense_id": "AS1", "lexical_entry_id": "LE1", "lemma": "테스트어휘",
        "pos": "명사", "nikl_vocabulary_grade": "1등급", "semantic_field": "일반어",
        "vocab_level": vocab_level, "target_grade_band": "E1_2", "target_grade_label": "초등 1~2학년",
        "level_score": "0.3", "level_confidence": "0.7", "level_status": level_status,
        "boundary_flag": "0", "level_source": "TEST", "level_version": VERSION,
        "level_reason_json": '[{"feature":"test","value":"x","delta":0.1}]',
    }
    row.update(overrides)
    return row


def _write_csv(path: Path, rows: list[dict]) -> None:
    import csv
    fieldnames = list(_level_row().keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _seed_contents(db_path: Path, content_ids) -> None:
    conn = sqlite3.connect(db_path)
    for i, cid in enumerate(content_ids):
        conn.execute(
            """INSERT OR REPLACE INTO vocabulary_contents
               (content_id, lemma, generation_status, student_exposure, public_ready, source_version)
               VALUES (?, ?, 'PRIVATE_SERVER_READY', 0, 0, '2.1.29')""",
            (cid, f"낱말{i}"),
        )
    conn.commit()
    conn.close()


def run() -> bool:
    tmp_dir = Path(tempfile.mkdtemp(prefix="vl_test_"))
    test_db = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd_TESTONLY_vl.db"
    try:
        import scripts.vocab.import_vocabulary_levels as imp

        if test_db.exists():
            test_db.unlink()
        with mock.patch.dict(os.environ, {"VOCABULARY_QUIZ_DB_PATH": str(test_db)}):
            imp.ensure_schema()
        _seed_contents(test_db, ["LC1", "LC2", "LC3"])

        # ---------- 1. 정상 데이터는 HARD 검사 전부 통과 ----------
        good_rows = [_level_row(cid, str(i % 5)) for i, cid in enumerate(["LC1", "LC2", "LC3"])]
        with mock.patch.object(imp, "EXPECTED_ROW_COUNT", 3):
            hard, soft = imp.validate(good_rows, {"LC1", "LC2", "LC3"})
        check(hard == [], f"정상 데이터는 HARD 검사 전부 통과 (실패: {hard})")
        check(any("L5" in s for s in soft) and any("L6" in s for s in soft),
              "L5/L6 0건은 SOFT 경고로만 기록(실패 아님)")

        # ---------- 2. CSV 행 수 불일치 ----------
        with mock.patch.object(imp, "EXPECTED_ROW_COUNT", 5):
            hard, _ = imp.validate(good_rows, {"LC1", "LC2", "LC3"})
        check(any("행 수가" in h for h in hard), "CSV 행 수 불일치 탐지")

        # ---------- 3. content_id 중복 ----------
        dup_rows = good_rows + [good_rows[0]]
        hard, _ = imp.validate(dup_rows, {"LC1", "LC2", "LC3"})
        check(any("content_id 중복" in h for h in hard), "content_id 중복 탐지")

        # ---------- 4. DB에 없는 content_id / DB에는 있는데 CSV에 없음 ----------
        hard, _ = imp.validate(good_rows, {"LC1", "LC2", "LC3", "LC4"})
        check(any("CSV에 없는 content_id" in h for h in hard), "DB에는 있지만 CSV에 없는 content_id 탐지")

        missing_ref_rows = [_level_row("NOPE")]
        hard, _ = imp.validate(missing_ref_rows, {"LC1"})
        check(any("없는 content_id" in h and "vocabulary_contents에 없는" in h for h in hard),
              "vocabulary_contents에 없는 content_id 참조 탐지")

        # ---------- 5. vocab_level 범위 밖 ----------
        bad_level_rows = [_level_row("LC1", "9")]
        hard, _ = imp.validate(bad_level_rows, {"LC1"})
        check(any("vocab_level" in h for h in hard), "vocab_level 0~6 범위 밖 탐지")

        non_int_rows = [_level_row("LC1", "abc")]
        hard, _ = imp.validate(non_int_rows, {"LC1"})
        check(any("vocab_level" in h for h in hard), "vocab_level이 정수가 아니면 탐지")

        # ---------- 6. level_status 허용값 밖 ----------
        bad_status_rows = [_level_row("LC1", level_status="PUBLIC_READY")]
        hard, _ = imp.validate(bad_status_rows, {"LC1"})
        check(any("level_status" in h for h in hard), "level_status 허용값(PROVISIONAL_AUTO/REVIEW_BOUNDARY) 밖 탐지")

        # ---------- 7. level_version 불일치 ----------
        bad_version_rows = [_level_row("LC1", **{"level_version": "level_policy_v0.2"})]
        hard, _ = imp.validate(bad_version_rows, {"LC1"})
        check(any("level_version" in h for h in hard), "level_version이 기대값과 다르면 탐지")

        # ---------- 8. level_confidence 범위 밖 ----------
        bad_conf_rows = [_level_row("LC1", **{"level_confidence": "1.5"})]
        hard, _ = imp.validate(bad_conf_rows, {"LC1"})
        check(any("level_confidence" in h for h in hard), "level_confidence 0~1 범위 밖 탐지")

        # ---------- 9. boundary_flag 이상값 ----------
        bad_flag_rows = [_level_row("LC1", **{"boundary_flag": "2"})]
        hard, _ = imp.validate(bad_flag_rows, {"LC1"})
        check(any("boundary_flag" in h for h in hard), "boundary_flag가 0/1이 아니면 탐지")

        # ---------- 10. level_reason_json 파싱 실패 ----------
        bad_json_rows = [_level_row("LC1", **{"level_reason_json": "{invalid json"})]
        hard, _ = imp.validate(bad_json_rows, {"LC1"})
        check(any("level_reason_json" in h for h in hard), "level_reason_json 파싱 실패 탐지")

        # ---------- 11. 필수 필드 누락 ----------
        missing_field_rows = [_level_row(content_id="")]
        hard, _ = imp.validate(missing_field_rows, {"LC1"})
        check(any("필수 필드" in h for h in hard), "필수 필드 누락 탐지")

        # ---------- 12. 환경(APP_ENV) 가드 ----------
        with mock.patch.dict(os.environ, {"APP_ENV": "production"}):
            try:
                imp.assert_apply_allowed()
                check(False, "APP_ENV=production에서 --apply 차단")
            except SystemExit:
                check(True, "APP_ENV=production에서 --apply 차단")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("APP_ENV", None)
            try:
                imp.assert_apply_allowed()
                check(False, "APP_ENV 미설정에서 --apply 차단")
            except SystemExit:
                check(True, "APP_ENV 미설정에서 --apply 차단")
        with mock.patch.dict(os.environ, {"APP_ENV": "local_rnd"}):
            try:
                imp.assert_apply_allowed()
                check(True, "APP_ENV=local_rnd에서는 통과")
            except SystemExit:
                check(False, "APP_ENV=local_rnd에서는 통과")

        # ---------- 13. DB 경로 가드 ----------
        with mock.patch.dict(os.environ, {"VOCABULARY_QUIZ_DB_PATH": str(test_db)}):
            try:
                imp.assert_local_database_path(test_db)
                check(True, "VOCABULARY_QUIZ_DB_PATH와 일치하는 --database는 통과")
            except SystemExit:
                check(False, "VOCABULARY_QUIZ_DB_PATH와 일치하는 --database는 통과")
            try:
                imp.assert_local_database_path(imp.get_idiom_db_path())
                check(False, "idiom.db 경로는 거부됨")
            except SystemExit:
                check(True, "idiom.db 경로는 거부됨")

        # ---------- 14. 트랜잭션 롤백 / 15. 멱등 재실행 / 기존 테이블 불변 / idiom.db 불변 ----------
        csv_path = tmp_dir / "levels.csv"
        rows = [_level_row(cid, str(i % 5)) for i, cid in enumerate(["LC1", "LC2", "LC3"])]
        _write_csv(csv_path, rows)

        idiom_path = imp.get_idiom_db_path()
        idiom_before = imp.sha256_file(idiom_path) if idiom_path.exists() else None

        env = {"APP_ENV": "local_rnd", "VOCABULARY_QUIZ_DB_PATH": str(test_db)}
        with mock.patch.dict(os.environ, env), mock.patch.object(imp, "EXPECTED_ROW_COUNT", 3):
            rc = imp.run(csv_path, test_db, apply=False)
        conn = sqlite3.connect(test_db)
        n_dry = conn.execute("SELECT COUNT(*) FROM vocabulary_content_levels").fetchone()[0]
        conn.close()
        check(rc == 0 and n_dry == 0, "dry-run은 트랜잭션을 ROLLBACK하여 DB에 아무 것도 남기지 않음")

        with mock.patch.dict(os.environ, env), mock.patch.object(imp, "EXPECTED_ROW_COUNT", 3):
            rc1 = imp.run(csv_path, test_db, apply=True)
        conn = sqlite3.connect(test_db)
        n_1 = conn.execute("SELECT COUNT(*) FROM vocabulary_content_levels").fetchone()[0]
        contents_1 = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        conn.close()
        check(rc1 == 0 and n_1 == 3, f"--apply 첫 적재 - 레벨 3건 삽입 (실제 {n_1})")
        check(contents_1 == 3, "적재 중 기존 vocabulary_contents 행 수 불변")

        with mock.patch.dict(os.environ, env), mock.patch.object(imp, "EXPECTED_ROW_COUNT", 3):
            rc2 = imp.run(csv_path, test_db, apply=True)
        conn = sqlite3.connect(test_db)
        n_2 = conn.execute("SELECT COUNT(*) FROM vocabulary_content_levels").fetchone()[0]
        conn.close()
        check(rc2 == 0 and n_2 == 3, "upsert 재실행(멱등성) - 같은 CSV 재실행해도 행 수 그대로")

        idiom_after = imp.sha256_file(idiom_path) if idiom_path.exists() else None
        check(idiom_before == idiom_after, "기존 idiom.db 체크섬 불변(사자성어 DB에 전혀 영향 없음)")

        conn = sqlite3.connect(test_db)
        exposure, public = conn.execute(
            "SELECT COUNT(*), (SELECT COUNT(*) FROM vocabulary_contents WHERE public_ready=1) "
            "FROM vocabulary_contents WHERE student_exposure=1"
        ).fetchone()
        conn.close()
        check(exposure == 0 and public == 0, "적재 후에도 student_exposure/public_ready는 여전히 0건(학생 출제 미연결)")

        # 정책 개정(값이 바뀐 재적재) - updated로 잡히는지 확인
        revised_rows = [_level_row(cid, "4") for cid in ["LC1", "LC2", "LC3"]]
        _write_csv(csv_path, revised_rows)
        with mock.patch.dict(os.environ, env), mock.patch.object(imp, "EXPECTED_ROW_COUNT", 3):
            imp.run(csv_path, test_db, apply=True)
        conn = sqlite3.connect(test_db)
        levels_after_revision = [r[0] for r in conn.execute(
            "SELECT vocab_level FROM vocabulary_content_levels WHERE level_version=?", (VERSION,)
        ).fetchall()]
        conn.close()
        check(all(lv == 4 for lv in levels_after_revision), "같은 level_version 재적재 시 값 변경은 UPDATE로 반영됨")

    finally:
        for f in (REPO_ROOT / "data" / "vocab").glob("vocabulary_quiz_rnd_TESTONLY_vl*"):
            f.unlink(missing_ok=True)
        report = tmp_dir / "import_vocabulary_levels_failures.json"
        if report.exists():
            report.unlink()
        for f in Path(tmp_dir).glob("*"):
            f.unlink(missing_ok=True)
        tmp_dir.rmdir()

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
