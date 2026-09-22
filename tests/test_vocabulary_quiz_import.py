"""일반 어휘 퀴즈 적재 파이프라인 회귀 테스트 - 로컬 R&D 전용.

pytest 없이(이 저장소 관례상 미설치) 독립 실행 스크립트로 작성한다 -
tests/test_segmenter_regression.py와 같은 PASS/WARN/FAIL 출력 방식을 따른다.

실행:
    python tests/test_vocabulary_quiz_import.py
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import zipfile
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


CONTENT_FIELDS = ["content_id", "batch_id", "sense_id", "lexical_entry_id", "lemma", "pos",
                  "canonical_definition", "student_definition", "example_sentence",
                  "example_target_form", "generation_method", "qa_method", "generation_status",
                  "student_exposure", "public_ready", "quality_batch_id", "hold_reason", "merge_source"]
ITEM_FIELDS = ["item_id", "content_id", "batch_id", "sense_id", "lemma", "item_type", "stem",
               "option_1", "option_2", "option_3", "option_4", "correct_option", "explanation",
               "generation_status", "student_exposure", "public_ready", "quality_batch_id", "merge_source"]


def _content_row(**overrides):
    row = {
        "content_id": "C1", "batch_id": "B1", "sense_id": "S1", "lexical_entry_id": "L1",
        "lemma": "테스트어휘", "pos": "명사", "canonical_definition": "정의1",
        "student_definition": "쉬운정의1", "example_sentence": "테스트어휘를 씁니다.",
        "example_target_form": "테스트어휘", "generation_method": "M", "qa_method": "Q",
        "generation_status": "PRIVATE_SERVER_READY", "student_exposure": "0", "public_ready": "0",
        "quality_batch_id": "", "hold_reason": "", "merge_source": "X",
    }
    row.update(overrides)
    return row


def _item_row(**overrides):
    row = {
        "item_id": "I1", "content_id": "C1", "batch_id": "B1", "sense_id": "S1",
        "lemma": "테스트어휘", "item_type": "TYPE_A", "stem": "뜻은?",
        "option_1": "쉬운정의1", "option_2": "오답1", "option_3": "오답2", "option_4": "오답3",
        "correct_option": "1", "explanation": "설명", "generation_status": "PRIVATE_SERVER_READY",
        "student_exposure": "0", "public_ready": "0", "quality_batch_id": "", "merge_source": "X",
    }
    row.update(overrides)
    return row


def _write_zip(path: Path, content_rows: list[dict], item_rows: list[dict],
               cumulative_verification="PASS", corrupt=False) -> None:
    def _csv_bytes(fields, rows):
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
        return buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(path, "w") as z:
        z.writestr("server_staging_candidate_content_v2.1.29.csv", _csv_bytes(CONTENT_FIELDS, content_rows))
        z.writestr("server_staging_candidate_items_v2.1.29.csv", _csv_bytes(ITEM_FIELDS, item_rows))
        z.writestr("cumulative_verification_v2.1.29.json",
                   json.dumps({"verification": cumulative_verification}).encode("utf-8"))
    if corrupt:
        # zip 뒷부분을 잘라내 손상시킨다
        data = path.read_bytes()
        path.write_bytes(data[: len(data) // 2])


def run() -> bool:
    tmp_dir = Path(tempfile.mkdtemp(prefix="vq_test_"))
    try:
        import scripts.vocab.import_vocabulary_quiz as imp

        # ---------- 1. ZIP 검증 테스트 ----------
        good_zip = tmp_dir / "good.zip"
        _write_zip(good_zip, [_content_row()], [_item_row()])
        try:
            imp.extract_archive(good_zip, "test1")
            check(True, "정상 ZIP은 압축 해제 성공")
        except SystemExit:
            check(False, "정상 ZIP은 압축 해제 성공")

        bad_zip = tmp_dir / "bad.zip"
        _write_zip(bad_zip, [_content_row()], [_item_row()], corrupt=True)
        try:
            imp.extract_archive(bad_zip, "test1b")
            check(False, "손상된 ZIP은 SystemExit로 거부됨")
        except (SystemExit, zipfile.BadZipFile):
            check(True, "손상된 ZIP은 SystemExit로 거부됨")

        # ---------- 2. CSV 파싱 테스트 ----------
        extract_dir = tmp_dir / "extract2"
        _write_zip(tmp_dir / "parse.zip", [_content_row(lemma="가루약")], [_item_row()])
        extract_dir = imp.extract_archive(tmp_dir / "parse.zip", "test2")
        rows = imp.load_csv(extract_dir / imp.CONTENT_CSV)
        check(len(rows) == 1 and rows[0]["lemma"] == "가루약", "CSV 파싱 - 필드값 정확히 읽힘")

        # ---------- 3. 필드 매핑 테스트 ----------
        expected_content_cols = set(imp.CONTENT_UPDATE_FIELDS) | {"content_id"}
        csv_cols = set(CONTENT_FIELDS)
        check(expected_content_cols <= csv_cols, "콘텐츠 필드 매핑 - CSV 컬럼이 DB 매핑 필드를 모두 포함")
        expected_item_cols = set(imp.ITEM_UPDATE_FIELDS) | {"item_id", "content_id"}
        check(expected_item_cols <= set(ITEM_FIELDS), "문항 필드 매핑 - CSV 컬럼이 DB 매핑 필드를 모두 포함")

        # ---------- 4. 콘텐츠·문항 연결(고아 방지) 테스트 ----------
        hard, soft = imp.validate([_content_row()], [_item_row(content_id="NOPE")], True, "ok")
        check(any("콘텐츠 없는 문항" in h for h in hard), "고아 문항(콘텐츠 없음) 탐지")
        hard, soft = imp.validate([_content_row()], [], True, "ok")
        check(any("문항 없는 콘텐츠" in h for h in hard), "문항 없는 콘텐츠 탐지")

        # ---------- 5. 중복 방지 테스트 ----------
        hard, soft = imp.validate([_content_row(), _content_row()], [_item_row()], True, "ok")
        check(any("중복" in h for h in hard), "content_id 중복 탐지")

        # ---------- 6. AUTO_HOLD 제외 테스트 ----------
        hard, soft = imp.validate([_content_row(generation_status="AUTO_HOLD")], [_item_row()], True, "ok")
        check(any("AUTO_HOLD" in h for h in hard), "AUTO_HOLD 유입 차단")

        # ---------- 7. 학생 노출 차단 테스트 ----------
        hard, soft = imp.validate([_content_row(student_exposure="1")], [_item_row()], True, "ok")
        check(any("student_exposure" in h for h in hard), "student_exposure!=0 차단")
        hard, soft = imp.validate([_content_row(public_ready="1")], [_item_row()], True, "ok")
        check(any("public_ready" in h for h in hard), "public_ready!=0 차단")

        # ---------- 8. 누적 검증 게이트 테스트 ----------
        hard, soft = imp.validate([_content_row()], [_item_row()], False, "FAIL로 확인됨")
        check(any("누적 검증" in h for h in hard), "누적 검증 JSON이 PASS 아니면 차단")

        # ---------- 9. 허용된 DB 경로(VOCABULARY_QUIZ_DB_PATH와 일치) / 10. 운영 경로 차단 테스트 ----------
        ok_path = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd_TESTONLY.db"
        with mock.patch.dict(os.environ, {"VOCABULARY_QUIZ_DB_PATH": str(ok_path)}):
            try:
                imp.assert_local_database_path(ok_path)
                check(True, "VOCABULARY_QUIZ_DB_PATH와 일치하는 --database는 통과")
            except SystemExit:
                check(False, "VOCABULARY_QUIZ_DB_PATH와 일치하는 --database는 통과")

            try:
                imp.assert_local_database_path(imp.get_idiom_db_path())
                check(False, "idiom.db 경로는 거부됨")
            except SystemExit:
                check(True, "idiom.db 경로는 거부됨")

            try:
                imp.assert_local_database_path(REPO_ROOT / "some_other_dir" / "x.db")
                check(False, "VOCABULARY_QUIZ_DB_PATH와 다른 --database는 거부됨")
            except SystemExit:
                check(True, "VOCABULARY_QUIZ_DB_PATH와 다른 --database는 거부됨")

        # ---------- 11. 환경(APP_ENV) 가드 테스트 - OS 종류가 아니라 명시적 환경변수 ----------
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
        with mock.patch.dict(os.environ, {"APP_ENV": "research"}):
            try:
                imp.assert_apply_allowed()
                check(True, "APP_ENV=research에서는 통과")
            except SystemExit:
                check(False, "APP_ENV=research에서는 통과")

        # ---------- 12. 트랜잭션 롤백 / 13. upsert 재실행(멱등성) / idiom.db 불변 테스트 ----------
        test_db = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd_TESTONLY_lifecycle.db"
        if test_db.exists():
            test_db.unlink()
        test_zip = tmp_dir / "lifecycle.zip"
        _write_zip(test_zip, [_content_row()], [_item_row()])

        idiom_path = imp.get_idiom_db_path()
        idiom_checksum_before_all = imp.sha256_file(idiom_path) if idiom_path.exists() else None

        env = {"APP_ENV": "local_rnd", "VOCABULARY_QUIZ_DB_PATH": str(test_db)}
        with mock.patch.dict(os.environ, env):
            rc = imp.run(test_zip, test_db, "test-lifecycle", apply=False)
        conn = sqlite3.connect(test_db)
        n_content = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        n_batches = conn.execute("SELECT COUNT(*) FROM vocabulary_import_batches").fetchone()[0]
        conn.close()
        check(rc == 0 and n_content == 0 and n_batches == 0,
              "dry-run은 트랜잭션을 ROLLBACK하여 DB에 아무 것도 남기지 않음")

        with mock.patch.dict(os.environ, env):
            rc1 = imp.run(test_zip, test_db, "test-lifecycle", apply=True)
        conn = sqlite3.connect(test_db)
        n_content_1 = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        conn.close()
        check(rc1 == 0 and n_content_1 == 1, "--apply 첫 적재 - 콘텐츠 1건 삽입")

        with mock.patch.dict(os.environ, env):
            rc2 = imp.run(test_zip, test_db, "test-lifecycle", apply=True)
        conn = sqlite3.connect(test_db)
        n_content_2 = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        conn.close()
        check(rc2 == 0 and n_content_2 == 1, "upsert 재실행(멱등성) - 같은 배치 재실행해도 행 수 그대로")

        idiom_checksum_after_all = imp.sha256_file(idiom_path) if idiom_path.exists() else None
        check(idiom_checksum_before_all == idiom_checksum_after_all,
              "기존 idiom.db 체크섬 불변(사자성어 DB에 전혀 영향 없음)")

        # 학생 노출값을 운영 승격 흉내로 바꾼 뒤 재적재해도 보존되는지
        conn = sqlite3.connect(test_db)
        conn.execute("UPDATE vocabulary_contents SET student_exposure=1, public_ready=1 WHERE content_id='C1'")
        conn.commit()
        conn.close()
        with mock.patch.dict(os.environ, env):
            imp.run(test_zip, test_db, "test-lifecycle", apply=True)
        conn = sqlite3.connect(test_db)
        exposure, public = conn.execute(
            "SELECT student_exposure, public_ready FROM vocabulary_contents WHERE content_id='C1'"
        ).fetchone()
        conn.close()
        check(exposure == 1 and public == 1,
              "재적재 시 이미 승격된 student_exposure/public_ready를 되돌리지 않음")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        for name in ["vocabulary_master_test1", "vocabulary_master_test1b", "vocabulary_master_test2",
                     "vocabulary_master_test-lifecycle"]:
            p = REPO_ROOT / "data" / "import" / name
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)
        for f in (REPO_ROOT / "data" / "import").glob("import_failures_vtest*.json"):
            f.unlink(missing_ok=True)
        for f in (REPO_ROOT / "data" / "vocab").glob("vocabulary_quiz_rnd_TESTONLY*"):
            f.unlink(missing_ok=True)

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
