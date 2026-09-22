"""초등 다유형 어휘 퀴즈(파일럿) 적재 파이프라인 회귀 테스트 - 로컬 R&D 전용.

tests/test_vocabulary_quiz_import.py와 같은 방식(pytest 없음, 독립 스크립트,
PASS/FAIL 출력). validate()의 각 HARD 검사를 단위로 확인하고, ZIP 손상/SHA
불일치/환경·경로 가드/트랜잭션 롤백/멱등 재실행/idiom.db 불변까지
end-to-end로 검증한다.

실행:
    python tests/test_multiformat_quiz_import.py
"""
from __future__ import annotations

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


SOURCE_VERSION = "2.1.29"


def _choice_item(item_id, item_type, content_id="TC1", options=None, correct_option=1, **overrides):
    row = {
        "item_id": item_id, "item_type": item_type, "content_id": content_id,
        "sense_id": "AS1", "lemma": "테스트어휘", "pos": "명사",
        "prompt": "뜻으로 알맞은 것은?",
        "options": options or ["뜻1", "뜻2", "뜻3", "뜻4"],
        "correct_option": correct_option,
        "explanation": "설명", "cognitive_level": 1, "source_version": SOURCE_VERSION, "qa_flags": [],
    }
    row.update(overrides)
    return row


def _cloze_item(item_id="T_D_1", content_id="TC2", answer_text="사과", accepted_answers=None, **overrides):
    row = {
        "item_id": item_id, "item_type": "CONTEXT_CLOZE", "content_id": content_id,
        "sense_id": "AS2", "lemma": answer_text, "pos": "명사",
        "prompt": "빈칸에 들어갈 낱말을 쓰세요.\n\n＿＿ 하나를 먹었다.",
        "answer_text": answer_text, "accepted_answers": accepted_answers or [answer_text],
        "input_hint": "2글자", "explanation": "설명", "cognitive_level": 3,
        "source_version": SOURCE_VERSION, "qa_flags": [],
    }
    row.update(overrides)
    return row


def _match_item(item_id="T_E_1", content_ids=None, words=None, definitions=None, answers=None, **overrides):
    content_ids = content_ids or ["TC1", "TC2", "TC3", "TC4"]
    words = words or ["낱말1", "낱말2", "낱말3", "낱말4"]
    definitions = definitions or ["뜻1", "뜻2", "뜻3", "뜻4"]
    answers = answers if answers is not None else dict(zip(words, definitions))
    row = {
        "item_id": item_id, "item_type": "MATCH_WORD_MEANING", "content_ids": content_ids,
        "sense_ids": ["AS1", "AS2", "AS3", "AS4"], "words": words, "definitions": definitions,
        "answers": answers, "prompt": "낱말과 뜻을 연결하세요.",
        "explanation": "설명", "cognitive_level": 1, "source_version": SOURCE_VERSION, "qa_flags": [],
    }
    row.update(overrides)
    return row


def _full_item_set():
    return [
        _choice_item("T_A_1", "MEANING_CHOICE", correct_option=1),
        _choice_item("T_B_1", "WORD_FROM_DEFINITION", correct_option=2),
        _choice_item("T_C_1", "CONTEXT_MEANING", correct_option=3),
        _cloze_item(),
        _match_item(),
    ]


def _meta_for(items):
    counts: dict[str, int] = {}
    for i in items:
        counts[i["item_type"]] = counts.get(i["item_type"], 0) + 1
    return {"name": "test", "source_version": SOURCE_VERSION, "seed": 999, "selected_words": 4,
            "item_type_counts": counts}


def _write_zip(path: Path, items: list[dict], validation_status="PASS", validation_errors=None,
               top_dir="vocabulary_quiz_multiformat_test", corrupt=False) -> None:
    payload = {"meta": _meta_for(items), "items": items}
    validation = {"status": validation_status, "errors": validation_errors or []}
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(f"{top_dir}/data/pilot_items_v1.json", json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        z.writestr(f"{top_dir}/data/pilot_validation_v1.json", json.dumps(validation, ensure_ascii=False).encode("utf-8"))
    if corrupt:
        data = path.read_bytes()
        path.write_bytes(data[: len(data) // 2])


def _seed_contents(db_path: Path, content_ids, generation_status="PRIVATE_SERVER_READY",
                    student_exposure=0, public_ready=0) -> None:
    conn = sqlite3.connect(db_path)
    for i, cid in enumerate(content_ids):
        conn.execute(
            """INSERT OR REPLACE INTO vocabulary_contents
               (content_id, lemma, generation_status, student_exposure, public_ready, source_version)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (cid, f"낱말{i}", generation_status, student_exposure, public_ready, SOURCE_VERSION),
        )
    conn.commit()
    conn.close()


def _seed_contents_with_lemma(db_path: Path, lemma_by_content_id: dict,
                               generation_status="PRIVATE_SERVER_READY") -> None:
    conn = sqlite3.connect(db_path)
    for cid, lemma in lemma_by_content_id.items():
        conn.execute(
            """INSERT OR REPLACE INTO vocabulary_contents
               (content_id, lemma, generation_status, student_exposure, public_ready, source_version)
               VALUES (?, ?, ?, 0, 0, ?)""",
            (cid, lemma, generation_status, SOURCE_VERSION),
        )
    conn.commit()
    conn.close()


def _build_crossword_entries() -> list[dict]:
    """10개 낱말이 지그재그로 한 칸씩 교차하는 체인(트리 구조, 교차 9개, 연결됨) -
    실제 파일럿 데이터 형태를 흉내 낸 최소 유효 픽스처."""
    words = ["가나", "나다", "다라", "라마", "마바", "바사", "사아", "아자", "자차", "차카"]
    entries = []
    row = col = 0
    for i, ans in enumerate(words):
        direction = "across" if i % 2 == 0 else "down"
        entries.append({
            "number": i + 1, "direction": direction, "row": row, "col": col, "length": len(ans),
            "content_id": f"CW_C{i}", "sense_id": f"CW_S{i}",
            "clue": f"{ans[0]} 시작 뜻풀이 {i}", "answer": ans,
        })
        if direction == "across":
            col += 1
        else:
            row += 1
    return entries


def _crossword_item(item_id="T_F_1", entries=None, **overrides) -> dict:
    import scripts.vocab.import_multiformat_quiz as imp
    entries = entries if entries is not None else _build_crossword_entries()
    cell_map, _ = imp._expand_crossword_entries(entries)
    cells = [{"row": r, "col": c, "number": None} for (r, c) in cell_map]
    for e in entries:
        for cell in cells:
            if cell["row"] == e["row"] and cell["col"] == e["col"]:
                cell["number"] = e["number"]
    rows = max((r for r, _ in cell_map), default=0) + 1
    cols = max((c for _, c in cell_map), default=0) + 1
    row = {
        "item_id": item_id, "item_type": "CROSSWORD",
        "content_ids": [e["content_id"] for e in entries],
        "sense_ids": [e["sense_id"] for e in entries],
        "prompt": "낱말을 읽고 십자말풀이를 완성하세요.",
        "grid": {"rows": rows, "cols": cols, "cells": cells},
        "entries": entries,
        "explanation": "설명", "cognitive_level": 3, "source_version": SOURCE_VERSION, "qa_flags": [],
    }
    row.update(overrides)
    return row


def _seed_crossword_contents(db_path: Path, item: dict) -> None:
    lemma_by_cid = {e["content_id"]: e["answer"] for e in item["entries"]}
    _seed_contents_with_lemma(db_path, lemma_by_cid)


def run() -> bool:
    tmp_dir = Path(tempfile.mkdtemp(prefix="mfq_test_"))
    test_db = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd_TESTONLY_mf.db"
    try:
        import scripts.vocab.import_multiformat_quiz as imp

        # ---------- 1. ZIP 손상 차단 ----------
        good_zip = tmp_dir / "good.zip"
        _write_zip(good_zip, _full_item_set())
        try:
            imp.extract_archive(good_zip, "test1")
            check(True, "정상 ZIP은 압축 해제 성공")
        except SystemExit:
            check(False, "정상 ZIP은 압축 해제 성공")

        bad_zip = tmp_dir / "bad.zip"
        _write_zip(bad_zip, _full_item_set(), corrupt=True)
        try:
            imp.extract_archive(bad_zip, "test1b")
            check(False, "손상된 ZIP은 SystemExit로 거부됨")
        except (SystemExit, zipfile.BadZipFile):
            check(True, "손상된 ZIP은 SystemExit로 거부됨")

        # ---------- 2. SHA-256 무결성(알려진 파일명일 때만 대조) ----------
        known_zip = tmp_dir / "vocabulary_quiz_multiformat_v1.zip"
        _write_zip(known_zip, _full_item_set())  # 내용이 달라 알려진 고정 해시와 다를 것
        with mock.patch.dict(imp.KNOWN_ARCHIVE_SHA256, {}, clear=False):
            # 알려진 고정값 그대로 두고, 내용이 다른 동일 파일명 zip은 해시 불일치로 거부돼야 함
            try:
                imp.verify_archive_sha256(known_zip)
                check(False, "알려진 파일명인데 SHA-256이 다르면 거부됨")
            except SystemExit:
                check(True, "알려진 파일명인데 SHA-256이 다르면 거부됨")
        unknown_name_zip = tmp_dir / "some_other_name.zip"
        _write_zip(unknown_name_zip, _full_item_set())
        try:
            imp.verify_archive_sha256(unknown_name_zip)
            check(True, "알려지지 않은 파일명은 SHA-256 대조를 건너뛰고 통과")
        except SystemExit:
            check(False, "알려지지 않은 파일명은 SHA-256 대조를 건너뛰고 통과")

        # ---------- content 시딩(원본 content_id 연결/자격 검사용) ----------
        if test_db.exists():
            test_db.unlink()
        with mock.patch.dict(os.environ, {"VOCABULARY_QUIZ_DB_PATH": str(test_db)}):
            imp.ensure_schema()
        _seed_contents(test_db, ["TC1", "TC2", "TC3", "TC4"])

        conn = sqlite3.connect(test_db)
        content_status = imp.load_content_status(conn)
        conn.close()

        # ---------- 3. 유형별 수량 정확성(meta 대조) ----------
        items = _full_item_set()
        meta = _meta_for(items)
        bad_meta = dict(meta, item_type_counts={**meta["item_type_counts"], "MEANING_CHOICE": 99})
        hard, soft = imp.validate(bad_meta, items, True, "ok", content_status)
        check(any("유형별 수량" in h for h in hard), "meta.item_type_counts 불일치 탐지")

        # ---------- 4. item_id 중복 없음 ----------
        dup_items = items + [items[0]]
        hard, soft = imp.validate(_meta_for(dup_items), dup_items, True, "ok", content_status)
        check(any("item_id 중복" in h for h in hard), "item_id 중복 탐지")

        # ---------- 5. 알 수 없는 유형 차단 ----------
        bad_type_items = [dict(items[0], item_type="UNKNOWN_TYPE")]
        hard, soft = imp.validate(_meta_for(bad_type_items), bad_type_items, True, "ok", content_status)
        check(any("알 수 없는 item_type" in h for h in hard), "알 수 없는 item_type 차단")

        # ---------- 6. 선택형 보기 4개/정답 범위/중복 ----------
        bad_opts = [_choice_item("BAD1", "MEANING_CHOICE", options=["a", "b", "c"])]
        hard, soft = imp.validate(_meta_for(bad_opts), bad_opts, True, "ok", content_status)
        check(any("보기 4개/정답 범위" in h for h in hard), "선택형 보기 4개 아님 탐지")

        bad_correct = [_choice_item("BAD2", "MEANING_CHOICE", correct_option=9)]
        hard, soft = imp.validate(_meta_for(bad_correct), bad_correct, True, "ok", content_status)
        check(any("보기 4개/정답 범위" in h for h in hard), "정답 번호 범위(1~4) 밖 탐지")

        dup_opts = [_choice_item("BAD3", "MEANING_CHOICE", options=["같음", "같음", "다름", "또다름"])]
        hard, soft = imp.validate(_meta_for(dup_opts), dup_opts, True, "ok", content_status)
        check(any("보기 중복" in h for h in hard), "선택형 보기 중복 탐지")

        # ---------- 7. 직접입력형 정답·허용답 ----------
        bad_cloze = [_cloze_item(answer_text="사과", accepted_answers=["다른말"])]
        hard, soft = imp.validate(_meta_for(bad_cloze), bad_cloze, True, "ok", content_status)
        check(any("직접입력형" in h for h in hard), "직접입력형 answer_text가 accepted_answers에 없으면 차단")

        empty_cloze = [_cloze_item(answer_text="")]
        hard, soft = imp.validate(_meta_for(empty_cloze), empty_cloze, True, "ok", content_status)
        check(any("직접입력형" in h for h in hard), "직접입력형 answer_text 누락 차단")

        # ---------- 8. 연결형 4낱말/4뜻/일대일 매핑 ----------
        bad_match_dup_def = [_match_item(definitions=["뜻1", "뜻1", "뜻3", "뜻4"],
                                          answers={"낱말1": "뜻1", "낱말2": "뜻1", "낱말3": "뜻3", "낱말4": "뜻4"})]
        hard, soft = imp.validate(_meta_for(bad_match_dup_def), bad_match_dup_def, True, "ok", content_status)
        check(any("연결형" in h for h in hard), "연결형 뜻 중복(일대일 아님) 탐지")

        bad_match_count = [_match_item(words=["낱말1", "낱말2", "낱말3"])]
        hard, soft = imp.validate(_meta_for(bad_match_count), bad_match_count, True, "ok", content_status)
        check(any("연결형" in h for h in hard), "연결형 낱말 4개 아님 탐지")

        # ---------- 9. source_version 불일치 ----------
        bad_version = [_choice_item("BADV", "MEANING_CHOICE", source_version="1.0.0")]
        hard, soft = imp.validate(_meta_for(bad_version), bad_version, True, "ok", content_status)
        check(any("source_version" in h for h in hard), "source_version 불일치 차단")

        # ---------- 10. 원본 content_id 연결 확인 ----------
        missing_ref = [_choice_item("MISS1", "MEANING_CHOICE", content_id="NOPE")]
        hard, soft = imp.validate(_meta_for(missing_ref), missing_ref, True, "ok", content_status)
        check(any("없는 원본 content_id" in h for h in hard), "존재하지 않는 content_id 참조 탐지")

        # ---------- 11. AUTO_HOLD/공개·노출 상태 원본 콘텐츠 차단 ----------
        _seed_contents(test_db, ["TC_HOLD"], generation_status="AUTO_HOLD")
        conn = sqlite3.connect(test_db)
        status_with_hold = imp.load_content_status(conn)
        conn.close()
        hold_ref = [_choice_item("HOLD1", "MEANING_CHOICE", content_id="TC_HOLD")]
        hard, soft = imp.validate(_meta_for(hold_ref), hold_ref, True, "ok", status_with_hold)
        check(any("AUTO_HOLD" in h for h in hard), "AUTO_HOLD 원본 콘텐츠 참조 차단")

        _seed_contents(test_db, ["TC_EXPOSED"], student_exposure=1)
        conn = sqlite3.connect(test_db)
        status_with_exposed = imp.load_content_status(conn)
        conn.close()
        exposed_ref = [_choice_item("EXP1", "MEANING_CHOICE", content_id="TC_EXPOSED")]
        hard, soft = imp.validate(_meta_for(exposed_ref), exposed_ref, True, "ok", status_with_exposed)
        check(any("AUTO_HOLD" in h for h in hard), "student_exposure!=0 원본 콘텐츠 참조 차단")

        # ---------- 12. 원본 파이프라인(누적) 검증 게이트 ----------
        hard, soft = imp.validate(_meta_for(items), items, False, "FAIL로 확인됨", content_status)
        check(any("원본 파이프라인 검증" in h for h in hard), "원본 검증 status!=PASS면 차단")

        # ---------- 13. 정상 데이터는 전부 통과 ----------
        hard, soft = imp.validate(_meta_for(items), items, True, "ok", content_status)
        check(hard == [], f"정상 데이터는 HARD 검사 전부 통과 (실패: {hard})")

        # ---------- 13.5 CROSSWORD 전용 검증 ----------
        import copy as _copy

        cw = _crossword_item()
        _seed_crossword_contents(test_db, cw)
        conn = sqlite3.connect(test_db)
        cw_content_status = imp.load_content_status(conn)
        conn.close()

        errs = imp.validate_crossword_item(cw, cw_content_status)
        check(errs == [], f"유효한 십자말 픽스처는 항목 검사 전부 통과 (실패: {errs})")
        errs = imp.validate_crossword_batch([cw] * 100)  # 세트 수 100개 흉내(내용은 동일해도 개수 체크만 확인)
        check(any("동일한 어휘 조합 중복" in e for e in errs), "십자말 100세트 시뮬레이션 - 동일 조합 반복 시 중복 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"] = bad["entries"][:9]
        check(any("entries가" in e and "10" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 entries 10개 아니면 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"][1]["answer"] = bad["entries"][0]["answer"]
        check(any("정답 중복" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 세트 내 정답 중복 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"][0]["clue"] = f"{bad['entries'][0]['answer']}을(를) 설명"
        check(any("단서에 정답" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 단서에 정답 노출 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"][0]["length"] = 99
        check(any("length" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 length와 정답 글자수 불일치 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"][0]["row"] = 999
        check(any("판 크기" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 시작 좌표가 판 크기 밖이면 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"][1]["answer"] = "XX"  # 교차 칸에서 entries[0]과 글자가 달라지도록
        check(any("교차 칸" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 교차 칸 글자 불일치 탐지")

        bad = _copy.deepcopy(cw)
        bad["grid"]["cells"].pop()
        check(any("선언 안 된 칸" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 entries가 쓰는데 grid.cells에 선언 안 된 칸 탐지")

        bad = _copy.deepcopy(cw)
        bad["grid"]["cells"].append({"row": 999, "col": 999, "number": None})
        check(any("어떤 entry도 쓰지 않는 칸" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 grid.cells에만 있고 안 쓰이는 칸 탐지")

        bad = _copy.deepcopy(cw)
        # 마지막 entry를 원점(0,0)에서 시작하는 별도 낱말로 바꿔 나머지 9개와 단절시킨다
        bad["entries"][9] = {
            "number": 10, "direction": "across", "row": 20, "col": 20, "length": 2,
            "content_id": "CW_C9", "sense_id": "CW_S9", "clue": "단절된 낱말", "answer": "타파",
        }
        bad["grid"]["cells"].extend([{"row": 20, "col": 20, "number": 10}, {"row": 20, "col": 21, "number": None}])
        check(any("연결망으로 이어지지 않음" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 연결 안 된 낱말 탐지")

        bad = _copy.deepcopy(cw)
        bad["content_ids"] = bad["content_ids"][:-1] + ["NOPE"]
        bad["entries"][-1]["content_id"] = "NOPE"
        check(any("원본 content_id" in e and "없음" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 존재하지 않는 content_id 참조 탐지")

        bad = _copy.deepcopy(cw)
        bad["entries"][0]["answer"] = "완전다름"
        bad["entries"][0]["length"] = 4
        check(any("lemma" in e for e in imp.validate_crossword_item(bad, cw_content_status)),
              "십자말 정답이 원본 lemma와 다르면 탐지")

        batch_errs = imp.validate_crossword_batch([cw])
        check(any("100개가 아님" in e for e in batch_errs), "십자말 세트 수가 100개 아니면 탐지")
        check(any("서로 다른 어휘 수" in e for e in batch_errs), "십자말 서로 다른 어휘 수(282개) 불일치 탐지")

        many_cw = []
        for i in range(7):
            c = _copy.deepcopy(cw)
            c["item_id"] = f"T_F_over_{i}"
            c["entries"][0]["content_id"] = "CW_SHARED"  # 7개 세트가 같은 content_id를 재사용(6회 초과)
            c["content_ids"] = [c["entries"][0]["content_id"]] + c["content_ids"][1:]
            many_cw.append(c)
        over_errs = imp.validate_crossword_batch(many_cw)
        check(any("재사용" in e and "초과" in e for e in over_errs), "십자말 어휘 재사용 6회 초과 탐지")

        # ---------- 14. 환경(APP_ENV) 가드 ----------
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

        # ---------- 15. DB 경로 가드 (idiom.db / 불일치 경로 차단) ----------
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
            try:
                imp.assert_local_database_path(REPO_ROOT / "some_other_dir" / "x.db")
                check(False, "VOCABULARY_QUIZ_DB_PATH와 다른 --database는 거부됨")
            except SystemExit:
                check(True, "VOCABULARY_QUIZ_DB_PATH와 다른 --database는 거부됨")

        # ---------- 16. 트랜잭션 롤백 / 17. 멱등 재실행 / idiom.db 불변 (end-to-end) ----------
        lifecycle_db = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd_TESTONLY_mf_lifecycle.db"
        if lifecycle_db.exists():
            lifecycle_db.unlink()
        with mock.patch.dict(os.environ, {"VOCABULARY_QUIZ_DB_PATH": str(lifecycle_db)}):
            imp.ensure_schema()
        _seed_contents(lifecycle_db, ["TC1", "TC2", "TC3", "TC4"])

        lifecycle_zip = tmp_dir / "lifecycle.zip"
        _write_zip(lifecycle_zip, _full_item_set())

        idiom_path = imp.get_idiom_db_path()
        idiom_before = imp.sha256_file(idiom_path) if idiom_path.exists() else None

        env = {"APP_ENV": "local_rnd", "VOCABULARY_QUIZ_DB_PATH": str(lifecycle_db)}
        with mock.patch.dict(os.environ, env):
            rc = imp.run(lifecycle_zip, lifecycle_db, "test-lifecycle", apply=False)
        conn = sqlite3.connect(lifecycle_db)
        n_items_dry = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        conn.close()
        check(rc == 0 and n_items_dry == 0, "dry-run은 트랜잭션을 ROLLBACK하여 DB에 아무 것도 남기지 않음")

        with mock.patch.dict(os.environ, env):
            rc1 = imp.run(lifecycle_zip, lifecycle_db, "test-lifecycle", apply=True)
        conn = sqlite3.connect(lifecycle_db)
        n_items_1 = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        conn.close()
        check(rc1 == 0 and n_items_1 == 5, f"--apply 첫 적재 - 문항 5건 삽입 (실제 {n_items_1})")

        with mock.patch.dict(os.environ, env):
            rc2 = imp.run(lifecycle_zip, lifecycle_db, "test-lifecycle", apply=True)
        conn = sqlite3.connect(lifecycle_db)
        n_items_2 = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        conn.close()
        check(rc2 == 0 and n_items_2 == 5, "upsert 재실행(멱등성) - 같은 배치 재실행해도 행 수 그대로")

        idiom_after = imp.sha256_file(idiom_path) if idiom_path.exists() else None
        check(idiom_before == idiom_after, "기존 idiom.db 체크섬 불변(사자성어 DB에 전혀 영향 없음)")

        conn = sqlite3.connect(lifecycle_db)
        exposure, public = conn.execute(
            "SELECT student_exposure, public_ready FROM vocabulary_contents WHERE content_id='TC1'"
        ).fetchone()
        conn.close()
        check(exposure == 0 and public == 0, "적재 중 원본 vocabulary_contents의 공개/노출 상태 변경 없음")

        # ---------- 18. 운영 DB 대상 차단(idiom.db) end-to-end ----------
        with mock.patch.dict(os.environ, {"VOCABULARY_QUIZ_DB_PATH": str(lifecycle_db)}):
            try:
                imp.run(lifecycle_zip, imp.get_idiom_db_path(), "test-lifecycle", apply=False)
                check(False, "idiom.db를 --database로 지정하면 즉시 차단")
            except SystemExit as e:
                check(e.code == 2, "idiom.db를 --database로 지정하면 즉시 차단")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        for name in ["vocabulary_multiformat_test1", "vocabulary_multiformat_test1b",
                     "vocabulary_multiformat_test-lifecycle"]:
            p = REPO_ROOT / "data" / "import" / name
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)
        for f in (REPO_ROOT / "data" / "import").glob("import_multiformat_failures_test*.json"):
            f.unlink(missing_ok=True)
        for f in (REPO_ROOT / "data" / "vocab").glob("vocabulary_quiz_rnd_TESTONLY_mf*"):
            f.unlink(missing_ok=True)

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
