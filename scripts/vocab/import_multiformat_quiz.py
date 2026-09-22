"""초등 다유형 어휘 퀴즈(파일럿) ZIP 적재 CLI - local_rnd/research 전용, production 거부.

<archive>(예: data/import/vocabulary_quiz_multiformat_v1.zip)를 압축 해제해
내부 JSON(예: data/pilot_items_v1.json - {"meta":..., "items":[...]})을
VOCABULARY_QUIZ_DB_PATH가 가리키는 DB(app/vocab/idiom.db와 완전히 별개)의
vocabulary_multiformat_items 테이블에 item_id 기준 upsert한다.

기존 scripts/vocab/import_vocabulary_quiz.py와 같은 안전장치를 그대로 따른다:
기본은 항상 dry-run(트랜잭션 ROLLBACK), --apply는 APP_ENV가 local_rnd/research일
때만 허용, --database가 idiom.db거나 VOCABULARY_QUIZ_DB_PATH와 다르면 거부,
HARD 검증 실패 시 전체 롤백, 재실행 시 unchanged 처리(멱등).

실행 예 (로컬, dry-run):
    APP_ENV=local_rnd VOCABULARY_QUIZ_DB_PATH=data/vocab/vocabulary_quiz_rnd.db \\
    python scripts/vocab/import_multiformat_quiz.py \\
        --archive data/import/vocabulary_quiz_multiformat_v1.zip \\
        --database data/vocab/vocabulary_quiz_rnd.db --version v1

    (위 + --apply 로 실제 적재)
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import io
import json
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocabulary_quiz.db import ensure_schema, get_db_path  # noqa: E402
from app.vocab.db import get_db_path as get_idiom_db_path  # noqa: E402

DEFAULT_ARCHIVE = REPO_ROOT / "data" / "import" / "vocabulary_quiz_multiformat_v1.zip"
DEFAULT_DATABASE = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd.db"
DEFAULT_VERSION = "v1"  # 파일럿 배치 라벨(아카이브/버전 식별용) - 콘텐츠 source_version("2.1.29")과는 다른 축
EXPECTED_SOURCE_VERSION = "2.1.29"  # 문항이 참조하는 원본 콘텐츠 버전 - 이 값과 다르면 거부
EXTRACT_ROOT = REPO_ROOT / "data" / "import"

# 알려진 배치의 원본 무결성 고정값 - 파일명이 일치하면 검증하고, 모르는
# 파일명(향후 배치)이면 건너뛴다(하드코딩된 해시 하나로 미래 버전을 막지 않기 위함).
KNOWN_ARCHIVE_SHA256 = {
    # 최종 패키지(v1, 1,289건 - 기존 1,189건 + CROSSWORD 100세트)로 교체됨.
    # 같은 파일명을 재사용하는 이전 배치(1,189건짜리)는 더 이상 이 고정값과 맞지 않는다 -
    # 그건 이미 서버/로컬에 적재 완료된 구버전이라 재검증 대상이 아니다.
    "vocabulary_quiz_multiformat_v1.zip":
        "c645b4f0f264403182f44d8615cfc7860196cbd8e3c8c50339a8028fcc2143b6",
}

ITEM_TYPES = (
    "MEANING_CHOICE", "WORD_FROM_DEFINITION", "CONTEXT_MEANING",
    "CONTEXT_CLOZE", "MATCH_WORD_MEANING", "CROSSWORD",
)
CHOICE_TYPES = ("MEANING_CHOICE", "WORD_FROM_DEFINITION", "CONTEXT_MEANING")
MULTI_CONTENT_TYPES = ("MATCH_WORD_MEANING", "CROSSWORD")  # source_content_id 대신 *_ids_json을 쓰는 유형
ELIGIBLE_CONTENT_STATUSES = ("PRIVATE_SERVER_READY", "PRIVATE_SERVER_READY_CANDIDATE")
CROSSWORD_EXPECTED_SETS = 100
CROSSWORD_ENTRIES_PER_SET = 10
CROSSWORD_MIN_INTERSECTIONS = 9
CROSSWORD_DISTINCT_VOCAB = 282
CROSSWORD_MAX_REUSE = 6


# ==================== 환경/경로 가드 (import_vocabulary_quiz.py와 동일 패턴) ====================

_ALLOWED_APPLY_ENVS = ("local_rnd", "research")


def assert_apply_allowed() -> None:
    import os
    app_env = os.environ.get("APP_ENV", "")
    if app_env not in _ALLOWED_APPLY_ENVS:
        print(f"거부: APP_ENV={app_env!r} 에서는 --apply를 허용하지 않습니다 "
              f"({', '.join(_ALLOWED_APPLY_ENVS)}만 허용). production이거나 APP_ENV 미설정이면 여기서 막힙니다.",
              file=sys.stderr)
        sys.exit(2)
    print(f"환경 확인: APP_ENV={app_env!r} (실제 적재 허용)")


def assert_local_database_path(db_path: Path) -> None:
    resolved = db_path.resolve()
    idiom_path = get_idiom_db_path().resolve()
    if resolved == idiom_path or resolved.name == "idiom.db":
        print(f"거부: --database가 기존 사자성어 DB(idiom.db)를 가리킵니다: {resolved}", file=sys.stderr)
        sys.exit(2)

    expected = get_db_path().resolve()
    if resolved != expected:
        print(f"거부: --database({resolved})가 VOCABULARY_QUIZ_DB_PATH로 지정된 공식 경로"
              f"({expected})와 다릅니다.", file=sys.stderr)
        sys.exit(2)
    print(f"DB 경로 확인: {resolved} (idiom.db 아님, VOCABULARY_QUIZ_DB_PATH와 일치)")


# ==================== 원본 파일 처리 ====================

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_archive_sha256(archive: Path) -> str:
    digest = sha256_file(archive)
    expected = KNOWN_ARCHIVE_SHA256.get(archive.name)
    if expected is None:
        print(f"원본 ZIP SHA-256: {digest} (파일명 {archive.name!r}은 알려진 고정값이 없어 대조 생략)")
    elif digest != expected:
        raise SystemExit(
            f"치명적 오류: {archive.name}의 SHA-256이 알려진 값과 다릅니다.\n"
            f"  기대값: {expected}\n  실제값: {digest}"
        )
    else:
        print(f"원본 ZIP SHA-256 확인: {digest} (알려진 고정값과 일치)")
    return digest


def extract_archive(zip_path: Path, version: str) -> Path:
    """원본 ZIP은 건드리지 않고, data/import/vocabulary_multiformat_<version>/에 압축 해제한다."""
    try:
        with zipfile.ZipFile(zip_path) as z:
            bad = z.testzip()
            if bad is not None:
                raise SystemExit(f"치명적 오류: ZIP이 손상되었습니다 (손상된 파일: {bad})")
            extract_dir = EXTRACT_ROOT / f"vocabulary_multiformat_{version}"
            extract_dir.mkdir(parents=True, exist_ok=True)
            z.extractall(extract_dir)
    except zipfile.BadZipFile as e:
        raise SystemExit(f"치명적 오류: ZIP이 손상되었습니다: {e}")
    return extract_dir


def find_items_json(extract_dir: Path) -> Path:
    matches = sorted(extract_dir.rglob("pilot_items_v*.json"))
    if not matches:
        raise SystemExit(f"치명적 오류: {extract_dir} 안에서 pilot_items_v*.json을 찾지 못했습니다.")
    if len(matches) > 1:
        raise SystemExit(f"치명적 오류: pilot_items_v*.json이 여러 개 발견됨: {matches}")
    return matches[0]


def find_validation_json(extract_dir: Path) -> Path | None:
    matches = sorted(extract_dir.rglob("pilot_validation_v*.json"))
    return matches[0] if matches else None


def load_items_json(path: Path) -> dict:
    raw = path.read_bytes()
    try:
        raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as e:
        raise SystemExit(f"치명적 오류: {path.name} 이 UTF-8이 아닙니다: {e}")
    return json.loads(raw.decode("utf-8"))


def check_upstream_validation(extract_dir: Path) -> tuple[bool, str]:
    path = find_validation_json(extract_dir)
    if path is None:
        return False, "pilot_validation_v*.json 파일이 없습니다"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return False, f"{path.name} 파싱 실패: {e}"
    status = data.get("status")
    ok = isinstance(status, str) and status.strip().upper() == "PASS"
    errors = data.get("errors") or []
    if ok and errors:
        ok = False
        return ok, f"{path.name} status=PASS이지만 errors가 비어있지 않음: {errors}"
    return ok, f"{path.name} status={status!r}, errors={len(errors)}건"


# ==================== 검증 ====================

def _expand_crossword_entries(entries: list[dict]) -> tuple[dict[tuple[int, int], str], list[str]]:
    """entries를 좌표별 정답 글자로 펼친다. 교차 칸에서 서로 다른 글자가 겹치면
    오류 메시지를 함께 반환한다(칸 값은 나중에 쓴 entry 것으로 남지만, 오류가
    있으면 어차피 HARD 실패라 적재되지 않는다)."""
    cell_map: dict[tuple[int, int], str] = {}
    errors: list[str] = []
    for entry in entries:
        direction = entry.get("direction")
        if direction == "across":
            dr, dc = 0, 1
        elif direction == "down":
            dr, dc = 1, 0
        else:
            errors.append(f"entry {entry.get('number')}: 알 수 없는 방향 {direction!r}")
            continue
        answer = entry.get("answer") or ""
        row, col = entry.get("row"), entry.get("col")
        if row is None or col is None:
            errors.append(f"entry {entry.get('number')}: row/col 없음")
            continue
        for idx, ch in enumerate(answer):
            pos = (row + dr * idx, col + dc * idx)
            if pos in cell_map and cell_map[pos] != ch:
                errors.append(f"entry {entry.get('number')}: 교차 칸 {pos} 글자 불일치 "
                               f"({cell_map[pos]!r} vs {ch!r})")
            cell_map[pos] = ch
    return cell_map, errors


def _build_answer_payload(item: dict) -> dict:
    t = item["item_type"]
    if t in CHOICE_TYPES:
        return {"correct_option": item.get("correct_option")}
    if t == "CONTEXT_CLOZE":
        return {
            "answer_text": item.get("answer_text"),
            "accepted_answers": item.get("accepted_answers"),
            "input_hint": item.get("input_hint"),
        }
    if t == "MATCH_WORD_MEANING":
        return {
            "words": item.get("words"),
            "definitions": item.get("definitions"),
            "answers": item.get("answers"),
        }
    if t == "CROSSWORD":
        cell_map, _ = _expand_crossword_entries(item.get("entries") or [])
        return {
            "cells": {f"{r},{c}": ch for (r, c), ch in cell_map.items()},
            "entries": [
                {"number": e.get("number"), "direction": e.get("direction"), "row": e.get("row"),
                 "col": e.get("col"), "length": e.get("length"), "clue": e.get("clue"),
                 "answer": e.get("answer")}
                for e in item.get("entries") or []
            ],
        }
    return {}


def _build_public_payload(item: dict) -> dict:
    """정답이 빠진, 문제 조회 API가 그대로 내려줘도 되는 표시용 정보."""
    t = item["item_type"]
    if t in CHOICE_TYPES:
        return {"options": item.get("options")}
    if t == "CONTEXT_CLOZE":
        return {"input_hint": item.get("input_hint")}
    if t == "MATCH_WORD_MEANING":
        return {"words": item.get("words"), "definitions": item.get("definitions")}
    if t == "CROSSWORD":
        grid = item.get("grid") or {}
        cells = [{"row": c.get("row"), "col": c.get("col"), "number": c.get("number")}
                 for c in grid.get("cells") or []]
        entries_public = [
            {"number": e.get("number"), "direction": e.get("direction"), "row": e.get("row"),
             "col": e.get("col"), "length": e.get("length"), "clue": e.get("clue")}
            for e in item.get("entries") or []
        ]
        return {"rows": grid.get("rows"), "cols": grid.get("cols"), "cells": cells, "entries": entries_public}
    return {}


def validate_crossword_item(item: dict, db_content_status: dict[str, tuple]) -> list[str]:
    """tests/test_generation.py(원본 ZIP)의 test_crosswords_have_ten_connected_source_words와
    같은 알고리즘(좌표 펼치기 -> 선언된 활성칸과 대조 -> 교차 그래프 연결성 BFS)을 그대로
    적재 게이트에도 적용한다 - 원본 검증을 다시 믿지 않고 우리 쪽에서도 독립적으로 확인."""
    iid = item.get("item_id")
    errs: list[str] = []
    entries = item.get("entries") or []
    grid = item.get("grid") or {}

    if len(entries) != CROSSWORD_ENTRIES_PER_SET:
        errs.append(f"{iid}: entries가 {CROSSWORD_ENTRIES_PER_SET}개가 아님(실제 {len(entries)}개)")
        return errs  # 개수부터 틀리면 이후 좌표 기반 검사는 의미가 없다

    answers = [e.get("answer") for e in entries]
    if len(set(answers)) != CROSSWORD_ENTRIES_PER_SET:
        errs.append(f"{iid}: 세트 내 정답 중복")

    rows, cols = grid.get("rows"), grid.get("cols")
    for e in entries:
        ans = e.get("answer") or ""
        clue = e.get("clue") or ""
        if ans and ans in clue:
            errs.append(f"{iid} entry {e.get('number')}: 단서에 정답 표제어 직접 노출")
        if e.get("length") != len(ans):
            errs.append(f"{iid} entry {e.get('number')}: length({e.get('length')})가 정답 글자수({len(ans)})와 다름")
        r, c = e.get("row"), e.get("col")
        if r is None or c is None or rows is None or cols is None or not (0 <= r < rows) or not (0 <= c < cols):
            errs.append(f"{iid} entry {e.get('number')}: 시작 좌표({r},{c})가 판 크기({rows}x{cols}) 밖")

    declared = {(c["row"], c["col"]) for c in grid.get("cells") or []}
    cell_map, cross_errors = _expand_crossword_entries(entries)
    errs.extend(f"{iid}: {m}" for m in cross_errors)

    used = set(cell_map.keys())
    if used != declared:
        missing, extra = used - declared, declared - used
        if missing:
            errs.append(f"{iid}: entries가 쓰지만 grid.cells에 선언 안 된 칸 {len(missing)}개")
        if extra:
            errs.append(f"{iid}: grid.cells에는 있지만 어떤 entry도 쓰지 않는 칸 {len(extra)}개")

    # 같은 방향(가로끼리/세로끼리) 중첩 금지 - 교차는 가로 X 세로 사이에서만 허용
    for direction in ("across", "down"):
        dir_cells: dict[tuple[int, int], int] = {}
        dr, dc = (0, 1) if direction == "across" else (1, 0)
        for e in entries:
            if e.get("direction") != direction:
                continue
            ans = e.get("answer") or ""
            r, c = e.get("row"), e.get("col")
            if r is None or c is None:
                continue
            for idx in range(len(ans)):
                pos = (r + dr * idx, c + dc * idx)
                if pos in dir_cells and dir_cells[pos] != e.get("number"):
                    errs.append(f"{iid}: 같은 방향({direction}) 중첩 - entry {dir_cells[pos]}와 "
                                f"{e.get('number')}가 둘 다 {pos} 사용")
                dir_cells[pos] = e.get("number")

    position_count: collections.Counter = collections.Counter()
    cell_entries: dict[tuple[int, int], list[int]] = {}
    for idx, e in enumerate(entries):
        direction = e.get("direction")
        dr, dc = (0, 1) if direction == "across" else (1, 0) if direction == "down" else (0, 0)
        ans = e.get("answer") or ""
        r, c = e.get("row"), e.get("col")
        if r is None or c is None:
            continue
        for i in range(len(ans)):
            pos = (r + dr * i, c + dc * i)
            position_count[pos] += 1
            cell_entries.setdefault(pos, []).append(idx)
    intersections = sum(1 for v in position_count.values() if v > 1)
    if intersections < CROSSWORD_MIN_INTERSECTIONS:
        errs.append(f"{iid}: 교차 지점 {intersections}개 ({CROSSWORD_MIN_INTERSECTIONS}개 이상 필요)")

    graph = {i: set() for i in range(len(entries))}
    for idxs in cell_entries.values():
        for a in idxs:
            graph[a].update(x for x in idxs if x != a)
    visited: set[int] = set()
    pending = [0]
    while pending:
        cur = pending.pop()
        if cur in visited:
            continue
        visited.add(cur)
        pending.extend(graph[cur] - visited)
    if len(visited) != len(entries):
        errs.append(f"{iid}: 10개 낱말이 하나의 연결망으로 이어지지 않음(연결된 낱말 {len(visited)}개)")

    for entry in entries:
        cid, ans = entry.get("content_id"), entry.get("answer")
        info = db_content_status.get(cid)
        if info is None:
            errs.append(f"{iid}: 원본 content_id {cid} 없음(entry {entry.get('number')})")
            continue
        status, exposure, public_ready, lemma = info
        if status not in ELIGIBLE_CONTENT_STATUSES or exposure != 0 or public_ready != 0:
            errs.append(f"{iid}: AUTO_HOLD/공개 상태 content_id {cid} 참조(entry {entry.get('number')})")
        if lemma is not None and ans != lemma:
            errs.append(f"{iid} entry {entry.get('number')}: 정답({ans!r})이 원본 lemma({lemma!r})와 다름")

    return errs


def validate_crossword_batch(crossword_items: list[dict]) -> list[str]:
    """100세트 전체를 대상으로 하는 집계 검사(조합 고유성, 어휘 재사용 횟수 등)."""
    errs: list[str] = []
    if len(crossword_items) != CROSSWORD_EXPECTED_SETS:
        errs.append(f"CROSSWORD 세트 수가 {CROSSWORD_EXPECTED_SETS}개가 아님(실제 {len(crossword_items)}개)")

    signatures: set[tuple[str, ...]] = set()
    usage: collections.Counter = collections.Counter()
    total_entries = 0
    for item in crossword_items:
        cids = item.get("content_ids") or []
        sig = tuple(sorted(cids))
        if sig in signatures:
            errs.append(f"{item.get('item_id')}: 다른 세트와 동일한 어휘 조합 중복")
        signatures.add(sig)
        for cid in cids:
            usage[cid] += 1
        total_entries += len(item.get("entries") or [])

    if total_entries != CROSSWORD_EXPECTED_SETS * CROSSWORD_ENTRIES_PER_SET:
        errs.append(f"CROSSWORD 전체 entry 수가 {CROSSWORD_EXPECTED_SETS * CROSSWORD_ENTRIES_PER_SET}개가 아님"
                     f"(실제 {total_entries}개)")

    distinct = len(usage)
    if distinct != CROSSWORD_DISTINCT_VOCAB:
        errs.append(f"CROSSWORD 서로 다른 어휘 수가 {CROSSWORD_DISTINCT_VOCAB}개가 아님(실제 {distinct}개)")

    over_used = [cid for cid, n in usage.items() if n > CROSSWORD_MAX_REUSE]
    if over_used:
        errs.append(f"CROSSWORD 어휘 재사용 {CROSSWORD_MAX_REUSE}회 초과 {len(over_used)}건: {over_used[:5]}...")

    return errs


def validate(meta: dict, items: list[dict], upstream_ok: bool, upstream_note: str,
             db_content_status: dict[str, tuple[str, int, int]]) -> tuple[list[str], list[str]]:
    hard: list[str] = []
    soft: list[str] = []

    if not upstream_ok:
        hard.append(f"원본 파이프라인 검증이 PASS가 아님: {upstream_note}")

    item_ids = [i.get("item_id") for i in items]
    dup = len(item_ids) - len(set(item_ids))
    if dup:
        hard.append(f"item_id 중복 {dup}건")

    unknown_type = [i["item_id"] for i in items if i.get("item_type") not in ITEM_TYPES]
    if unknown_type:
        hard.append(f"알 수 없는 item_type {len(unknown_type)}건: {unknown_type[:5]}...")

    expected_counts = meta.get("item_type_counts") or {}
    actual_counts: dict[str, int] = {}
    for i in items:
        actual_counts[i.get("item_type")] = actual_counts.get(i.get("item_type"), 0) + 1
    if expected_counts and expected_counts != actual_counts:
        hard.append(f"유형별 수량이 meta와 다름: meta={expected_counts}, 실제={actual_counts}")

    bad_choice = 0
    dup_options = 0
    for i in items:
        if i.get("item_type") not in CHOICE_TYPES:
            continue
        opts = i.get("options") or []
        if len(opts) != 4:
            bad_choice += 1
            continue
        if len(set(opts)) != 4:
            dup_options += 1
        if i.get("correct_option") not in (1, 2, 3, 4):
            bad_choice += 1
    if bad_choice:
        hard.append(f"선택형 보기 4개/정답 범위 오류 {bad_choice}건")
    if dup_options:
        hard.append(f"선택형 보기 중복 {dup_options}건")

    bad_cloze = 0
    for i in items:
        if i.get("item_type") != "CONTEXT_CLOZE":
            continue
        answer_text = i.get("answer_text")
        accepted = i.get("accepted_answers") or []
        if not answer_text or not accepted or answer_text not in accepted:
            bad_cloze += 1
    if bad_cloze:
        hard.append(f"직접입력형 정답/허용답 누락 또는 불일치 {bad_cloze}건")

    bad_match = 0
    for i in items:
        if i.get("item_type") != "MATCH_WORD_MEANING":
            continue
        words = i.get("words") or []
        definitions = i.get("definitions") or []
        answers = i.get("answers") or {}
        if len(words) != 4 or len(definitions) != 4 or len(answers) != 4:
            bad_match += 1
            continue
        if set(answers.keys()) != set(words):
            bad_match += 1
            continue
        if set(answers.values()) != set(definitions) or len(set(answers.values())) != 4:
            bad_match += 1
    if bad_match:
        hard.append(f"연결형 4낱말/4뜻/일대일 매핑 오류 {bad_match}건")

    version_mismatch = sum(1 for i in items if i.get("source_version") != EXPECTED_SOURCE_VERSION)
    if version_mismatch:
        hard.append(f"source_version이 기대값({EXPECTED_SOURCE_VERSION!r})과 다른 문항 {version_mismatch}건")

    # CROSSWORD는 entry 단위로 더 정밀하게 검사하므로(validate_crossword_item) 여기서는 제외 -
    # 같은 문제를 두 번 다른 표현으로 보고하지 않기 위함.
    missing_content = 0
    ineligible_content = 0
    for i in items:
        if i.get("item_type") == "CROSSWORD":
            continue
        cids = i.get("content_ids") if i.get("item_type") in MULTI_CONTENT_TYPES else [i.get("content_id")]
        for cid in cids:
            info = db_content_status.get(cid)
            if info is None:
                missing_content += 1
                continue
            status, exposure, public_ready, _lemma = info
            if status not in ELIGIBLE_CONTENT_STATUSES or exposure != 0 or public_ready != 0:
                ineligible_content += 1
    if missing_content:
        hard.append(f"vocabulary_contents에 없는 원본 content_id 참조 {missing_content}건")
    if ineligible_content:
        hard.append(f"AUTO_HOLD/공개·노출 상태 원본 콘텐츠를 참조하는 문항 {ineligible_content}건")

    crossword_items = [i for i in items if i.get("item_type") == "CROSSWORD"]
    if crossword_items:
        for ci in crossword_items:
            hard.extend(validate_crossword_item(ci, db_content_status))
        hard.extend(validate_crossword_batch(crossword_items))

    return hard, soft


# ==================== DB 헬퍼 ====================

def load_content_status(conn: sqlite3.Connection) -> dict[str, tuple[str, int, int, str]]:
    rows = conn.execute(
        "SELECT content_id, generation_status, student_exposure, public_ready, lemma FROM vocabulary_contents"
    ).fetchall()
    return {r[0]: (r[1], r[2], r[3], r[4]) for r in rows}


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


_UPDATE_FIELDS = [
    "item_type", "source_content_id", "source_content_ids_json", "sense_id", "sense_ids_json",
    "lemma", "pos", "prompt", "options_json", "correct_option", "public_payload_json",
    "answer_payload_json", "explanation", "cognitive_level", "qa_flags_json", "generator_version",
]


def _row_values(item: dict, generator_version: str) -> dict:
    t = item["item_type"]
    is_multi = t in MULTI_CONTENT_TYPES  # MATCH_WORD_MEANING/CROSSWORD는 content_id가 여러 개
    return {
        "item_type": t,
        "source_content_id": None if is_multi else item.get("content_id"),
        "source_content_ids_json": json.dumps(item.get("content_ids"), ensure_ascii=False) if is_multi else None,
        "sense_id": None if is_multi else item.get("sense_id"),
        "sense_ids_json": json.dumps(item.get("sense_ids"), ensure_ascii=False) if is_multi else None,
        "lemma": item.get("lemma"),
        "pos": item.get("pos"),
        "prompt": item["prompt"],
        "options_json": json.dumps(item.get("options"), ensure_ascii=False) if item.get("options") else None,
        "correct_option": item.get("correct_option"),
        "public_payload_json": json.dumps(_build_public_payload(item), ensure_ascii=False),
        "answer_payload_json": json.dumps(_build_answer_payload(item), ensure_ascii=False),
        "explanation": item.get("explanation"),
        "cognitive_level": item.get("cognitive_level"),
        "qa_flags_json": json.dumps(item.get("qa_flags") or [], ensure_ascii=False),
        "generator_version": generator_version,
    }


def upsert(conn: sqlite3.Connection, items: list[dict], generator_version: str) -> dict:
    """source_version 컬럼에는 --version(아카이브/배치 라벨)이 아니라 각 문항이
    실제로 참조하는 콘텐츠 버전(item['source_version'], validate()가 이미
    EXPECTED_SOURCE_VERSION과 같음을 보장)을 그대로 저장한다 - vocabulary_contents/
    items 테이블과 같은 의미로 맞추기 위함."""
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}
    existing = {row[0]: row for row in conn.execute(
        f"SELECT item_id, {', '.join(_UPDATE_FIELDS)} FROM vocabulary_multiformat_items"
    )}

    for item in items:
        iid = item["item_id"]
        item_source_version = item["source_version"]
        values = _row_values(item, generator_version)
        new_tuple = tuple(values[f] for f in _UPDATE_FIELDS)
        if iid not in existing:
            columns = ["item_id", *_UPDATE_FIELDS, "source_version"]
            placeholders = ", ".join("?" for _ in columns)
            conn.execute(
                f"INSERT INTO vocabulary_multiformat_items ({', '.join(columns)}) VALUES ({placeholders})",
                (iid, *new_tuple, item_source_version),
            )
            counts["inserted"] += 1
        else:
            old_tuple = existing[iid][1:]
            if old_tuple != new_tuple:
                set_clause = ", ".join(f"{f} = ?" for f in _UPDATE_FIELDS)
                conn.execute(
                    f"UPDATE vocabulary_multiformat_items SET {set_clause}, source_version = ?, "
                    f"updated_at = datetime('now') WHERE item_id = ?",
                    (*new_tuple, item_source_version, iid),
                )
                counts["updated"] += 1
            else:
                conn.execute(
                    "UPDATE vocabulary_multiformat_items SET source_version = ? WHERE item_id = ?",
                    (item_source_version, iid),
                )
                counts["unchanged"] += 1
    return counts


# ==================== 메인 ====================

def run(archive: Path, database: Path, version: str, apply: bool) -> int:
    print(f"=== 다유형 어휘 퀴즈(파일럿) {version} 적재 ({'실제 적재' if apply else 'DRY-RUN'}) ===")

    if not archive.exists():
        print(f"파일이 없습니다: {archive}", file=sys.stderr)
        print(f"예상 경로: {archive} (프로젝트 루트 또는 다운로드 폴더에서도 같은 파일명을 찾아보세요)", file=sys.stderr)
        return 1
    print(f"원본 ZIP: {archive}")

    assert_local_database_path(database)
    if apply:
        assert_apply_allowed()

    idiom_db_path = get_idiom_db_path()
    idiom_checksum_before = sha256_file(idiom_db_path) if idiom_db_path.exists() else None

    verify_archive_sha256(archive)

    extract_dir = extract_archive(archive, version)
    print(f"압축 해제: {extract_dir} (원본 ZIP은 변경하지 않음)")

    items_path = find_items_json(extract_dir)
    payload = load_items_json(items_path)
    meta = payload.get("meta", {})
    items = payload.get("items", [])
    print(f"문항 JSON: {items_path.name} / {len(items)}건 (meta.item_type_counts={meta.get('item_type_counts')})")

    upstream_ok, upstream_note = check_upstream_validation(extract_dir)
    print(f"원본 파이프라인 검증: {upstream_note}")

    ensure_schema()
    conn = sqlite3.connect(database)
    conn.execute("PRAGMA foreign_keys=ON")

    content_status = load_content_status(conn)
    hard_failures, soft_warnings = validate(meta, items, upstream_ok, upstream_note, content_status)

    print(f"\n--- HARD 검사: {'실패 ' + str(len(hard_failures)) + '건' if hard_failures else '전부 통과'} ---")
    for f in hard_failures:
        print(f"  X {f}")
    print(f"\n--- SOFT 검사(참고용): {'경고 ' + str(len(soft_warnings)) + '건' if soft_warnings else '전부 통과'} ---")
    for w in soft_warnings:
        print(f"  ! {w}")

    started_at = datetime.now(timezone.utc).isoformat()
    source_sha256 = sha256_file(archive)
    actual_counts: dict[str, int] = {}
    for i in items:
        actual_counts[i.get("item_type")] = actual_counts.get(i.get("item_type"), 0) + 1

    if hard_failures:
        conn.execute(
            """INSERT INTO vocabulary_multiformat_import_batches
               (version, source_filename, source_sha256, seed, selected_words, started_at, completed_at,
                status, item_count, item_type_counts_json, validation_result, notes)
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?)""",
            (version, archive.name, source_sha256, meta.get("seed"), meta.get("selected_words"),
             started_at, "DRY_RUN_FAILED" if not apply else "FAILED",
             len(items), json.dumps(actual_counts, ensure_ascii=False),
             json.dumps({"hard": hard_failures, "soft": soft_warnings}, ensure_ascii=False),
             "검사 실패로 적재 중단"),
        )
        conn.commit()
        conn.close()
        report_path = EXTRACT_ROOT / f"import_multiformat_failures_{version}.json"
        report_path.write_text(json.dumps({"hard_failures": hard_failures, "soft_warnings": soft_warnings},
                                           ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n검사 실패 - 실제 적재를 진행하지 않습니다. 상세: {report_path}")
        return 1

    backup_path = None
    if apply and database.exists():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = database.with_name(f"{database.name}.bak-{timestamp}")
        src = sqlite3.connect(database)
        dst = sqlite3.connect(backup_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        print(f"\n백업 완료(sqlite3.Connection.backup API): {backup_path}")

    before = _table_count(conn, "vocabulary_multiformat_items")
    generator_version = f"pilot_multiformat_{version}"
    counts = upsert(conn, items, generator_version)
    after = _table_count(conn, "vocabulary_multiformat_items")

    print(f"\n--- 결과 ({'적용' if apply else 'dry-run, 아래는 실제 적용 시 예상값'}) ---")
    print(f"문항: 신규 {counts['inserted']} / 갱신 {counts['updated']} / 변경없음 {counts['unchanged']} "
          f"(전 {before} -> 후 {after})")
    print(f"유형별: {actual_counts}")

    conn.execute(
        """INSERT INTO vocabulary_multiformat_import_batches
           (version, source_filename, source_sha256, seed, selected_words, started_at, completed_at,
            status, item_count, item_type_counts_json, inserted_count, updated_count, unchanged_count,
            validation_result)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)""",
        (version, archive.name, source_sha256, meta.get("seed"), meta.get("selected_words"),
         started_at, "COMPLETED" if apply else "DRY_RUN_OK",
         len(items), json.dumps(actual_counts, ensure_ascii=False),
         counts["inserted"], counts["updated"], counts["unchanged"],
         json.dumps({"hard": hard_failures, "soft": soft_warnings}, ensure_ascii=False)),
    )

    if apply:
        conn.commit()
        print("\n실제 적재 완료 (COMMIT)")
    else:
        conn.rollback()
        print("\nDRY-RUN이므로 ROLLBACK - DB는 변경되지 않았습니다. 적재하려면 --apply를 붙여 재실행하세요.")
    conn.close()

    idiom_checksum_after = sha256_file(idiom_db_path) if idiom_db_path.exists() else None
    if idiom_checksum_before != idiom_checksum_after:
        print(f"\n치명적 경고: idiom.db 체크섬이 변경되었습니다! before={idiom_checksum_before} after={idiom_checksum_after}",
              file=sys.stderr)
        return 1
    print(f"\nidiom.db 체크섬 불변 확인: {idiom_checksum_before}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="초등 다유형 어휘 퀴즈(파일럿) ZIP 적재 (기본 dry-run, 로컬 R&D 전용)")
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE, help="적재할 zip 경로")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE, help="적재 대상 SQLite 경로")
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument("--apply", action="store_true", help="실제 적재 (기본은 dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="명시적 dry-run (기본값과 동일, 문서화용)")
    args = parser.parse_args()
    return run(args.archive, args.database, args.version, args.apply)


if __name__ == "__main__":
    sys.exit(main())
