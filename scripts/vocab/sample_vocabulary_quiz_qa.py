"""일반 어휘 퀴즈 QA 표본 추출 - 고정 seed 층화 표본, 로컬 R&D 전용.

1차 층화축은 (generation_status, pos_group) - 이 둘의 조합별로 전체 크기에
비례해 500개를 배분한다(최대잔여법으로 반올림 오차 보정). pos는 50건 미만인
희귀 품사를 '기타'로 묶는다(그렇지 않으면 1~4건짜리 셀이 너무 많이 생겨서
배분이 무의미해진다).

나머지 요청 기준(correct_option, quality_batch_id, 표제어 첫 글자, 뜻풀이
길이, 예문 길이)까지 전부 곱한 완전 교차 층화는 셀이 수천 개로 쪼개져
500개 표본으로는 대부분 빈 셀이 된다 - 대신 각 1차 층(stratum) 안에서
이 다섯 기준을 합친 정렬키로 정렬한 뒤 등간격(systematic) 추출을 한다.
정렬 기준이 서로 다른 값끼리 뭉치므로, 등간격으로 훑으면 자연히 이
다섯 축에 걸쳐 퍼진 표본이 나온다 - 결과 분포는 manifest에 그대로
기록해 사람이 검증할 수 있게 한다("가능한 범위에서 균형있게 포함"을
느슨한 목표로 두고, 실제 분포를 투명하게 보고하는 쪽을 택했다).

자동검증 경계값에 가까운 문항(정답 위치 선택지가 student_definition과
다른 경우, example_target_form이 example_sentence에 없는 경우)은 몇 건
안 되므로 무조건 강제 포함한다.

실행:
    python scripts/vocab/sample_vocabulary_quiz_qa.py --version 2.1.29
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import random
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocabulary_quiz.db import get_db_path  # noqa: E402

SAMPLE_TARGET = 500
SAMPLE_SEED = 211290  # 버전 '2.1.29'를 딴 고정값 - 재실행해도 항상 같은 표본이 나온다.
RARE_POS_THRESHOLD = 50


def _pos_group(pos: str | None, pos_counts: dict[str, int]) -> str:
    if not pos or pos_counts.get(pos, 0) < RARE_POS_THRESHOLD:
        return "기타"
    return pos


def _len_bucket(text: str | None) -> str:
    n = len(text or "")
    if n <= 15:
        return "short"
    if n <= 30:
        return "medium"
    return "long"


def _lemma_bucket(lemma: str) -> str:
    """표제어 첫 글자 - 초성 기준 5구간으로 묶는다(가~ 값 자체는 618개
    고유 첫글자가 나와 그대로 쓰면 사실상 매 표제어가 자기 그룹이 됨)."""
    if not lemma:
        return "?"
    cho = "ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ"
    code = ord(lemma[0]) - 0xAC00
    if 0 <= code < 11172:
        cho_idx = code // (21 * 28)
        c = cho[cho_idx]
        if c in "ㄱㄲㄴ":
            return "1(ㄱ-ㄴ)"
        if c in "ㄷㄸㄹㅁ":
            return "2(ㄷ-ㅁ)"
        if c in "ㅂㅃㅅㅆ":
            return "3(ㅂ-ㅅ)"
        if c in "ㅇㅈㅉㅊ":
            return "4(ㅇ-ㅊ)"
        return "5(ㅋ-ㅎ)"
    return "0(비한글)"


def find_boundary_items(rows: list[dict]) -> set[str]:
    boundary = set()
    for r in rows:
        opts = [r["option_1"], r["option_2"], r["option_3"], r["option_4"]]
        co = r["correct_option"]
        chosen = opts[co - 1] if co and 1 <= co <= 4 else None
        if r["student_definition"] and chosen and r["student_definition"] != chosen:
            boundary.add(r["item_id"])
        if r["example_sentence"] and r["example_target_form"] and r["example_target_form"] not in r["example_sentence"]:
            boundary.add(r["item_id"])
    return boundary


def largest_remainder_allocate(sizes: dict, total_target: int) -> dict:
    grand_total = sum(sizes.values())
    raw = {k: v * total_target / grand_total for k, v in sizes.items()}
    floor_alloc = {k: int(v) for k, v in raw.items()}
    remainder = total_target - sum(floor_alloc.values())
    remainders_sorted = sorted(raw.keys(), key=lambda k: raw[k] - floor_alloc[k], reverse=True)
    for k in remainders_sorted[:remainder]:
        floor_alloc[k] += 1
    return floor_alloc


def systematic_sample(items: list[dict], n: int, rng: random.Random) -> list[dict]:
    if n <= 0 or not items:
        return []
    if n >= len(items):
        return list(items)
    step = len(items) / n
    start = rng.uniform(0, step)
    picked_idx = sorted({min(int(start + i * step), len(items) - 1) for i in range(n)})
    # 중복 인덱스가 생기면(반올림) 남은 자리를 앞에서부터 채운다
    picked = [items[i] for i in picked_idx]
    if len(picked) < n:
        used = set(picked_idx)
        for i in range(len(items)):
            if len(picked) >= n:
                break
            if i not in used:
                picked.append(items[i])
                used.add(i)
    return picked[:n]


def build_sample(rows: list[dict], target: int, seed: int) -> tuple[list[dict], dict]:
    pos_counts: dict[str, int] = {}
    for r in rows:
        pos_counts[r["pos"]] = pos_counts.get(r["pos"], 0) + 1

    for r in rows:
        r["_pos_group"] = _pos_group(r["pos"], pos_counts)
        r["_def_len"] = _len_bucket(r["student_definition"])
        r["_ex_len"] = _len_bucket(r["example_sentence"])
        r["_lemma_bucket"] = _lemma_bucket(r["lemma"])
        r["_qb"] = r["quality_batch_id"] or "NONE"

    boundary_ids = find_boundary_items(rows)
    boundary_rows = [r for r in rows if r["item_id"] in boundary_ids]
    remaining_target = max(target - len(boundary_rows), 0)

    strata: dict[tuple, list[dict]] = {}
    for r in rows:
        if r["item_id"] in boundary_ids:
            continue
        key = (r["generation_status"], r["_pos_group"])
        strata.setdefault(key, []).append(r)

    sizes = {k: len(v) for k, v in strata.items()}
    allocation = largest_remainder_allocate(sizes, remaining_target)

    selected: list[dict] = list(boundary_rows)
    for r in selected:
        r["_selection_reason"] = "boundary_case"

    stratum_report = {}
    for key, candidates in strata.items():
        n_target = allocation.get(key, 0)
        # 층 내부를 다섯 축(correct_option/quality_batch_id/표제어 첫글자/
        # 뜻풀이길이/예문길이) 합친 키로 정렬 후 등간격 추출 - 서로 다른
        # 값끼리 뭉쳐 있으니 등간격으로 훑으면 자연히 다섯 축에 퍼진다.
        candidates_sorted = sorted(
            candidates,
            key=lambda r: (r["correct_option"], r["_qb"], r["_lemma_bucket"], r["_def_len"], r["_ex_len"], r["item_id"]),
        )
        rng = random.Random(f"{seed}-{key}")
        picked = systematic_sample(candidates_sorted, n_target, rng)
        for r in picked:
            r["_selection_reason"] = "stratified"
        selected.extend(picked)
        stratum_report[f"{key[0]} / {key[1]}"] = {"pool_size": len(candidates), "target": n_target, "picked": len(picked)}

    return selected, stratum_report


def _coverage(selected: list[dict], key: str) -> dict:
    counts: dict[str, int] = {}
    for r in selected:
        v = str(r.get(key))
        counts[v] = counts.get(v, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))


def run(db_path: Path, version: str, out_dir: Path) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("""
        SELECT i.item_id AS item_id, i.content_id AS content_id, c.lemma AS lemma, c.pos AS pos,
               c.generation_status AS generation_status, i.correct_option AS correct_option,
               c.quality_batch_id AS quality_batch_id,
               i.option_1 AS option_1, i.option_2 AS option_2, i.option_3 AS option_3, i.option_4 AS option_4,
               c.student_definition AS student_definition, c.example_sentence AS example_sentence,
               c.example_target_form AS example_target_form
        FROM vocabulary_items i JOIN vocabulary_contents c ON c.content_id = i.content_id
        WHERE i.is_active = 1 AND c.is_active = 1
    """).fetchall()]
    print(f"표본 추출 대상 풀: {len(rows)}건")

    selected, stratum_report = build_sample(rows, SAMPLE_TARGET, SAMPLE_SEED)
    print(f"표본 크기: {len(selected)}건 (목표 {SAMPLE_TARGET}건)")
    if len(selected) != SAMPLE_TARGET:
        print(f"주의: 정확히 {SAMPLE_TARGET}개를 구성할 수 없어 가장 가까운 수({len(selected)}개)로 생성했습니다 "
              f"(1차 층 배분의 반올림 + 경계 사례 강제 포함으로 인한 차이).")

    now = datetime.now(timezone.utc).isoformat()
    existing = conn.execute("SELECT COUNT(*) FROM vocabulary_review_samples WHERE sample_version = ?",
                             (version,)).fetchone()[0]
    if existing:
        print(f"이미 sample_version={version} 표본이 {existing}건 있어 다시 만들지 않습니다 "
              f"(재현성 - 같은 버전은 한 번만 추출). 다시 만들려면 DB에서 해당 행을 지우고 재실행하세요.")
    else:
        for r in selected:
            group_tag = json.dumps({
                "generation_status": r["generation_status"], "pos_group": r["_pos_group"],
                "correct_option": r["correct_option"], "quality_batch_id": r["_qb"],
                "lemma_bucket": r["_lemma_bucket"], "def_len": r["_def_len"], "ex_len": r["_ex_len"],
                "reason": r["_selection_reason"],
            }, ensure_ascii=False)
            conn.execute(
                """INSERT INTO vocabulary_review_samples
                   (sample_version, item_id, content_id, sampling_group, sampling_seed, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (version, r["item_id"], r["content_id"], group_tag, SAMPLE_SEED, now, now),
            )
        conn.commit()
        print(f"vocabulary_review_samples에 {len(selected)}건 기록 완료 (review_status=UNREVIEWED)")

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"qa_sample_v{version}.csv"
    manifest_path = out_dir / f"qa_sample_manifest_v{version}.json"

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["item_id", "content_id", "lemma", "pos", "generation_status", "correct_option",
                    "quality_batch_id", "selection_reason", "stem", "option_1", "option_2", "option_3",
                    "option_4", "explanation", "canonical_definition", "student_definition", "example_sentence"])
        content_by_id = {r["content_id"]: r for r in rows}
        for r in selected:
            w.writerow([r["item_id"], r["content_id"], r["lemma"], r["pos"], r["generation_status"],
                        r["correct_option"], r["quality_batch_id"] or "", r["_selection_reason"],
                        "", r["option_1"], r["option_2"], r["option_3"], r["option_4"], "",
                        "", r["student_definition"], r["example_sentence"]])

    manifest = {
        "sample_version": version,
        "generated_at": now,
        "seed": SAMPLE_SEED,
        "target": SAMPLE_TARGET,
        "actual": len(selected),
        "boundary_case_count": sum(1 for r in selected if r["_selection_reason"] == "boundary_case"),
        "primary_stratum_allocation": stratum_report,
        "secondary_coverage": {
            "correct_option": _coverage(selected, "correct_option"),
            "quality_batch_id": _coverage(selected, "_qb"),
            "lemma_bucket": _coverage(selected, "_lemma_bucket"),
            "definition_length": _coverage(selected, "_def_len"),
            "example_length": _coverage(selected, "_ex_len"),
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nCSV: {csv_path}")
    print(f"Manifest: {manifest_path}")
    conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="일반 어휘 퀴즈 QA 표본 추출 (고정 seed 층화)")
    parser.add_argument("--database", type=Path, default=get_db_path())
    parser.add_argument("--version", default="2.1.29")
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "data" / "import")
    args = parser.parse_args()
    return run(args.database, args.version, args.out_dir)


if __name__ == "__main__":
    sys.exit(main())
