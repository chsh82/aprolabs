"""스키마리딩x어휘 DB 통합 3단계 dry-run 분석기 (읽기 전용 전용).

이 스크립트는 어떤 DB에도 쓰지 않는다. literacy.db는 항상
`file:<path>?mode=ro`(URI 모드)로만 연다. vocabulary_quiz 쪽(서버
연구용 DB)은 SSH 콘솔 접근만 허용되므로, 이 스크립트를 직접 서버에 붙여
쓰지 않고 사전에 `sqlite3 ... -readonly` SELECT로 뽑아둔 CSV 스냅샷
(vocabulary_contents.csv, vocabulary_content_levels.csv)을 입력으로
받는다 - 이 CSV들 자체도 서버 DB에 대한 INSERT/UPDATE/DELETE 없이
SELECT 전용으로 생성된 것이다(생성 명령은 보고서에 기록).

목적: 2단계 보고서(`reports/schema_reading_phase2_readonly_audit_20260924.md`)
3절이 확립한 "표제어 완전일치 148건" 기준집합을 동일한 방법(V.lemma ==
S.headword 문자열 완전일치, literacy 전체 7,312건 대상)으로 재현하고,
각 건에 대해 V/S 양쪽 표제어·품사·뜻·source·현재 레벨과 연결 기수성을
행 단위로 출력한다. 자동으로 "의미가 같다"고 승인하지 않는다 - status
필드는 이 스크립트가 만드는 초안 힌트(definition_heuristic_note)를
사람(에이전트)이 직접 읽고 별도로 채워 넣는 2단계 구조로 설계했다.

사용 예:
    python scripts/vocab/analyze_schema_reading_link_dryrun.py \\
        --literacy-db data/literacy.db \\
        --vocab-contents-csv <scratch>/vocabulary_contents.csv \\
        --vocab-levels-csv <scratch>/vocabulary_content_levels.csv \\
        --out-jsonl <scratch>/candidate_rows_run1.jsonl
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]


def normalize(s: str | None) -> str:
    """대조용 정규화: NFC 유니코드 정규화 + 앞뒤 공백 제거만. 그 외 가공 없음."""
    if s is None:
        return ""
    return unicodedata.normalize("NFC", s).strip()


def load_literacy_terms(db_path: Path) -> list[dict]:
    """literacy.db terms를 mode=ro로만 연다 - 쓰기 금지."""
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT id, category, headword, origin, definition, pos, sense_category, "
        "subject_category, grade_level, grade_source, source, license, external_id, "
        "review_status, note, level, reviewed_at FROM terms"
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def build_index(rows: list[dict], key_field: str) -> dict[str, list[dict]]:
    idx: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        idx[normalize(r[key_field])].append(r)
    return idx


def classify_cardinality(n_s_for_this_v: int, n_v_for_this_s_headword: int) -> str:
    """이 매칭 쌍이 속한 관계 기수성을 표시한다 (0:1/1:1/1:N/N:M 중 하나).

    n_s_for_this_v: 이 V content_id와 같은 headword를 가진 S term 행 수
    n_v_for_this_s_headword: 이 S headword와 같은 lemma를 가진 V content_id 수
    """
    if n_s_for_this_v <= 1 and n_v_for_this_s_headword <= 1:
        return "1:1"
    if n_s_for_this_v > 1 and n_v_for_this_s_headword <= 1:
        return "1:N"  # V 1건이 여러 S(다른 source/의미)에 대응
    if n_s_for_this_v <= 1 and n_v_for_this_s_headword > 1:
        return "N:1"  # 여러 V가 같은 S 1건에 대응
    return "N:M"


def definition_heuristic_note(v_def: str | None, s_def: str | None) -> str:
    """자동 승인이 아니라 사람이 읽을 때 참고할 힌트 문자열만 생성한다."""
    vd = normalize(v_def)
    sd = normalize(s_def)
    if not vd or not sd:
        return "DEFINITION_MISSING_ONE_SIDE"
    if vd == sd:
        return "IDENTICAL_STRING"
    v_set = set(vd)
    s_set = set(sd)
    overlap = len(v_set & s_set) / max(1, len(v_set | s_set))
    return f"CHAR_JACCARD={overlap:.2f}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--literacy-db", type=Path, default=REPO_ROOT / "data" / "literacy.db")
    ap.add_argument("--vocab-contents-csv", type=Path, required=True)
    ap.add_argument("--vocab-levels-csv", type=Path, required=True)
    ap.add_argument("--out-jsonl", type=Path, required=True)
    args = ap.parse_args()

    terms = load_literacy_terms(args.literacy_db)
    vcontents = load_csv(args.vocab_contents_csv)
    vlevels = load_csv(args.vocab_levels_csv)

    print(f"literacy terms: {len(terms)}건 (mode=ro로 읽음: {args.literacy_db})")
    print(f"vocabulary_contents 스냅샷: {len(vcontents)}건 ({args.vocab_contents_csv})")
    print(f"vocabulary_content_levels 스냅샷: {len(vlevels)}건 ({args.vocab_levels_csv})")

    # content_id -> level rows (있으면 여러 level_version 존재 가능, is_active=1 우선)
    levels_by_content: dict[str, list[dict]] = defaultdict(list)
    for lv in vlevels:
        levels_by_content[lv["content_id"]].append(lv)

    # V lemma 중복 여부 재검증 (2단계는 0건이라 보고)
    v_lemma_counts: dict[str, int] = defaultdict(int)
    for v in vcontents:
        v_lemma_counts[normalize(v["lemma"])] += 1
    v_dup_lemmas = {k: c for k, c in v_lemma_counts.items() if c > 1}
    print(f"V lemma 중복(같은 lemma가 2개 이상 content_id를 가짐): {len(v_dup_lemmas)}건")

    s_by_headword = build_index(terms, "headword")
    v_by_lemma = build_index(vcontents, "lemma")

    matched_headwords = sorted(set(s_by_headword.keys()) & set(v_by_lemma.keys()) - {""})
    print(f"표제어 완전일치(고유 headword 기준): {len(matched_headwords)}건")

    out_rows = []
    total_row_pairs = 0
    for hw in matched_headwords:
        s_rows = s_by_headword[hw]
        v_rows = v_by_lemma[hw]
        for v in v_rows:
            for s in s_rows:
                total_row_pairs += 1
                content_id = v["content_id"]
                lvl_rows = [r for r in levels_by_content.get(content_id, []) if r.get("is_active") == "1"]
                lvl = lvl_rows[0] if lvl_rows else (levels_by_content.get(content_id, [{}]) or [{}])[0]
                cardinality = classify_cardinality(len(s_rows), len(v_rows))
                rec = {
                    "headword": hw,
                    "vocab_content_id": content_id,
                    "vocab_lemma": v["lemma"],
                    "vocab_pos": v.get("pos"),
                    "vocab_canonical_definition": v.get("canonical_definition"),
                    "vocab_student_definition": v.get("student_definition"),
                    "vocab_generation_method": v.get("generation_method"),
                    "vocab_source_version": v.get("source_version"),
                    "vocab_current_level": lvl.get("vocab_level"),
                    "vocab_level_status": lvl.get("level_status"),
                    "vocab_level_version": lvl.get("level_version"),
                    "literacy_term_id": s["id"],
                    "literacy_headword": s["headword"],
                    "literacy_pos": s.get("pos"),
                    "literacy_definition": s.get("definition"),
                    "literacy_source": s.get("source"),
                    "literacy_level": s.get("level"),
                    "literacy_grade_level": s.get("grade_level"),
                    "literacy_review_status": s.get("review_status"),
                    "literacy_note": s.get("note"),
                    "s_rows_for_this_headword": len(s_rows),
                    "v_rows_for_this_headword": len(v_rows),
                    "cardinality": cardinality,
                    "definition_heuristic_note": definition_heuristic_note(
                        v.get("canonical_definition"), s.get("definition")
                    ),
                }
                out_rows.append(rec)

    print(f"headword x source 조합까지 펼친 총 행(비교쌍) 수: {total_row_pairs}")

    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for rec in out_rows:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"저장: {args.out_jsonl}")

    # 멱등성 확인용 요약 해시(행 순서에 안 흔들리도록 정렬 후 직렬화)
    import hashlib
    canon = json.dumps(
        sorted(out_rows, key=lambda r: (r["vocab_content_id"], r["literacy_term_id"])),
        ensure_ascii=False, sort_keys=True,
    )
    print(f"idempotency_hash={hashlib.sha256(canon.encode('utf-8')).hexdigest()}")


if __name__ == "__main__":
    main()
