#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""읽기 전용 품질 감사 스크립트 (phase15 최초 작성, phase16에서 하드코딩 캐주션 제거).

목적: schema_reading_phase13(L4)/phase14(L5) 결과표(`data/import/*_final_*.csv`)에
실제로 적재된 행(`content_id`가 비어있지 않은 행 = HOLD 제외)을 `data/literacy.db`
원천과 행별로 대조해, 다음 3분류 중 하나로 판정한다:

  - 자동검사통과 (AUTO_PASS)
  - 의미검토필요 (MEANING_REVIEW) - 동형이의 위험, 뜻 범위 차이(좁아짐/넓어짐), 결과표
    자체가 이미 표시한 caution
  - 원천근거부족 (SOURCE_EVIDENCE_WEAK) - term_id 불일치/미존재, 표제어 불일치, 정의
    문자열 불일치, 정의가 표제어를 직접 정의하지 않는 서술문 패턴(예: "가변성" HOLD류)

**절대 하지 않는 것**: literacy.db/vocabulary_quiz_research.db 어느 쪽도 쓰지 않는다
(literacy.db는 `mode=ro`로만 연다). 자동검사 통과를 "전문가 검수 완료"로 표시하지
않는다 - 특히 S(교과개념어)는 카드에 "전문가 검수 필요"를 항상 별도로 남긴다.

**phase16 리팩터 (중요)**: phase15는 "결과표 caution 컬럼에는 없지만 보고서 본문에만
서술된 캐주션"(유추/유사/사법권 3건)을 스크립트 내부 하드코딩 딕셔너리
(`SUPPLEMENTARY_MEANING_REVIEW`)로 보충해서 MEANING_REVIEW로 판정했었다. 이는 "기계가
읽는 필드"와 "사람이 읽는 문서"가 분리되는 문서화 간극이었다(phase15 보고서 1-2절/7-4절
참고). phase16에서 그 3건의 캐주션 텍스트를 원천 결과표
(`data/import/schema_reading_phase13_l4_core50_final_20260925.csv`)의 `caution` 컬럼에
직접 기록했고, 이 스크립트에서 하드코딩 딕셔너리를 완전히 제거했다. 이제 MEANING_REVIEW
판정은 **오직 결과표 `caution` 컬럼(`row["csv_caution"]`)과 literacy.db 재조회로 얻는
구조적 신호(동형이의 복수 행, 사실 서술형 정의문 패턴)만으로** 이뤄진다. 향후 새 배치도
캐주션을 반드시 결과표 `caution` 컬럼에 직접 기록해야 하며, 이 스크립트에 새 하드코딩
딕셔너리를 추가하는 방식으로 되돌리지 않는다.

사용법 (다음 배치가 추가돼도 그대로 재사용):
    python scripts/vocab/audit_l4_l5_batch_quality.py \\
        --literacy-db data/literacy.db \\
        --csv data/import/schema_reading_phase13_l4_core50_final_20260925.csv \\
        --csv data/import/schema_reading_phase14_l5_core_final_20260925.csv \\
        --out-csv data/import/schema_reading_phase15_l4l5_audit_<date>.csv \\
        --out-jsonl data/import/schema_reading_phase15_l4l5_audit_<date>.jsonl \\
        --out-s-cards reports/schema_reading_phase15_s_cards_<date>.md
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. 결과표 스키마가 배치마다 조금씩 다르므로(L4: v_s/literacy_definition/l4_basis,
#    L5: is_S/canonical_definition) 공통 필드로 정규화한다.
# ---------------------------------------------------------------------------

DEFINITION_STATEMENT_ENDINGS = (
    "함", "임", "됨", "짐",  # 명사형 전성어미로 끝나면 정의문일 가능성이 높음
)
# "가변성" HOLD류(표제어를 직접 정의하지 않고 현상을 서술)를 걸러내기 위한 약한
# 휴리스틱: 정의문이 사실 서술 어미로 끝나면(예: "~따라 변함") 의심 후보로 표시.
# 이 휴리스틱은 참고용 신호일 뿐이며, 최종 판단은 사람이 각 캐주션에 남긴 이유를
# 그대로 채택한다(스크립트가 새 값을 창작하지 않는다).
FACT_STATEMENT_HINTS = ("따라 변함", "따라 다름", "경우가 있음", "하기도 함")


def normalize_row(raw: dict, source_file: str) -> dict | None:
    """L4/L5 CSV 한 행을 공통 스키마로 정규화. HOLD(미적재) 행은 None을 반환."""
    content_id = (raw.get("content_id") or "").strip()
    if not content_id:
        return None  # HOLD - 이번 감사(97건) 범위 밖. 현재 보류 상태 그대로 둔다.

    if "v_s" in raw:  # L4 CSV
        v_s = raw["v_s"].strip()
        csv_definition = raw.get("literacy_definition", "")
        level_basis = raw.get("l4_basis", "")
    else:  # L5 CSV
        v_s = "S" if raw.get("is_S", "").strip() == "True" else "V"
        csv_definition = raw.get("canonical_definition", "")
        level_basis = ""

    return {
        "source_file": source_file,
        "group": raw.get("group", ""),
        "v_s": v_s,
        "lemma": raw.get("lemma", ""),
        "pos": raw.get("pos", ""),
        "literacy_source": raw.get("literacy_source", ""),
        "literacy_term_id": raw.get("literacy_term_id", ""),
        "csv_definition": csv_definition,
        "level_basis": level_basis,
        "student_definition": raw.get("student_definition", ""),
        "example_sentence": raw.get("example_sentence", ""),
        "example_target_form": raw.get("example_target_form", ""),
        "subject_category": raw.get("subject_category", ""),
        "note_week": raw.get("note_week", ""),
        "reuse_check": raw.get("reuse_check", ""),
        "final_classification": raw.get("final_classification", ""),
        "csv_caution": raw.get("caution", ""),
        "content_id": content_id,
        "vocab_level": raw.get("vocab_level", ""),
        "level_status": raw.get("level_status", ""),
    }


def load_batch_rows(csv_paths: list[Path]) -> list[dict]:
    rows = []
    for path in csv_paths:
        with open(path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                norm = normalize_row(raw, path.name)
                if norm is not None:
                    rows.append(norm)
    return rows


# ---------------------------------------------------------------------------
# 2. literacy.db 대조 (읽기 전용)
# ---------------------------------------------------------------------------

def open_literacy_db_ro(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.execute("PRAGMA query_only=ON")
    return con


def fetch_term(con: sqlite3.Connection, term_id: str) -> dict | None:
    if not term_id:
        return None
    cur = con.execute(
        "SELECT id, category, headword, origin, definition, pos, sense_category, "
        "subject_category, grade_level, grade_source, source, review_status, note, "
        "level, reviewed_at FROM terms WHERE id = ?",
        (term_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def homonym_count(con: sqlite3.Connection, headword: str) -> int:
    """literacy.db 안에서 같은 표제어(headword)가 몇 개 행으로 존재하는지(동형이의
    분리 행 존재 여부의 구조적 신호)."""
    cur = con.execute("SELECT COUNT(*) FROM terms WHERE headword = ?", (headword,))
    return cur.fetchone()[0]


SOURCE_LABEL_TO_DB = {
    "V": "schemareading-tooldict",
    "S": "schemareading-schema",
}


def audit_row(con: sqlite3.Connection, row: dict) -> dict:
    term = fetch_term(con, row["literacy_term_id"])
    flags: list[str] = []
    checks: dict[str, str] = {}

    if term is None:
        checks["term_exists"] = "FAIL"
        flags.append("literacy.db에 term_id가 존재하지 않음")
    else:
        checks["term_exists"] = "PASS"

        # 표제어 일치
        if term["headword"] == row["lemma"]:
            checks["headword_match"] = "PASS"
        else:
            checks["headword_match"] = "FAIL"
            flags.append(f"headword 불일치: db={term['headword']!r} vs csv={row['lemma']!r}")

        # 출처(V/S) 일치
        expected_source = SOURCE_LABEL_TO_DB.get(row["v_s"])
        if expected_source and term["source"] == expected_source:
            checks["source_match"] = "PASS"
        else:
            checks["source_match"] = "FAIL"
            flags.append(f"source 불일치: db={term['source']!r} vs 기대={expected_source!r}")

        # 레벨 일치
        try:
            level_match = str(term["level"]) == str(row["vocab_level"])
        except Exception:
            level_match = False
        checks["level_match"] = "PASS" if level_match else "FAIL"
        if not level_match:
            flags.append(f"level 불일치: db={term['level']!r} vs csv vocab_level={row['vocab_level']!r}")

        # 품사 일치 (literacy.db pos가 NULL/빈 값이면 결과표가 보충 작성한 것 -
        # 근거 약화 신호로만 기록, 자동 FAIL 처리하지 않음)
        db_pos = (term["pos"] or "").strip()
        csv_pos = (row["pos"] or "").strip()
        if db_pos and csv_pos and db_pos == csv_pos:
            checks["pos_match"] = "PASS"
        elif not db_pos:
            checks["pos_match"] = "DB_POS_MISSING"
            flags.append("literacy.db pos가 비어 있어 결과표 pos를 검증할 수 없음")
        else:
            checks["pos_match"] = "FAIL"
            flags.append(f"pos 불일치: db={db_pos!r} vs csv={csv_pos!r}")

        # 정의 문자열 일치 (canonical_definition/literacy_definition은 literacy.db
        # definition을 그대로 옮겨적은 것이어야 한다는 전제)
        db_def = (term["definition"] or "").strip()
        csv_def = (row["csv_definition"] or "").strip()
        if db_def and csv_def and db_def == csv_def:
            checks["definition_verbatim_match"] = "PASS"
        elif not db_def:
            checks["definition_verbatim_match"] = "DB_DEFINITION_MISSING"
            flags.append("literacy.db definition이 비어 있음 - 원천 근거 부족 후보")
        else:
            checks["definition_verbatim_match"] = "FAIL"
            flags.append(f"definition 문자열 불일치: db={db_def!r} vs csv={csv_def!r}")

        # 동형이의 구조적 신호: literacy.db 안에 같은 headword 행이 2개 이상
        hc = homonym_count(con, term["headword"])
        checks["literacy_db_homonym_row_count"] = str(hc)
        if hc > 1:
            flags.append(f"literacy.db에 동일 headword 행이 {hc}개 존재(동형이의 분리 저장 가능성)")

        # "표제어를 직접 정의하지 않는 서술문" 약한 휴리스틱 (가변성류)
        if db_def and any(hint in db_def for hint in FACT_STATEMENT_HINTS):
            flags.append(f"정의문이 사실 서술형 어미 패턴('{[h for h in FACT_STATEMENT_HINTS if h in db_def][0]}')을 포함 - "
                         "표제어를 직접 정의하는 문장인지 수동 확인 필요(가변성 HOLD류 패턴)")

        # S(교과개념어) 메타데이터 일치
        if row["v_s"] == "S":
            db_subject = (term["subject_category"] or "").strip()
            csv_subject = (row["subject_category"] or "").strip()
            checks["subject_category_match"] = "PASS" if db_subject == csv_subject else "FAIL"
            if db_subject != csv_subject:
                flags.append(f"subject_category 불일치: db={db_subject!r} vs csv={csv_subject!r}")

    # 결과표 자체가 이미 남긴 caution(선행 phase13/14/16의 동형이의/근접어 캐주션).
    # phase16부터는 이 필드가 MEANING_REVIEW 판정의 유일한 "캐주션 출처"다 - 스크립트
    # 내부 하드코딩 딕셔너리로 보충하지 않는다(위 모듈 docstring 참고).
    csv_caution = row["csv_caution"].strip()
    if csv_caution:
        flags.append(f"결과표 caution 필드 기존 표시: {csv_caution}")

    # --- 3분류 판정 ---
    verdict = "AUTO_PASS"
    reason_parts: list[str] = []

    structural_fail = any(
        checks.get(k) == "FAIL"
        for k in ("term_exists", "headword_match", "source_match", "level_match", "definition_verbatim_match")
    )
    db_definition_missing = checks.get("definition_verbatim_match") == "DB_DEFINITION_MISSING"

    fact_statement_flag = any("사실 서술형 어미" in f for f in flags)
    homonym_flag = checks.get("literacy_db_homonym_row_count", "0") not in ("0", "1")

    if structural_fail or db_definition_missing:
        verdict = "SOURCE_EVIDENCE_WEAK"
        reason_parts.append("구조적 대조 실패(term_id/headword/source/level/정의 문자열 중 하나 이상 불일치 또는 원천 정의 부재)")
    elif csv_caution or homonym_flag or fact_statement_flag:
        verdict = "MEANING_REVIEW"
        if csv_caution:
            reason_parts.append(f"결과표 caution: {csv_caution}")
        if homonym_flag:
            reason_parts.append("literacy.db 동일 headword 복수 행 존재(동형이의 분리 저장 가능성)")
        if fact_statement_flag:
            reason_parts.append("정의문이 표제어를 직접 정의하지 않는 서술형 패턴 의심")
    else:
        reason_parts.append("구조 대조 전부 PASS, caution 없음, literacy.db 내 동형이의 분리 행 없음")

    return {
        **row,
        "db_headword": term["headword"] if term else "",
        "db_pos": term["pos"] if term else "",
        "db_source": term["source"] if term else "",
        "db_level": term["level"] if term else "",
        "db_subject_category": term["subject_category"] if term else "",
        "db_note": term["note"] if term else "",
        "db_definition": term["definition"] if term else "",
        "db_review_status": term["review_status"] if term else "",
        "checks_json": json.dumps(checks, ensure_ascii=False),
        "flags_json": json.dumps(flags, ensure_ascii=False),
        "verdict": verdict,
        "verdict_reason": " / ".join(reason_parts),
    }


# ---------------------------------------------------------------------------
# 3. S(교과개념어) 검수 카드
# ---------------------------------------------------------------------------

def build_s_cards(audited_rows: list[dict]) -> str:
    lines = [
        "# S(교과개념어) 검수 카드",
        "",
        "**주의: 자동 검사 통과 ≠ 전문가 검수 완료.** 이 카드들은 literacy.db 원문과",
        "결과표 간의 구조적 일치(표제어/정의 문자열/교과·주차 메타데이터)만 자동으로",
        "확인한 것이며, 과학·사회 교과 내용의 정확성/적절성에 대한 전문가 검수를",
        "대신하지 않는다. 모든 카드에 \"전문가 검수 필요\"가 표시된 이유다.",
        "",
    ]
    s_rows = [r for r in audited_rows if r["v_s"] == "S"]
    by_batch: dict[str, list[dict]] = {}
    for r in s_rows:
        key = "L4" if "phase13" in r["source_file"] else "L5"
        by_batch.setdefault(key, []).append(r)

    for batch in ("L4", "L5"):
        rows = by_batch.get(batch, [])
        lines.append(f"## {batch} — S {len(rows)}건")
        lines.append("")
        for r in sorted(rows, key=lambda x: (x["subject_category"], x["note_week"])):
            lines.append(f"### {r['lemma']} ({batch}, content_id={r['content_id']})")
            lines.append("")
            lines.append(f"- 뜻(학생용): {r['student_definition']}")
            lines.append(f"- literacy.db 원문 정의: {r['db_definition']}")
            lines.append(f"- 교과: {r['subject_category']} / 주차·소분류: {r['note_week']}")
            lines.append(f"- 자동검사 결과: {r['verdict']} — {r['verdict_reason']}")
            lines.append("- **전문가 검수 필요: 예 (자동 검사 통과와 무관하게 항상 표시)**")
            lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 4. main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--literacy-db", type=Path, required=True)
    ap.add_argument("--csv", type=Path, action="append", required=True, help="반복 지정 가능")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-jsonl", type=Path, required=True)
    ap.add_argument("--out-s-cards", type=Path, required=True)
    args = ap.parse_args()

    rows = load_batch_rows(args.csv)
    print(f"입력 배치 CSV {len(args.csv)}개, 적재된(HOLD 제외) 행 {len(rows)}건")

    con = open_literacy_db_ro(args.literacy_db)
    audited = [audit_row(con, r) for r in rows]
    con.close()

    from collections import Counter
    verdict_counts = Counter(r["verdict"] for r in audited)
    print("판정 분포:", dict(verdict_counts))

    fieldnames = list(audited[0].keys())
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_csv, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(audited)
    print(f"결과 CSV 저장: {args.out_csv}")

    args.out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_jsonl, "w", encoding="utf-8") as fh:
        for r in audited:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"결과 JSONL 저장: {args.out_jsonl}")

    s_cards_md = build_s_cards(audited)
    args.out_s_cards.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_s_cards, "w", encoding="utf-8") as fh:
        fh.write(s_cards_md)
    print(f"S 검수 카드 저장: {args.out_s_cards}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
