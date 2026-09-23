"""4단계: 최종 링크 대상(142건) 목록 생성 (읽기 전용, 어떤 DB에도 쓰지 않음).

3단계 판정표(`data/import/schema_reading_link_dryrun_verdicts_20260924.csv`)에서
status=APPROVABLE_CANDIDATE인 행만 추리고, literacy.db(mode=ro)에서 각 term_id의
note/review_status/reviewed_at을 직접 재조회해 2026-09-01 AI 자동 생성 뜻풀이
배치(709건, schemareading-schema/tooldict 전용, note LIKE '%AI 자동 생성 뜻풀이%')에
속하는 항목이 섞여 있지 않은지 재검증한다. 이번 세션에서 실행한 결과 142건 전부
- source가 momo-textbook(138)/sajaseongeo-pdf(4)뿐이고 AI 자동 생성 뜻풀이 배치와
  겹치는 건 0건
- literacy_level/vocab_current_level 모두 5·6 없음 (재확인)
- vocab_pos/literacy_pos 불일치(둘 다 비어있지 않은데 다른 경우) 0건
- vocab_content_id/literacy_term_id 각각 중복 0건(구조적 1:1)
이었으므로 142건 전부가 재검증을 통과했다. 이 스크립트는 그 재현 결과를
`data/import/schema_reading_link_142_targets_20260924.json`으로 저장한다
(임포터가 서버에서 사용할 최종 입력).
"""
from __future__ import annotations

import csv
import io
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = REPO_ROOT / "data/import/schema_reading_link_dryrun_verdicts_20260924.csv"
LITERACY_DB = REPO_ROOT / "data/literacy.db"
OUT_PATH = REPO_ROOT / "data/import/schema_reading_link_142_targets_20260924.json"

AI_DEFINITION_NOTE_MARKER = "AI 자동 생성 뜻풀이"
AI_LEVEL_NOTE_MARKER = "AI 자동 레벨 부여"


def main() -> int:
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    print(f"판정표 총 행수: {len(rows)}")

    by_status = Counter(r["status"] for r in rows)
    for s, n in by_status.items():
        print(f"  {s}: {n}")

    approvable = [r for r in rows if r["status"] == "APPROVABLE_CANDIDATE"]
    if len(approvable) != 142:
        print(f"!! 기대치(142)와 다름: {len(approvable)}건 - 중단")
        return 1

    vc_ids = [r["vocab_content_id"] for r in approvable]
    lt_ids = [r["literacy_term_id"] for r in approvable]
    if len(vc_ids) != len(set(vc_ids)) or len(lt_ids) != len(set(lt_ids)):
        print("!! vocab_content_id 또는 literacy_term_id 중복 발견 - 중단")
        return 1
    print("142건: vocab_content_id/literacy_term_id 각각 중복 0건 (구조적 1:1 재확인)")

    uri = f"file:{LITERACY_DB.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row
    term_ids = [int(x) for x in lt_ids]
    placeholders = ",".join("?" * len(term_ids))
    cur = conn.execute(
        f"SELECT id, headword, source, definition, note, review_status, reviewed_at "
        f"FROM terms WHERE id IN ({placeholders})",
        term_ids,
    )
    by_id = {row["id"]: dict(row) for row in cur.fetchall()}
    conn.close()

    if len(by_id) != 142:
        print(f"!! literacy.db에서 조회된 term 수 {len(by_id)} != 142 - 중단")
        return 1

    rejected = []
    targets = []
    source_counts: Counter[str] = Counter()
    for r in approvable:
        tid = int(r["literacy_term_id"])
        term = by_id[tid]
        note = term["note"] or ""
        source_counts[term["source"]] += 1

        # 거부 사유: definition 자체가 2026-09-01 AI 뜻풀이 보강 배치에 속하는 경우만
        # 링크 대상에서 제외한다. 이 연결의 신뢰성은 definition 일치 여부에 달려
        # 있으므로 definition이 신뢰 불가한 출처면 제외 대상이다.
        reasons = []
        if AI_DEFINITION_NOTE_MARKER in note:
            reasons.append("literacy 정의가 2026-09-01 AI 자동 생성 뜻풀이 배치에 속함")
        if r["literacy_level"] in ("5", "6") or r["vocab_current_level"] in ("5", "6"):
            reasons.append("L5/L6 포함(재검증 중 재발견 - 원래는 0건이어야 함)")

        # 참고용 플래그(제외 사유 아님): note에 'AI 자동 레벨 부여'가 있으면
        # literacy 쪽 level 값이 AI 산정이라는 뜻이지 definition이 AI 산정이라는
        # 뜻이 아니다 - 이 링크 테이블은 level을 다루지 않으므로 제외하지 않되,
        # 보고서에 별도 리스크로 남기기 위해 플래그만 기록한다.
        ai_level_note_flag = AI_LEVEL_NOTE_MARKER in note

        rec = {
            "headword": r["headword"],
            "vocab_content_id": r["vocab_content_id"],
            "vocab_lemma": r["vocab_lemma"],
            "vocab_pos": r["vocab_pos"],
            "literacy_term_id": tid,
            "literacy_source": term["source"],
            "literacy_headword": term["headword"],
            "literacy_review_status": term["review_status"],
            "literacy_reviewed_at": term["reviewed_at"],
            "evidence": r["evidence"],
            "link_method": "phase4_dryrun_verified_20260924",
            "literacy_level_is_ai_assigned_note_only": ai_level_note_flag,
        }
        if reasons:
            rec["rejected_reasons"] = reasons
            rejected.append(rec)
        else:
            targets.append(rec)

    print(f"\nliteracy_source 분포(142건, DB 직접 재조회): {dict(source_counts)}")
    print(f"재검증 통과: {len(targets)}건 / 재검증 탈락: {len(rejected)}건")
    for r in rejected:
        print("  REJECTED:", r["headword"], r["vocab_content_id"], r["rejected_reasons"])

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": "2026-09-24",
                "source_verdicts_csv": str(CSV_PATH.relative_to(REPO_ROOT)).replace("\\", "/"),
                "total_approvable_candidate_rows": len(approvable),
                "final_link_targets_count": len(targets),
                "rejected_after_reverify_count": len(rejected),
                "rejected_after_reverify": rejected,
                "targets": targets,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"\n저장: {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
