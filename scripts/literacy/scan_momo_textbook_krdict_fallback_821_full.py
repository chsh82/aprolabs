# -*- coding: utf-8 -*-
"""phase8 작업1 - momo-textbook 821건 전체에서 krdict definitions[0] 고정 채택
패턴을 찾는 전수 스캔. 읽기 전용(literacy.db mode=ro, momo_book.db mode=ro).

방법(기존 스크립트 재사용/확장, 새로 발명하지 않음):
- import_textbook_vocab.py의 실제 로직을 그대로 재현한다:
    definition = rep.definition or entry.definitions[0]
  즉 momo_book.db 원본 definition이 falsy(NULL/빈 문자열)였고 krdict 매칭
  entry가 있었던 행이 "krdict definitions[0] 고정 채택" 후보다.
- scan_momo_book_repr_errors_26.py/build_repr_errors_26_verdict_table.py가 쓴
  "krdict 원본 재조회 + momo_book.db 내 다른 후보 정의와 대조" 방법을 그대로
  재사용하되, 이번에는 "56개 다중레벨 중복 그룹" 제한을 풀고 momo_book.db 전체
  939행에서 같은 정제 표제어를 가진 모든 행(레벨 무관, 단일 레벨 중복 포함)을
  교차검증 후보로 쓴다.
- 위험 신호: krdict 동음이의(homonym_count>1) 또는 단일 항목 다의어
  (len(entry.definitions)>1). 위험 신호가 없으면(=krdict 자체가 뜻이 하나뿐)
  독립 근거 없이도 오류 가능성이 낮다고 보되, 그 자체도 "정황 근거"로만 보고
  자동으로 NO_ISSUE 확정하지 않는다(아래 분류 로직 참고).
- 위험 신호가 있는 행은 momo_book.db 내 동일 표제어의 다른 행(정의가 있는 것)과
  krdict 전체 sense 목록을 대조해서만 REPLACE_CANDIDATE로 승격한다. 교차검증
  대상이 아예 없으면(momo_book.db에 해당 표제어가 이 행 하나뿐) HOLD.
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))
from krdict_dump import Entry, iter_entries  # noqa: E402
from normalize import clean_headword, is_suspected_swap  # noqa: E402

TEXTBOOK_DB = REPO_ROOT / "momo_book_db" / "momo_book.db"
LITERACY_DB = REPO_ROOT / "data" / "literacy.db"
KRDICT_XML_DIR = REPO_ROOT / "raw" / "krdict" / "krdict_dump"

# 기존 26건 판정표(재현 확인용) - 하드코딩하지 않고 CSV를 그대로 읽는다.
VERDICT_26_CSV = REPO_ROOT / "reports" / "literacy_repr_errors_26_verdict_20260924.csv"


def load_momo_book_rows():
    conn = sqlite3.connect(f"file:{TEXTBOOK_DB.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT v.id, v.word, v.definition, v.example_sentence, d.level
        FROM vocabulary v JOIN documents d ON v.doc_id = d.doc_id
        """
    ).fetchall()
    conn.close()
    return rows


def load_literacy_momo_textbook_terms():
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    rows = conn.execute(
        """
        SELECT id, headword, definition, pos, external_id, note, review_status, level
        FROM terms WHERE source = 'momo-textbook'
        """
    ).fetchall()
    conn.close()
    return rows


def build_krdict_index():
    xml_paths = sorted(KRDICT_XML_DIR.glob("*.xml"))
    index: dict[str, list[Entry]] = defaultdict(list)
    for entry in iter_entries(xml_paths):
        index[entry.headword].append(entry)
    return index


def pick_krdict_match(index, headword):
    """import_textbook_vocab.py의 pick_krdict_match를 그대로 재현."""
    candidates = index.get(headword)
    if not candidates:
        return None, 0
    word_candidates = [e for e in candidates if e.lexical_unit == "단어"]
    chosen_pool = word_candidates or candidates
    return chosen_pool[0], len(candidates)


def _norm_text(s: str) -> str:
    return re.sub(r"[\s.。,]", "", s or "")


def text_overlap_ratio(a: str, b: str) -> float:
    """단순 자카드 유사도(문자 2-gram 기준) - 외부 라이브러리 없이 의미 근접도
    추정. 완전한 의미 대조는 아니지만 "명백히 다른 문장인지/거의 같은 문장인지"
    를 가르는 데는 충분하다(사람이 evidence 텍스트를 최종 확인하는 것을 전제)."""
    na, nb = _norm_text(a), _norm_text(b)
    if not na or not nb:
        return 0.0
    ga = {na[i:i + 2] for i in range(len(na) - 1)} or {na}
    gb = {nb[i:i + 2] for i in range(len(nb) - 1)} or {nb}
    inter = len(ga & gb)
    union = len(ga | gb)
    return inter / union if union else 0.0


def main():
    momo_rows = load_momo_book_rows()
    momo_by_id = {r["id"]: r for r in momo_rows}

    # momo_book.db 전체(939행)에서 정제 표제어별로 그룹화 - "56개 다중레벨 그룹"
    # 제한을 풀고 전체를 교차검증 후보 풀로 쓴다(단일 레벨 중복 포함).
    momo_by_clean_headword = defaultdict(list)
    for r in momo_rows:
        clean = clean_headword(r["word"])
        if is_suspected_swap(clean.cleaned):
            continue
        momo_by_clean_headword[clean.cleaned].append(r)

    terms = load_literacy_momo_textbook_terms()
    assert len(terms) == 821, f"기대 821건, 실제 {len(terms)}건 - 범위 재확인 필요"

    print(f"momo-textbook terms: {len(terms)}건 (literacy.db, mode=ro)")

    krdict_index = build_krdict_index()
    print(f"krdict index 표제어 수: {len(krdict_index)}")

    fallback_rows = []  # definitions[0] 고정 채택이 일어난 행
    not_fallback = []   # 원본 정의가 있었거나 krdict 매칭이 아예 없었던 행

    for t in terms:
        ext_id = t["external_id"]
        momo_row = momo_by_id.get(int(ext_id)) if ext_id else None
        orig_def = momo_row["definition"] if momo_row else None
        orig_falsy = not orig_def or not orig_def.strip()

        entry, homonym_count = pick_krdict_match(krdict_index, t["headword"])

        is_fallback = bool(orig_falsy and entry is not None and entry.definitions)
        rec = {
            "literacy_id": t["id"],
            "headword": t["headword"],
            "external_id": ext_id,
            "momo_orig_definition": orig_def,
            "literacy_current_definition": t["definition"],
            "note": t["note"],
            "review_status": t["review_status"],
            "entry_definitions": entry.definitions if entry else None,
            "homonym_count": homonym_count,
            "is_fallback": is_fallback,
        }
        if is_fallback:
            fallback_rows.append(rec)
        else:
            not_fallback.append(rec)

    print(f"\n=== krdict definitions[0] 고정 채택 후보(fallback) 건수: {len(fallback_rows)} / 821 ===")
    print(f"    (원본 momo_book.db definition 없음 + krdict 매칭 있음)")
    print(f"나머지(원본 정의 있음 또는 krdict 매칭 없음): {len(not_fallback)}")

    # fallback_rows 중 실제로 현재 literacy.db definition == entry.definitions[0]
    # 인지 검증(코드가 실제로 이렇게 동작했는지 재확인 - 불일치가 있으면 이상 신호)
    mismatch = [r for r in fallback_rows if r["literacy_current_definition"] != r["entry_definitions"][0]]
    print(f"\nfallback 행 중 literacy.db definition != entry.definitions[0]: {len(mismatch)}건 "
          f"(0이어야 정상 - import 코드 재현이 맞다는 뜻)")
    if mismatch:
        for r in mismatch[:10]:
            print(f"  id={r['literacy_id']} {r['headword']!r} 현재={r['literacy_current_definition']!r} "
                  f"vs entry[0]={r['entry_definitions'][0]!r}")

    # 위험 신호 분류
    risk_rows = []
    no_risk_rows = []
    for r in fallback_rows:
        has_homonym_risk = r["homonym_count"] > 1
        has_polysemy_risk = len(r["entry_definitions"]) > 1
        if has_homonym_risk or has_polysemy_risk:
            r["risk_type"] = ("동음이의" if has_homonym_risk else "") + \
                              ("+" if has_homonym_risk and has_polysemy_risk else "") + \
                              ("다의어" if has_polysemy_risk else "")
            risk_rows.append(r)
        else:
            no_risk_rows.append(r)

    print(f"\n=== 위험 신호 있음(동음이의 또는 다의어) fallback 행: {len(risk_rows)} ===")
    print(f"위험 신호 없음(krdict 뜻이 애초에 하나뿐): {len(no_risk_rows)}")

    # 위험 신호 있는 행에 대해 momo_book.db 내 동일 표제어의 다른 행과 교차검증.
    # 중요: 동음이의(homonym) 케이스는 "선택된 entry의 definitions"만 봐서는
    # 안 된다 - 유용하다/관대하다처럼 정답 뜻이 "선택 안 된 다른 동음이의
    # entry"에 있을 수 있다(phase7이 실측한 패턴). 그래서 sense pool은 항상
    # krdict_index[headword]의 모든 entry(모든 동음이의)를 대상으로 한다.
    SIM_THRESHOLD = 0.35
    for r in risk_rows:
        headword = r["headword"]
        rep_ext_id = int(r["external_id"])
        others = [m for m in momo_by_clean_headword.get(headword, []) if m["id"] != rep_ext_id]
        others_with_def = [m for m in others if m["definition"] and m["definition"].strip()]

        all_entries_for_headword = krdict_index.get(headword, [])
        # 선택된(1번 채택된) entry와 sense index 0 = "현재 채택값"의 위치
        chosen_entry, _ = pick_krdict_match(krdict_index, headword)

        verdict = None
        evidence = ""

        if not others_with_def:
            verdict = "HOLD"
            evidence = (f"momo_book.db에 '{headword}'의 다른 정의 있는 후보가 없음"
                        f"(이 표제어는 momo_book.db에 이 행 하나뿐이거나 다른 중복이 전부 정의 NULL) - "
                        f"krdict 후보 {len(all_entries_for_headword)}개 entry, "
                        f"총 senses {sum(len(e.definitions) for e in all_entries_for_headword)}개 중 "
                        f"교재 맥락과 대조할 독립 근거가 없어 HOLD.")
        else:
            # 모든 동음이의 entry의 모든 sense를 후보 풀로 삼아 momo_book.db
            # 다른 후보 정의들과 유사도 비교
            best = None  # (entry, sense_idx, other_row, ratio)
            for e in all_entries_for_headword:
                for idx, sense in enumerate(e.definitions):
                    for o in others_with_def:
                        ratio = text_overlap_ratio(sense, o["definition"])
                        if best is None or ratio > best[3]:
                            best = (e, idx, o, ratio)
            if best and best[3] >= SIM_THRESHOLD:
                b_entry, b_idx, matched_other, ratio = best
                is_current = (b_entry is chosen_entry and b_idx == 0)
                if is_current:
                    verdict = "NO_ISSUE"
                    evidence = (f"momo_book.db 다른 후보(id={matched_other['id']}, level={matched_other['level']}, "
                                f"def={matched_other['definition']!r})가 현재 채택된 정의(entry={b_entry.external_id}, "
                                f"sense[0])와 가장 유사(유사도 {ratio:.2f}) - 오류 아님.")
                else:
                    verdict = "REPLACE_CANDIDATE"
                    cross_homonym = " (다른 동음이의 entry)" if b_entry is not chosen_entry else " (같은 entry의 다른 sense)"
                    evidence = (f"momo_book.db 다른 후보(id={matched_other['id']}, level={matched_other['level']}, "
                                f"def={matched_other['definition']!r})가 krdict entry={b_entry.external_id} "
                                f"sense[{b_idx}]({b_entry.definitions[b_idx]!r})"
                                f"{cross_homonym}과 가장 유사(유사도 {ratio:.2f}) - "
                                f"현재 채택된 entry={chosen_entry.external_id if chosen_entry else None} sense[0]"
                                f"({chosen_entry.definitions[0] if chosen_entry else None!r})과 다름 - "
                                f"definitions[0] 고정 채택이 다른 뜻을 놓쳤다는 근거.")
            else:
                verdict = "HOLD"
                evidence = (f"momo_book.db 다른 후보 {len(others_with_def)}건이 있으나 krdict 어느 sense와도 "
                            f"뚜렷하게(유사도>={SIM_THRESHOLD})일치하지 않음 - 독립 근거 불충분, HOLD.")

        r["verdict"] = verdict
        r["evidence"] = evidence
    verdicts = risk_rows

    replace = [r for r in verdicts if r["verdict"] == "REPLACE_CANDIDATE"]
    no_issue = [r for r in verdicts if r["verdict"] == "NO_ISSUE"]
    hold = [r for r in verdicts if r["verdict"] == "HOLD"]

    print(f"\n=== 위험 신호 행 {len(risk_rows)}건 판정 결과 ===")
    print(f"  REPLACE_CANDIDATE: {len(replace)}")
    print(f"  NO_ISSUE: {len(no_issue)}")
    print(f"  HOLD: {len(hold)}")

    print("\n--- REPLACE_CANDIDATE 목록 ---")
    for r in replace:
        print(f"  id={r['literacy_id']} {r['headword']!r} (homonym={r['homonym_count']}, "
              f"senses={len(r['entry_definitions'])})")
        print(f"    현재: {r['literacy_current_definition']!r}")
        print(f"    근거: {r['evidence']}")

    # 기존 26건 판정표와 대조(재현 확인) - NULL_DEF err_type 25건 중 krdict
    # fallback에 해당하는 행만(SHORT_DEF/PUNCT_DAMAGE는 krdict fallback 패턴이
    # 아니라 momo_book.db 자체 텍스트 손상이므로 이 절에서는 제외하고 별도 보고)
    print("\n=== 기존 26건 판정표(NULL_DEF 21건)와의 교차 확인 ===")
    import csv
    existing = {}
    with open(VERDICT_26_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            existing[int(row["literacy_id"])] = row

    fallback_by_id = {r["literacy_id"]: r for r in fallback_rows}
    verdict_by_id = {r["literacy_id"]: r for r in verdicts}

    reproduced_replace = 0
    reproduced_no_issue = 0
    not_reproduced = []
    for lid, row in existing.items():
        if row["err_type"] != "NULL_DEF":
            continue  # SHORT_DEF/PUNCT_DAMAGE는 krdict fallback과 무관 - 별도 취급
        in_fallback = lid in fallback_by_id
        new_verdict = verdict_by_id.get(lid)
        old_verdict = row["verdict"]
        status = "in_fallback_pool" if in_fallback else "NOT_IN_FALLBACK_POOL(이상)"
        computed = new_verdict["verdict"] if new_verdict else ("NO_RISK(krdict 뜻 1개뿐)" if in_fallback else "N/A")
        match = (old_verdict == computed)
        if old_verdict == "REPLACE_CANDIDATE" and match:
            reproduced_replace += 1
        elif old_verdict == "NO_ISSUE" and (computed in ("NO_ISSUE", "NO_RISK(krdict 뜻 1개뿐)")):
            reproduced_no_issue += 1
        else:
            not_reproduced.append((lid, row["headword"], old_verdict, computed, status))
        print(f"  id={lid} {row['headword']} 기존판정={old_verdict} 신규계산={computed} {status}")

    print(f"\n재현 요약: 기존 REPLACE_CANDIDATE 중 새 방법으로도 REPLACE_CANDIDATE로 재현: {reproduced_replace}/4")
    print(f"기존 NO_ISSUE(NULL_DEF) 중 새 방법으로도 NO_ISSUE/NO_RISK로 재현: {reproduced_no_issue}/17")
    if not_reproduced:
        print("재현 불일치:")
        for x in not_reproduced:
            print(f"  {x}")

    # 신규 발견(기존 26건에 없던 REPLACE_CANDIDATE)
    existing_ids = set(existing.keys())
    new_replace = [r for r in replace if r["literacy_id"] not in existing_ids]
    print(f"\n=== 신규 발견 REPLACE_CANDIDATE(기존 26건 밖, 821 전수에서 새로 찾음): {len(new_replace)}건 ===")
    for r in new_replace:
        print(f"  id={r['literacy_id']} {r['headword']!r}")
        print(f"    현재: {r['literacy_current_definition']!r}")
        print(f"    근거: {r['evidence']}")

    new_hold = [r for r in hold if r["literacy_id"] not in existing_ids]
    print(f"\n신규 HOLD(기존 26건 밖): {len(new_hold)}건")

    # 저장
    out = {
        "total_momo_textbook_terms": len(terms),
        "fallback_count": len(fallback_rows),
        "fallback_mismatch_count": len(mismatch),
        "risk_count": len(risk_rows),
        "no_risk_count": len(no_risk_rows),
        "risk_verdicts": {
            "REPLACE_CANDIDATE": len(replace),
            "NO_ISSUE": len(no_issue),
            "HOLD": len(hold),
        },
        "reproduced_replace_of_4": reproduced_replace,
        "reproduced_no_issue_of_17": reproduced_no_issue,
        "not_reproduced": not_reproduced,
        "new_replace_candidates": [
            {"literacy_id": r["literacy_id"], "headword": r["headword"],
             "current_definition": r["literacy_current_definition"],
             "evidence": r["evidence"]}
            for r in new_replace
        ],
        "new_hold": [
            {"literacy_id": r["literacy_id"], "headword": r["headword"], "evidence": r["evidence"]}
            for r in new_hold
        ],
        "all_replace_candidates": [
            {"literacy_id": r["literacy_id"], "headword": r["headword"],
             "current_definition": r["literacy_current_definition"],
             "entry_definitions": r["entry_definitions"],
             "evidence": r["evidence"]}
            for r in replace
        ],
    }
    outpath = REPO_ROOT / "data" / "import" / "krdict_fallback_821_full_audit_20260924.json"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n저장: {outpath}")


if __name__ == "__main__":
    main()
