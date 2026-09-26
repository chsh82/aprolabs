"""`scripts/vocab/audit_l4_l5_batch_quality.py` phase16 리팩터 회귀 테스트.

배경(`reports/schema_reading_phase15_l4l5_quality_audit_20260925.md` 1-2절/7-4절,
`reports/schema_reading_phase16_quiz_pilot_dryrun_20260925.md` 작업 A):
phase15 버전은 "결과표 `caution` 컬럼에는 없지만 phase13 보고서 본문에만 서술된
캐주션"(유추/유사/사법권 3건)을 스크립트 내부 하드코딩 딕셔너리
(`SUPPLEMENTARY_MEANING_REVIEW`)로 보충해서 MEANING_REVIEW 판정에 반영했다. phase16은
1) 그 3건의 캐주션 텍스트를 원천 결과표
   `data/import/schema_reading_phase13_l4_core50_final_20260925.csv`의 `caution`
   컬럼에 직접 기록하고,
2) 스크립트에서 하드코딩 딕셔너리를 완전히 제거해, MEANING_REVIEW 판정이 오직 결과표
   `caution` 컬럼(과 literacy.db 재조회로 얻는 구조적 신호)만으로 재현되도록 리팩터했다.

이 테스트는 (a) 합성 fixture(임시 CSV + 임시 sqlite literacy.db)로 caution 컬럼 유무에
따른 AUTO_PASS/MEANING_REVIEW 분기를 검증하고, (b) 모듈에 더 이상
`SUPPLEMENTARY_MEANING_REVIEW`류 하드코딩 딕셔너리가 존재하지 않음을 직접 확인하며,
(c) 실제 97건 배치 재실행 결과가 phase15가 보고한 분포(L4 43/6, L5 31/17, "유추"·
"유사"·"사법권" 포함 MEANING_REVIEW)와 완전히 동일한지 재확인한다.

**읽기 전용** - `data/literacy.db`는 열지 않고 임시 sqlite 파일만 쓰며(fixture),
실배치 CSV/DB에는 아무것도 쓰지 않는다.

실행:
    python tests/test_audit_l4_l5_caution_column.py
"""
from __future__ import annotations

import csv
import io
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "vocab"))

import audit_l4_l5_batch_quality as albq  # noqa: E402

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    _results.append((name, condition, detail))
    print(f"{_PASS if condition else _FAIL} {name}" + (f" - {detail}" if detail and not condition else ""))


# ---------------------------------------------------------------------------
# fixture 헬퍼
# ---------------------------------------------------------------------------

L4_FIELDNAMES = [
    "group", "v_s", "lemma", "pos", "literacy_source", "literacy_term_id",
    "literacy_definition", "l4_basis", "student_definition", "example_sentence",
    "example_target_form", "subject_category", "note_week", "reuse_check",
    "final_classification", "classification_reason", "caution", "content_id",
    "vocab_level", "level_status",
]


def make_terms_db(tmp_path: Path, terms: list[dict]) -> Path:
    """임시 literacy.db 스타일 sqlite 파일 생성 (읽기 전용 스크립트가 재조회할 대상)."""
    db_path = tmp_path / "fixture_literacy.db"
    con = sqlite3.connect(db_path)
    con.execute(
        """CREATE TABLE terms (
            id TEXT PRIMARY KEY, category TEXT, headword TEXT, origin TEXT,
            definition TEXT, pos TEXT, sense_category TEXT, subject_category TEXT,
            grade_level TEXT, grade_source TEXT, source TEXT, review_status TEXT,
            note TEXT, level INTEGER, reviewed_at TEXT
        )"""
    )
    for t in terms:
        con.execute(
            "INSERT INTO terms (id, category, headword, origin, definition, pos, "
            "sense_category, subject_category, grade_level, grade_source, source, "
            "review_status, note, level, reviewed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                t["id"], "", t["headword"], "", t["definition"], t.get("pos", "명사"), "",
                t.get("subject_category", ""), "", "", t["source"], "검수전", "",
                t["level"], "",
            ),
        )
    con.commit()
    con.close()
    return db_path


def make_l4_csv(tmp_path: Path, rows: list[dict]) -> Path:
    csv_path = tmp_path / "fixture_l4.csv"
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=L4_FIELDNAMES)
        writer.writeheader()
        for r in rows:
            full = {k: "" for k in L4_FIELDNAMES}
            full.update(r)
            writer.writerow(full)
    return csv_path


def base_row(lemma: str, term_id: str, content_id: str, caution: str = "") -> dict:
    return {
        "group": "TEST", "v_s": "V", "lemma": lemma, "pos": "명사",
        "literacy_source": "schemareading-tooldict", "literacy_term_id": term_id,
        "literacy_definition": f"{lemma}의 정의", "l4_basis": "테스트 근거",
        "student_definition": f"{lemma}의 정의", "example_sentence": f"{lemma} 예문.",
        "example_target_form": lemma, "subject_category": "", "note_week": "",
        "reuse_check": "", "final_classification": "V", "classification_reason": "",
        "caution": caution, "content_id": content_id, "vocab_level": "4",
        "level_status": "",
    }


# ---------------------------------------------------------------------------
# 1) 합성 fixture - caution 컬럼 유무에 따른 분기
# ---------------------------------------------------------------------------

def test_caution_column_drives_verdict() -> None:
    print("\n=== 1) 합성 fixture: caution 컬럼 유무에 따른 AUTO_PASS/MEANING_REVIEW 분기 ===")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        terms = [
            {"id": "T1", "headword": "테스트어1", "definition": "테스트어1의 정의",
             "source": "schemareading-tooldict", "level": 4},
            {"id": "T2", "headword": "테스트어2", "definition": "테스트어2의 정의",
             "source": "schemareading-tooldict", "level": 4},
            # phase13 "유추"/"유사"/"사법권" 패턴 재현용 - caution 텍스트가 채워진 행
            {"id": "T3", "headword": "가짜유추어", "definition": "가짜유추어의 정의",
             "source": "schemareading-tooldict", "level": 4},
        ]
        db_path = make_terms_db(tmp_path, terms)

        rows = [
            base_row("테스트어1", "T1", "FIX_0001", caution=""),  # caution 없음 -> AUTO_PASS
            base_row("테스트어2", "T2", "FIX_0002", caution="테스트용 근접어 혼동 위험"),  # caution 있음 -> MEANING_REVIEW
            base_row("가짜유추어", "T3", "FIX_0003", caution="'가짜유사어'와 어근 공유 - 혼동 위험(caution 컬럼에 직접 기록)"),
        ]
        csv_path = make_l4_csv(tmp_path, rows)

        loaded = albq.load_batch_rows([csv_path])
        check("3행 전부 적재됨(content_id 비어있지 않음)", len(loaded) == 3, f"got {len(loaded)}")

        con = albq.open_literacy_db_ro(db_path)
        audited = [albq.audit_row(con, r) for r in loaded]
        con.close()

        by_lemma = {a["lemma"]: a for a in audited}

        check("caution 컬럼이 비어있으면 AUTO_PASS",
              by_lemma["테스트어1"]["verdict"] == "AUTO_PASS",
              f"got {by_lemma['테스트어1']['verdict']}")
        check("caution 컬럼에 값이 있으면 MEANING_REVIEW",
              by_lemma["테스트어2"]["verdict"] == "MEANING_REVIEW",
              f"got {by_lemma['테스트어2']['verdict']}")
        check("MEANING_REVIEW 사유에 caution 텍스트가 그대로 인용됨",
              "테스트용 근접어 혼동 위험" in by_lemma["테스트어2"]["verdict_reason"],
              f"got {by_lemma['테스트어2']['verdict_reason']!r}")
        check("'유추'류 패턴(가짜유추어)도 caution 컬럼만으로 MEANING_REVIEW",
              by_lemma["가짜유추어"]["verdict"] == "MEANING_REVIEW",
              f"got {by_lemma['가짜유추어']['verdict']}")


# ---------------------------------------------------------------------------
# 2) 하드코딩 딕셔너리 미의존 확인
# ---------------------------------------------------------------------------

def test_no_hardcoded_dictionary_dependency() -> None:
    print("\n=== 2) 하드코딩 딕셔너리(SUPPLEMENTARY_MEANING_REVIEW) 미의존 확인 ===")
    check("모듈에 SUPPLEMENTARY_MEANING_REVIEW 속성이 더 이상 존재하지 않음",
          not hasattr(albq, "SUPPLEMENTARY_MEANING_REVIEW"),
          "속성이 남아있음 - 하드코딩 딕셔너리 제거가 완료되지 않음")
    check("모듈에 SUPPLEMENTARY로 시작하는 어떤 전역 딕셔너리도 남아있지 않음",
          not any(name.startswith("SUPPLEMENTARY") for name in dir(albq)),
          f"got names={[n for n in dir(albq) if n.startswith('SUPPLEMENTARY')]}")

    # audit_row 소스 코드 자체에도 하드코딩된 lemma->사유 매핑이 없어야 한다
    # (예: {"유추": ..., "유사": ..., "사법권": ...} 같은 리터럴)
    import inspect
    src = inspect.getsource(albq.audit_row)
    check("audit_row 함수 소스에 '유추'/'유사'/'사법권' 리터럴이 없음(캐주션 컬럼만 읽음)",
          "유추" not in src and "유사" not in src and "사법권" not in src,
          "audit_row가 여전히 특정 lemma를 하드코딩하고 있음")


# ---------------------------------------------------------------------------
# 3) 실제 97건 배치 재실행 - phase15 보고 분포와 완전히 동일한지 재확인
# ---------------------------------------------------------------------------

def test_real_97_batch_matches_phase15_report() -> None:
    print("\n=== 3) 실제 L4(49)+L5(48)=97건 배치 재실행 - phase15 보고 분포 재확인 ===")
    l4_csv = REPO_ROOT / "data" / "import" / "schema_reading_phase13_l4_core50_final_20260925.csv"
    l5_csv = REPO_ROOT / "data" / "import" / "schema_reading_phase14_l5_core_final_20260925.csv"
    literacy_db = REPO_ROOT / "data" / "literacy.db"

    if not (l4_csv.exists() and l5_csv.exists() and literacy_db.exists()):
        check("phase13/14 결과표 + literacy.db 존재", False, "파일 없음 - 이 절 건너뜀")
        return

    rows = albq.load_batch_rows([l4_csv, l5_csv])
    check("HOLD 제외 97건 적재(L4 49 + L5 48)", len(rows) == 97, f"got {len(rows)}")

    con = albq.open_literacy_db_ro(literacy_db)
    audited = [albq.audit_row(con, r) for r in rows]
    con.close()

    from collections import Counter
    l4_rows = [r for r in audited if "phase13" in r["source_file"]]
    l5_rows = [r for r in audited if "phase14" in r["source_file"]]
    l4_counts = Counter(r["verdict"] for r in l4_rows)
    l5_counts = Counter(r["verdict"] for r in l5_rows)

    check("L4: AUTO_PASS 43 / MEANING_REVIEW 6 (phase15 보고와 동일)",
          l4_counts.get("AUTO_PASS") == 43 and l4_counts.get("MEANING_REVIEW", 0) == 6,
          f"got {dict(l4_counts)}")
    check("L5: AUTO_PASS 31 / MEANING_REVIEW 17 (phase15 보고와 동일)",
          l5_counts.get("AUTO_PASS") == 31 and l5_counts.get("MEANING_REVIEW", 0) == 17,
          f"got {dict(l5_counts)}")
    check("SOURCE_EVIDENCE_WEAK 0건 (phase15 보고와 동일)",
          Counter(r["verdict"] for r in audited).get("SOURCE_EVIDENCE_WEAK", 0) == 0,
          "원천근거부족이 발생함 - literacy.db 또는 결과표가 변경됐을 가능성")

    by_lemma = {r["lemma"]: r["verdict"] for r in audited}
    for lemma in ("유추", "유사", "사법권"):
        check(f"'{lemma}': caution 컬럼만으로(하드코딩 없이) 여전히 MEANING_REVIEW",
              by_lemma.get(lemma) == "MEANING_REVIEW",
              f"got {by_lemma.get(lemma)!r}")


def main() -> int:
    test_caution_column_drives_verdict()
    test_no_hardcoded_dictionary_dependency()
    test_real_97_batch_matches_phase15_report()

    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_fail = len(_results) - n_pass
    print(f"\n{n_pass}/{len(_results)} passed" + (f", {n_fail} FAILED" if n_fail else ""))
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
