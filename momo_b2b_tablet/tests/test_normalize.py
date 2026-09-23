"""① 정규화 단계 완료 기준 검증 - SPEC §9 1단계: "샘플 전부 이름 기준으로
올바르게 매핑, unknown 행 0". 3개 기준 시안(L2-Q2-W08/L5-Q3-W10/L9-Q3-W07)에
직접 조회해 확인한 실제 문제 패턴이 전부 처리되는지도 같이 검증한다.

momo_book_db/momo_book.db를 읽기 전용으로 쓴다(원본 미변경).

실행:
    python tests/test_normalize.py
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

PKG_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = PKG_ROOT.parent
for p in (PKG_ROOT, REPO_ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from normalize.run import normalize_document  # noqa: E402

_PASS, _FAIL = "[PASS]", "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def run() -> bool:
    docs = {did: normalize_document(did) for did in ("L2-Q2-W08", "L5-Q3-W10", "L9-Q3-W07")}

    # ---------- 완료 기준: unknown 행 0 (SPEC §9) ----------
    for did, doc in docs.items():
        check(doc.unresolved_unknown_count() == 0, f"{did}: unknown 행 0건")

    # ---------- band/quarter 이름 기준 매핑 ----------
    check(docs["L2-Q2-W08"].band == "lower", "L2 -> band=lower")
    check(docs["L5-Q3-W10"].band == "elem-upper", "L5 -> band=elem-upper")
    check(docs["L9-Q3-W07"].band == "mid", "L9 -> band=mid")
    check(docs["L2-Q2-W08"].quarter == "spring", "L2 -> quarter=spring(2분기 탐구)")
    check(docs["L5-Q3-W10"].quarter == "summer", "L5 -> quarter=summer(3분기 문학)")

    # ---------- L2(저학년): 독해유형 라벨이 unknown 행에서 추출됨 ----------
    l2 = docs["L2-Q2-W08"]
    check(all(q.reading_type for q in l2.qa), "L2: 모든 QA에 reading_type 채워짐(unknown 행에서 추출)")
    # order_no=2는 실제로 2-1/2-2 두 소문항으로 나뉘어 있어(원본 DB 확인됨) 7개
    # order_no에 총 8개 QA(쓰레기 행 "(21쪽)"/"2"/"(29쪽)" 등은 이미 제거된 수).
    check(len(l2.qa) == 8, f"L2: 쓰레기 행 제거 후 QA 8건(2번은 2-1/2-2 소문항 2개) (실제 {len(l2.qa)}건)")
    check(len({q.order_no for q in l2.qa}) == 7, "L2: 서로 다른 order_no는 7개(1~7)")

    # ---------- L2: ox_quiz가 비어있어 background_text에서 OX 5개 복원됨 (SPEC §4 사례) ----------
    check(len(l2.ox) == 5, f"L2: ox_quiz 비어있어도 background_text에서 OX 5건 복원 (실제 {len(l2.ox)}건)")
    check(l2.ox[0].question.startswith("은비네 가족들은 고양이를 무척 좋아해서"),
          f"L2: 첫 OX 문장이 골든 샘플과 일치 (실제 {l2.ox[0].question[:30]!r})")

    # ---------- L2: 어휘 뜻 5개 전부 누락 -> sup 플래그(missing 아님, LLM 보충 대상) ----------
    l2_sup = [v for v in l2.vocab if v.definition is None]
    check(len(l2_sup) == 5, f"L2: 어휘 뜻풀이 누락 5건 (실제 {len(l2_sup)}건)")
    check(all(any(f.kind == "sup" for f in v.flags) for v in l2_sup),
          "L2: 어휘 뜻풀이 누락은 sup 플래그(LLM/사전DB 보충 대상)")

    # ---------- L5(고학년): 소문항 분리 후 남은 빈 질문 중복 행 제거 ----------
    l5 = docs["L5-Q3-W10"]
    check(all((q.question_text or "").strip() for q in l5.qa),
          "L5: 모든 QA에 빈 question_text 없음(중복 부모 행 제거됨)")
    check(all(q.reading_type for q in l5.qa), "L5: 모든 QA에 reading_type 있음(원본에 이미 있었음)")

    # ---------- L5: OX 정답 5개 전부 null -> missing 플래그(sup 아님) ----------
    l5_ox_missing = [o for o in l5.ox if o.answer is None]
    check(len(l5_ox_missing) == 5, f"L5: OX 정답 누락 5건 (실제 {len(l5_ox_missing)}건)")
    check(all(any(f.kind == "missing" for f in o.flags) for o in l5_ox_missing),
          "L5: OX 정답 누락은 missing 플래그(sup 아님 - 원본에 값 자체가 없는 경우)")

    # ---------- L5: 심하게 뒤섞인 제시문이 split 플래그로 잡힘 ----------
    l5_scrambled = [f for f in l5.all_flags() if f.kind == "split" and "어순" in f.message]
    check(len(l5_scrambled) > 0, f"L5: 어순 뒤섞임 의심 텍스트 split 플래그 감지됨 ({len(l5_scrambled)}건)")

    # ---------- L9(중학생): 한자 뜻풀이 unknown/text_long 행이 hanja_glossary로 분리됨 ----------
    # 사용자가 직접 발견해 지시한 사례: 8개 용어(1,2,3,4,16,17,18,19) - 19번(표현하다)은
    # ui_type='text_long' 행에 5번 질문과 함께 컬럼 밀림으로 붙어 있었다.
    l9 = docs["L9-Q3-W07"]
    check(len(l9.hanja_glossary) == 8, f"L9: 한자 뜻풀이 8건 hanja_glossary로 분리 (실제 {len(l9.hanja_glossary)}건)")
    check(all("\n" not in g.term for g in l9.hanja_glossary), "L9: hanja_glossary 항목이 전부 한 줄짜리 짧은 용어")
    # 골든 샘플(ref.rows)에는 {gap:true} 행이 term 4와 16 "사이"에 딱 하나만
    # 있다 - 19번(표현하다)의 "~"는 생략 표시가 아니라 컬럼 밀림 구분자였을
    # 뿐이므로 gap_after=False가 맞다.
    gap_terms = {g.order_no for g in l9.hanja_glossary if g.gap_after}
    check(gap_terms == {4}, f"L9: 진짜 생략 표시(gap_after=True)는 4번 뒤 하나뿐 (실제 {gap_terms})")

    # ---------- L9: order_no=5 질문이 19번 행에서 컬럼 밀림으로 복원됨(derived 아님) ----------
    l9_derived = [q for q in l9.qa if any(f.kind == "derived" for f in q.flags)]
    check(len(l9_derived) == 0, f"L9: derived 스텁이 더 이상 없음(질문 복원됨) (실제 {len(l9_derived)}건)")
    l9_o5 = sorted((q for q in l9.qa if q.order_no == 5), key=lambda q: q.order_label)
    check(len(l9_o5) == 2 and [q.order_label for q in l9_o5] == ["5-1", "5-2"],
          f"L9: order_no=5가 5-1/5-2 두 질문으로 복원됨 (실제 {[q.order_label for q in l9_o5]})")
    check(all(q.question_text.strip() for q in l9_o5), "L9: 5-1/5-2 둘 다 질문 텍스트가 실제로 채워짐")
    check(all(q.excerpt_text for q in l9_o5), "L9: 5-1/5-2 둘 다 원래 order_no=5의 제시문을 공유")

    # ---------- L9: essay writing_guide에 STEP1이 섞인 것 split 플래그 ----------
    check(l9.essay is not None and any("STEP1" in f.message for f in l9.essay.flags),
          "L9: essay writing_guide의 STEP1 혼입이 split 플래그로 잡힘")

    # ---------- 이미지 저해상도 - SPEC §4가 명시한 정확한 사례(열하일기 204x299) ----------
    l9_lowres = [f for f in l9.all_flags() if f.kind == "lowres" and "204x299" in f.message]
    check(len(l9_lowres) == 1, f"L9: 표지 204x299 저해상도가 SPEC 명시 그대로 감지됨 (실제 {len(l9_lowres)}건)")

    # ---------- 원본 DB는 읽기 전용으로만 열렸는지(연결 자체가 ro 모드) ----------
    from normalize.db import get_connection
    conn = get_connection()
    try:
        try:
            conn.execute("UPDATE documents SET book_title = book_title WHERE 1=0")
            check(False, "원본 DB가 쓰기 가능함(읽기 전용이어야 함)")
        except Exception:
            check(True, "원본 DB 연결이 실제로 읽기 전용(쓰기 시도 시 예외 발생)")
    finally:
        conn.close()

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
