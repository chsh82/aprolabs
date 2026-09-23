"""③ 조판 초안 골든 비교 - 사용자 지시(2026-09-23) "3단계: 조판 규칙 엔진" 4번.

samples/*.layout.json 3종을 기준으로, layout.generate가 만든 초안의 "구조"
(페이지 type, form, part_id, 답란 kind)를 order_label(문항 id) 기준으로 비교한다.
텍스트 내용 비교는 ①단계 golden_compare.py의 몫이라 여기서는 하지 않는다.

골든은 사람이 소수 문항만 골라 실은 "시안"이고, 이 생성기는 정규화된 문항을
전부 초안으로 뽑는다(SPEC §1 "자동화는 초안까지" - 빼는 건 검수 단계 판단이라
생성기가 추측하지 않는다). 그래서:
  - 생성 쪽에만 있는 문항(order_label)  -> 실패 아님, [INFO]로만 남김
  - 골든에만 있는 문항                  -> 실패(정규화가 그 문항을 통째로 잃어버린 것)
  - 둘 다 있는 문항의 구조가 다르면      -> 그 문항에 설명 플래그(derived/split)가
                                           실제로 달려 있는지만 확인한다(사용자 지시
                                           3번 원칙: "검수 단계 수정" 차이는 실패로
                                           보지 않고 플래그 생성 여부만 본다). 플래그가
                                           있으면 [INFO], 없으면 진짜 버그이므로 [FAIL].

실행: python tests/test_layout_golden.py
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from layout.generate import generate_layout  # noqa: E402

_PASS, _FAIL, _INFO = "[PASS]", "[FAIL]", "[INFO]"
_results: list[tuple[str, str]] = []  # (state, label) state in {pass, fail, info}


def check(state: str, label: str) -> None:
    _results.append((state, label))
    tag = {"pass": _PASS, "fail": _FAIL, "info": _INFO}[state]
    print(f"{tag} {label}")


def load_golden(doc_id: str) -> dict:
    return json.loads((REPO_ROOT / "samples" / f"{doc_id}.layout.json").read_text(encoding="utf-8"))


# ---------- renderer.js widget()/question()의 part_id·kind 파생을 그대로 포팅 ----------
def _widget_parts(q: dict) -> list[dict]:
    f = q.get("form") or ("blanks" if q.get("blanks") else "single")
    qid = q["id"]
    if f == "blanks":
        return [{"id": f"{qid}#{i + 1}", "kind": "blankTall"} for i in range(len(q.get("blanks", [])))]
    if f in ("single", "speech"):
        return [{"id": qid, "kind": q.get("kind") or ("cardInk" if f == "speech" else "long")}]
    if f == "list":
        return [{"id": f"{qid}#{i + 1}", "kind": q.get("rowKind", "row")} for i in range(len(q.get("items", [])))]
    if f == "compare":
        parts = []
        for ci, c in enumerate(q.get("cards", []), start=1):
            if c.get("n"):
                parts += [{"id": f"{qid}#{ci}-{k + 1}", "kind": "row"} for k in range(c["n"])]
            else:
                parts.append({"id": f"{qid}#{ci}", "kind": "cardInk"})
        return parts
    if f == "table":
        return [{"id": f"{qid}#{i + 1}", "kind": "cell"} for i in range(len(q.get("rows", [])))]
    if f == "pledge":
        n = q.get("n", 3)
        return [{"id": f"{qid}#name", "kind": "inline"}] + [{"id": f"{qid}#{i + 1}", "kind": "row"} for i in range(n)]
    if f == "choice":
        return [{"id": f"{qid}#choice", "kind": "choice"}, {"id": f"{qid}#reason", "kind": "cardInk"}]
    if f == "choiceList":
        return [{"id": f"{qid}#choice", "kind": "choiceList"}]
    return [{"id": qid, "kind": q.get("kind", "long")}]


def _question_parts(q: dict) -> list[dict]:
    if q.get("form") and q["form"] != "blanks":
        return _widget_parts(q)
    qid = q["id"]
    if q.get("kind") == "multi":
        return [{"id": f"{qid}#{i + 1}", "kind": "blank"} for i in range(len(q.get("blanks", [])))]
    return [{"id": qid, "kind": q.get("kind", "long")}]


def parts_for_page(page: dict) -> list[dict]:
    """renderPage()가 "qa" 타입만 question()으로, 나머지(qaband/qaref/solo)는
    widget()로 직접 넘긴다 - 그래서 같은 q라도 페이지 타입에 따라 답란 kind가
    달라질 수 있다(blank vs blankTall). 이 구분을 그대로 반영해야 한다."""
    q = page.get("q")
    if q is None:
        return []
    if page["type"] == "qa":
        return _question_parts(q)
    return _widget_parts(q)


def form_signature(q: dict) -> tuple:
    return (q.get("form"), q.get("kind"))


def index_step2_by_label(layout: dict) -> dict[str, dict]:
    return {p["q"]["id"]: p for p in layout["pages"] if "q" in p}


def flags_for_order_no(flags, order_no: int) -> list:
    return [f for f in flags if f.order_no == order_no]


def run() -> bool:
    for doc_id in ("L2-Q2-W08", "L5-Q3-W10", "L9-Q3-W07"):
        print(f"\n--- {doc_id} ---")
        layout, flags = generate_layout(doc_id)
        golden = load_golden(doc_id)

        # ---------- 문서 헤더: band/grade/quarter (가장 깨끗하게 결정론적인 부분) ----------
        check("pass" if layout["tone"]["band"] == golden["tone"]["band"] else "fail",
              f"{doc_id} tone.band 일치 (생성 {layout['tone']['band']} / 골든 {golden['tone']['band']})")
        check("pass" if layout["tone"]["grade"] == golden["tone"]["grade"] else "fail",
              f"{doc_id} tone.grade 일치 (생성 {layout['tone']['grade']} / 골든 {golden['tone']['grade']})")
        check("pass" if layout["quarter"] == golden["quarter"] else "fail",
              f"{doc_id} quarter 일치 (생성 {layout['quarter']} / 골든 {golden['quarter']})")

        # ---------- STEP1(중등 배경지식 연표) - 사용자 지시(2026-09-23) "L9-Q3-W07의
        # 연표가 golden의 bgline.rows와 구조적으로 일치하는지 비교" ----------
        mine_bgline = next((p for p in layout["pages"] if p["type"] == "bgline"), None)
        gold_bgline = next((p for p in golden["pages"] if p["type"] == "bgline"), None)
        if gold_bgline is not None:
            if mine_bgline is None:
                check("fail", f"{doc_id} bgline: 골든에는 있는데 생성 결과에 없음")
            else:
                mine_nodes = [n for row in mine_bgline["rows"] for n in row]
                gold_nodes = [n for row in gold_bgline["rows"] for n in row]
                check("pass" if len(mine_nodes) == len(gold_nodes) else "fail",
                      f"{doc_id} bgline 노드 총 개수 일치 (생성 {len(mine_nodes)} / 골든 {len(gold_nodes)})")
                mine_e = [n["e"] for n in mine_nodes]
                gold_e = [n["e"] for n in gold_nodes]
                exact = mine_e == gold_e
                check("pass" if exact else "info",
                      f"{doc_id} bgline 노드 텍스트(e) 순서까지 완전 일치: {exact} "
                      f"(다르면 괄호 부속어구 정리 등 자잘한 표현 차이 - 아래 y 대조 참고)")
                gold_y_by_e = {n["e"]: n.get("y") for n in gold_nodes}
                y_matches = sum(1 for n in mine_nodes if n["e"] in gold_y_by_e and n.get("y") == gold_y_by_e[n["e"]])
                check("pass" if y_matches == len(gold_nodes) else "info",
                      f"{doc_id} bgline 연도(y) 값이 golden과 같은 노드 {y_matches}/{len(gold_nodes)}건 "
                      f"(같은 e값 노드 기준) - hl/end 강조는 검수 몫이라 비교 안 함(사용자 지시)")
                mine_row_sizes = [len(r) for r in mine_bgline["rows"]]
                gold_row_sizes = [len(r) for r in gold_bgline["rows"]]
                check("pass" if sorted(mine_row_sizes) == sorted(gold_row_sizes) else "info",
                      f"{doc_id} bgline 줄 크기 분포 일치(멀티셋 기준, 경계 위치는 다를 수 있음) "
                      f"(생성 {mine_row_sizes} / 골든 {gold_row_sizes})")

        mine_by_label = index_step2_by_label(layout)
        gold_by_label = index_step2_by_label(golden)

        missing_in_mine = set(gold_by_label) - set(mine_by_label)
        for label in sorted(missing_in_mine):
            check("fail", f"{doc_id} 문항[{label}]: 골든에는 있는데 생성 결과에 없음(정규화가 문항을 잃어버림)")

        extra_in_mine = set(mine_by_label) - set(gold_by_label)
        if extra_in_mine:
            check("info", f"{doc_id}: 생성 결과에만 있는 문항 {len(extra_in_mine)}건 {sorted(extra_in_mine)} - "
                          f"골든은 사람이 고른 일부만 실은 시안이라 실패 아님(SPEC §1 '자동화는 초안까지')")

        for label in sorted(set(mine_by_label) & set(gold_by_label)):
            mine_p, gold_p = mine_by_label[label], gold_by_label[label]
            order_no = int(label.split("-")[0])
            related_flags = flags_for_order_no(flags, order_no)
            has_explain_flag = any(f.kind in ("derived", "split") for f in related_flags)

            if mine_p["type"] == gold_p["type"]:
                check("pass", f"{doc_id} 문항[{label}] 페이지 type 일치 ({mine_p['type']})")
            else:
                state = "info" if has_explain_flag else "fail"
                check(state, f"{doc_id} 문항[{label}] 페이지 type 다름 (생성 {mine_p['type']} / 골든 {gold_p['type']})"
                             f"{' - 설명 플래그 있음' if has_explain_flag else ' - 설명 플래그 없음(버그로 봐야 함)'}")

            mine_sig, gold_sig = form_signature(mine_p["q"]), form_signature(gold_p["q"])
            if mine_sig == gold_sig:
                check("pass", f"{doc_id} 문항[{label}] form/kind 일치 {mine_sig}")
            else:
                state = "info" if has_explain_flag else "fail"
                check(state, f"{doc_id} 문항[{label}] form/kind 다름 (생성 {mine_sig} / 골든 {gold_sig})"
                             f"{' - 설명 플래그 있음' if has_explain_flag else ' - 설명 플래그 없음(버그로 봐야 함)'}")

            mine_parts, gold_parts = parts_for_page(mine_p), parts_for_page(gold_p)
            mine_ids, gold_ids = [p["id"] for p in mine_parts], [p["id"] for p in gold_parts]
            if mine_ids == gold_ids:
                check("pass", f"{doc_id} 문항[{label}] part_id 목록 일치 {mine_ids}")
            else:
                state = "info" if has_explain_flag else "fail"
                check(state, f"{doc_id} 문항[{label}] part_id 목록 다름 (생성 {mine_ids} / 골든 {gold_ids})"
                             f"{' - 설명 플래그 있음' if has_explain_flag else ' - 설명 플래그 없음(버그로 봐야 함)'}")

            mine_kinds = [p["kind"] for p in mine_parts]
            gold_kinds = [p["kind"] for p in gold_parts]
            if mine_ids == gold_ids:  # part 개수가 다르면 kind 비교는 의미가 없어서 개수 일치할 때만
                if mine_kinds == gold_kinds:
                    check("pass", f"{doc_id} 문항[{label}] 답란 kind 일치 {mine_kinds}")
                else:
                    state = "info" if has_explain_flag else "fail"
                    check(state, f"{doc_id} 문항[{label}] 답란 kind 다름 (생성 {mine_kinds} / 골든 {gold_kinds})"
                                 f"{' - 설명 플래그 있음' if has_explain_flag else ' - 설명 플래그 없음(버그로 봐야 함)'}")

    n_fail = sum(1 for state, _ in _results if state == "fail")
    n_info = sum(1 for state, _ in _results if state == "info")
    n_pass = sum(1 for state, _ in _results if state == "pass")
    print(f"\n총 {len(_results)}건 - 통과 {n_pass} / 정보(설명된 차이) {n_info} / 실패 {n_fail}")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
