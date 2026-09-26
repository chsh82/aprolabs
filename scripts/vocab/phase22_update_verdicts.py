# -*- coding: utf-8 -*-
"""Phase22 - phase20 40건 판정표에서 "정기" 2건(HOLD)만 PASS로 전환.
다른 38건은 절대 건드리지 않는다. 1회성 유틸리티(재실행해도 이미 PASS이므로
안전하게 스킵됨 - assert가 이중 실행을 막는다)."""
from __future__ import annotations

import csv
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
csv_path = REPO / "data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.csv"
jsonl_path = REPO / "data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.jsonl"

TARGET_IDS = {"MF_A_SC_SRL4L5PILOT_20260925_L4_008", "MF_C_SC_SRL4L5PILOT_20260925_L4_008"}

NEW_REASON = (
    "정답 유일성/오답 타당성 자체는 문제 없음(오답 공간, 집단, 하위 전부 '정기'와 무관한 개념). "
    "phase22가 vocabulary_quiz_research.db(서버)에서 SR_L4CORE_4786.student_definition을 "
    "게이트 기반 단일 트랜잭션으로 '일정한 기간마다 되풀이하도록 정한 것' -> "
    "'기한이나 기간이 일정하게 정해져 있는 것'으로 갱신했다(적용 완료, 전 게이트 PASS). "
    "재대조 결과 문항 정답 텍스트('기한이나 기간이 일정하게 정해져 있는 것')와 "
    "vocabulary_contents.student_definition(갱신 후, 동일값)이 이제 정확히 일치하며, "
    "원천(literacy.db id=4786, '기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 "
    "기간.')과도 핵심 의미(고정된 기한/기간)가 일치한다(phase17/21이 이미 확정한 판단 재확인). "
    "품사(명사)-레벨(4)-예문('우리 반은 매달 첫째 주에 정기 모임을 엽니다.')과도 의미 충돌 없음. "
    "HOLD 사유였던 문항-콘텐츠 텍스트 불일치가 해소되어 PASS로 전환한다(phase22 재판정)."
)


def main() -> None:
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    fieldnames = list(rows[0].keys())

    changed_csv = 0
    for r in rows:
        if r["item_id"] in TARGET_IDS:
            if r["verdict"] == "PASS":
                print(f"이미 PASS - 스킵: {r['item_id']}")
                continue
            assert r["verdict"] == "HOLD", (r["item_id"], r["verdict"])
            r["verdict"] = "PASS"
            r["reason"] = NEW_REASON
            changed_csv += 1

    if changed_csv:
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    lines = jsonl_path.read_text(encoding="utf-8").splitlines()
    new_lines = []
    changed_jsonl = 0
    for line in lines:
        if not line.strip():
            new_lines.append(line)
            continue
        obj = json.loads(line)
        if obj["item_id"] in TARGET_IDS:
            if obj["verdict"] == "PASS":
                new_lines.append(json.dumps(obj, ensure_ascii=False))
                continue
            assert obj["verdict"] == "HOLD", (obj["item_id"], obj["verdict"])
            obj["verdict"] = "PASS"
            obj["reason"] = NEW_REASON
            changed_jsonl += 1
        new_lines.append(json.dumps(obj, ensure_ascii=False))

    if changed_jsonl:
        jsonl_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    print(f"CSV changed: {changed_csv}, JSONL changed: {changed_jsonl}")


if __name__ == "__main__":
    main()
