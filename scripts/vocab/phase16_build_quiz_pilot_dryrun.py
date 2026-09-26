# -*- coding: utf-8 -*-
"""phase16 파일럿 문항 초안 생성 (dry-run, DB 미적재). 결과: CSV/JSONL만 파일로 출력.

선정 20건(L4 V10 + L5 V10, 전부 V항목 - S 불필요)에 대해 기존 vocabulary_multiformat_items
포맷(MEANING_CHOICE/CONTEXT_MEANING)을 그대로 따라 문항 초안 40건(20x2형식)을 만든다.
오답 선택지는 "같은 배치 안 다른 표제어의 실제 정의"를 그대로 가져와 의미를 대조한다
(무작위 생성 금지). 자동검증(정답 유일성/선택지 중복/정의-목표뜻 일치)도 이 스크립트가 함께 수행한다.
"""
import csv
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# (lemma, content_id, term_id, pos, definition, example_sentence, example_target_form, vocab_level)
L4 = [
    ("착수", "SR_L4CORE_4813", "4813", "명사", "어떤 일을 시작하여 손을 대는 것",
     "도서관 공사가 다음 주에 착수될 예정입니다.", "착수", "4"),
    ("조치", "SR_L4CORE_4794", "4794", "명사", "상황을 살펴보고 필요한 일을 하는 것",
     "학교는 미끄러운 계단에 안전 표지를 붙이는 조치를 했습니다.", "조치", "4"),
    ("집단", "SR_L4CORE_4812", "4812", "명사", "여러 사람이 모여 이룬 무리",
     "합창단은 노래를 함께 부르려고 모인 집단입니다.", "집단", "4"),
    ("종속", "SR_L4CORE_4796", "4796", "명사", "스스로 결정하지 못하고 다른 것에 딸려 따르는 것",
     "그 조직은 본부에 종속되어 중요한 일을 혼자 결정할 수 없습니다.", "종속되어", "4"),
    ("합성", "SR_L4CORE_4827", "4827", "명사", "둘 이상의 것을 합쳐 하나로 만드는 것",
     "미술 시간에는 여러 사진을 합성하여 새로운 장면을 만들었습니다.", "합성하여", "4"),
    ("하위", "SR_L4CORE_4825", "4825", "명사", "지위나 등급, 위치가 더 낮은 쪽",
     "이 표에서는 큰 주제 아래에 세부 항목을 하위 항목으로 적었습니다.", "하위", "4"),
    ("중복", "SR_L4CORE_4801", "4801", "명사", "같은 것이 거듭되거나 서로 겹치는 것",
     "명단에서 이름의 중복을 확인하고 한 번만 적었습니다.", "중복", "4"),
    ("정기", "SR_L4CORE_4786", "4786", "명사", "일정한 기간마다 되풀이하도록 정한 것",
     "우리 반은 매달 첫째 주에 정기 모임을 엽니다.", "정기", "4"),
    ("간과", "SR_L4CORE_4749", "4749", "명사", "중요한 것을 대충 보고 넘겨서 미처 신경 쓰지 않는 것",
     "우리는 안전 규칙을 간과해서 작은 사고가 났습니다.", "간과해서", "4"),
    ("공간", "SR_L4CORE_4750", "4750", "명사", "무엇이 있거나 어떤 일이 일어날 수 있는 자리나 빈 곳",
     "교실 뒤쪽에 책상을 놓을 공간이 남아 있습니다.", "공간", "4"),
]

L5 = [
    ("대등", "SR_L5CORE_4868", "4868", "명사", "서로 비교했을 때 높고 낮음이나 잘하고 못함의 차이가 없이 비슷한 것",
     "두 팀의 실력은 대등해서 누가 이길지 예측하기 어려웠다.", "대등해서", "5"),
    ("본론", "SR_L5CORE_4904", "4904", "명사", "말이나 글에서 하고 싶은 주장이나 중심 내용을 담은 부분",
     "서론에서 문제를 제기한 뒤, 본론에서 자신의 주장을 자세히 설명했다.", "본론에서", "5"),
    ("논술", "SR_L5CORE_4859", "4859", "명사", "어떤 주제에 대한 자신의 의견을 논리적으로 밝혀 쓰는 것",
     "그는 환경 문제에 대한 논술을 써서 상을 받았다.", "논술을", "5"),
    ("부가", "SR_L5CORE_4906", "4906", "명사", "주된 것에 다른 것을 더 덧붙이는 것",
     "기본 요금에 부가 서비스 비용이 추가로 붙었다.", "부가", "5"),
    ("가치관", "SR_L5CORE_4835", "4835", "명사", "무엇이 옳고 그른지, 중요한지 아닌지를 판단하는 자기만의 기준이나 관점",
     "청소년기에는 자신만의 가치관을 세워 나가는 것이 중요하다.", "가치관을", "5"),
    ("간략", "SR_L5CORE_4836", "4836", "명사", "손쉽고 간단한 것",
     "발표 내용을 간략하게 정리해서 발표했다.", "간략하게", "5"),
    ("감안", "SR_L5CORE_4837", "4837", "명사", "여러 사정을 참고하여 생각에 넣는 것",
     "날씨가 좋지 않은 점을 감안해서 일정을 미루기로 했다.", "감안해서", "5"),
    ("계승", "SR_L5CORE_4843", "4843", "명사", "전통이나 문화유산, 업적 등을 물려받아 이어 나가는 것",
     "그는 전통 도자기 기술을 계승한 장인이다.", "계승한", "5"),
    ("국면", "SR_L5CORE_4848", "4848", "명사", "어떤 일이 벌어지고 있는 장면이나 상황",
     "협상이 새로운 국면으로 접어들었다.", "국면으로", "5"),
    ("급진", "SR_L5CORE_4849", "4849", "명사", "서두르고 급하게 목표를 향해 나아가는 것",
     "그는 천천히 바꾸기보다 급진적인 개혁을 주장했다.", "급진적인", "5"),
]

DISTRACTOR_OFFSETS = (2, 5, 8)


def _has_batchim(word: str) -> bool:
    """word의 마지막 글자가 받침이 있는 한글 음절인지 (받침 있으면 True)."""
    ch = word.strip()[-1]
    code = ord(ch) - 0xAC00
    if 0 <= code < 11172:
        return (code % 28) != 0
    return False  # 한글 음절이 아니면(숫자/영문/기호 등) 받침 없음으로 취급


def eun_neun(word: str) -> str:
    return "은" if _has_batchim(word) else "는"


def i_ga(word: str) -> str:
    return "이" if _has_batchim(word) else "가"


def gwa_wa(word: str) -> str:
    return "과" if _has_batchim(word) else "와"


def eul_reul(word: str) -> str:
    return "을" if _has_batchim(word) else "를"


def build_items(pool, level_label):
    items = []
    n = len(pool)
    for i, (lemma, cid, tid, pos, definition, ex_sentence, ex_form, vocab_level) in enumerate(pool):
        distractor_idx = [(i + off) % n for off in DISTRACTOR_OFFSETS]
        distractors = [pool[j] for j in distractor_idx]

        correct_pos = (i % 4) + 1  # 1~4 순환 배치 (다양성)
        options = [None, None, None, None]
        options[correct_pos - 1] = definition
        slot_order = [s for s in (1, 2, 3, 4) if s != correct_pos]
        wrong_reasons = []
        for slot, (d_lemma, d_cid, d_tid, d_pos, d_def, d_ex, d_form, d_level) in zip(slot_order, distractors):
            options[slot - 1] = d_def
            wrong_reasons.append({
                "option_no": slot,
                "distractor_lemma": d_lemma,
                "distractor_content_id": d_cid,
                "reason": (
                    f"이 설명은 '{lemma}'{i_ga(lemma)} 아니라 '{d_lemma}'의 뜻이다({d_def}). "
                    f"'{lemma}'({definition}){gwa_wa(lemma)} 의미 영역이 다르므로 오답이다."
                ),
            })

        # --- MEANING_CHOICE ---
        mc_item_id = f"MF_A_SC_SRL4L5PILOT_20260925_{level_label}_{i+1:03d}"
        mc = {
            "content_id_for_draft": f"{mc_item_id}",
            "item_id": mc_item_id,
            "item_type": "MEANING_CHOICE",
            "source_content_id": cid,
            "literacy_term_id": tid,
            "lemma": lemma,
            "pos": pos,
            "vocab_level": vocab_level,
            "prompt": f"'{lemma}'의 뜻으로 가장 알맞은 것은?",
            "options_json": json.dumps(options, ensure_ascii=False),
            "correct_option": correct_pos,
            "answer_payload_json": json.dumps({"correct_option": correct_pos}, ensure_ascii=False),
            "explanation": f"'{lemma}'{eun_neun(lemma)} '{definition}'이라는 뜻입니다.",
            "wrong_option_reasons_json": json.dumps(wrong_reasons, ensure_ascii=False),
            "source_version": "schema_reading_l4l5_pilot_dryrun_v1",
            "expert_review_status": "관리자 검토용 초안 (전문가 검수 완료 아님) - V항목, phase15 AUTO_PASS 기반",
            "l5_grade_caveat": (
                "L5 고1 근거는 개별 학년 태그가 아니라 정책 매핑임(phase14 보고서 한계 재인용)"
                if vocab_level == "5" else ""
            ),
        }

        # --- CONTEXT_MEANING ---
        if ex_form in ex_sentence:
            bracketed = ex_sentence.replace(ex_form, f"【{ex_form}】", 1)
        else:
            bracketed = ex_sentence + f" (목표어: 【{ex_form}】 - 원문에 정확한 활용형이 없어 표시 실패, 수동 확인 필요)"
        cm_item_id = f"MF_C_SC_SRL4L5PILOT_20260925_{level_label}_{i+1:03d}"
        cm = {
            "content_id_for_draft": f"{cm_item_id}",
            "item_id": cm_item_id,
            "item_type": "CONTEXT_MEANING",
            "source_content_id": cid,
            "literacy_term_id": tid,
            "lemma": lemma,
            "pos": pos,
            "vocab_level": vocab_level,
            "prompt": f"다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은 것은?\n\n{bracketed}",
            "options_json": json.dumps(options, ensure_ascii=False),
            "correct_option": correct_pos,
            "answer_payload_json": json.dumps({"correct_option": correct_pos}, ensure_ascii=False),
            "explanation": f"문장 속 '{ex_form}'{eun_neun(ex_form)} '{definition}'{eul_reul(definition)} 뜻합니다.",
            "wrong_option_reasons_json": json.dumps(wrong_reasons, ensure_ascii=False),
            "source_version": "schema_reading_l4l5_pilot_dryrun_v1",
            "expert_review_status": "관리자 검토용 초안 (전문가 검수 완료 아님) - V항목, phase15 AUTO_PASS 기반",
            "l5_grade_caveat": (
                "L5 고1 근거는 개별 학년 태그가 아니라 정책 매핑임(phase14 보고서 한계 재인용)"
                if vocab_level == "5" else ""
            ),
        }
        items.append(mc)
        items.append(cm)
    return items


def validate(items):
    results = []
    for it in items:
        opts = json.loads(it["options_json"])
        issues = []
        # (1) 선택지 중복 없음
        if len(set(opts)) != len(opts):
            issues.append("DUP_OPTIONS")
        # (2) 정답 유일성: 정답 텍스트가 옵션 내에서 정확히 1번만 등장
        correct_text = opts[it["correct_option"] - 1]
        if opts.count(correct_text) != 1:
            issues.append("ANSWER_NOT_UNIQUE")
        # (3) 정답 옵션이 explanation에 인용된 정의와 일치
        if correct_text not in it["explanation"]:
            issues.append("ANSWER_EXPLANATION_MISMATCH")
        # (4) 옵션 4개 존재
        if len(opts) != 4:
            issues.append("OPTION_COUNT_NOT_4")
        # (5) CONTEXT_MEANING 문항은 프롬프트에 【 】로 목표어가 실제로 표시됐는지
        if it["item_type"] == "CONTEXT_MEANING" and "【" not in it["prompt"]:
            issues.append("CONTEXT_TARGET_NOT_MARKED")
        results.append({"item_id": it["item_id"], "status": "PASS" if not issues else "HOLD", "issues": issues})
    return results


def main():
    items = build_items(L4, "L4") + build_items(L5, "L5")
    val = validate(items)
    val_by_id = {v["item_id"]: v for v in val}
    for it in items:
        it["auto_validation_status"] = val_by_id[it["item_id"]]["status"]
        it["auto_validation_issues"] = ",".join(val_by_id[it["item_id"]]["issues"]) or "-"

    out_dir = REPO_ROOT / "data" / "import"
    csv_path = out_dir / "schema_reading_phase16_quiz_pilot_dryrun_20260925.csv"
    jsonl_path = out_dir / "schema_reading_phase16_quiz_pilot_dryrun_20260925.jsonl"

    fieldnames = list(items[0].keys())
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(items)

    with open(jsonl_path, "w", encoding="utf-8") as fh:
        for it in items:
            fh.write(json.dumps(it, ensure_ascii=False) + "\n")

    n_pass = sum(1 for v in val if v["status"] == "PASS")
    n_hold = len(val) - n_pass
    print(f"총 문항 {len(items)}건 (MEANING_CHOICE {len(items)//2} + CONTEXT_MEANING {len(items)//2})")
    print(f"자동검증 PASS {n_pass} / HOLD {n_hold}")
    for v in val:
        if v["status"] != "PASS":
            print(" HOLD:", v["item_id"], v["issues"])
    print(f"CSV: {csv_path}")
    print(f"JSONL: {jsonl_path}")


if __name__ == "__main__":
    main()
