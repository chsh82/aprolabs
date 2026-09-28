# -*- coding: utf-8 -*-
"""L0~L3 기존 코퍼스 중 문항 없는 콘텐츠에 대해, phase16 스크립트와 동일한
방식(오답=같은 풀 안 다른 표제어의 실제 정의, 정답 위치 순환 배치)으로
MEANING_CHOICE/CONTEXT_MEANING dry-run 문항을 파일로만 생성한다.
DB에는 전혀 쓰지 않는다. 임의 목표 건수를 채우려고 대체 후보를 추가하지
않는다 - 시도한 풀에서 실제로 검증을 통과한 만큼만 남긴다.

입력(data/import/existing_l0l3_candidates_20260929.json)은 aprolabs
연구 서버에서 별도 조회 스크립트로 미리 뽑아 둔 것 - 이 스크립트
자체는 DB에 연결하지 않는다(순수 파일 입출력)."""
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CANDIDATES_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_candidates_20260929.json"
ITEMS_OUT_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_dryrun_items_20260929.json"
HELD_OUT_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_dryrun_held_20260929.json"

with open(CANDIDATES_PATH, encoding="utf-8") as f:
    CANDIDATES = json.load(f)

ATTEMPT_SIZE = {"0": 25, "1": 25, "2": 17, "3": 25}


def _has_batchim(word):
    ch = word.strip()[-1]
    code = ord(ch) - 0xAC00
    if 0 <= code < 11172:
        return (code % 28) != 0
    return False


def eun_neun(w): return "은" if _has_batchim(w) else "는"
def i_ga(w): return "이" if _has_batchim(w) else "가"
def gwa_wa(w): return "과" if _has_batchim(w) else "와"
def eul_reul(w): return "을" if _has_batchim(w) else "를"
def ira_neun(w): return "이라는" if _has_batchim(w) else "라는"

DISTRACTOR_OFFSETS = (2, 5, 8)

STOPWORDS = {"것", "따위", "그런", "등", "및", "또는", "혹은", "그", "이", "저", "수", "때",
             "위해", "위하여", "대한", "대해", "이르는", "말", "하는", "되는", "않는"}


def tokenize(text):
    return {t for t in re.split(r"[\s,.·ㆍ()~/'\"“”‘’]+", text) if len(t) >= 2 and t not in STOPWORDS}


def overlap_ratio(a, b):
    ta, tb = tokenize(a), tokenize(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / min(len(ta), len(tb))


all_generated = []
all_held = []
summary = {}

for level_str, pool_full in CANDIDATES.items():
    level = int(level_str)
    attempt_n = min(ATTEMPT_SIZE[level_str], len(pool_full))
    pool = pool_full[:attempt_n]
    n = len(pool)
    generated = []
    held = []

    for i, c in enumerate(pool):
        lemma = c["lemma"]
        definition = c["student_definition"].strip()
        cid = c["content_id"]
        pos = c["pos"]
        ex_sentence = c["example_sentence"]
        ex_form = (c["example_target_form"] or lemma).strip()

        issues = []

        # 동형이의어 경고(짧은 상용 표제어는 별도 의미가 더 있을 가능성 - 보수적으로 HOLD)
        if len(lemma) <= 1:
            issues.append("HOMOGRAPH_RISK_SHORT_LEMMA")

        # 문맥 표시 가능 여부(원문에 정확한 활용형이 있어야만 진행 - 실패 시 HOLD)
        if ex_form not in ex_sentence:
            issues.append("CONTEXT_TARGET_NOT_FOUND_IN_SENTENCE")

        distractor_idx = [(i + off) % n for off in DISTRACTOR_OFFSETS]
        distractors = [pool[j] for j in distractor_idx]
        if len(set(d["content_id"] for d in distractors) | {cid}) != 4:
            issues.append("DISTRACTOR_SELF_OR_DUP_COLLISION")

        # 오답 의미 중복 검사(정답 정의와 토큰 겹침 비율이 높으면 의미 구분 불명확)
        for d in distractors:
            r = overlap_ratio(definition, d["student_definition"])
            if r >= 0.34:
                issues.append(f"DISTRACTOR_MEANING_OVERLAP({d['lemma']},{r:.2f})")

        if issues:
            held.append({"content_id": cid, "lemma": lemma, "level": level, "issues": issues})
            continue

        correct_pos = (i % 4) + 1
        options = [None, None, None, None]
        options[correct_pos - 1] = definition
        slot_order = [s for s in (1, 2, 3, 4) if s != correct_pos]
        wrong_reasons = []
        for slot, d in zip(slot_order, distractors):
            options[slot - 1] = d["student_definition"].strip()
            wrong_reasons.append({
                "option_no": slot, "distractor_lemma": d["lemma"], "distractor_content_id": d["content_id"],
                "reason": (f"이 설명은 '{lemma}'{i_ga(lemma)} 아니라 '{d['lemma']}'의 뜻이다"
                           f"({d['student_definition'].strip()}). '{lemma}'({definition}){gwa_wa(lemma)} "
                           f"의미 영역이 다르므로 오답이다."),
            })

        # 자동 검증(phase16 validate()와 동일 기준)
        opts_check_issues = []
        if len(set(options)) != len(options):
            opts_check_issues.append("DUP_OPTIONS")
        if options.count(definition) != 1:
            opts_check_issues.append("ANSWER_NOT_UNIQUE")
        if len(options) != 4:
            opts_check_issues.append("OPTION_COUNT_NOT_4")
        if opts_check_issues:
            held.append({"content_id": cid, "lemma": lemma, "level": level, "issues": opts_check_issues})
            continue

        mc_id = f"MF_A_SC_EXCORE_L{level}_{cid}"
        cm_id = f"MF_C_SC_EXCORE_L{level}_{cid}"
        bracketed = ex_sentence.replace(ex_form, f"【{ex_form}】", 1)

        mc = {
            "item_id": mc_id, "item_type": "MEANING_CHOICE", "source_content_id": cid,
            "lemma": lemma, "pos": pos, "vocab_level": level,
            "prompt": f"'{lemma}'의 뜻으로 가장 알맞은 것은?",
            "options_json": json.dumps(options, ensure_ascii=False),
            "correct_option": correct_pos,
            "answer_payload_json": json.dumps({"correct_option": correct_pos}, ensure_ascii=False),
            "explanation": f"'{lemma}'{eun_neun(lemma)} '{definition}'{ira_neun(definition)} 뜻입니다.",
            "wrong_option_reasons_json": json.dumps(wrong_reasons, ensure_ascii=False),
            "source_version": "schema_reading_existing_l0l3_dryrun_v1",
            "expert_review_status": "관리자 검토용 초안(자동 생성, 전문가 검수 완료 아님)",
        }
        cm = {
            "item_id": cm_id, "item_type": "CONTEXT_MEANING", "source_content_id": cid,
            "lemma": lemma, "pos": pos, "vocab_level": level,
            "prompt": f"다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은 것은?\n\n{bracketed}",
            "options_json": json.dumps(options, ensure_ascii=False),
            "correct_option": correct_pos,
            "answer_payload_json": json.dumps({"correct_option": correct_pos}, ensure_ascii=False),
            "explanation": f"문장 속 '{ex_form}'{eun_neun(ex_form)} '{definition}'{eul_reul(definition)} 뜻합니다.",
            "wrong_option_reasons_json": json.dumps(wrong_reasons, ensure_ascii=False),
            "source_version": "schema_reading_existing_l0l3_dryrun_v1",
            "expert_review_status": "관리자 검토용 초안(자동 생성, 전문가 검수 완료 아님)",
        }

        # 최종 검증(CONTEXT_TARGET_NOT_MARKED 등)
        final_issues = []
        if "【" not in cm["prompt"]:
            final_issues.append("CONTEXT_TARGET_NOT_MARKED")
        if definition not in mc["explanation"] or definition not in cm["explanation"]:
            final_issues.append("ANSWER_EXPLANATION_MISMATCH")
        if final_issues:
            held.append({"content_id": cid, "lemma": lemma, "level": level, "issues": final_issues})
            continue

        generated.append(mc)
        generated.append(cm)

    all_generated.extend(generated)
    all_held.extend(held)
    summary[level] = {
        "attempted_content": n,
        "passed_content": len(generated) // 2,
        "held_content": len(held),
        "generated_items": len(generated),
    }

with open(ITEMS_OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(all_generated, f, ensure_ascii=False, indent=2)
with open(HELD_OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(all_held, f, ensure_ascii=False, indent=2)

print("=== 생성 결과 요약 ===")
for lv in sorted(summary.keys()):
    s = summary[lv]
    print(f"L{lv}: 시도 {s['attempted_content']}건 콘텐츠 -> PASS {s['passed_content']}건(문항 {s['generated_items']}건), HOLD {s['held_content']}건")

print("\n=== HOLD 사유 분포 ===")
from collections import Counter
reason_counter = Counter()
for h in all_held:
    for issue in h["issues"]:
        key = issue.split("(")[0]
        reason_counter[key] += 1
for k, v in reason_counter.most_common():
    print(f"  {k}: {v}건")
