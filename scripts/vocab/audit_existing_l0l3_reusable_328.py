# -*- coding: utf-8 -*-
"""기존 '재사용가능 후보' 328건 전수 감사(읽기 전용, DB 재조회 스냅샷
사용, 재INSERT 없음). 이전 1차 분류(step2)와 별개로, 훨씬 엄격한
독립 기준(연령적합성 길이 임계값 포함)으로 다시 판정한다.

입력(data/import/existing_l0l3_reusable_328_fresh_snapshot_20260929.json)은
328건 item_id에 대해 연구 서버에서 별도로 다시 뽑은 스냅샷이다."""
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SNAPSHOT_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_reusable_328_fresh_snapshot_20260929.json"
RESULT_OUT_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_audit_328_result_20260929.json"

with open(SNAPSHOT_PATH, encoding="utf-8") as f:
    data = json.load(f)
items = data["items"]
content_by_id = {c["content_id"]: c for c in data["contents"]}

LEVEL_MAXLEN = {0: 20, 1: 26, 2: 34, 3: 40}


def batchim(ch):
    code = ord(ch) - 0xAC00
    if 0 <= code < 11172:
        return (code % 28) != 0
    return False


def check_particle(text):
    m = re.search(r"'([^']+)'(은|는) '([^']+)'(이라는|라는) 뜻", text)
    if not m:
        m = re.search(r"'([^']+)'(은|는)? ?'([^']+)'(을|를) 뜻", text)
        if not m:
            return None
        word, p1, word2, p2 = m.groups()
        expected = "을" if batchim(word2.strip()[-1]) else "를"
        return None if p2 == expected else f"조사오류:{word2}+{p2}(기대{expected})"
    word, p1, word2, p2 = m.groups()
    exp1 = "은" if batchim(word.strip()[-1]) else "는"
    exp2 = "이라는" if batchim(word2.strip()[-1]) else "라는"
    errs = []
    if p1 != exp1:
        errs.append(f"조사오류:{word}+{p1}(기대{exp1})")
    if p2 != exp2:
        errs.append(f"조사오류:{word2}+{p2}(기대{exp2})")
    return ";".join(errs) if errs else None


rows = []
by_content = {}
for it in items:
    by_content.setdefault(it["source_content_id"], []).append(it)

for cid, its in by_content.items():
    c = content_by_id.get(cid)
    for it in its:
        r = {"item_id": it["item_id"], "content_id": cid, "item_type": it["item_type"],
             "lemma": c["lemma"] if c else "?", "level": c["vocab_level"] if c else "?",
             "auto_checks": {}, "issues": []}

        # 1) 콘텐츠-문항 연결
        if c is None:
            r["issues"].append("LINK_BROKEN: 원본 콘텐츠 없음")
            r["auto_checks"]["콘텐츠연결"] = "FAIL"
        elif not it["is_active"] or not c["is_active"]:
            r["issues"].append("비활성 상태(문항 또는 콘텐츠)")
            r["auto_checks"]["콘텐츠연결"] = "FAIL"
        else:
            r["auto_checks"]["콘텐츠연결"] = "PASS"

        if c and c["hold_reason"] and c["hold_reason"].strip():
            r["issues"].append(f"콘텐츠 HOLD: {c['hold_reason']}")

        # 2) 현재 뜻풀이-정답 일치
        try:
            options = json.loads(it["options_json"])
        except (TypeError, ValueError):
            r["issues"].append("options_json 파싱 실패")
            r["auto_checks"]["뜻풀이일치"] = "FAIL"
            r["auto_checks"]["선택지중복"] = "FAIL"
            rows.append(r)
            continue

        correct_idx = it["correct_option"] - 1 if it["correct_option"] else -1
        correct_text = options[correct_idx].strip() if 0 <= correct_idx < len(options) else None
        student_def = (c["student_definition"] or "").strip() if c else None
        canonical_def = (c["canonical_definition"] or "").strip() if c else None
        def_match = correct_text is not None and (correct_text == student_def or correct_text == canonical_def)
        r["auto_checks"]["뜻풀이일치"] = "PASS" if def_match else "FAIL"
        if not def_match:
            r["issues"].append(f"정답 선택지가 현재 뜻풀이와 불일치(콘텐츠 수정 가능성): '{correct_text}' vs student='{student_def}'")

        # 3) 선택지 중복 / 정답 유일성
        dup = len(set(o.strip() for o in options)) != len(options)
        exact_dups = sum(1 for o in options if correct_text and o.strip() == correct_text)
        uniq_ok = (not dup) and (correct_text is not None and exact_dups == 1) and len(options) == 4
        r["auto_checks"]["선택지중복_정답유일성"] = "PASS" if uniq_ok else "FAIL"
        if dup:
            r["issues"].append("선택지 중복 텍스트 존재")
        if correct_text and exact_dups != 1:
            r["issues"].append(f"정답 유일성 위반(완전일치 {exact_dups}회)")
        if len(options) != 4:
            r["issues"].append(f"선택지 4개 아님({len(options)})")

        # 4) 문맥 자연스러움
        context_ok = True
        if it["item_type"] == "CONTEXT_MEANING":
            if "【" not in it["prompt"] or "】" not in it["prompt"]:
                context_ok = False
                r["issues"].append("대상어 표시(【】) 없음")
            else:
                sentence_part = it["prompt"].split("\n\n", 1)[-1]
                if not re.search(r"(다|요|니다|습니다)[.!?]?\s*$", sentence_part.strip()):
                    context_ok = False
                    r["issues"].append("예문이 완결된 문장으로 끝나지 않음")
                marked = it["prompt"][it["prompt"].find("【")+1: it["prompt"].find("】")]
                if c and c["lemma"] not in marked and marked not in c["lemma"]:
                    context_ok = False
                    r["issues"].append(f"표시 어형('{marked}')과 표제어('{c['lemma']}') 관련성 불명확")
        r["auto_checks"]["문맥자연스러움"] = "PASS" if context_ok else ("N/A" if it["item_type"] != "CONTEXT_MEANING" else "FAIL")

        # 5) 연령 적합성(레벨별 임계값 - 328건에도 42건과 동일 기준 적용)
        age_ok = True
        if c:
            maxlen = LEVEL_MAXLEN.get(c["vocab_level"], 40)
            if student_def and len(student_def) > maxlen:
                age_ok = False
                r["issues"].append(f"뜻풀이 길이 초과(L{c['vocab_level']} 기준 {maxlen}자, 실제 {len(student_def)}자)")
            if student_def and re.search(r"[一-鿿]", student_def):
                age_ok = False
                r["issues"].append("뜻풀이에 한자 포함")
        r["auto_checks"]["연령적합성"] = "PASS" if age_ok else "FAIL"

        # 6) 레벨 상태(별도 축 - PASS/FAIL이 아니라 상태만 기록, 감점 아님)
        if c:
            r["level_status"] = c["level_status"]
            r["boundary_flag"] = bool(c["boundary_flag"])
            r["level_confirmed"] = (c["level_status"] == "PROVISIONAL_AUTO" and not c["boundary_flag"])
        else:
            r["level_status"] = None
            r["level_confirmed"] = False

        # 조사 표기(참고 - 이미 통과한 문항이라 대부분 정상이지만 재확인)
        perr = check_particle(it["explanation"])
        if perr:
            r["issues"].append(perr)
            r["auto_checks"]["조사표기"] = "FAIL"
        else:
            r["auto_checks"]["조사표기"] = "PASS"

        auto_pass = all(v in ("PASS", "N/A") for v in r["auto_checks"].values())
        r["auto_verdict"] = "PASS" if auto_pass else "HOLD"
        # 사람 검수 완료 여부 - 이번 자동 감사만으로는 "사람 검수 완료"라고 주장하지 않는다
        r["human_review_status"] = "NOT_PERFORMED(자동 감사만 완료, 최종 공개 전 사람 확인 필요)"

        rows.append(r)

print(f"총 {len(rows)}건")
auto_pass = [r for r in rows if r["auto_verdict"] == "PASS"]
auto_hold = [r for r in rows if r["auto_verdict"] == "HOLD"]
print(f"자동감사 PASS: {len(auto_pass)}건")
print(f"자동감사 HOLD: {len(auto_hold)}건")

from collections import defaultdict
by_level = defaultdict(lambda: {"PASS": 0, "HOLD": 0, "level_confirmed": 0, "level_not_confirmed": 0})
for r in rows:
    by_level[r["level"]][r["auto_verdict"]] += 1
    if r["level_confirmed"]:
        by_level[r["level"]]["level_confirmed"] += 1
    else:
        by_level[r["level"]]["level_not_confirmed"] += 1

print("\n레벨별:")
for lv in sorted(by_level.keys(), key=str):
    print(f"  L{lv}: {dict(by_level[lv])}")

print("\n=== HOLD 사유 상위 ===")
from collections import Counter
reason_counter = Counter()
for r in auto_hold:
    for issue in r["issues"]:
        key = issue.split(":")[0].split("(")[0]
        reason_counter[key] += 1
for k, v in reason_counter.most_common(20):
    print(f"  {k}: {v}건")

with open(RESULT_OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, indent=2)
