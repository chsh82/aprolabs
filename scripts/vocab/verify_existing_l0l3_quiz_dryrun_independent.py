# -*- coding: utf-8 -*-
"""신규초안 184문항 독립 검증 - 생성 스크립트(expand_existing_l0l3_
quiz_dryrun.py)의 코드를 import하지 않고, 별도 로직으로 처음부터
다시 계산한다. DB 값도 새로 재조회한 스냅샷(fresh_content_92.json)만
쓴다.

fresh_content_92.json은 aprolabs 연구 서버에서 이 92건 content_id에
대해서만 별도 read-only 조회로 새로 뽑은 스냅샷
(existing_l0l3_fresh_content_snapshot_20260929.json)이다 - 생성
스크립트가 쓴 candidates 파일을 재사용하지 않는다."""
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ITEMS_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_dryrun_items_20260929.json"
FRESH_CONTENT_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_fresh_content_snapshot_20260929.json"
RESULT_OUT_PATH = REPO_ROOT / "data" / "import" / "existing_l0l3_independent_qa_result_20260929.json"

with open(ITEMS_PATH, encoding="utf-8") as f:
    ITEMS = json.load(f)
with open(FRESH_CONTENT_PATH, encoding="utf-8") as f:
    CONTENT_ROWS = json.load(f)
content_by_id = {c["content_id"]: c for c in CONTENT_ROWS}

LEVEL_MAXLEN = {0: 20, 1: 26, 2: 34, 3: 40}  # L0~L3 학생용 뜻풀이 최대 허용 길이(연령 적합성 휴리스틱, 독립 임계값)


def batchim(ch):
    code = ord(ch) - 0xAC00
    if 0 <= code < 11172:
        return (code % 28) != 0
    return False


def check_particle(text):
    """설명문에서 '~은/는 ~이라는/라는 뜻입니다' 패턴의 조사가 실제
    받침 규칙과 맞는지 재계산(생성 스크립트와 무관한 새 구현)."""
    m = re.search(r"'([^']+)'(은|는) '([^']+)'(이라는|라는) 뜻", text)
    if not m:
        m = re.search(r"'([^']+)'(은|는)? ?'([^']+)'(을|를) 뜻", text)
        if not m:
            return None
        word, p1, word2, p2 = m.groups()
        expected = "을" if batchim(word2.strip()[-1]) else "를"
        return None if p2 == expected else f"조사 오류: '{word2}'+'{p2}' (기대 '{expected}')"
    word, p1, word2, p2 = m.groups()
    exp1 = "은" if batchim(word.strip()[-1]) else "는"
    exp2 = "이라는" if batchim(word2.strip()[-1]) else "라는"
    errs = []
    if p1 != exp1:
        errs.append(f"조사 오류: '{word}'+'{p1}' (기대 '{exp1}')")
    if p2 != exp2:
        errs.append(f"조사 오류: '{word2}'+'{p2}' (기대 '{exp2}')")
    return "; ".join(errs) if errs else None


results = {"PASS": [], "HOLD": []}
l2_level_review = []

by_content = {}
for it in ITEMS:
    by_content.setdefault(it["source_content_id"], []).append(it)

for cid, items in by_content.items():
    c = content_by_id.get(cid)
    hold_reasons = []

    if c is None:
        hold_reasons.append("DB 재조회 시 콘텐츠 없음(생성 시점 이후 변경/삭제 가능성)")
    else:
        # 1) 레벨 근거 재확인(모든 레벨, L2는 추가로 별도 로그)
        if c["level_status"] != "PROVISIONAL_AUTO" or c["boundary_flag"]:
            hold_reasons.append(f"레벨 미확정 재확인됨(level_status={c['level_status']}, boundary_flag={c['boundary_flag']})")
        if c["level_confidence"] is not None and c["level_confidence"] < 0.5:
            hold_reasons.append(f"level_confidence 낮음({c['level_confidence']})")
        if c["hold_reason"]:
            hold_reasons.append(f"콘텐츠 HOLD 사유 재발견: {c['hold_reason']}")
        if not c["is_active"]:
            hold_reasons.append("콘텐츠 비활성 재확인됨")

        if c["vocab_level"] == 2:
            l2_level_review.append({
                "content_id": cid, "lemma": c["lemma"], "level_status": c["level_status"],
                "boundary_flag": c["boundary_flag"], "target_grade_band": c["target_grade_band"],
                "level_source": c["level_source"], "level_version": c["level_version"],
                "level_confidence": c["level_confidence"],
            })

        # 2) 연령 적합성은 DB content.student_definition이 아니라 "문항에 실제로
        #    쓰인 정답 텍스트"를 기준으로 판단한다(아래 문항별 루프에서 처리) -
        #    2026-09-29 개정판은 사람이 다듬은 대체 정의를 문항에만 반영하고
        #    DB 원본은 건드리지 않으므로, content 필드만 보면 이미 해결된
        #    문항도 계속 HOLD로 잘못 판정하게 된다(실제로 학생에게 보이는
        #    텍스트를 검사해야 독립 검증의 의미가 있다).

    for it in items:
        issues = list(hold_reasons)
        try:
            options = json.loads(it["options_json"])
        except (TypeError, ValueError):
            results["HOLD"].append({"item_id": it["item_id"], "issues": ["options_json 파싱 실패"]})
            continue

        correct_idx = it["correct_option"] - 1
        if not (0 <= correct_idx < len(options)):
            issues.append("correct_option 범위 오류")
        else:
            correct_text = options[correct_idx].strip()

            # 정답 유일성 재검증(독립: 완전 일치 + 부분 포함 양쪽 확인)
            exact_dups = sum(1 for o in options if o.strip() == correct_text)
            if exact_dups != 1:
                issues.append(f"정답 유일성 위반(완전일치 {exact_dups}회)")
            for j, o in enumerate(options):
                if j == correct_idx:
                    continue
                ot = o.strip()
                if ot and (ot in correct_text or correct_text in ot) and len(ot) > 3:
                    issues.append(f"오답 {j+1}번이 정답과 텍스트 포함 관계(다른 뜻 가능성/혼동 위험): '{ot}'")

            # 선택지 길이 편차(정답이 유난히 길거나 짧으면 눈대중으로 정답 추측 가능)
            lens = [len(o) for o in options]
            other_lens = [l for j, l in enumerate(lens) if j != correct_idx]
            avg_other = sum(other_lens) / len(other_lens) if other_lens else 0
            if avg_other > 0 and (lens[correct_idx] > avg_other * 2.2 or lens[correct_idx] < avg_other * 0.45):
                issues.append(f"정답 선택지 길이 이상치(정답 {lens[correct_idx]}자 vs 오답 평균 {avg_other:.0f}자) - 눈대중 추측 위험")

            # 연령 적합성 - 문항에 실제로 노출되는 선택지 4개 전부 기준(DB content
            # 필드가 아니라 학생이 실제로 읽는 텍스트) - 오답도 학생이 읽으므로 포함
            level = c["vocab_level"] if c else None
            maxlen = LEVEL_MAXLEN.get(level, 40)
            for j, o in enumerate(options):
                ot = o.strip()
                if len(ot) > maxlen:
                    tag = "정답" if j == correct_idx else f"오답{j+1}"
                    issues.append(f"{tag} 선택지 길이 초과(L{level} 기준 {maxlen}자, 실제 {len(ot)}자) - 연령 적합성 재검토 필요")
                if re.search(r"[一-鿿]", ot):
                    tag = "정답" if j == correct_idx else f"오답{j+1}"
                    issues.append(f"{tag} 선택지에 한자 포함 - 저학년 적합성 의심")

        # 조사 재계산(완전히 새 구현)
        perr = check_particle(it["explanation"])
        if perr:
            issues.append(perr)

        # 문맥 자연스러움(CONTEXT_MEANING) - 문장 완결성 재확인
        if it["item_type"] == "CONTEXT_MEANING":
            prompt = it["prompt"]
            if "【" not in prompt or "】" not in prompt:
                issues.append("대상어 표시(【】) 없음")
            else:
                sentence_part = prompt.split("\n\n", 1)[-1]
                if not re.search(r"(다|요|니다|습니다)[.!?]?\s*$", sentence_part.strip()):
                    issues.append("예문이 완결된 문장으로 끝나지 않음(문맥 자연스러움 의심)")
                if "  " in sentence_part or " 】" in sentence_part or "【 " in sentence_part:
                    issues.append("괄호 표시 주변 공백 이상(표기 오류)")

        if issues:
            results["HOLD"].append({"item_id": it["item_id"], "content_id": cid,
                                     "lemma": (c["lemma"] if c else "?"), "level": (c["vocab_level"] if c else "?"),
                                     "issues": issues})
        else:
            results["PASS"].append({"item_id": it["item_id"], "content_id": cid,
                                     "lemma": (c["lemma"] if c else "?"), "level": (c["vocab_level"] if c else "?")})

print(f"PASS: {len(results['PASS'])}건")
print(f"HOLD: {len(results['HOLD'])}건")

from collections import defaultdict
by_level = defaultdict(lambda: defaultdict(int))
for k, rows in results.items():
    for r in rows:
        by_level[r["level"]][k] += 1
print("\n레벨별:")
for lv in sorted(by_level.keys(), key=str):
    print(f"  L{lv}: {dict(by_level[lv])}")

print("\n=== HOLD 사유 ===")
for r in results["HOLD"]:
    print(f"- {r['item_id']}({r['lemma']}, L{r['level']}): {r['issues']}")

print(f"\n=== L2(17어휘) 레벨 근거 상세 ===")
seen = set()
for r in l2_level_review:
    if r["content_id"] in seen:
        continue
    seen.add(r["content_id"])
    print(f"- {r['content_id']}({r['lemma']}): level_status={r['level_status']}, boundary={r['boundary_flag']}, "
          f"grade_band={r['target_grade_band']}, source={r['level_source']}, version={r['level_version']}, "
          f"confidence={r['level_confidence']}")

with open(RESULT_OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
