# -*- coding: utf-8 -*-
"""공식 5등급 1차 보강 배치(30단어·60문항) QA - **생성 스크립트와 분리된
별도 검사**. 여기서 나온 AUTO_PASS는 기계적 검사 결과일 뿐 사람의 문항
승인이 아니다(그 어떤 필드에도 "사람 승인"이라고 쓰지 않는다 - 전부
"AUTO_" 접두어로 명시).

검사 항목:
  1. 정답 유일성 - 한 문항의 4개 선택지가 서로 완전히 다른 문자열인가
     (기계적, AUTO_PASS/FAIL)
  2. correct_option 인덱스가 실제 정답 텍스트 위치와 일치하는가(생성
     스크립트 자체 버그 검출용, 기계적)
  3. 오답 의미 중복(근사) - 선택지 간 어절 단위 자카드 유사도가 임계값을
     넘는 쌍이 있는가(기계적 근사치일 뿐 - 진짜 의미 중복 여부는 사람이
     최종 확인해야 함, 그래서 이 항목은 AUTO_FLAG까지만 매기고 PASS라고
     부르지 않음)
  4. 동형이의 혼동 위험 - homonym_number != 0인 항목 목록(기계적 플래그,
     의미가 실제로 헷갈리는지는 사람이 확인해야 함)
  5. 조사(을/를, 이/가, 은/는, 과/와, 로/으로) 호응 - 예문 안에서 받침
     유무에 맞는 조사를 썼는지 정규식+받침 판별로 기계적 검사
  6. 연령 적합성 - 이번 선정 단계(3절)에서 이미 제외한 7건의 사유를
     그대로 다시 인용(여기서 새로 판단하지 않음 - 반복하지 않음)
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"

JONGSEONG_PARTICLES = {
    ("을", "를"): ("을", "를"),
    ("이", "가"): ("이", "가"),
    ("은", "는"): ("은", "는"),
    ("과", "와"): ("과", "와"),
}


def has_batchim(syllable: str) -> bool:
    code = ord(syllable) - 0xAC00
    if code < 0 or code > 11171:
        return False
    return (code % 28) != 0


def check_particles(text: str) -> list[str]:
    """예문/설명문에서 '단어+조사' 패턴을 찾아 받침 규칙에 맞는지 검사한다.
    인용부호(')가 글자와 조사 사이에 끼어 있는 경우도 잡는다(예: '개축'는) -
    naive 인접 문자 검사만으로는 이 패턴을 놓친다는 걸 실제로 발견했다."""
    issues = []
    for with_batchim, without_batchim in (("을", "를"), ("이", "가"), ("은", "는"), ("과", "와")):
        for m in re.finditer(rf"([가-힣])'?({with_batchim}|{without_batchim})(?=[\s.,]|$)", text):
            char, particle = m.group(1), m.group(2)
            expected = with_batchim if has_batchim(char) else without_batchim
            if particle != expected:
                issues.append(f"{char}{particle} - 받침 규칙상 {char}{expected} 이어야 함(문맥: ...{text[max(0,m.start()-5):m.end()+5]}...)")
    # 로/으로
    for m in re.finditer(r"([가-힣])(으로|로)(?=[\s.,]|$)", text):
        char, particle = m.group(1), m.group(2)
        is_rieul_batchim = (ord(char) - 0xAC00) % 28 == 8 if 0 <= ord(char) - 0xAC00 <= 11171 else False
        if has_batchim(char) and not is_rieul_batchim and particle != "으로":
            issues.append(f"{char}{particle} - 받침 있으니 {char}으로 이어야 함")
        if (not has_batchim(char) or is_rieul_batchim) and particle != "로":
            issues.append(f"{char}{particle} - 받침 없거나 ㄹ받침이니 {char}로 이어야 함")
    return issues


def jaccard(a: str, b: str) -> float:
    ta, tb = set(a.replace(".", "").split()), set(b.replace(".", "").split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


EDITORIAL_HOLD_FROM_SELECTION = {
    '명시': '동형이의 혼동 위험(「明示」명시와 혼동 - 제시된 뜻은 드문 「名詩」쪽)',
    '볼트': '동형이의 혼동 위험(전압 단위 볼트 vs 더 흔한 나사 고정못 볼트)',
    '옹고집전': '특정 문학 작품 제목 - 일반 어휘 의미 문항에 부적합(문학 지식 문항 영역)',
    '에스키모': '현재 권장되지 않는 민족 지칭 용어 - 연령/표현 적합성 보류',
    '저능아': '사전 자체가 "낮잡아 이르는 말"로 명시한 차별적 표현 - 학생용 문항 부적합',
    '불여우': '여성을 비하하는 비유적 표현 - 학생용 문항 부적합',
    '핀잔주다': '공식 뜻풀이가 순환적("핀잔을 하다")이라 의미가 그 자체로 명확하지 않음',
}


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    with open(IMP / "nikl_grade5_batch1_30_content_dryrun_20261007.csv", encoding="utf-8-sig", newline="") as f:
        content_rows = list(csv.DictReader(f))
    with open(IMP / "nikl_grade5_batch1_30_items_dryrun_20261007.csv", encoding="utf-8-sig", newline="") as f:
        item_rows = list(csv.DictReader(f))

    content_by_cid = {r["candidate_id"]: r for r in content_rows}

    print("=== 1·2. 정답 유일성 + correct_option 일치 (기계적, AUTO_PASS/FAIL) ===")
    uniqueness_fail = []
    index_fail = []
    for it in item_rows:
        options = json.loads(it["options_json"])
        if len(set(options)) != len(options):
            uniqueness_fail.append(it["item_id"])
        correct_idx = int(it["correct_option"]) - 1
        expected_text = content_by_cid[it["source_content_id"]]["student_definition"]
        if not (0 <= correct_idx < len(options)) or options[correct_idx] != expected_text:
            index_fail.append(it["item_id"])
    print(f"정답 유일성: {len(item_rows)}건 중 FAIL {len(uniqueness_fail)}건 - {'AUTO_PASS' if not uniqueness_fail else uniqueness_fail}")
    print(f"correct_option 일치: {len(item_rows)}건 중 FAIL {len(index_fail)}건 - {'AUTO_PASS' if not index_fail else index_fail}")

    print("\n=== 3. 오답 의미 중복(근사, 어절 자카드) - AUTO_FLAG만, PASS 아님 ===")
    similarity_flags = []
    for it in item_rows:
        options = json.loads(it["options_json"])
        for i in range(len(options)):
            for j in range(i + 1, len(options)):
                sim = jaccard(options[i], options[j])
                if sim >= 0.2:
                    similarity_flags.append((it["item_id"], i, j, round(sim, 2), options[i], options[j]))
    if similarity_flags:
        print(f"AUTO_FLAG {len(similarity_flags)}건(사람 확인 필요):")
        for f in similarity_flags:
            print(" ", f)
    else:
        print("자카드 임계값(0.2) 이상 겹치는 선택지 쌍 없음 - 그래도 이건 근사치일 뿐 의미 중복이 '없다'고 단정하지 않음")

    print("\n=== 4. 동형이의 혼동 위험(homonym_number != 0) - 플래그만, 사람 확인 필요 ===")
    homonym_flagged = [r for r in content_rows if r["homonym_number"] not in ("0", "")]
    print(f"{len(homonym_flagged)}건: {[(r['lemma'], r['homonym_number']) for r in homonym_flagged]}")
    print("편집 검토 결과(저작자 본인 재확인, 공식 판정 아님): 단음·대소·도시민... 등 전부 그 어휘의 "
          "가장 널리 쓰이는 뜻과 일치하는 것으로 판단됨 - 별도로 동형이의 혼동이 뚜렷한 후보(명시·볼트)는 "
          "이미 3절 선정 단계에서 제외했음(아래 6절 재인용)")

    print("\n=== 5. 조사 호응(을/를,이/가,은/는,과/와,로/으로) - 기계적 검사 ===")
    particle_issues_total = 0
    for r in content_rows:
        issues = check_particles(r["example_sentence"])
        if issues:
            particle_issues_total += len(issues)
            print(f"  [FAIL] {r['lemma']}: {issues}")
    for it in item_rows:
        issues = check_particles(it["explanation"])
        if issues:
            particle_issues_total += len(issues)
            print(f"  [FAIL] {it['item_id']} 설명문: {issues}")
    print(f"조사 호응 검사: 총 FAIL {particle_issues_total}건 - {'AUTO_PASS' if particle_issues_total == 0 else 'AUTO_FAIL(위 목록 확인)'}")

    print("\n=== 6. 연령/표현 적합성 - 선정 단계(3절)에서 이미 제외한 사유 재인용(재판단 아님) ===")
    for lemma, reason in EDITORIAL_HOLD_FROM_SELECTION.items():
        print(f"  {lemma}: {reason}")

    print("\n=== 중요: 이 결과의 성격 ===")
    print("위 AUTO_PASS/AUTO_FLAG는 전부 기계적 검사이거나 저작자 본인의 1차 재검토일 뿐,")
    print("'사람의 문항 승인'으로 기록된 것이 아니다 - 실제 서비스 반영 전에는")
    print("별도 검수자의 승인 절차(기존 공개검토 화면과 같은 패턴)가 반드시 필요하다.")


if __name__ == "__main__":
    main()
