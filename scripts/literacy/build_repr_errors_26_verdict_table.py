# -*- coding: utf-8 -*-
"""momo_book.db 대표선정 오류 26건 판정표 생성 (제안만, DB 변경 없음).

이번 세션이 krdict 원본 senses/homonyms까지 재조회해 직접 검증한 결과를
반영한다(단순 phase6 재인용이 아니라, "동음이의어의 다른 뜻이 채택" 여부를
krdict raw XML의 전체 definitions 리스트로 실측 검증함 - 유용하다·관대하다는
실제로 krdict 자체에 동음이의 2건이 있고 코드가 첫 번째를 기계적으로 택함을
확인했고, 모락모락·선구자는 단일 krdict 항목 안의 여러 뜻(sense) 중 첫 번째가
아닌 다른 뜻이 교재 맥락에 맞음을 확인했다).
"""
import csv
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

rows = [
    # (literacy_id, headword, category(NULL/SHORT/PUNCT), momo_vocab_id, momo_level,
    #  current_def, other_candidates[list of (vocab_id, level, def)],
    #  verdict, evidence)
    dict(literacy_id=2891, headword="유용하다", err_type="NULL_DEF", rep_vocab_id=565, rep_level="L2",
         current_def="남의 것이나 이미 용도가 정해져 있는 것을 다른 데에 쓰다.",
         other_candidates=[(55, "L5", "쓸모가 있다.")],
         verdict="REPLACE_CANDIDATE",
         evidence="krdict raw XML 재조회: '유용하다' 표제어에 동음이의 2건 존재(동사:'남의 것이나...流用', 형용사:'쓸모가 있다.'有用). "
                  "import_textbook_vocab.py의 pick_krdict_match가 첫 번째(동사, 流用)를 기계적 채택. "
                  "momo_book.db 같은 표제어의 L5 중복 행(id=55)이 정확히 형용사 뜻('쓸모가 있다.')과 일치 - 교재 맥락과 올바른 동음이의 확인됨.",
         proposed_new_def="쓸모가 있다."),
    dict(literacy_id=3168, headword="관대하다", err_type="NULL_DEF", rep_vocab_id=597, rep_level="L2",
         current_def="친절하고 정성스럽게 대하다.",
         other_candidates=[(865, "L5", "마음이 너그럽고 크다.")],
         verdict="REPLACE_CANDIDATE",
         evidence="krdict raw XML 재조회: '관대하다' 표제어에 동음이의 2건 존재(동사:'친절하고 정성스럽게 대하다.', 형용사:'마음이 넓고 이해심이 많다.'). "
                  "note 컬럼에도 'krdict 동음이의 2건 중 1번 채택'이 실제로 기록돼 있어 이 케이스가 유용하다와 동일한 버그 패턴임을 DB 자체가 증언. "
                  "momo_book.db L5 중복(id=865, '마음이 너그럽고 크다.')이 형용사(올바른 '너그럽다' 뜻)와 일치.",
         proposed_new_def="마음이 넓고 이해심이 많다."),
    dict(literacy_id=3085, headword="모락모락", err_type="NULL_DEF", rep_vocab_id=511, rep_level="L1",
         current_def="작은 것이 순조롭게 잘 자라는 모양.",
         other_candidates=[(1153, "L5", "연기나 냄새, 김 따위가 계속 조금씩 피어오르는 모양.")],
         verdict="REPLACE_CANDIDATE",
         evidence="krdict raw XML 재조회: '모락모락' 단일 항목에 3개 뜻(순서대로: 1.자라는 모양 2.연기/냄새 피어오르는 모양 3.느낌이 이는 모양). "
                  "import 코드는 definitions[0](1번, 자라는 모양)을 기계적 채택. momo_book.db L5 중복(id=1153)의 정의가 krdict 2번 뜻과 "
                  "텍스트까지 거의 동일 - 교재 원본이 2번 뜻으로 채택했음을 시사. 다만 두 후보 모두 example_sentence가 NULL이라 실제 "
                  "문맥 용례로 확정하지는 못했다(간접 증거만 있음).",
         proposed_new_def="연기나 냄새, 김 따위가 계속 조금씩 피어오르는 모양."),
    dict(literacy_id=3180, headword="선구자", err_type="NULL_DEF", rep_vocab_id=609, rep_level="L2",
         current_def="행렬에서 맨 앞에 가는 사람.",
         other_candidates=[(786, "L4", "어떤 일이나 사상에서 다른 사람보다 앞선 사람.")],
         verdict="REPLACE_CANDIDATE",
         evidence="krdict raw XML 재조회: '선구자' 단일 항목에 2개 뜻(1.행렬에서 맨 앞에 가는 사람(원의미) 2.사회적으로 중요한 일/사상에서 앞선 "
                  "사람(비유, 통상 쓰이는 '선구자' 뜻)). import 코드가 definitions[0](원의미)를 채택했으나 momo_book.db L4 중복(id=786)의 "
                  "정의가 krdict 2번 뜻과 정확히 일치 - 교재가 의도한 뜻은 비유적 '선구자'임이 명확.",
         proposed_new_def="사회적으로 중요한 일이나 사상에서 다른 사람보다 앞선 사람."),
    dict(literacy_id=2894, headword="기리다", err_type="NULL_DEF", rep_vocab_id=612, rep_level="L2",
         current_def="뛰어난 업적이나 본받을 만한 정신, 위대한 사람 등을 칭찬하고 기억하다.",
         other_candidates=[(173, "L5", "뛰어난 업적이나 바람직한 정신, 위대한 사람 따위를 칭찬하고 기억하다.")],
         verdict="NO_ISSUE", evidence="krdict 단일 항목·단일 뜻. 현재 값과 교재 L5 중복 값이 사실상 동일 문장(어휘만 미세 차이) - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=2923, headword="으레", err_type="NULL_DEF", rep_vocab_id=1044, rep_level="L1",
         current_def="두말할 것 없이 당연히.",
         other_candidates=[(202, "L5", "틀림없이 언제나.")],
         verdict="NO_ISSUE", evidence="krdict 2개 뜻('두말할 것 없이 당연히'/'언제나 늘')이 사실상 동의어 수준으로 수렴 - 서로 다른 개념이 아님. 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=2945, headword="비아냥거리다", err_type="NULL_DEF", rep_vocab_id=971, rep_level="L2",
         current_def="자꾸 비웃는 말을 하며 놀리다.",
         other_candidates=[(364, "L3", "얄밉게 빈정거리며 자꾸 놀리다.")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻. 현재 값과 교재 L3 중복 값이 동일 의미(빈정대며 놀림) - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=2974, headword="읊다", err_type="NULL_DEF", rep_vocab_id=718, rep_level="L2",
         current_def="시나 노래 등을 억양을 넣어 읽거나 외다.",
         other_candidates=[(394, "L3", "억양을 넣어서 소리를 내어 시를 읽거나 외다.")],
         verdict="NO_ISSUE", evidence="krdict 2개 뜻 중 정확히 교재 맥락과 일치하는 1번 뜻(낭송)이 채택됨 - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3001, headword="문명", err_type="NULL_DEF", rep_vocab_id=533, rep_level="L1",
         current_def="사람의 물질적, 기술적, 사회적 생활이 발전한 상태.",
         other_candidates=[(425, "L4", "인류가 이룩한 기술적, 사회적 발전과 삶의 양식")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻. 교재 L4 중복 값과 동일 개념(문명의 정의) - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3008, headword="바래다", err_type="NULL_DEF", rep_vocab_id=501, rep_level="L1",
         current_def="볕이나 습기 때문에 색이 희미해지거나 누렇게 변하다.",
         other_candidates=[(691, "L2", None), (432, "L4", "볕이나 습기를 받아 색이 변하다")],
         verdict="NO_ISSUE",
         evidence="krdict에 '바래다' 동음이의 3건(색 바래다/배웅하다/바라다의 옛말) 존재하나, 코드가 채택한 1번(색 바래다)이 "
                  "교재 L4 중복 값(색이 변하다)과 정확히 일치 - 우연히 정답을 골랐음을 확인. 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3057, headword="으름장", err_type="NULL_DEF", rep_vocab_id=965, rep_level="L2",
         current_def="말과 행동으로 으르고 협박하는 짓.",
         other_candidates=[(481, "L6", "말과 행동으로 위협하는 짓. (~을 놓다.)")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻, 교재 L6 중복 값과 동일 의미 - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3149, headword="안달하다", err_type="NULL_DEF", rep_vocab_id=577, rep_level="L2",
         current_def="속을 태우면서 조급하게 굴다.",
         other_candidates=[(831, "L4", "속을 태우며 조급하게 굴다.")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻, 교재 L4 중복 값과 사실상 동일 문장 - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3166, headword="미심쩍다", err_type="NULL_DEF", rep_vocab_id=595, rep_level="L2",
         current_def="분명하지 못해 마음이 편하지 않다.",
         other_candidates=[(725, "L2", None), (1086, "L3", "분명하지 못하여 마음이 놓이지 않는 데가 있다.")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻, 교재 L3 중복 값과 동일 의미 - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3172, headword="애지중지", err_type="NULL_DEF", rep_vocab_id=601, rep_level="L2",
         current_def="매우 사랑하고 소중히 여기는 모양.",
         other_candidates=[(785, "L4", "매우 사랑하고 소중히 여기는 모양."), (903, "L6", "매우 사랑하고 소중히 여기는 모양")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻, 교재 L4/L6 중복 값과 완전 동일 문장 - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3208, headword="거드름", err_type="NULL_DEF", rep_vocab_id=680, rep_level="L2",
         current_def="잘난 체하며 남을 자기보다 낮고 하찮게 여기는 태도.",
         other_candidates=[(821, "L4", "거만스러운 태도.")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻, 교재 L4 중복 값(거만한 태도)과 같은 개념 - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3242, headword="모질다", err_type="NULL_DEF", rep_vocab_id=717, rep_level="L2",
         current_def="마음씨나 말씨나 행동이 몹시 쌀쌀맞고 독하다.",
         other_candidates=[(1066, "L3", "마음씨가 몹시 매섭고 독하다."), (1182, "L5", "참고 견디기 힘든 일을 거뜬히 견딜 만큼 억세다.")],
         verdict="NO_ISSUE",
         evidence="krdict 3개 뜻 중 1번(독한 마음씨)이 채택됨 - 교재 L3 중복 값과 일치. L5 중복은 3번 뜻(인내심)으로 별개 의미이나 "
                  "현재 채택된 뜻 자체는 틀리지 않음(다의어 중 유효한 한 뜻). 오류 아님(다의어 특성상 완전 커버는 별도 정책 사안).",
         proposed_new_def=None),
    dict(literacy_id=3250, headword="감싸다", err_type="NULL_DEF", rep_vocab_id=727, rep_level="L2",
         current_def="둘러서 덮다.",
         other_candidates=[(1151, "L5", "전체를 둘러서 싸다.")],
         verdict="NO_ISSUE", evidence="krdict 3개 뜻 중 1번(둘러서 덮다)이 채택됨 - 교재 L5 중복 값과 일치. 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3278, headword="기색", err_type="NULL_DEF", rep_vocab_id=931, rep_level="L2",
         current_def="마음속의 생각이나 감정이 얼굴이나 행동에 나타나는 것.",
         other_candidates=[(756, "L3", "마음의 작용으로 얼굴에 드러나는 빛. 어떠한 행동이나 현상 따위가 일어나는 것을 짐작할 수 있게 하여 주는 눈치나 낌새"),
                            (833, "L5", "마음의 작용으로 얼굴에 드러나는 빛. 어떠한 행동이나 현상 따위가 일어나는 것을 짐작할 수 있게 하여 주는 눈치나 낌새.")],
         verdict="NO_ISSUE", evidence="krdict 1번 뜻과 교재 정의가 같은 개념(내적 상태의 외적 표현) - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3295, headword="으스대다", err_type="NULL_DEF", rep_vocab_id=985, rep_level="L2",
         current_def="보기에 좋지 않게 우쭐거리며 뽐내다.",
         other_candidates=[(773, "L3", "어울리지 아니하게 우쭐거리며 뽐내다.")],
         verdict="NO_ISSUE", evidence="krdict 단일 뜻, 교재 L3 중복 값과 동일 의미(문구만 상이) - 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3426, headword="고즈넉하다", err_type="NULL_DEF", rep_vocab_id=968, rep_level="L2",
         current_def="분위기 등이 조용하고 편안하다.",
         other_candidates=[(927, "L6", "고요하고 아늑하다.")],
         verdict="NO_ISSUE", evidence="krdict 3개 뜻 중 1번(분위기가 조용하고 편안함)이 채택됨 - 교재 L6 중복 값과 같은 개념. 오류 아님.",
         proposed_new_def=None),
    dict(literacy_id=3450, headword="숭배하다", err_type="NULL_DEF", rep_vocab_id=955, rep_level="L2",
         current_def="우러러 공경하다.",
         other_candidates=[(1057, "L3", "우러러 .공경하다")],
         verdict="NO_ISSUE", evidence="krdict 2개 뜻 중 1번이 채택됨 - 교재 L3 중복 값과 사실상 동일(교재 쪽에 OCR 마침표 오류만 있고, "
                  "현재 literacy.db 값이 오히려 더 깨끗함). 오류 아님.",
         proposed_new_def=None),

    # 부분정의(SHORT_DEF) 3건
    dict(literacy_id=2940, headword="궁색하다", err_type="SHORT_DEF", rep_vocab_id=359, rep_level="L3",
         current_def="아주 가난하다.",
         other_candidates=[(817, "L4", "1. 아주 가난하다.\n2. 말이나 태도, 행동의 이유나 근거 따위가 부족하다."),
                            (1179, "L5", "말이나 태도, 행동의 이유나 근거 따위가 부족하다.")],
         verdict="HOLD",
         evidence="krdict 원본도 2개 뜻(1.가난하다 2.근거가 부족하다)을 갖고 있고, 현재 채택된 정의(1번 뜻)는 틀리지 않았으나 2번 뜻이 "
                  "누락돼 있다(momo_book.db L4 중복은 두 뜻을 모두 담고 있음). '오류'는 아니고 '다의어 커버리지 부족'이라 교체 여부는 "
                  "정책 판단(다의어를 얼마나 포괄적으로 실어야 하는지) 필요 - 근거 확정적이지 않아 HOLD로 유지.",
         proposed_new_def=None),
    dict(literacy_id=3289, headword="허사", err_type="SHORT_DEF", rep_vocab_id=767, rep_level="L3",
         current_def="보람을 얻지 못하고 쓸데없이 한 노력.",
         other_candidates=[(784, "L4", "사실이 아닌 것을 사실인 것처럼 꾸며 대어 말을 함. 또는 그런 말.")],
         verdict="NO_ISSUE",
         evidence="krdict 재조회 결과 '허사' 표제어는 krdict에 단 1개 뜻만 존재('노력을 한 만큼의 좋은 결과를 얻지 못한 일')하며 이는 "
                  "현재 채택된 L3 정의와 정확히 일치한다. momo_book.db의 L4 중복 행(id=784, '거짓말을 꾸며 말함')은 krdict 어디에도 "
                  "대응하는 뜻이 없어 momo_book.db 자체의 표제어-정의 매칭 오류(다른 단어의 정의가 잘못 태그됐을 가능성)로 추정된다 - "
                  "phase6이 '정보 손실'로 분류했던 것과 달리, 재검증 결과 현재 literacy.db 값이 오히려 올바른 쪽이었다.",
         proposed_new_def=None),
    dict(literacy_id=3324, headword="자초지종", err_type="SHORT_DEF", rep_vocab_id=1070, rep_level="L3",
         current_def="처음부터 끝까지의 과정.",
         other_candidates=[(808, "L4", "처음부터 끝까지. 일의 처음부터 끝까지의 전체 과정을 가리키는 말"),
                            (850, "L5", "처음부터 끝까지의 과정."), (1227, "L6", "처음부터 끝까지의 과정.")],
         verdict="NO_ISSUE",
         evidence="krdict 재조회 결과 '자초지종'은 단일 뜻('처음부터 끝까지의 모든 과정')이며 현재 채택된 정의와 사실상 동일하다. "
                  "다른 후보(L5, L6)도 현재 값과 완전히 같은 문자열이고 L4만 더 장황한 표현일 뿐 - 정보 손실 없음. 오류 아님.",
         proposed_new_def=None),

    # 구두점/어순 손상(PUNCT_DAMAGE) 2건
    dict(literacy_id=3015, headword="혼비백산", err_type="PUNCT_DAMAGE", rep_vocab_id=1063, rep_level="L3",
         current_def="몹시 놀라 넋을 잃음을 이르는. 말",
         other_candidates=[(439, "L4", "혼백이 어지러이 흩어진다는 뜻으로, 몹시 놀라 넋을 잃음을 이르는 말.")],
         verdict="REPLACE_CANDIDATE",
         evidence="현재 값은 마침표가 '이르는' 뒤에 잘못 찍히고 '말'이 마침표 뒤로 밀려난 명백한 구두점/어순 손상(원문 추출 과정 오류로 추정). "
                  "L4 중복 값이 같은 내용을 정상적인 구두점으로 담고 있어 순수 텍스트 정정 - 의미 변경 없음.",
         proposed_new_def="혼백이 어지러이 흩어진다는 뜻으로, 몹시 놀라 넋을 잃음을 이르는 말."),
    dict(literacy_id=2924, headword="독불장군", err_type="PUNCT_DAMAGE", rep_vocab_id=1058, rep_level="L3",
         current_def="무슨 일이든 자기 생각대로 혼자서 처리하는. 사람",
         other_candidates=[(203, "L5", "무슨 일이든 자기 생각대로 혼자서 처리하는 사람.")],
         verdict="REPLACE_CANDIDATE",
         evidence="현재 값은 '처리하는' 뒤에 마침표, '사람'이 마침표 뒤로 밀려난 동일 유형의 구두점 손상. L5 중복 값이 정상 구두점으로 "
                  "같은 내용을 담고 있어 순수 텍스트 정정 - 의미 변경 없음.",
         proposed_new_def="무슨 일이든 자기 생각대로 혼자서 처리하는 사람."),
]

assert len(rows) == 26, len(rows)

# CSV
csv_path = REPO_ROOT / "reports" / "literacy_repr_errors_26_verdict_20260924.csv"
with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f)
    w.writerow(["literacy_id", "headword", "err_type", "momo_book_vocab_id", "momo_book_level",
                "current_definition_in_literacy_db", "other_candidates(vocab_id/level/definition)",
                "verdict", "evidence", "proposed_new_definition(dry-run only, not applied)"])
    for r in rows:
        others_str = " | ".join(f"{vid}/{lvl}/{d!r}" for vid, lvl, d in r["other_candidates"])
        w.writerow([r["literacy_id"], r["headword"], r["err_type"], r["rep_vocab_id"], r["rep_level"],
                    r["current_def"], others_str, r["verdict"], r["evidence"], r["proposed_new_def"] or ""])

# JSONL
jsonl_path = REPO_ROOT / "reports" / "literacy_repr_errors_26_verdict_20260924.jsonl"
with open(jsonl_path, "w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

# 요약
from collections import Counter
c = Counter(r["verdict"] for r in rows)
print("판정 요약:", dict(c))
print("저장:", csv_path)
print("저장:", jsonl_path)
