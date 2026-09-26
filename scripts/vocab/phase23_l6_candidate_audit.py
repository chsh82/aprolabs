# -*- coding: utf-8 -*-
"""
phase23 -- L6(고2~3) 신규 후보 재집계 + AI 태그 분리 + 나선형 반복/의미 분리 검사 +
           REUSE 대조 + 최종 판정 + 첫 배치(최대 50건) 제안.

읽기 전용. data/literacy.db 는 mode=ro + PRAGMA query_only=ON 으로만 연다.
DB에 어떤 쓰기도 수행하지 않는다.

재현 절차:
1. (서버 REUSE 대조용 스냅샷을 새로 뜨고 싶다면, 최초 1회만)
   ssh aprolabs "sqlite3 -readonly -json ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
     \"SELECT content_id, lemma, pos, canonical_definition, student_definition FROM vocabulary_contents;\"" \
     > data/import/schema_reading_phase23_vq_contents_snapshot_20260926.json
2. python scripts/vocab/phase23_l6_candidate_audit.py
   (data/literacy.db, data/import/schema_reading_phase23_vq_contents_snapshot_20260926.json 를 그대로 재사용)

출력:
- data/import/schema_reading_phase23_l6_candidates_20260926.csv / .jsonl
  (V 84 + S 32 = 116건 AI 미태그 후보 전수, 행별 판정)
- data/import/schema_reading_phase23_l6_ai_requeue_20260926.csv / .jsonl
  (V 1 + S 576 = 577건 AI 자동생성 태그 재검수 대기열)
"""
import sqlite3
import json
import csv
import re
from collections import Counter

REPO_ROOT = __file__
import os
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
LITERACY_DB = os.path.join(REPO_ROOT, 'data', 'literacy.db')
VQ_SNAPSHOT = os.path.join(REPO_ROOT, 'data', 'import',
                            'schema_reading_phase23_vq_contents_snapshot_20260926.json')
OUT_DIR = os.path.join(REPO_ROOT, 'data', 'import')

AI_TAG = 'AI 자동 생성'

# ---------------------------------------------------------------------------
# 이번 세션이 직접 읽고 판단해 정리한 분류표 (재실행 시에도 동일 결과를 내기 위해
# 고정값으로 둔다 -- 표제어 목록이 바뀌면 이 표도 사람이 다시 검토해야 한다).
# ---------------------------------------------------------------------------

# V(학습도구어) -- literacy.db 자체가 번호를 붙여 동형이의 분리를 표시한 항목
# (phase11/12가 L4/L5에서 이미 확립한 배제 기준을 이번 세션이 L6에도 동일 적용)
NUMERIC_HOMONYM = {'이용01', '인하다01', '개설02', '역설02', '양20', '외04', '이론01'}

# V -- 정의 자체가 매우 기초적/일반적이어서 고2~3(L6) 수준 근거가 약한 항목
# (level=6 배정은 grade_level 개별 태그 없이 grade_source='manual' 정책 매핑에만 의존)
WEAK_GENERIC_V = {'지', '내', '원'}

# V -- 형태/어근이 비슷하거나 의미가 인접해 혼동 위험이 있는 짝 (phase13 "유추/유사"류와 동일 성격)
V_MEANING_REVIEW_PAIRS = {
    '전면': '이면(뒷면)과 정반대 의미의 대비 짝(앞면/뒷면) - 동시 출제 시 구분 문구 권장',
    '이면': '전면(앞면)과 정반대 의미의 대비 짝(앞면/뒷면) - 동시 출제 시 구분 문구 권장',
    '통찰': '고찰과 의미 인접(둘 다 "깊이 살펴봄" 계열) - 혼동 가능, 구분 문구 권장',
    '고찰': '통찰과 의미 인접(둘 다 "깊이 살펴봄" 계열) - 혼동 가능, 구분 문구 권장',
    '객관성': '객관적과 같은 어근("객관")의 명사/관형사 파생쌍 - 같은 세트 출제 시 개념 중복 위험',
    '객관적': '객관성과 같은 어근("객관")의 명사/관형사 파생쌍 - 같은 세트 출제 시 개념 중복 위험',
    '실재': '실질과 의미 인접("실제로 존재/실제 본바탕") - 혼동 가능',
    '실질': '실재와 의미 인접("실제로 존재/실제 본바탕") - 혼동 가능',
}

# V -- literacy.db 전체에서 낮은 레벨에도 같은 headword가 있는 경우의 SENSE_REVIEW 사유
SENSE_REVIEW_V = {
    '운동': ('literacy.db 내 동일 headword가 schemareading-schema level=3에 '
             '"물체의 위치 변화(물리)" 뜻으로 별도 존재 - L6 V 뜻(신체 단련)과 동형이의, '
             '의미 분리 확인 필요'),
    '공동체': ('literacy.db 내 동일 headword가 momo-textbook level=0, '
               'schemareading-schema level=3에 핵심 의미가 사실상 동일한 정의로 이미 존재'
               '("생활/행동/목적을 같이하는 집단") - 자구는 다르나 개념 중복 위험, '
               '저학년 개념과의 관계 확인 필요'),
}

# S(교과개념어) -- 선택과목(법과 정치) 심화 절차·제도 전문용어로 판단해 배제한 19건
# (phase9/10이 인문철학 L4/L5에서 적용한 "선택과목 전문용어 배제" 논리를 이번 세션이
#  L6 법 영역에 새로 적용한 것 -- 재인용 아님, 이번 세션의 신규 판단)
S_NARROW_SPECIALIZED_HOLD = {
    '위법성조각사유', '형벌의 종류', '보안처분', '형사절차', '수사', '공소제기', '재정신청', '공판',
    '국선변호인', '미란다원칙', '범죄피해자구조제도', '배상명령제도',
    '행정행위', '행정지도', '행정구제제도', '행정쟁송제도', '행정심판', '행정소송', '김영란법',
}

# S -- 대비 개념 짝 (phase15의 "발산형/수렴형 경계"류와 동일 성격, 배제 아니라 캐주션)
S_MEANING_REVIEW_PAIRS = {
    '실체법': '절차법과 대비 짝(실체법=권리·의무의 내용을 규정 / 절차법=그 실현 절차를 규정) - 같은 세트 출제 시 구분 문구 권장',
    '절차법': '실체법과 대비 짝(위와 동일) - 같은 세트 출제 시 구분 문구 권장',
    '일반법': '특별법과 대비 짝(일반법=일반 적용 / 특별법=특정 대상 한정 적용) - 같은 세트 출제 시 구분 문구 권장',
    '특별법': '일반법과 대비 짝(위와 동일) - 같은 세트 출제 시 구분 문구 권장',
}


def base_form(hw: str) -> str:
    """번호 접미(01, 02, 20...)를 뗀 원형 -- REUSE 대조용."""
    return re.sub(r'\d+$', '', hw)


def classify(r, reuse_exact):
    hw = r['headword']
    src = r['source']
    if reuse_exact:
        return 'REUSE', '기존 vocabulary_contents(5,820건)와 표제어 정확 일치'
    if r['lower_level_matches']:
        exact = [o for o in r['lower_level_matches']
                 if (o['definition'] or '').strip() == (r['definition'] or '').strip()]
        if exact:
            return ('SPIRAL_REVIEW',
                    f"literacy.db 낮은 레벨(level={exact[0]['level']}, source={exact[0]['source']})에 "
                    f"정의 문자열까지 완전히 동일한 행이 존재")
        return 'SENSE_REVIEW', SENSE_REVIEW_V.get(
            hw, '낮은 레벨에 동일 headword가 다른/근접한 정의로 존재 - 의미 분리 확인 필요')
    if src == 'schemareading-tooldict':
        if hw in NUMERIC_HOMONYM:
            return ('HOLD', '동형이의 번호 분리 표기(literacy.db 자체 표기) - phase11/12와 동일 '
                             '기준으로 의미 미확정, 콘텐츠화 전 확정 필요')
        if hw in WEAK_GENERIC_V:
            return ('HOLD', f"정의가 매우 기초적/일반적('{r['definition']}')이라 고2~3(L6) 수준 근거가 "
                             f"약함 - level=6 배정은 grade_level 개별 태그 없이 정책 매핑(grade_source="
                             f"manual)에만 의존, 개별 근거로 보강 필요")
        return 'NEW_CANDIDATE', 'literacy.db 정의 원문 그대로 사용 가능, 동형이의/나선형 반복/기존 콘텐츠 중복 없음'
    if hw in S_NARROW_SPECIALIZED_HOLD:
        return ('HOLD', '선택과목(법과 정치) 심화 절차·제도 전문용어로 일반 고2~3 스키마리딩 어휘 범위를 '
                         '초과 - phase9/10의 인문철학 L4/L5 제외 논리를 이번 세션이 L6에 동일 적용'
                         '(신규 판단, 재인용 아님)')
    return 'NEW_CANDIDATE', '통합사회/법과정치 교과에서 폭넓게 다뤄지는 기초 법 개념으로 판단, 정의 원문 그대로 사용 가능'


def main():
    con = sqlite3.connect(f'file:{LITERACY_DB}?mode=ro', uri=True)
    con.execute('PRAGMA query_only=ON')
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("""
        SELECT id, headword, source, definition, pos, sense_category, subject_category,
               note, grade_level, grade_source, review_status, external_id, reviewed_at
        FROM terms
        WHERE level=6 AND source IN ('schemareading-tooldict', 'schemareading-schema')
        ORDER BY source, id
    """)
    all_rows = [dict(row) for row in cur.fetchall()]
    for r in all_rows:
        r['ai_tagged'] = bool(r['note'] and AI_TAG in r['note'])

    ai_rows = [r for r in all_rows if r['ai_tagged']]
    non_ai_rows = [r for r in all_rows if not r['ai_tagged']]
    print(f"L6 V+S 전체: {len(all_rows)} / AI 태그: {len(ai_rows)} / 비AI 후보: {len(non_ai_rows)}")

    def other_rows(headword, self_id):
        cur.execute(
            "SELECT id, headword, source, level, definition FROM terms "
            "WHERE headword=? AND id != ? ORDER BY level", (headword, self_id))
        return [dict(row) for row in cur.fetchall()]

    for r in non_ai_rows:
        others = other_rows(r['headword'], r['id'])
        r['lower_level_matches'] = [o for o in others if o['level'] is not None and o['level'] < 6]

    with open(VQ_SNAPSHOT, encoding='utf-8') as f:
        vq = json.load(f)
    vq_lemmas = {row['lemma'] for row in vq}
    print(f"REUSE 대조 대상 vocabulary_contents 스냅샷: {len(vq)}건")

    rows_out = []
    for r in non_ai_rows:
        hw = r['headword']
        reuse_exact = hw in vq_lemmas or base_form(hw) in vq_lemmas
        verdict, reason = classify(r, reuse_exact)
        caution = V_MEANING_REVIEW_PAIRS.get(hw) or S_MEANING_REVIEW_PAIRS.get(hw) or ''
        rows_out.append({
            'literacy_term_id': r['id'],
            'v_s': 'V' if r['source'] == 'schemareading-tooldict' else 'S',
            'source': r['source'],
            'headword': hw,
            'pos': r['pos'],
            'definition': r['definition'],
            'sense_category': r['sense_category'],
            'subject_category': r['subject_category'],
            'note_raw': r['note'],
            'grade_level_raw': r['grade_level'],
            'grade_source_raw': r['grade_source'],
            'grade_evidence_strength': 'POLICY_MAPPING_ONLY(개별 학년 태그 없음, level=6→L6 정책 매핑에만 의존)',
            'review_status_raw': r['review_status'],
            'ai_tagged': 'N',
            'external_id': r['external_id'],
            'lower_level_match_count': len(r['lower_level_matches']),
            'lower_level_match_detail': (json.dumps(r['lower_level_matches'], ensure_ascii=False)
                                          if r['lower_level_matches'] else ''),
            'reuse_check': 'MATCH' if reuse_exact else 'NO_MATCH(exact lemma vs vocabulary_contents 5,820)',
            'homonym_numeric_suffix': 'Y' if hw in NUMERIC_HOMONYM else 'N',
            'caution': caution,
            'final_classification': verdict,
            'classification_reason': reason,
        })

    new_candidates = [r for r in rows_out if r['final_classification'] == 'NEW_CANDIDATE']
    v_new = [r for r in new_candidates if r['v_s'] == 'V']
    s_new = [r for r in new_candidates if r['v_s'] == 'S']
    print(f"NEW_CANDIDATE {len(new_candidates)}건 (V {len(v_new)} / S {len(s_new)})")

    s_new_sorted = sorted(s_new, key=lambda r: r['literacy_term_id'])
    v_clean = sorted([r for r in v_new if not r['caution']], key=lambda r: r['literacy_term_id'])
    v_caution = sorted([r for r in v_new if r['caution']], key=lambda r: r['literacy_term_id'])

    budget = 50
    batch = list(s_new_sorted)
    batch += (v_clean + v_caution)[:max(0, budget - len(batch))]
    batch_ids = {r['literacy_term_id'] for r in batch}
    for r in rows_out:
        r['proposed_first_l6_batch'] = 'Y' if r['literacy_term_id'] in batch_ids else ''

    print(f"첫 배치 제안: {len(batch)}건 (V {sum(1 for r in batch if r['v_s']=='V')} / "
          f"S {sum(1 for r in batch if r['v_s']=='S')})")
    print('판정 분포:', Counter(r['final_classification'] for r in rows_out))

    requeue_rows = [{
        'literacy_term_id': r['id'],
        'v_s': 'V' if r['source'] == 'schemareading-tooldict' else 'S',
        'source': r['source'],
        'headword': r['headword'],
        'definition': r['definition'],
        'subject_category': r['subject_category'],
        'note_raw': r['note'],
        'review_status_raw': r['review_status'],
        'reviewed_at': r['reviewed_at'],
        'ai_tagged': 'Y',
        'external_id': r['external_id'],
        'classification': 'REQUEUE_AI_GENERATED_PENDING_HUMAN_REVIEW',
    } for r in ai_rows]

    out_csv = os.path.join(OUT_DIR, 'schema_reading_phase23_l6_candidates_20260926.csv')
    with open(out_csv, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        w.writeheader()
        w.writerows(rows_out)

    out_jsonl = os.path.join(OUT_DIR, 'schema_reading_phase23_l6_candidates_20260926.jsonl')
    with open(out_jsonl, 'w', encoding='utf-8') as f:
        for r in rows_out:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    req_csv = os.path.join(OUT_DIR, 'schema_reading_phase23_l6_ai_requeue_20260926.csv')
    with open(req_csv, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(requeue_rows[0].keys()))
        w.writeheader()
        w.writerows(requeue_rows)

    req_jsonl = os.path.join(OUT_DIR, 'schema_reading_phase23_l6_ai_requeue_20260926.jsonl')
    with open(req_jsonl, 'w', encoding='utf-8') as f:
        for r in requeue_rows:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    print('출력 완료:', out_csv, out_jsonl, req_csv, req_jsonl)


if __name__ == '__main__':
    main()
