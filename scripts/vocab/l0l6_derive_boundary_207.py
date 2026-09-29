# -*- coding: utf-8 -*-
"""읽기 전용: 레벨보류(207건) item_id/content_id 목록을 독립적으로 재도출.
기존 audit_existing_l0l3_reusable_328.py의 검사 로직(선택지 4개, 정답 유일성,
정답-뜻풀이 일치, CONTEXT_MEANING 대상어-표제어 관련성)을 동일 기준으로 재적용해
report(vocab_quiz_existing_l0l3_expansion_dryrun_20260929.md) 표(L0:4/L1:34/L2:159/L3:10=207,
재사용가능 L0:59/L1:116/L2:2/L3:151=328)와 대조 검증한다."""
import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

SCRATCH_DIR = Path.home() / "l0l6_scratch"  # 실행 환경에 맞게 조정
DB = str(SCRATCH_DIR / "vq_research_snapshot_readonly.db")  # 연구 DB 읽기 전용 사본
con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
con.row_factory = sqlite3.Row
cur = con.cursor()

cur.execute("""
    SELECT mi.item_id, mi.item_type, mi.source_content_id, mi.prompt, mi.options_json,
           mi.correct_option, mi.explanation, mi.is_active AS item_active,
           vc.lemma, vc.canonical_definition, vc.student_definition, vc.hold_reason,
           vc.is_active AS content_active,
           vcl.vocab_level, vcl.level_status, vcl.boundary_flag
    FROM vocabulary_multiformat_items mi
    JOIN vocabulary_contents vc ON vc.content_id = mi.source_content_id
    JOIN vocabulary_content_levels vcl ON vcl.content_id = vc.content_id AND vcl.is_active = 1
    WHERE mi.source_version = '2.1.29'
      AND mi.item_type IN ('MEANING_CHOICE', 'CONTEXT_MEANING')
      AND vcl.vocab_level IN (0, 1, 2, 3)
""")
rows = [dict(r) for r in cur.fetchall()]
print(f"유형호환(MC+CM, L0~L3) 총 문항: {len(rows)}건")


def quality_pass(r):
    if not r["item_active"] or not r["content_active"]:
        return False, "비활성"
    if r["hold_reason"] and r["hold_reason"].strip():
        return False, "콘텐츠 HOLD"
    try:
        options = json.loads(r["options_json"])
    except (TypeError, ValueError):
        return False, "options_json 파싱 실패"
    if len(options) != 4:
        return False, "선택지 4개 아님"
    correct_idx = (r["correct_option"] or 0) - 1
    correct_text = options[correct_idx].strip() if 0 <= correct_idx < len(options) else None
    if correct_text is None:
        return False, "정답 인덱스 이상"
    exact_dups = sum(1 for o in options if o.strip() == correct_text)
    if exact_dups != 1:
        return False, "정답 유일성 위반"
    if len(set(o.strip() for o in options)) != len(options):
        return False, "선택지 중복"
    student_def = (r["student_definition"] or "").strip()
    canonical_def = (r["canonical_definition"] or "").strip()
    if not (correct_text == student_def or correct_text == canonical_def):
        return False, "정답-뜻풀이 불일치"
    if r["item_type"] == "CONTEXT_MEANING":
        prompt = r["prompt"] or ""
        if "【" not in prompt or "】" not in prompt:
            return False, "대상어 표시 없음"
        marked = prompt[prompt.find("【") + 1: prompt.find("】")]
        lemma = r["lemma"] or ""
        if lemma not in marked and marked not in lemma:
            return False, "내용보류(표시어형-표제어 관련성 불명확)"
    return True, None


by_level_reuse = defaultdict(int)
by_level_boundary = defaultdict(int)
by_level_contenthold = defaultdict(int)
by_level_otherfail = defaultdict(int)
boundary_content_ids = set()
boundary_item_ids = []
reuse_content_ids = set()

for r in rows:
    ok, reason = quality_pass(r)
    lv = r["vocab_level"]
    if not ok:
        if reason == "내용보류(표시어형-표제어 관련성 불명확)":
            by_level_contenthold[lv] += 1
        else:
            by_level_otherfail[lv] += 1
        continue
    if r["level_status"] == "REVIEW_BOUNDARY":
        by_level_boundary[lv] += 1
        boundary_content_ids.add(r["source_content_id"])
        boundary_item_ids.append(r["item_id"])
    else:
        by_level_reuse[lv] += 1
        reuse_content_ids.add(r["source_content_id"])

print("\n레벨보류(REVIEW_BOUNDARY, quality-pass) 레벨별:")
tot_boundary = 0
for lv in (0, 1, 2, 3):
    print(f"  L{lv}: {by_level_boundary[lv]}")
    tot_boundary += by_level_boundary[lv]
print(f"  합계: {tot_boundary} (report 기대값 207)")

print("\n재사용가능(PROVISIONAL_AUTO, quality-pass) 레벨별:")
tot_reuse = 0
for lv in (0, 1, 2, 3):
    print(f"  L{lv}: {by_level_reuse[lv]}")
    tot_reuse += by_level_reuse[lv]
print(f"  합계: {tot_reuse} (report 기대값 328)")

print("\n내용보류(표시어형 관련성 불명확) 레벨별:")
tot_ch = 0
for lv in (0, 1, 2, 3):
    print(f"  L{lv}: {by_level_contenthold[lv]}")
    tot_ch += by_level_contenthold[lv]
print(f"  합계: {tot_ch} (report 기대값 61)")

print("\n기타 실패(구조결함 등) 레벨별:", dict(by_level_otherfail))

print(f"\n레벨보류 distinct content_id 수: {len(boundary_content_ids)}")
print(f"재사용가능 distinct content_id 수: {len(reuse_content_ids)}")

with open(SCRATCH_DIR / "boundary_207_content_ids.json", "w", encoding="utf-8") as f:
    json.dump(sorted(boundary_content_ids), f, ensure_ascii=False, indent=2)
with open(SCRATCH_DIR / "boundary_207_item_ids.json", "w", encoding="utf-8") as f:
    json.dump(sorted(boundary_item_ids), f, ensure_ascii=False, indent=2)
