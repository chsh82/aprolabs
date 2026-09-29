# -*- coding: utf-8 -*-
"""L0~L6 어휘 레벨 1차 자동 분류기(읽기 전용). DB 기존 vocab_level은 추천 계산에
쓰지 않고 사후 비교에만 쓴다. 1차 근거: DB에 이미 있는 nikl_vocabulary_grade
(국립국어원 어휘 등급, level_reason_json 안에 content_id별로 보존돼 있음) -
docs/vocabulary/LEVEL_POLICY_v0.1.md에 명시된 원천등급->학년군 정책을 그대로
따르되, 2개 레벨에 걸치는 3등급/4등급은 사용자 지시대로 하나로 강제 확정하지
않고 경계로 표시한다. 2차 근거(정오표): KRDict(한국어기초사전) 표제어+뜻풀이
대조 - 표제어만 같다고 뜻풀이 일치로 보지 않는다(문자 bigram Jaccard로 정의문
유사도를 직접 계산).

재실행 방법: 1) scripts/vocab/l0l6_parse_krdict.py로 KRDict 인덱스를 먼저 만들고
(raw/krdict/krdict_dump/*.xml 필요, git 미포함 - scripts/literacy/krdict_dump.py로 재다운로드),
2) 연구 DB를 읽기 전용 사본으로 로컬에 내려받은 뒤(scp, 운영 DB 직접 접속 금지),
3) 아래 SCRATCH_DIR/DB 경로를 실제 환경에 맞게 바꿔 실행한다. 순수 읽기 전용 -
DB에 쓰지 않는다."""
import csv
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRATCH_DIR = Path.home() / "l0l6_scratch"  # 실행 환경에 맞게 조정

DB = str(SCRATCH_DIR / "vq_research_snapshot_readonly.db")  # 연구 DB 읽기 전용 사본
KRDICT_INDEX_PATH = str(SCRATCH_DIR / "krdict_lemma_index.json")
OUT_DIR = str(SCRATCH_DIR)

with open(KRDICT_INDEX_PATH, encoding="utf-8") as f:
    KRDICT = json.load(f)

# docs/vocabulary/LEVEL_POLICY_v0.1.md 원천 등급 정책
NIKL_TO_BAND = {
    "2등급": ("L0", None),           # 정규교육 이전 습득 어휘 -> L0 중심(단일 밴드)
    "3등급": ("경계", "L1~L2"),      # 초등교육 단계 습득 어휘 -> L1~L2(두 밴드, 강제분할 안 함)
    "4등급": ("경계", "L3~L4"),      # 사춘기 이후 중등교육 단계 -> L3~L4(두 밴드, 강제분할 안 함)
}


def bigrams(s):
    s = re.sub(r"\s+", "", s or "")
    return set(s[i:i + 2] for i in range(len(s) - 1)) if len(s) >= 2 else set()


def jaccard(a, b):
    A, B = bigrams(a), bigrams(b)
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)


DEF_SIM_THRESHOLD = 0.30  # 문자 bigram Jaccard 임계값(경험적) - 이 이상이면 "뜻풀이까지 일치"

con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
con.row_factory = sqlite3.Row
cur = con.cursor()
cur.execute("""
    SELECT vc.content_id, vc.lemma, vc.pos, vc.source_version, vc.canonical_definition,
           vc.student_definition, vc.hold_reason, vc.student_exposure, vc.public_ready,
           vcl.vocab_level AS db_vocab_level, vcl.level_status AS db_level_status,
           vcl.boundary_flag AS db_boundary_flag, vcl.level_source, vcl.level_reason_json
    FROM vocabulary_contents vc
    JOIN vocabulary_content_levels vcl ON vcl.content_id = vc.content_id AND vcl.is_active = 1
    WHERE vc.is_active = 1
""")
rows = [dict(r) for r in cur.fetchall()]
print(f"전체 활성 콘텐츠(레벨 포함): {len(rows)}건")

with open(OUT_DIR + r"\boundary_207_content_ids.json", encoding="utf-8") as f:
    boundary_207 = set(json.load(f))
with open(OUT_DIR + r"\l2_17_content_ids.json", encoding="utf-8") as f:
    l2_17 = set(json.load(f))

full_rows = []
match_rate_counter = Counter()  # 매칭없음/표제어만일치/뜻풀이까지일치
level_dist_counter = Counter()
conflict_counter = Counter()  # MATCH / CONFLICT / DB_OVERCONFIDENT_BOUNDARY / OUT_OF_SCOPE / HOLD_NO_COMPARISON

for r in rows:
    lemma = r["lemma"]
    pos = r["pos"]
    canon = (r["canonical_definition"] or "").strip()
    stud = (r["student_definition"] or "").strip()
    db_def = stud or canon

    # ---- 1차 근거: NIKL 어휘등급(level_reason_json) / 수동 교재 매칭 ----
    nikl_grade = None
    try:
        reasons = json.loads(r["level_reason_json"]) if r["level_reason_json"] else []
    except (TypeError, ValueError):
        reasons = []
    for feat in reasons:
        if isinstance(feat, dict) and feat.get("feature") == "nikl_vocabulary_grade":
            nikl_grade = feat.get("value")
            break

    risk_notes = []
    if r["level_source"] in ("MANUAL_LITERACY_L4_MATCH", "MANUAL_LITERACY_L5_MATCH", "MANUAL_LITERACY_L6_MATCH"):
        recommended = "제외(수동 교재 근거 확정군)"
        evidence_source = f"{r['level_source']}(기존 L6 근거기반 집필 파이프라인의 수동 검증 - 이번 자동분류 범위 밖)"
        confidence = "해당없음"
        primary_band_note = "제외"
    elif nikl_grade in NIKL_TO_BAND:
        band, span = NIKL_TO_BAND[nikl_grade]
        recommended = band if span is None else f"경계({span})"
        evidence_source = f"NIKL 어휘등급({nikl_grade})"
        if span is None:
            confidence = "상"
        else:
            confidence = "중"
            risk_notes.append(f"원천 {nikl_grade}는 정책상 {span} 두 밴드에 걸침 - 세부레벨 강제분할 안 함")
        primary_band_note = recommended
    else:
        recommended = "보류(근거부족)"
        evidence_source = f"NIKL 등급 확인 불가(level_source={r['level_source']}, value={nikl_grade})"
        confidence = "하"
        primary_band_note = "보류"

    # ---- 2차 근거(대조): KRDict 표제어+뜻풀이 ----
    candidates = KRDICT.get(lemma, [])
    krdict_status = "매칭없음"
    krdict_level = None
    best_sim = 0.0
    homonym_levels = set()
    homonym_risk = False
    if candidates:
        krdict_status = "표제어만 일치"
        for c in candidates:
            if c.get("level"):
                homonym_levels.add(c["level"])
            for d in c.get("defs", []):
                sim = jaccard(db_def, d)
                if sim > best_sim:
                    best_sim = sim
                    krdict_level = c.get("level")
        if best_sim >= DEF_SIM_THRESHOLD:
            krdict_status = "뜻풀이까지 일치"
        if len(candidates) > 1 and len(homonym_levels - {"없음"}) > 1:
            homonym_risk = True
            risk_notes.append(f"KRDict 동형이의 {len(candidates)}건, 등급 상이({sorted(homonym_levels)})")

    match_rate_counter[krdict_status] += 1

    # KRDict 등급과 1차 추천의 방향성 충돌(정보용, 추천을 덮어쓰지 않음)
    krdict_conflict = False
    if krdict_status == "뜻풀이까지 일치" and krdict_level and krdict_level != "없음":
        # 아주 거친 방향성 체크: NIKL 2등급(=L0 권장)인데 KRDict가 고급이면 상충 신호
        if nikl_grade == "2등급" and krdict_level == "고급":
            krdict_conflict = True
            risk_notes.append("NIKL 2등급(L0)과 KRDict 고급이 상충 - 검토 권장")

    level_dist_counter[primary_band_note] += 1

    # ---- 기존 DB 레벨과의 사후 비교(추천 계산에는 미사용) ----
    db_level_label = f"L{r['db_vocab_level']}"
    if recommended.startswith("제외"):
        cmp_flag = "OUT_OF_SCOPE"
    elif recommended.startswith("보류"):
        cmp_flag = "HOLD_NO_COMPARISON"
    elif recommended.startswith("경계"):
        cmp_flag = "DB_OVERCONFIDENT_BOUNDARY" if r["db_level_status"] == "PROVISIONAL_AUTO" else "BOUNDARY_DB_ALSO_BOUNDARY"
    else:
        cmp_flag = "MATCH" if recommended == db_level_label else "CONFLICT"
    conflict_counter[cmp_flag] += 1

    # 뜻풀이 검토 대상은 "경계 판정 자체"(정상적인 1차 산출물)와는 분리한다 - 사용자 지시대로
    # 충돌(추천 vs 기존 DB의 확정 레벨이 서로 다름)·동형이의 위험·KRDict 방향성 상충에만 표시한다.
    definition_review_needed = homonym_risk or krdict_conflict or cmp_flag == "CONFLICT"

    full_rows.append({
        "content_id": r["content_id"],
        "lemma": lemma,
        "pos": pos,
        "추천_레벨": recommended,
        "근거_출처": evidence_source,
        "신뢰도": confidence,
        "경계_동형이의_위험": "; ".join(risk_notes) if risk_notes else "없음",
        "사람_판정": "",
        "뜻풀이_검토대상": "예" if definition_review_needed else "아니오",
        "krdict_매칭상태": krdict_status,
        "krdict_등급": krdict_level or "",
        "기존DB_vocab_level": r["db_vocab_level"],
        "기존DB_level_status": r["db_level_status"],
        "기존DB_boundary_flag": r["db_boundary_flag"],
        "비교_플래그": cmp_flag,
        "우선순위_207": r["content_id"] in boundary_207,
        "우선순위_L2_17": r["content_id"] in l2_17,
    })

print("\n=== KRDict 매칭률 ===")
for k, v in match_rate_counter.most_common():
    print(f"  {k}: {v}건 ({v/len(rows)*100:.1f}%)")

print("\n=== 추천 레벨 분포 ===")
for k, v in level_dist_counter.most_common():
    print(f"  {k}: {v}건")

print("\n=== 기존 DB 대비 비교 플래그 ===")
for k, v in conflict_counter.most_common():
    print(f"  {k}: {v}건")

human_review_needed = sum(1 for r in full_rows if r["뜻풀이_검토대상"] == "예")
print(f"\n사람이 봐야 할 실제 건수(위험/충돌 플래그 있는 항목): {human_review_needed}건")

priority_ids = boundary_207 | l2_17
print(f"\n우선순위 검토 집합(207∪17, 중복제거): {len(priority_ids)}건")

# ---- 산출물 1: 전체 분류 ----
with open(OUT_DIR + r"\l0l6_level_classification_full.csv", "w", encoding="utf-8-sig", newline="") as f:
    cols = ["content_id", "lemma", "pos", "추천_레벨", "근거_출처", "신뢰도",
            "경계_동형이의_위험", "사람_판정", "뜻풀이_검토대상"]
    w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for r in full_rows:
        w.writerow(r)

# ---- 산출물 2: DB 비교표 ----
with open(OUT_DIR + r"\l0l6_level_vs_db_comparison.csv", "w", encoding="utf-8-sig", newline="") as f:
    cols = ["content_id", "lemma", "pos", "추천_레벨", "기존DB_vocab_level",
            "기존DB_level_status", "기존DB_boundary_flag", "비교_플래그", "신뢰도"]
    w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for r in full_rows:
        w.writerow(r)

# ---- 산출물 3: 우선순위 검토(207∪17) ----
by_cid = {r["content_id"]: r for r in full_rows}
with open(OUT_DIR + r"\l0l6_priority_review_207plus17.csv", "w", encoding="utf-8-sig", newline="") as f:
    cols = ["content_id", "lemma", "pos", "출처집합", "추천_레벨", "근거_출처", "신뢰도",
            "경계_동형이의_위험", "기존DB_vocab_level", "기존DB_level_status", "비교_플래그",
            "뜻풀이_검토대상", "사람_판정"]
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for cid in sorted(priority_ids):
        r = by_cid.get(cid)
        if not r:
            continue
        tags = []
        if cid in boundary_207:
            tags.append("207(레벨보류)")
        if cid in l2_17:
            tags.append("17(L2 PROVISIONAL_AUTO)")
        w.writerow({
            "content_id": cid, "lemma": r["lemma"], "pos": r["pos"],
            "출처집합": "+".join(tags),
            "추천_레벨": r["추천_레벨"], "근거_출처": r["근거_출처"], "신뢰도": r["신뢰도"],
            "경계_동형이의_위험": r["경계_동형이의_위험"],
            "기존DB_vocab_level": r["기존DB_vocab_level"], "기존DB_level_status": r["기존DB_level_status"],
            "비교_플래그": r["비교_플래그"], "뜻풀이_검토대상": r["뜻풀이_검토대상"], "사람_판정": "",
        })

print("\n산출물 저장 완료.")
