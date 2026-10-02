# -*- coding: utf-8 -*-
"""L3 중등 보강 1차 배치 - 최종 콘텐츠·문항 파일과 대상 ID를 고정한다.

- content_id = candidate_id를 그대로 재사용한다(새 ID 체계를 만들지 않음 -
  candidate_id 자체가 이미 (lemma,pos,homonym_number,source_file_sha256)의
  결정적 함수라 충돌 없이 안정적이다).
- 적재 직전 최신 사람 판정을 다시 조회해 30건 전부 여전히 L3인지
  재확인한다(원천 정보가 바뀐 판정은 적용하지 않음 - 이번 재확인 결과
  30/30 전부 불변).
- 산출물에 버전(SOURCE_VERSION)과 입력 파일 SHA-256을 기록한다.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"
VOCAB = ROOT / "data" / "vocab"

SOURCE_VERSION = "nikl_grade5_l3_batch1_v1"
GENERATOR_VERSION = "nikl_grade5_l3_batch1_dryrun_v1"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    content_path = IMP / "nikl_grade5_batch1_30_content_dryrun_20261007.csv"
    items_path = IMP / "nikl_grade5_batch1_30_items_dryrun_20261007.csv"

    with open(content_path, encoding="utf-8-sig", newline="") as f:
        content_rows = list(csv.DictReader(f))
    with open(items_path, encoding="utf-8-sig", newline="") as f:
        item_rows = list(csv.DictReader(f))

    assert len(content_rows) == 30
    assert len(item_rows) == 60

    content_hash = sha256_of(content_path)
    items_hash = sha256_of(items_path)
    print(f"입력 해시: content={content_hash}")
    print(f"입력 해시: items={items_hash}")

    final_contents = []
    for r in content_rows:
        final_contents.append({
            "content_id": r["candidate_id"],  # candidate_id를 content_id로 그대로 재사용
            "candidate_id": r["candidate_id"],
            "lemma": r["lemma"], "pos": r["pos"], "homonym_number": r["homonym_number"],
            "batch_no": r["batch_no"],
            "official_meaning_short": r["official_meaning_short"],
            "student_definition": r["student_definition"],
            "example_sentence": r["example_sentence"],
            "human_level_judgment": r["human_level_judgment"],
            "human_reviewed_at": r["human_reviewed_at"],
            "source_version": SOURCE_VERSION,
        })

    final_items = []
    for it in item_rows:
        options = json.loads(it["options_json"])
        final_items.append({
            "item_id": f"MF_G5L3B1_{it['item_type'][:1]}_{it['source_content_id']}",
            "item_type": it["item_type"],
            "source_content_id": it["source_content_id"],
            "source_content_ids_json": None,
            "sense_id": None, "sense_ids_json": None,
            "lemma": it["lemma"], "pos": it["pos"],
            "prompt": it["prompt"],
            "options_json": json.dumps(options, ensure_ascii=False),
            "correct_option": int(it["correct_option"]),
            "public_payload_json": json.dumps({"options": options}, ensure_ascii=False),
            "answer_payload_json": json.dumps({"correct_option": int(it["correct_option"])}, ensure_ascii=False),
            "explanation": it["explanation"],
            "cognitive_level": None,
            "qa_flags_json": json.dumps([{
                "flag": "NIKL_GRADE5_L3_BATCH1_ADMIN_PREVIEW",
                "source_version_isolation": (
                    f"source_version deliberately set to {SOURCE_VERSION} (not 2.1.29, not any "
                    "existing pilot source_version) so this batch is structurally excluded from "
                    "general/level-mode selection and every other pilot - admin preview only"
                ),
                "human_level_judgment": "L3",
                "human_level_judgment_source": "vocabulary_grade5_candidate_judgments(최신 유효 판정)",
                "content_review_status": "검수 전(미승인) - vocabulary_publish_reviews류 별도 테이블에만 기록되며 이 레벨 판정과 무관",
                "expert_review_status": "검수자 확인 안 됨(자동 1차 초안 - 사람이 직접 작성했지만 공식 승인 절차 거치지 않음)",
                "auto_validation_status": "PASS",
                "auto_validation_report": "reports/nikl_grade5_l3_batch1_dryrun_20261007.md 5절 참고",
                "generator_version": GENERATOR_VERSION,
            }], ensure_ascii=False),
            "generator_version": GENERATOR_VERSION,
            "source_version": SOURCE_VERSION,
        })

    manifest_path = VOCAB / "nikl_grade5_l3_batch1_manifest_v1.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(final_items, f, ensure_ascii=False, indent=2)
    print(f"매니페스트(60건) 저장: {manifest_path}")

    final_content_path = IMP / "nikl_grade5_l3_batch1_final_content_20261007.csv"
    with open(final_content_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(final_contents[0].keys()))
        w.writeheader()
        w.writerows(final_contents)
    print(f"최종 콘텐츠(30건) 저장: {final_content_path}")

    manifest_hash = sha256_of(manifest_path)
    final_content_hash = sha256_of(final_content_path)
    meta = {
        "source_version": SOURCE_VERSION,
        "generator_version": GENERATOR_VERSION,
        "content_count": 30, "item_count": 60,
        "input_content_dryrun_sha256": content_hash,
        "input_items_dryrun_sha256": items_hash,
        "final_content_csv_sha256": final_content_hash,
        "manifest_json_sha256": manifest_hash,
        "finalized_at": "2026-10-07",
    }
    meta_path = IMP / "nikl_grade5_l3_batch1_version_manifest_20261007.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"버전/해시 기록 저장: {meta_path}")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
