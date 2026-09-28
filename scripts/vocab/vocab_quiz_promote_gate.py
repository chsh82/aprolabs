# -*- coding: utf-8 -*-
"""1순위(파일럿 연결) 40콘텐츠 승격 dry-run 게이트.

세 가지 모드로 나뉜다(모두 읽기 전용 - 아무 DB도 쓰지 않는다):

1. `--export-aprolabs OUT.json`
   aprolabs 서버에서 실행. 1순위 40개 content_id 중 "유효한 판정"만
   (최신 판정 verdict == APPROVED_CANDIDATE, 그리고 그 판정 시점 저장된
   content_hash/item_hash가 현재 DB 값과 완전히 일치 = stale 아님) 골라
   콘텐츠·문항 필드값과 해시를 고정한다. 유효하지 않은 항목은
   `blocked`에 사유와 함께 담는다 - 자동으로 무시하거나 스킵하지 않는다.

2. `--export-momolib OUT.json --ids IDS.json`
   momolib 서버에서 실행. IDS.json(1번 export가 같이 만든 content_id/
   item_id 목록)에 있는 항목의 현재 필드값만 조회한다.

3. `--compare APROLABS.json MOMOLIB.json`
   아무 DB에도 연결하지 않는다(순수 로컬 비교) - 1·2번 export 파일
   두 개만 갖고 필드 단위로 대조해 게이트 결과를 출력한다.

이 스크립트는 momolib의 vocab_quiz_admin_reviews 테이블에 아무것도
쓰지 않고, momolib의 공개검토(publish-review) 블루프린트를 다시 열지도
않는다 - 완전히 별도의 오프라인 감사 도구다. 1건이라도 `blocked`(유효
판정 아님)이거나 필드 불일치가 있으면 전체 게이트는 PASS를 내지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

TIER1_VERSIONS = ("schema_reading_l4l5_pilot_dryrun_v1", "schema_reading_l6_pilot_dryrun_v1")
CONTENT_HASH_FIELDS = ("lemma", "pos", "canonical_definition", "student_definition",
                       "example_sentence", "example_target_form")
ITEM_HASH_FIELDS = ("prompt", "options_json", "correct_option", "explanation")
CONTENT_FIELD_MAP = {f: f for f in CONTENT_HASH_FIELDS}
ITEM_FIELD_MAP = {f: f for f in ITEM_HASH_FIELDS}
EXPECTED_TIER1_COUNT = 40


def hash_row(values: dict) -> str:
    blob = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def norm(v):
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    return v


def norm_options(v):
    if v is None:
        return None
    try:
        return json.dumps(json.loads(v), ensure_ascii=False)
    except (TypeError, ValueError):
        return norm(v)


# ---------------------------------------------------------------- export: aprolabs

def export_aprolabs(out_path: str) -> None:
    import sqlite3

    db_path = os.path.expanduser("~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db")
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    placeholders = ",".join("?" for _ in TIER1_VERSIONS)
    cur.execute(
        f"SELECT DISTINCT source_content_id FROM vocabulary_multiformat_items "
        f"WHERE source_version IN ({placeholders}) AND source_content_id IS NOT NULL",
        TIER1_VERSIONS,
    )
    tier1_ids = sorted(r[0] for r in cur.fetchall())

    valid_rows = []
    blocked = []

    for cid in tier1_ids:
        cur.execute("SELECT * FROM vocabulary_contents WHERE content_id=?", (cid,))
        content = cur.fetchone()
        if content is None:
            blocked.append({"content_id": cid, "reason": "콘텐츠 없음"})
            continue
        content_fields = {f: content[f] for f in CONTENT_HASH_FIELDS}
        content_hash = hash_row(content_fields)

        cur.execute(
            f"SELECT * FROM vocabulary_multiformat_items WHERE source_content_id=? "
            f"AND source_version IN ({placeholders}) ORDER BY item_id",
            (cid, *TIER1_VERSIONS),
        )
        items = cur.fetchall()
        item_records = []
        for it in items:
            item_fields = {f: it[f] for f in ITEM_HASH_FIELDS}
            item_records.append({
                "item_id": it["item_id"],
                "fields": item_fields,
                "item_hash": hash_row(item_fields),
            })
        item_pairs = sorted([[r["item_id"], r["item_hash"]] for r in item_records])

        cur.execute(
            "SELECT * FROM vocabulary_publish_reviews WHERE content_id=? "
            "ORDER BY reviewed_at DESC, id DESC LIMIT 1",
            (cid,),
        )
        review = cur.fetchone()
        if review is None:
            blocked.append({"content_id": cid, "lemma": content["lemma"], "reason": "판정 없음"})
            continue
        if review["verdict"] != "APPROVED_CANDIDATE":
            blocked.append({"content_id": cid, "lemma": content["lemma"],
                             "reason": f"판정이 승인후보 아님({review['verdict']})",
                             "reviewer_email": review["reviewer_email"], "reviewed_at": review["reviewed_at"]})
            continue
        stale = review["content_hash_at_review"] != content_hash
        if not stale:
            try:
                stored_pairs = json.loads(review["item_hashes_at_review_json"])
            except (TypeError, ValueError):
                stale = True
            else:
                stale = stored_pairs != item_pairs
        if stale:
            blocked.append({"content_id": cid, "lemma": content["lemma"],
                             "reason": "판정 이후 콘텐츠/문항 변경(stale)"})
            continue

        valid_rows.append({
            "content_id": cid, "lemma": content["lemma"],
            "content_fields": content_fields, "content_hash": content_hash,
            "items": item_records,
            "reviewer_email": review["reviewer_email"], "reviewed_at": review["reviewed_at"],
        })

    out = {
        "tier1_total": len(tier1_ids),
        "expected_tier1_count": EXPECTED_TIER1_COUNT,
        "valid_count": len(valid_rows),
        "blocked": blocked,
        "valid_rows": valid_rows,
        "all_content_ids": tier1_ids,
        "all_item_ids": sorted(it["item_id"] for r in valid_rows for it in r["items"]),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[aprolabs export] tier1={len(tier1_ids)} valid={len(valid_rows)} blocked={len(blocked)} -> {out_path}")
    if blocked:
        for b in blocked:
            print(f"  BLOCKED {b['content_id']}: {b['reason']}")


# ---------------------------------------------------------------- export: momolib

def export_momolib(out_path: str, ids_path: str) -> None:
    sys.path.insert(0, os.path.expanduser("~/momolib"))
    os.environ.setdefault("FLASK_ENV", "production")

    with open(ids_path, encoding="utf-8") as f:
        ids = json.load(f)
    content_ids = ids["content_ids"]
    item_ids = ids["item_ids"]

    from app import create_app
    from app.models.vocab_quiz import VocabQuizContent, VocabQuizContentLevel, VocabQuizPilotItem

    app = create_app(os.environ.get("FLASK_ENV", "production"))
    with app.app_context():
        out = {"contents": {}, "items": {}, "missing_contents": [], "missing_items": []}
        for cid in content_ids:
            c = VocabQuizContent.query.filter_by(content_id=cid).first()
            if c is None:
                out["missing_contents"].append(cid)
                continue
            levels = VocabQuizContentLevel.query.filter_by(content_id=cid).all()
            out["contents"][cid] = {
                **{f: getattr(c, f) for f in CONTENT_HASH_FIELDS},
                "student_exposure": c.student_exposure, "public_ready": c.public_ready,
                "levels": [{"vocab_level": lv.vocab_level, "level_status": lv.level_status,
                            "boundary_flag": lv.boundary_flag} for lv in levels],
            }
        for iid in item_ids:
            it = VocabQuizPilotItem.query.filter_by(item_id=iid).first()
            if it is None:
                out["missing_items"].append(iid)
                continue
            out["items"][iid] = {f: getattr(it, f) for f in ITEM_HASH_FIELDS}

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[momolib export] contents={len(out['contents'])} items={len(out['items'])} -> {out_path}")


# ---------------------------------------------------------------- compare

def compare(aprolabs_path: str, momolib_path: str) -> bool:
    with open(aprolabs_path, encoding="utf-8") as f:
        ap = json.load(f)
    with open(momolib_path, encoding="utf-8") as f:
        mo = json.load(f)

    print(f"aprolabs tier1 전체: {ap['tier1_total']}건, 유효 판정: {ap['valid_count']}건, "
          f"차단(재검토/미판정/stale): {len(ap['blocked'])}건")
    for b in ap["blocked"]:
        print(f"  BLOCKED {b['content_id']}({b.get('lemma', '')}): {b['reason']}")

    content_diffs = []
    item_diffs = []
    missing_content = []
    missing_item = []

    for row in ap["valid_rows"]:
        cid = row["content_id"]
        mo_c = mo["contents"].get(cid)
        if mo_c is None:
            missing_content.append(cid)
            continue
        diffs = {}
        for k in CONTENT_FIELD_MAP:
            if norm(row["content_fields"].get(k)) != norm(mo_c.get(k)):
                diffs[k] = {"aprolabs": norm(row["content_fields"].get(k)), "momolib": norm(mo_c.get(k))}
        if diffs:
            content_diffs.append({"content_id": cid, "diffs": diffs})

        for it in row["items"]:
            iid = it["item_id"]
            mo_it = mo["items"].get(iid)
            if mo_it is None:
                missing_item.append(iid)
                continue
            idiffs = {}
            for k in ITEM_FIELD_MAP:
                if k == "options_json":
                    a, m = norm_options(it["fields"].get(k)), norm_options(mo_it.get(k))
                else:
                    a, m = norm(it["fields"].get(k)), norm(mo_it.get(k))
                if a != m:
                    idiffs[k] = {"aprolabs": a, "momolib": m}
            if idiffs:
                item_diffs.append({"item_id": iid, "content_id": cid, "diffs": idiffs})

    print(f"\nmomolib에 없는 콘텐츠: {missing_content}")
    print(f"momolib에 없는 문항: {missing_item}")
    print(f"필드 불일치 콘텐츠: {len(content_diffs)}건")
    for d in content_diffs:
        print(f"  {d['content_id']}: {list(d['diffs'].keys())}")
    print(f"필드 불일치 문항: {len(item_diffs)}건")
    for d in item_diffs:
        print(f"  {d['item_id']}({d['content_id']}): {list(d['diffs'].keys())}")

    gate_pass = (
        ap["valid_count"] == ap["expected_tier1_count"]
        and not ap["blocked"]
        and not missing_content and not missing_item
        and not content_diffs and not item_diffs
    )

    print(f"\n=== 승격 dry-run 게이트: {'PASS' if gate_pass else 'FAIL'} "
          f"({ap['valid_count']}/{ap['expected_tier1_count']} 유효, 불일치 {len(content_diffs) + len(item_diffs)}건) ===")
    return gate_pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--export-aprolabs")
    p.add_argument("--export-momolib")
    p.add_argument("--ids")
    p.add_argument("--compare", nargs=2, metavar=("APROLABS_JSON", "MOMOLIB_JSON"))
    args = p.parse_args()

    if args.export_aprolabs:
        export_aprolabs(args.export_aprolabs)
    elif args.export_momolib:
        if not args.ids:
            p.error("--export-momolib에는 --ids가 필요합니다")
        export_momolib(args.export_momolib, args.ids)
    elif args.compare:
        ok = compare(*args.compare)
        sys.exit(0 if ok else 1)
    else:
        p.error("--export-aprolabs, --export-momolib, --compare 중 하나가 필요합니다")


if __name__ == "__main__":
    main()
