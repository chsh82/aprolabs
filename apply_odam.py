"""
apply_odam.py — AI '오탐' 판정을 사람 판정(warning_reviews)에 자동 반영

동작:
  - ai_reviews[key]["judgment"] == "오탐"인 경고를
    warning_reviews[key] = {"judgment": "odam", "source": "auto_from_ai"}로 저장
  - 이미 사람이 판정한 경우(기존 문자열 값) 덮어쓰지 않음 (--force 없으면)

사용법:
  python3 apply_odam.py [--dry-run] [--force]
"""
import sys, os, json, sqlite3, argparse
from collections import Counter

DB = os.path.join(os.path.dirname(__file__), "aprolabs.db")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="저장 없이 목록만 출력")
    ap.add_argument("--force",   action="store_true", help="이미 사람 판정된 항목도 덮어쓰기")
    args = ap.parse_args()

    conn = sqlite3.connect(DB)
    cur  = conn.cursor()
    cur.execute(
        "SELECT id, filename, raw_result FROM pipeline_jobs WHERE raw_result IS NOT NULL"
    )
    rows = cur.fetchall()

    applied = []
    skipped_human = []
    skipped_no_ai = []

    for job_id, fname, raw_str in rows:
        raw = json.loads(raw_str)
        if not isinstance(raw, dict):
            continue

        ai_reviews   = raw.get("ai_reviews",   {})
        warn_reviews = raw.get("warning_reviews", {})
        changed      = False

        for key, ai_val in ai_reviews.items():
            if not isinstance(ai_val, dict):
                continue
            if ai_val.get("judgment") != "오탐":
                skipped_no_ai.append(key)
                continue

            existing = warn_reviews.get(key)
            # 기존 값 추출 (string 또는 dict)
            if isinstance(existing, dict):
                existing_judgment = existing.get("judgment", "")
            else:
                existing_judgment = existing or ""

            if existing_judgment and not args.force:
                skipped_human.append((fname, key, existing_judgment))
                continue

            loc, msg_prefix = key.split("|||", 1) if "|||" in key else (key, "")
            applied.append({
                "fname": fname,
                "loc":   loc,
                "msg":   msg_prefix,
                "key":   key,
                "job_id": job_id,
            })

            if not args.dry_run:
                warn_reviews[key] = {"judgment": "오탐", "source": "auto_from_ai"}
                changed = True

        if changed and not args.dry_run:
            raw["warning_reviews"] = warn_reviews
            cur.execute(
                "UPDATE pipeline_jobs SET raw_result = ? WHERE id = ?",
                (json.dumps(raw, ensure_ascii=False), job_id)
            )

    if not args.dry_run:
        conn.commit()
    conn.close()

    # ── 카테고리 분류 (간이) ──
    import re
    def cat(msg):
        if "PDF 밑줄 텍스트" in msg: return "밑줄못찾음"
        if re.search(r"\[[A-E]\]", msg): return "bracket텍스트불일치"
        if "선택지" in msg and "불일치" in msg: return "텍스트불일치"
        if "텍스트 불일치" in msg: return "텍스트불일치"
        if "지문을 PDF" in msg or "대응하는 PDF 지문" in msg: return "지문못찾음"
        if "문항을 PDF" in msg or "PDF에서 해당 문항" in msg: return "문항못찾음"
        return "기타"

    print(f"\n{'[dry-run] ' if args.dry_run else ''}오탐 자동 반영: {len(applied)}건\n")
    print("-" * 65)
    print(f"{'파일':<38} {'위치':<8} {'카테고리'}")
    print("-" * 65)
    cat_ctr = Counter()
    for w in applied:
        c = cat(w["msg"])
        cat_ctr[c] += 1
        print(f"  {w['fname'][-35:]:<35} {w['loc']:<8} {c}")

    print("-" * 65)
    print(f"\n카테고리별:")
    for c, n in sorted(cat_ctr.items()):
        print(f"  {n:2d}건 | {c}")

    if skipped_human:
        print(f"\n사람 판정 보존(스킵): {len(skipped_human)}건")
        for fname, key, j in skipped_human:
            loc = key.split("|||")[0]
            print(f"  {fname[-35:]} | {loc} | 기존={j}")

    if args.dry_run:
        print("\n[dry-run: DB 저장 안 함]")
    else:
        print(f"\n✅ DB 저장 완료 — {len(applied)}건 반영")


if __name__ == "__main__":
    main()
