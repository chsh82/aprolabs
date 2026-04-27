"""현재 DB의 segments를 스냅샷으로 저장 → tests/segmenter_baseline.json"""
import sys
import os
import json
import subprocess
import io
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database import SessionLocal

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "segmenter_baseline.json")

# 합본(국어()) + 정답해설 제외
_EXCLUDE = ["국어()", "정답", "해설", "_answer"]


def _excluded(filename: str) -> bool:
    return any(p in filename for p in _EXCLUDE)


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ).strip()
    except Exception:
        return "unknown"


def main():
    db = SessionLocal()
    try:
        rows = db.execute(text(
            "SELECT id, filename, file_path, segments FROM pipeline_jobs "
            "WHERE segments IS NOT NULL ORDER BY filename"
        )).fetchall()
    finally:
        db.close()

    # dict-like 접근을 위해 변환
    jobs = [{"id": r[0], "filename": r[1], "file_path": r[2], "segments": r[3]} for r in rows]

    files = {}
    skipped = []

    for job in jobs:
        if _excluded(job["filename"]):
            skipped.append(job["filename"])
            continue

        segments = job["segments"]
        if isinstance(segments, str):
            segments = json.loads(segments)

        questions = segments.get("questions", [])
        q_snapshots = []
        for q in sorted(questions, key=lambda x: x.get("number") or 0):
            stem = (q.get("stem") or "").strip()
            q_snapshots.append({
                "number": q.get("number"),
                "stem_length": len(stem),
                "stem_preview": stem[:30],
                "has_bogi": bool(q.get("bogi")),
                "choices_count": len(q.get("choices") or {}),
                "content_length": len((q.get("content") or "").strip()),
            })

        files[job["filename"]] = {
            "job_id": job["id"],
            "file_path": job["file_path"],
            "question_count": len(questions),
            "questions": q_snapshots,
        }

    baseline = {
        "created_at": datetime.now().isoformat(),
        "commit": _git_commit(),
        "total_files": len(files),
        "files": files,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(baseline, f, ensure_ascii=False, indent=2)

    total_q = sum(v["question_count"] for v in files.values())
    print(f"[OK] {len(files)}개 파일, {total_q}개 문항 → {OUTPUT_PATH}")
    if skipped:
        print(f"[SKIP] {len(skipped)}개 제외: {', '.join(skipped)}")


if __name__ == "__main__":
    main()
