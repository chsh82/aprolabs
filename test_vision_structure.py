"""
샘플 5개 PDF에 대해 vision_structure를 실행하고 결과 저장
사용: venv/bin/python3 test_vision_structure.py
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DATABASE_URL", "sqlite:///./aprolabs.db")

from app.database import SessionLocal
from app.models.passage import PipelineJob
from app.services.vision_structure import analyze_page_structure

# 5개 샘플 job_id (각 카테고리 1개씩)
# 화법과작문 / 문학 / 독서 / 언어와매체 / 합본분할 1개씩
SAMPLES = [
    # 사용자가 실행 시 직접 채워야 함
]

db = SessionLocal()
results = {}

for job_id in SAMPLES:
    job = db.get(PipelineJob, job_id)
    if not job:
        print(f"job {job_id} not found")
        continue

    page_dir = f"uploads/suneung/{job_id}/pages"
    if not os.path.isdir(page_dir):
        print(f"page images not found for {job_id}")
        continue

    print(f"\n=== {job_id} ({job.filename}) ===")
    page_results = []
    for i, fname in enumerate(sorted(os.listdir(page_dir))):
        if not fname.endswith(('.png', '.jpg')):
            continue
        try:
            result = analyze_page_structure(os.path.join(page_dir, fname), i + 1)
            page_results.append(result)
            print(f"  page {i+1}: passages={len(result.get('passages', []))}, "
                  f"questions={len(result.get('questions', []))}, "
                  f"visuals={len(result.get('visuals', []))}")
        except Exception as e:
            print(f"  page {i+1}: ERROR {e}")

    results[job_id] = page_results

# 결과 저장
with open("test_vision_results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

# 요약 통계
print("\n=== 요약 ===")
for jid, pages in results.items():
    total_q = sum(len(p.get("questions", [])) for p in pages)
    total_v = sum(len(p.get("visuals", [])) for p in pages)
    total_p = sum(len(p.get("passages", [])) for p in pages)
    print(f"{jid}: pages={len(pages)}, passages={total_p}, "
          f"questions={total_q}, visuals={total_v}")

db.close()
