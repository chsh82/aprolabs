"""
텍스트 앵커 방식 실험: 1개 샘플 × 3페이지, 시각화 포함.
사용: venv/bin/python3 test_vision_anchor.py
"""
import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DATABASE_URL", "sqlite:///./aprolabs.db")

from app.database import SessionLocal
from app.models.passage import PipelineJob
from app.services.vision_anchor import extract_structure_with_anchors, visualize_anchors

SAMPLE_ID = "09742ab3-3905-4ade-b768-f8b2fbd790d7"

db = SessionLocal()
job = db.get(PipelineJob, SAMPLE_ID)
if not job:
    print(f"job {SAMPLE_ID} not found")
    db.close()
    sys.exit(1)
pdf_path = job.file_path
db.close()

page_dir = f"uploads/suneung/{SAMPLE_ID}/pages"
output_dir = "anchor_test_output"
os.makedirs(output_dir, exist_ok=True)

results = []
pages = sorted(f for f in os.listdir(page_dir) if f.endswith(('.png', '.jpg')))[:3]

for i, fname in enumerate(pages):
    page_path = os.path.join(page_dir, fname)
    print(f"\n분석 중: {fname}")
    try:
        result = extract_structure_with_anchors(pdf_path, page_path, i + 1)
        results.append(result)

        elements = result.get("elements", [])
        success = sum(1 for e in elements if e.get("anchor_found"))
        rate = success / max(len(elements), 1) * 100
        print(f"  요소 {len(elements)}개, 앵커 매칭 {success}개 ({rate:.0f}%)")

        viz_path = os.path.join(output_dir, f"viz_{fname}")
        visualize_anchors(page_path, pdf_path, i + 1, result, viz_path)
        print(f"  시각화: {viz_path}")
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()

with open(os.path.join(output_dir, "results.json"), "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"\n완료: {output_dir}/")
