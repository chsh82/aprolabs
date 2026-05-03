"""
bbox 실험: 1개 샘플 × 3페이지만 처리, 시각화 포함.
사용: venv/bin/python3 test_vision_bbox.py
"""
import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DATABASE_URL", "sqlite:///./aprolabs.db")

from app.services.vision_structure_bbox import analyze_page_with_bbox, visualize_bboxes

# 2025 10월 학력평가 언매 (이전 테스트에서 가장 정확했던 샘플)
SAMPLE_ID = "09742ab3-3905-4ade-b768-f8b2fbd790d7"

page_dir = f"uploads/suneung/{SAMPLE_ID}/pages"
output_dir = "bbox_test_output"
os.makedirs(output_dir, exist_ok=True)

results = []
all_files = sorted(f for f in os.listdir(page_dir) if f.endswith(('.png', '.jpg')))
pages = all_files[:3]  # 첫 3페이지만

for fname in pages:
    page_path = os.path.join(page_dir, fname)
    print(f"\n분석 중: {fname}")
    try:
        result = analyze_page_with_bbox(page_path, len(results) + 1)
        results.append(result)

        # 시각화
        viz_path = os.path.join(output_dir, f"viz_{fname}")
        visualize_bboxes(page_path, result, viz_path)

        # 요약 출력
        elements = result.get("elements", [])
        by_type: dict[str, int] = {}
        for e in elements:
            t = e.get("type", "unknown")
            by_type[t] = by_type.get(t, 0) + 1
        print(f"  감지: {by_type}")
        print(f"  시각화 저장: {viz_path}")
    except Exception as e:
        print(f"  ERROR: {e}")

# JSON 결과 저장
results_path = os.path.join(output_dir, "results.json")
with open(results_path, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"\n완료. 결과: {output_dir}/")
