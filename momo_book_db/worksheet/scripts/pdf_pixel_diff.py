# -*- coding: utf-8 -*-
"""pdf_pixel_diff.py — 수정 전/후 PDF를 같은 렌더 환경(PyMuPDF, 같은 DPI)에서 페이지별로
래스터화해 실제 픽셀 차이를 계산한다. "HTML 문자열이 같다"/"육안으로 봤다"가 아니라
숫자로 된 비교 결과를 남기기 위한 용도(2026-09-19 13차 - 사용자 지시: "전체 육안 확인"이나
"HTML 동일"만으로 대체하지 말 것).

실행: python pdf_pixel_diff.py <before.pdf> <after.pdf> <출력 보고서 json 경로> [DPI(기본 150)]

결과: 페이지별로 {page, before_size, after_size, size_mismatch, diff_pixel_count,
total_pixels, diff_ratio, max_channel_diff}를 JSON으로 남기고, 콘솔에도 요약을 찍는다.
diff_ratio==0.0이고 max_channel_diff==0이면 "완전히 동일한 래스터"를 의미한다(안티앨리어싱
등으로 인한 1픽셀 미만 오차까지 전부 0이어야 함 - 임의 허용치를 두지 않는다).
"""
import sys
import json
import fitz
import numpy as np
from PIL import Image
import io


def render_page(doc, page_index, dpi):
    page = doc[page_index]
    zoom = dpi / 72.0
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
    return np.array(img)


def main():
    before_path, after_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    dpi = int(sys.argv[4]) if len(sys.argv) > 4 else 150

    doc_b = fitz.open(before_path)
    doc_a = fitz.open(after_path)

    n_b, n_a = len(doc_b), len(doc_a)
    n = max(n_b, n_a)

    report = {
        "before_pdf": before_path, "after_pdf": after_path, "dpi": dpi,
        "before_page_count": n_b, "after_page_count": n_a,
        "page_count_mismatch": n_b != n_a,
        "pages": [],
    }

    for i in range(n):
        entry = {"page": i + 1}
        if i >= n_b or i >= n_a:
            entry["error"] = "페이지 수가 달라 비교 불가(한쪽에만 존재)"
            report["pages"].append(entry)
            continue
        arr_b = render_page(doc_b, i, dpi)
        arr_a = render_page(doc_a, i, dpi)
        entry["before_size"] = list(arr_b.shape[:2])
        entry["after_size"] = list(arr_a.shape[:2])
        if arr_b.shape != arr_a.shape:
            entry["size_mismatch"] = True
            entry["identical"] = False
            report["pages"].append(entry)
            continue
        entry["size_mismatch"] = False
        diff = np.abs(arr_a.astype(np.int16) - arr_b.astype(np.int16))
        # 채널 중 하나라도 다르면 그 픽셀은 "다른 픽셀"로 센다(엄격 기준 - 임의 허용치 없음).
        diff_pixel_mask = np.any(diff > 0, axis=2)
        diff_pixel_count = int(diff_pixel_mask.sum())
        total_pixels = int(arr_b.shape[0] * arr_b.shape[1])
        entry["diff_pixel_count"] = diff_pixel_count
        entry["total_pixels"] = total_pixels
        entry["diff_ratio"] = diff_pixel_count / total_pixels if total_pixels else 0.0
        entry["max_channel_diff"] = int(diff.max())
        entry["identical"] = diff_pixel_count == 0
        report["pages"].append(entry)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"=== PDF 픽셀 비교 완료 (DPI={dpi}) ===")
    print(f"before: {n_b}페이지, after: {n_a}페이지")
    for e in report["pages"]:
        if "error" in e:
            print(f"  {e['page']}쪽: ERROR - {e['error']}")
        elif e.get("size_mismatch"):
            print(f"  {e['page']}쪽: 래스터 크기 불일치 before={e['before_size']} after={e['after_size']}")
        else:
            mark = "동일" if e["identical"] else f"차이 있음(diff_pixel={e['diff_pixel_count']}/{e['total_pixels']}, ratio={e['diff_ratio']:.6f}, max_channel_diff={e['max_channel_diff']})"
            print(f"  {e['page']}쪽: {mark}")


if __name__ == "__main__":
    main()
