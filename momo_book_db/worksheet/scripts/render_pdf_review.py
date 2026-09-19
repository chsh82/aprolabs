# -*- coding: utf-8 -*-
"""PDF를 PyMuPDF로 실제 래스터화해서 PNG로 남긴다 - check.js의 답란 괘선 검사가 쓰는
것과 같은 PyMuPDF 경로라 "PDF 생성은 됐지만 실제로 어떻게 찍히는지는 안 봤다"는 간극을
메운다. poppler(pdftoppm)가 이 환경에 없어 Read 도구로 PDF 페이지를 직접 못 열길래
대신 이 스크립트로 PNG를 만들어 Read 도구로 본다.

사용법: python render_pdf_review.py <pdf경로> <출력폴더> [문제페이지 콤마목록(1-based)]
출력:
  <출력폴더>/contact_sheet.png   - 전체 페이지 축소 모음(그리드)
  <출력폴더>/page_XX.png         - 문제 페이지로 지정된 것만 확대(실제 크기, 150dpi)
  <출력폴더>/render_log.json     - 페이지 수·크기·실패 여부 기록(육안 검수와 PNG 생성 성공 여부를 구분)
"""
import sys, os, json, io
import fitz
from PIL import Image

def main():
    pdf_path = sys.argv[1]
    out_dir = sys.argv[2]
    zoom_pages = set()
    if len(sys.argv) > 3 and sys.argv[3]:
        zoom_pages = set(int(x) for x in sys.argv[3].split(','))

    os.makedirs(out_dir, exist_ok=True)
    log = {"pdf_path": pdf_path, "pages": [], "contact_sheet": None, "zoomed": [], "errors": []}

    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        log["errors"].append(f"PDF 열기 실패: {e}")
        json.dump(log, open(os.path.join(out_dir, "render_log.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print("PNG_GENERATION: FAILED -", e)
        return 1

    THUMB_ZOOM = 0.35   # 축소 모음용
    FULL_ZOOM = 150 / 72  # 150dpi 확대본

    thumbs = []
    for i, page in enumerate(doc):
        try:
            pix = page.get_pixmap(matrix=fitz.Matrix(THUMB_ZOOM, THUMB_ZOOM))
            im = Image.open(io.BytesIO(pix.tobytes("png")))
            thumbs.append((i, im))
            log["pages"].append({"index": i, "width": pix.width, "height": pix.height, "ok": True})
        except Exception as e:
            log["pages"].append({"index": i, "ok": False, "error": str(e)})
            log["errors"].append(f"page {i} 렌더 실패: {e}")

    # 축소 모음(contact sheet) - 4열 그리드로 직접 합성(PIL - fitz.Pixmap.copy는 이
    # 용도로 안 맞아서(전체 1페이지만 나오는 버그 발견) PIL로 교체함).
    if thumbs:
        cols = 4
        rows = (len(thumbs) + cols - 1) // cols
        cw = max(im.width for _, im in thumbs)
        ch = max(im.height for _, im in thumbs)
        pad = 8
        sheet = Image.new("RGB", (cols * (cw + pad) + pad, rows * (ch + pad) + pad), (0xF0, 0xF0, 0xF0))
        for i, im in thumbs:
            r, c = divmod(i, cols)
            x0, y0 = pad + c * (cw + pad), pad + r * (ch + pad)
            sheet.paste(im, (x0, y0))
        sheet_path = os.path.join(out_dir, "contact_sheet.png")
        sheet.save(sheet_path)
        log["contact_sheet"] = sheet_path

    for idx1 in sorted(zoom_pages):
        i = idx1 - 1
        if i < 0 or i >= len(doc):
            log["errors"].append(f"확대 요청 페이지 {idx1}가 범위 밖(전체 {len(doc)}페이지)")
            continue
        try:
            pix = doc[i].get_pixmap(matrix=fitz.Matrix(FULL_ZOOM, FULL_ZOOM))
            p = os.path.join(out_dir, f"page_{idx1:02d}.png")
            pix.save(p)
            log["zoomed"].append({"page": idx1, "path": p, "width": pix.width, "height": pix.height})
        except Exception as e:
            log["errors"].append(f"확대 페이지 {idx1} 렌더 실패: {e}")

    json.dump(log, open(os.path.join(out_dir, "render_log.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    status = "OK" if not log["errors"] else "PARTIAL"
    print(f"PNG_GENERATION: {status} - pages={len(doc)} zoomed={len(log['zoomed'])} errors={len(log['errors'])}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
