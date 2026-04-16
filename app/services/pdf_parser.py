"""
Phase 1: PDF → 페이지 이미지 변환
PyMuPDF(fitz) 사용 — poppler 불필요
"""
import os
import fitz  # pymupdf


def pdf_to_images(pdf_path: str, output_dir: str, dpi: int = 150) -> list[str]:
    """PDF 각 페이지를 PNG 이미지로 변환, 경로 리스트 반환"""
    os.makedirs(output_dir, exist_ok=True)
    doc = fitz.open(pdf_path)
    image_paths = []

    mat = fitz.Matrix(dpi / 72, dpi / 72)
    for i, page in enumerate(doc):
        pix = page.get_pixmap(matrix=mat)
        img_path = os.path.join(output_dir, f"page_{i+1:03d}.png")
        if os.path.exists(img_path):
            os.remove(img_path)
        pix.save(img_path)
        image_paths.append(img_path)

    doc.close()
    return image_paths
