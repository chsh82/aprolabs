"""
합본 PDF 분할 서비스.
고3 수능/모평 화작+언매 합본 → 화작 / 언매 두 파일로 분리.
"""
import os
import re
import uuid

import fitz  # PyMuPDF


def is_combined_exam(pdf_path: str) -> bool:
    """
    PDF에 '화법과 작문'과 '언어와 매체' 양쪽이 모두 포함되면 합본으로 판정.
    12페이지 미만은 단일 시험으로 간주.
    """
    try:
        doc = fitz.open(pdf_path)
        if len(doc) < 12:
            doc.close()
            return False
        text_all = "\n".join(page.get_text() for page in doc)
        doc.close()
        has_hwajak = "화법과 작문" in text_all
        has_eonmae = "언어와 매체" in text_all
        return has_hwajak and has_eonmae
    except Exception:
        return False


def find_split_point(doc: fitz.Document) -> int:
    """
    언매 섹션이 시작되는 페이지 인덱스(0-based) 반환.

    탐색 순서:
    1. 각 페이지 상단 10줄에서 '언어와 매체' 키워드 (3페이지 이후)
    2. '1.' 문항번호가 두 번째로 나타나는 페이지
    3. 전체 페이지 수 // 2 (폴백)
    """
    n = len(doc)
    _Q1_RE = re.compile(r"^\s*1\s*[\.．]")

    # 방법 1: 언어와 매체 섹션 헤더
    for pnum in range(3, n):
        top_text = "\n".join(doc[pnum].get_text().split("\n")[:10])
        if "언어와 매체" in top_text:
            return pnum

    # 방법 2: '1.' 문항번호 두 번째 출현
    first_q1 = -1
    for pnum in range(n):
        lines = doc[pnum].get_text().split("\n")
        if any(_Q1_RE.match(l) for l in lines):
            if first_q1 < 0:
                first_q1 = pnum
            elif pnum > first_q1 + 2:
                return pnum

    return n // 2


def split_combined_exam(pdf_path: str, output_dir: str) -> list[dict]:
    """
    합본 PDF를 화작/언매 두 파일로 분할.

    반환:
        [
            {"path": str, "sub_type": "화작", "filename": str},
            {"path": str, "sub_type": "언매", "filename": str},
        ]
    실패 시 빈 리스트 반환.
    """
    try:
        doc = fitz.open(pdf_path)
        split = find_split_point(doc)

        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        # 원본 파일명에서 과목 표기 교체
        def make_name(sub: str) -> str:
            name = base_name
            for pat in ("국어(통합)", "국어(화작+언매)", "국어"):
                if pat in name:
                    name = name.replace(pat, f"국어({sub})", 1)
                    break
            else:
                name = f"{name}_{sub}"
            return name + ".pdf"

        results = []
        for sub_type, page_range in [
            ("화작", range(0, split)),
            ("언매", range(split, len(doc))),
        ]:
            if not page_range:
                continue
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=page_range[0], to_page=page_range[-1])
            out_id   = str(uuid.uuid4())
            filename = make_name(sub_type)
            out_path = os.path.join(output_dir, f"{out_id}.pdf")
            new_doc.save(out_path)
            new_doc.close()
            results.append({"path": out_path, "sub_type": sub_type, "filename": filename})

        doc.close()
        return results
    except Exception:
        return []
