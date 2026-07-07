# -*- coding: utf-8 -*-
"""독서논술 교재 PDF 추출 - 템플릿 계열별 서브모듈.

계열:
  A1 - 초3~초6 챕터북형 (extract_a1)
  A2 - 초1~초2 그림책형 (extract_a2)
  B  - 중1~중3/초6 소설·수필형 (extract_b)
  C  - 중3 소설이론표형 (extract_c)

classify.classify_family(pdf_path)로 파일이 어느 계열인지 판별하고,
extract_for_family(family, student_path, teacher_path)로 해당 계열의
extract()를 호출한다.
"""
from pathlib import Path

from . import extract_a1, extract_a2, extract_b, extract_c
from .classify import classify_family

FAMILIES = ["A1", "A2", "B", "C"]


def extract_for_family(family, student_path, teacher_path=None):
    """계열에 맞는 extract()를 호출. B/C는 교사용 단독으로 동작."""
    if family == "A1":
        return extract_a1.extract(student_path, teacher_path)
    if family == "A2":
        return extract_a2.extract(student_path, teacher_path)
    if family == "B":
        return extract_b.extract(teacher_path or student_path)
    if family == "C":
        return extract_c.extract(teacher_path or student_path)
    raise ValueError(f"알 수 없는 템플릿 계열: {family}")


__all__ = ["extract_a1", "extract_a2", "extract_b", "extract_c",
           "classify_family", "extract_for_family", "FAMILIES"]
