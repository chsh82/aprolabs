# -*- coding: utf-8 -*-
"""분기 폴더를 훑어서 학생용/교사용 PDF 쌍을 찾고, 계열을 판별해 추출 -> DB 저장.

폴더 구조 (분기마다 동일): {ROOT}\\{학년}\\[N주차] 제목(교사용/학생용).pdf
"""
import glob
from pathlib import Path

from sqlalchemy.orm import Session

from app.models.reading_essay import ReadingMaterial
from . import extract_a1, extract_a2, extract_b, extract_c
from .classify import classify_family

QUARTER_ROOTS = {
    "25년4분기": r"D:\25년도 4분기 교재",
    "26년1분기": r"D:\26년 1분기 교재",
    "26년2분기": r"D:\26년 2분기 교재",
    "26년3분기": r"D:\26년 3분기 교재",
}
GRADES = ['초1', '초2', '초3', '초4', '초5', '초6', '중1', '중2', '중3']


def find_student_pdf(teacher_path):
    """교사용 PDF 경로로부터 짝이 되는 학생용(베이직 아님) PDF를 추정."""
    p = Path(teacher_path)
    candidates = [
        p.with_name(p.name.replace('교사용', '학생용')),
        p.with_name(p.name.replace('(교사용)', '(학생용)')),
    ]
    for c in candidates:
        if c.exists():
            return c

    # 정확한 치환이 안 맞으면 같은 폴더에서 "학생용"이 들어간 유사 파일 탐색
    # (베이직-학생용은 난이도가 다른 별도 버전이라 제외)
    stem_prefix = p.stem.split('(')[0].strip()
    for f in p.parent.glob('*학생용*.pdf'):
        if '베이직' in f.name:
            continue
        if f.stem.split('(')[0].strip() == stem_prefix:
            return f
    return None


def iter_teacher_pdfs(quarters=None, grades=None):
    """(quarter, grade, teacher_pdf_path) 튜플을 순회."""
    quarters = quarters or list(QUARTER_ROOTS.keys())
    grades = grades or GRADES
    for quarter in quarters:
        root = Path(QUARTER_ROOTS.get(quarter, quarter))
        if not root.exists():
            continue
        for grade in grades:
            gdir = root / grade
            if not gdir.exists():
                continue
            for f in sorted(glob.glob(str(gdir / '*교사용*.pdf'))):
                yield quarter, grade, Path(f)


def _week_from_filename(filename):
    import re
    m = re.search(r'\[([^\]]+)\]', filename)
    return m.group(1) if m else None


def process_one(db: Session, quarter, grade, teacher_path, force=False):
    """교사용 PDF 1개를 처리해서 ReadingMaterial 1건을 만들거나 갱신."""
    teacher_path = Path(teacher_path)

    existing = (
        db.query(ReadingMaterial)
        .filter(ReadingMaterial.teacher_pdf_path == str(teacher_path))
        .first()
    )
    if existing and existing.status == 'extracted' and not force:
        return existing, 'skipped'

    material = existing or ReadingMaterial(
        quarter=quarter, grade=grade,
        week=_week_from_filename(teacher_path.name),
        teacher_pdf_path=str(teacher_path),
        status='pending',
    )
    if not existing:
        db.add(material)
        db.flush()

    try:
        family = classify_family(teacher_path)
        if family is None:
            material.status = 'error'
            material.error_message = '템플릿 계열 판별 실패 (텍스트 부족 또는 알려지지 않은 형식)'
            db.commit()
            return material, 'error'

        student_path = find_student_pdf(teacher_path)
        if family in ('A1', 'A2') and student_path is None:
            material.status = 'error'
            material.error_message = f'{family} 계열은 학생용 PDF가 필요하지만 찾지 못함'
            db.commit()
            return material, 'error'

        if student_path:
            material.student_pdf_path = str(student_path)

        if family == 'A1':
            extraction = extract_a1.extract(student_path, teacher_path)
        elif family == 'A2':
            extraction = extract_a2.extract(student_path, teacher_path)
        elif family == 'B':
            extraction = extract_b.extract(teacher_path)
        else:
            extraction = extract_c.extract(teacher_path)

        from .adapters import save_material
        save_material(db, material, family, extraction)
        return material, 'extracted'

    except Exception as e:
        db.rollback()
        material = db.merge(material)
        material.status = 'error'
        material.error_message = f'{type(e).__name__}: {e}'
        db.commit()
        return material, 'error'


def scan(db: Session, quarters=None, grades=None, force=False, limit=None):
    """배치 스캔. 결과 카운트 dict 반환."""
    counts = {'extracted': 0, 'skipped': 0, 'error': 0, 'total': 0}
    errors = []
    for quarter, grade, teacher_path in iter_teacher_pdfs(quarters, grades):
        if limit and counts['total'] >= limit:
            break
        counts['total'] += 1
        material, outcome = process_one(db, quarter, grade, teacher_path, force=force)
        counts[outcome] = counts.get(outcome, 0) + 1
        if outcome == 'error':
            errors.append({'file': teacher_path.name, 'quarter': quarter, 'grade': grade,
                            'error': material.error_message})
    counts['errors'] = errors
    return counts
