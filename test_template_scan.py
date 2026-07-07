# -*- coding: utf-8 -*-
"""
독서논술 교재 전체 학년 스캔 -> 템플릿 시그니처(구조 지문) 분류.

각 교사용 PDF에서 구조적 특징(마커 존재 여부)을 뽑아 지문(fingerprint)을
만들고, 같은 지문을 가진 파일들을 그룹으로 묶어 템플릿 종류를 파악한다.
"""
import re
import sys
import glob
from pathlib import Path
import fitz

ROOT = Path(r"D:\26년 3분기 교재")
GRADES = ['초1', '초2', '초3', '초4', '초5', '초6', '중1', '중2', '중3']

MARKERS = {
    'cover_LV': re.compile(r'LV\s*\d+'),
    'step1_label': re.compile(r'1\s*단계'),
    'vocab_pattern': re.compile(r'뜻\n.+\n문장', re.MULTILINE) if False else re.compile(r'뜻\s*[\s\S]{0,80}?문장'),
    'ox_pattern': re.compile(r'[○O]\s*\nX'),
    'discussion_header': re.compile(r'질문과\s*토론'),
    'reading_type_label': re.compile(r'[가-힣]+(?:\s*/\s*[가-힣]+)*\s*독해\n'),
    'writing_header_v1': re.compile(r'내\s*글로\s*엮기'),
    'writing_header_v2': re.compile(r'글쓰기\s*주제'),
    'step_numbered_v1': re.compile(r'Step\s*1\.'),
    'step_numbered_v2': re.compile(r'Step1\.'),
    'answer_colon': re.compile(r'^답\s*[:：]', re.MULTILINE),
    'novel_3elements_table': re.compile(r'구성의\s*3요소|소설의\s*3요소'),
    'bullet_dash_answer': re.compile(r'^-\s*[가-힣]', re.MULTILINE),
}


def fingerprint(text):
    return tuple(sorted(name for name, pat in MARKERS.items() if pat.search(text)))


def scan_file(path):
    try:
        doc = fitz.open(path)
        text = '\n'.join(page.get_text() for page in doc)
        return {
            'pages': len(doc),
            'fp': fingerprint(text),
        }
    except Exception as e:
        return {'pages': None, 'fp': ('ERROR:' + str(e)[:60],)}


def main():
    rows = []
    for grade in GRADES:
        gdir = ROOT / grade
        if not gdir.exists():
            continue
        files = sorted(glob.glob(str(gdir / '*교사용*.pdf')))
        for f in files:
            info = scan_file(f)
            rows.append({'grade': grade, 'file': Path(f).name, **info})

    # 그룹핑: fingerprint -> [(grade, file), ...]
    groups = {}
    for r in rows:
        groups.setdefault(r['fp'], []).append((r['grade'], r['file']))

    out_lines = []
    out_lines.append(f'총 파일: {len(rows)}건, 고유 지문(fingerprint) 종류: {len(groups)}개\n')

    for i, (fp, items) in enumerate(sorted(groups.items(), key=lambda kv: -len(kv[1])), 1):
        grades_in_group = sorted(set(g for g, _ in items))
        out_lines.append(f'--- 그룹 {i} ({len(items)}건) 학년: {grades_in_group} ---')
        out_lines.append('  마커: ' + ', '.join(fp) if fp else '  마커: (없음)')
        for g, fn in items[:5]:
            out_lines.append(f'    - [{g}] {fn}')
        if len(items) > 5:
            out_lines.append(f'    ... 외 {len(items)-5}건')
        out_lines.append('')

    report = '\n'.join(out_lines)
    Path('template_scan_report.txt').write_text(report, encoding='utf-8')
    print(report)


if __name__ == '__main__':
    main()
