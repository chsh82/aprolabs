# -*- coding: utf-8 -*-
"""
모모의책장 교재DB — Phase 4: 폴더 전체 배치 처리.

폴더 안의 "(학생용).pdf" 파일들을 훑어서:
1. 파일명에서 분기/전체주차/도서명을 뽑는다.
2. 커리큘럼 DB(aprolabs.db의 momo_bookshelf_weeks)와 대조해 curriculum_id/학년(label)을 찾는다
   (요청서 규칙: 파일명에서 뽑은 값은 커리큘럼 DB와 대조 검증, 충돌 시 커리큘럼 DB를 따름).
3. PDF 1페이지에서 "LV N"을 읽어 레벨을 확인한다.
4. parser.parse_pdf() -> loader.save_document()로 저장한다. source_hash가 같으면 건너뜀.
5. 끝나면 성공/경고/실패/건너뜀 건수를 요약해서 출력한다.

사용법: python batch_run.py "<폴더 경로>" --year 2026 --quarter-num 4 --grade 초5 [--llm]
"""
import argparse
import glob
import os
import re
import sqlite3
import sys

import fitz

from parser import parse_pdf
from loader import load_pdf_into_db, init_db, DB_PATH, file_hash

APROLABS_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'aprolabs.db')

GRADE_FOLDER_TO_LABEL = {
    '초1': '초등 1학년', '초2': '초등 2학년', '초3': '초등 3학년',
    '초4': '초등 4학년', '초5': '초등 5학년', '초6': '초등 6학년',
    '중1': '중학교 1학년', '중2': '중학교 2학년', '중3': '중학교 3학년',
}

# PDF 1페이지에서 "LV N"을 못 찾았을 때 쓰는 대체 레벨. 폴더 자체가 이미 학년별로
# 나뉘어 있으므로 그 학년에 맞는 레벨로 대체함(예전에는 학년 구분 없이 전부 'LX'
# 하나로 몰아서, 서로 다른 학년의 같은 주차 번호가 같은 doc_id를 덮어쓰는 사고가 있었음).
GRADE_FOLDER_TO_LEVEL = {
    '초1': 'L1', '초2': 'L2', '초3': 'L3',
    '초4': 'L4', '초5': 'L5', '초6': 'L6',
    '중1': 'L7', '중2': 'L8', '중3': 'L9',
}

# 파일명 대괄호 안에 학년이 잘못 섞여 들어간 경우([중3 1분기 3주차])와, 같은 파일을 두 번
# 내려받아 윈도우가 자동으로 붙인 "(1)" 같은 중복 표시가 붙은 경우를 모두 허용함.
FNAME_RE = re.compile(
    r'^\[(?:[가-힣0-9]+\s+)?(?P<q>\d+)분기\s*(?P<w>\d+)주차\]\s*(?P<title>.+?)'
    r'(?:\(학생용\))?(?:\s*\(\d+\))?\.pdf$',
    re.IGNORECASE,
)
TITLE_SUFFIX_RE = re.compile(r'\s*\(연장\)\s*$|\s*\d+(-\d+)?\s*주차\s*$')
# 폰트 인코딩이 깨진 PDF는 "LV# 8"처럼 글자 사이에 이상한 문자(#, $ 등)가 끼어 나옴
# (요청자 확인 - 나중에 검수하면서 수정). 숫자만 못 찾게 되는 걸 막기 위해 LV와 숫자
# 사이에 숫자가 아닌 문자가 끼어도 인식되게 함.
LV_RE = re.compile(r'LV[^\d]*(\d+)')
# 학생용 학습지가 아니라 곁다리로 같이 들어있는 원문 발췌/참고 자료(구조화된 어휘·OX·토론
# 형식이 없어서 파싱 대상이 아님) - 파일명에 이 표현이 있으면 제외.
NON_WORKSHEET_RE = re.compile(r'읽기자료|참고\s*자료|추가자료')
# 같은 파일을 두 번 내려받아 윈도우가 자동으로 붙인 "(1)", "(2)" 같은 중복 표시.
DUPLICATE_SUFFIX_RE = re.compile(r'\s*\(\d+\)\.pdf$', re.IGNORECASE)


def clean_title(raw_title: str) -> str:
    t = raw_title.strip()
    changed = True
    while changed:
        changed = False
        m = TITLE_SUFFIX_RE.search(t)
        if m and m.start() > 0:
            t = t[:m.start()].strip()
            changed = True
    return t


def find_curriculum_weeks(conn, year, quarter_num, grade_folder):
    """momo_bookshelf_weeks에서 (연도, N분기, 학년)의 휴강이 아닌 주차들을 week_number 순으로 반환.

    파일명의 [N분기 M주차] 숫자는 휴강 주(추석 연휴 등)를 세지 않고 매긴 경우가 있어
    커리큘럼DB의 week_number와 그대로 안 맞을 수 있음 - 그래서 숫자 비교 대신,
    휴강 주를 뺀 커리큘럼 목록과 실제 학생용 파일 목록을 순서대로 1:1로 짝짓는다.
    """
    grade_label = GRADE_FOLDER_TO_LABEL.get(grade_folder)
    if not grade_label:
        return []
    return conn.execute(
        "SELECT id, quarter, title, author, week_number FROM momo_bookshelf_weeks "
        "WHERE year=? AND quarter LIKE ? AND grade=? AND is_holiday=0 ORDER BY week_number",
        (year, f'{quarter_num}분기%', grade_label),
    ).fetchall()


def get_pdf_level(pdf_path: str) -> str | None:
    doc = fitz.open(pdf_path)
    text = doc[0].get_text()
    m = LV_RE.search(text)
    return f'L{m.group(1)}' if m else None


def run_batch(folder: str, year: int, quarter_num: int, grade_folder: str, use_llm: bool = False):
    if not os.path.exists(DB_PATH):
        init_db()

    curriculum_conn = sqlite3.connect(APROLABS_DB)
    curriculum_conn.row_factory = sqlite3.Row

    # "학생용" 표시가 없는 파일도 있어서, "교사용"이 아닌 pdf를 학생용으로 취급.
    # 초1/초2는 같은 주차에 정규 학생용과 별도로 "베이직"(쉬운 버전) 파일도 있어서 제외함.
    # 읽기자료/참고자료/추가자료는 구조화된 학습지가 아닌 곁다리 원문 파일이라 제외함.
    raw_pdf_files = [
        p for p in glob.glob(os.path.join(folder, '*.pdf'))
        if '교사용' not in os.path.basename(p) and '베이직' not in os.path.basename(p)
        and not NON_WORKSHEET_RE.search(os.path.basename(p))
    ]
    results = {'success': [], 'skipped': [], 'warning': [], 'failed': []}

    # 파일명에서 [N분기 M주차]를 뽑아 숫자 기준으로 정렬한다.
    # 문자열 정렬("10주차" < "1주차")로는 실제 순서가 안 나와서 반드시 숫자로 정렬해야 함.
    parsed_files = []
    for p in raw_pdf_files:
        fname = os.path.basename(p)
        m = FNAME_RE.match(fname)
        if not m:
            results['failed'].append((fname, '파일명 패턴이 안 맞음(분기/주차/제목 추출 실패)'))
            continue
        parsed_files.append((int(m.group('w')), p, m))
    parsed_files.sort(key=lambda x: x[0])

    # 같은 주차 번호로 파일이 2개 이상 잡히면(윈도우가 중복 다운로드에 자동으로 "(1)" 등을
    # 붙인 경우) 내용이 완전히 같은지 확인한 뒤 하나만 남김. 내용이 다르면 사람이 직접
    # 골라야 하므로 그 주차는 건너뛰고 실패 목록에 올림(추측해서 고르지 않음).
    by_week = {}
    for item in parsed_files:
        by_week.setdefault(item[0], []).append(item)
    deduped_files = []
    for week_no, items in sorted(by_week.items()):
        if len(items) == 1:
            deduped_files.append(items[0])
            continue
        hashes = {file_hash(p) for _, p, _ in items}
        if len(hashes) == 1:
            # 완전히 같은 파일 - "(숫자)" 중복 표시가 없는 파일을 우선 선택
            items.sort(key=lambda it: bool(DUPLICATE_SUFFIX_RE.search(os.path.basename(it[1]))))
            deduped_files.append(items[0])
        else:
            fnames = ', '.join(os.path.basename(p) for _, p, _ in items)
            results['failed'].append((
                fnames,
                f'{week_no}주차에 서로 다른 학생용 파일이 {len(items)}개 있어서 어느 것을 써야 할지 '
                f'알 수 없음 - 사람이 직접 확인 필요',
            ))
    parsed_files = deduped_files

    curr_weeks = find_curriculum_weeks(curriculum_conn, year, quarter_num, grade_folder)
    if len(curr_weeks) != len(parsed_files):
        results['failed'].append((
            folder,
            f'커리큘럼DB 주차 수({len(curr_weeks)})와 학생용 파일 수({len(parsed_files)})가 달라서 '
            f'순서 매칭을 할 수 없음 - 사람이 직접 확인 필요',
        ))
        curriculum_conn.close()
        return results

    # 순서 기반 매칭: 파일명의 주차 숫자 대신, 휴강 주를 뺀 커리큘럼 목록과
    # 파일명 순서(분기/주차 오름차순)를 1:1로 짝짓는다 (사용자 승인된 방식).
    for (fname_week_number, pdf_path, m), curr_row in zip(parsed_files, curr_weeks):
        fname = os.path.basename(pdf_path)
        title_from_fname = clean_title(m.group('title'))

        # 요청서 규칙: 충돌 시 커리큘럼 DB를 따름
        curriculum_id = curr_row['id']
        quarter_label = curr_row['quarter']
        week_number = curr_row['week_number']
        book_title = clean_title(curr_row['title'])
        book_author = curr_row['author']
        if title_from_fname and title_from_fname not in book_title and book_title not in title_from_fname:
            results['warning'].append((fname, f'파일명 제목"{title_from_fname}" vs 커리큘럼DB 제목"{book_title}" 불일치 - 커리큘럼DB 값 사용'))
        if fname_week_number != week_number:
            results['warning'].append((fname, f'파일명 주차표기({fname_week_number}주차) != 커리큘럼DB week_number({week_number}) - 순서 매칭으로 커리큘럼DB 값 사용'))

        # PDF 안에서 "LV N"을 못 찾으면(폰트 깨짐 등) 폴더 학년으로 대체.
        detected_level = get_pdf_level(pdf_path)
        fallback_level = GRADE_FOLDER_TO_LEVEL.get(grade_folder, 'LX')
        if detected_level and detected_level != fallback_level:
            results['warning'].append((
                fname,
                f'PDF에서 읽은 레벨({detected_level})이 폴더 학년({grade_folder}={fallback_level})과 달라서 '
                f'폴더 학년 값을 사용함',
            ))
        level = fallback_level
        doc_id = f'{level}-Q{quarter_num}-W{week_number:02d}'

        try:
            doc_meta = {
                'doc_id': doc_id,
                'curriculum_id': curriculum_id,
                'level': level,
                'quarter': quarter_label,
                'week': week_number,
                'book_title': book_title,
                'book_author': book_author,
            }
            result = load_pdf_into_db(pdf_path, doc_meta, use_llm=use_llm)
            if result['skipped']:
                results['skipped'].append((fname, doc_id))
            else:
                stats = result['stats']
                # 중등 교재처럼 배경지식(background_text)이 있는 경우는 vocab/ox가 0건인 게 정상이라 경고 대상 아님
                vocab_ox_missing = (stats['vocabulary'] == 0 or stats['ox_quiz'] == 0) and not stats['has_background_text']
                if vocab_ox_missing or stats['discussion_qa'] == 0:
                    results['warning'].append((fname, f'{doc_id}: 일부 섹션이 0건 추출됨 {stats}'))
                else:
                    results['success'].append((fname, doc_id, stats))
        except Exception as e:
            results['failed'].append((fname, f'예외 발생: {e}'))

    curriculum_conn.close()
    return results


def print_report(results):
    print(f"\n성공 {len(results['success'])}건 / 건너뜀(이미 처리됨) {len(results['skipped'])}건 "
          f"/ 경고 {len(results['warning'])}건 / 실패 {len(results['failed'])}건\n")

    if results['success']:
        print('--- 성공 ---')
        for fname, doc_id, stats in results['success']:
            print(f'  {doc_id} <- {fname}  {stats}')
    if results['skipped']:
        print('--- 건너뜀(이미 처리됨) ---')
        for fname, doc_id in results['skipped']:
            print(f'  {doc_id} <- {fname}')
    if results['warning']:
        print('--- 경고 ---')
        for fname, msg in results['warning']:
            print(f'  {fname}: {msg}')
    if results['failed']:
        print('--- 실패 ---')
        for fname, msg in results['failed']:
            print(f'  {fname}: {msg}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('folder')
    ap.add_argument('--year', type=int, required=True)
    ap.add_argument('--quarter-num', type=int, required=True, help='파일명 대괄호의 분기 숫자(예: 4)')
    ap.add_argument('--grade', required=True, help='폴더 학년 코드(예: 초5)')
    ap.add_argument('--llm', action='store_true')
    args = ap.parse_args()

    if args.llm:
        sys.stdout.reconfigure(encoding='utf-8')
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

    results = run_batch(args.folder, args.year, args.quarter_num, args.grade, use_llm=args.llm)
    print_report(results)
