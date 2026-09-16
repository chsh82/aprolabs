#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""momo_book.db의 doc_id 1건을 학습지 생성기 입력 JSON으로 변환한다.

DESIGN_GUIDE.md §4.1 스키마를 뼈대로 하되, 렌더러(worksheet/scripts/*.js)가 바로 쓰기
편하도록 DB 필드명·ui_type을 그대로 살려서 내보낸다(값 변환은 여기서 전부 끝냄 -
Node 쪽은 매핑 없이 그대로 클래스/텍스트에 꽂기만 하면 되게).

사용법: python extract_worksheet_json.py <doc_id> [출력경로]
기본 출력: momo_book_db/generated/<doc_id>/data.json
"""
import json
import re
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'momo_book.db'

# level(L1~L9) -> 학년 톤 클래스. L1~L2(초1~2)는 가이드에 톤 정의가 없어 tone-elem34로
# 대체 적용(알려진 한계 - DESIGN_GUIDE.md 1.4절/플랜 문서 참고, TODO).
LEVEL_TO_TONE = {
    'L1': 'tone-elem34', 'L2': 'tone-elem34',
    'L3': 'tone-elem34', 'L4': 'tone-elem34',
    'L5': 'tone-elem56', 'L6': 'tone-elem56',
    'L7': 'tone-mid', 'L8': 'tone-mid', 'L9': 'tone-mid',
}

# 분기 라벨 안의 키워드 -> 분기 테마 클래스(styles.css .q-*)
SEASON_KEYWORDS = [
    ('고전', 'q-winter'),
    ('탐구', 'q-spring'),
    ('문학', 'q-summer'),
]
DEFAULT_SEASON = 'q-autumn'  # 인문예술 등 나머지 전부


def season_class(quarter_text):
    for kw, cls in SEASON_KEYWORDS:
        if kw in (quarter_text or ''):
            return cls
    return DEFAULT_SEASON


def quarter_label(quarter_text):
    m = re.search(r'\(([^)]+)\)', quarter_text or '')
    return (m.group(1) if m else (quarter_text or '')) + '분기'


def reading_type_label(reading_type):
    if not reading_type:
        return None
    parts = [p.strip() for p in reading_type.split('/') if p.strip()]
    if parts and all(p.endswith('적') for p in parts):
        return ' · '.join(parts) + ' 독해'
    return ' · '.join(parts)


def row_to_dict(row):
    return dict(row) if row else None


def extract(doc_id):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    doc = row_to_dict(con.execute('SELECT * FROM documents WHERE doc_id=?', (doc_id,)).fetchone())
    if not doc:
        raise SystemExit(f'문서를 찾을 수 없음: {doc_id}')

    level = doc['level']
    tone_class = LEVEL_TO_TONE.get(level, 'tone-elem56')
    level_num = level[1:] if level and level.startswith('L') else level

    images = [row_to_dict(r) for r in con.execute(
        'SELECT * FROM document_image WHERE doc_id=? ORDER BY id', (doc_id,))]
    cover_image = next((im['file_path'] for im in images if im['image_type'] == 'cover'), None)

    vocab = [row_to_dict(r) for r in con.execute(
        'SELECT * FROM vocabulary WHERE doc_id=? ORDER BY order_no', (doc_id,))]
    ox = [row_to_dict(r) for r in con.execute(
        'SELECT * FROM ox_quiz WHERE doc_id=? ORDER BY order_no', (doc_id,))]

    qa_rows = [row_to_dict(r) for r in con.execute(
        'SELECT * FROM discussion_qa WHERE doc_id=? ORDER BY order_no, order_label', (doc_id,))]
    for r in qa_rows:
        r['ui_config'] = json.loads(r['ui_config']) if r['ui_config'] else {}
        r['reading_type_label'] = reading_type_label(r['reading_type'])
        # excerpt_image_path/reference_image_path는 DB에 이미 "<doc_id>/파일명" 형태로
        # 저장돼 있음(document_image.file_path와 같은 규칙) - 별도 변환 불필요.
        if r.get('review_status') != 'approved':
            print(f'[경고] {doc_id} discussion_qa#{r["id"]} (order {r["order_label"]}) '
                  f'review_status={r["review_status"]!r} - 승인 안 된 행 포함됨', file=sys.stderr)

    essay = row_to_dict(con.execute('SELECT * FROM essay_prompt WHERE doc_id=?', (doc_id,)).fetchone())
    outline = []
    if essay:
        outline = [row_to_dict(r) for r in con.execute(
            'SELECT * FROM essay_outline_question WHERE essay_id=? ORDER BY order_no', (essay['id'],))]

    result = {
        'doc_id': doc_id,
        'meta': {
            'level': level,
            'level_label': f'LV {level_num}',
            'tone_class': tone_class,
            'quarter_class': season_class(doc['quarter']),
            'quarter_label': quarter_label(doc['quarter']),
            'book': {
                'title': doc['book_title'],
                'author': doc['book_author'],
                'cover_image': cover_image,
            },
            'cover_message': doc['cover_message'],
        },
        'step1': {
            'vocab': vocab,
            'ox': ox,
        },
        'step2': qa_rows,
        'step3': {
            'essay': essay,
            'outline_questions': outline,
        } if essay else None,
    }
    return result


def main():
    if len(sys.argv) < 2:
        raise SystemExit('사용법: python extract_worksheet_json.py <doc_id> [출력경로]')
    doc_id = sys.argv[1]
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else BASE_DIR / 'generated' / doc_id / 'data.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = extract(doc_id)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'작성 완료: {out_path}')
    print(f'  vocab {len(data["step1"]["vocab"])}건 / ox {len(data["step1"]["ox"])}건 / '
          f'discussion_qa {len(data["step2"])}건 / step3 {"있음" if data["step3"] else "없음"}')


if __name__ == '__main__':
    main()
