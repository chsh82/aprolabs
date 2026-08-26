# -*- coding: utf-8 -*-
"""
모모의책장 교재DB — Phase 2: parser.py 결과를 schema.sql 구조의 SQLite로 저장.

- 문서 1개(doc_id)를 다시 저장하면 기존 하위 데이터(어휘/OX/토론/글쓰기)는 지우고 새로 채움
  (source_hash가 같으면 아예 건너뜀 - Phase 4에서 재실행해도 중복 안 생기게 하는 핵심 규칙).
- documents 메타데이터(level/quarter/week/curriculum_id 등)는 PDF 안에 없으므로 호출하는
  쪽에서 넘겨줌(커리큘럼 DB와 매칭하는 로직은 Phase 4에서 다룸).

사용법: python loader.py <학생용 PDF 경로> --doc-id L5-Q4-W10 --curriculum-id <uuid>
           --level L5 --quarter "4분기(인문예술)" --week 10
           --book-title "나의 행복과 모두의 행복" --book-author 서정욱 [--llm]
"""
import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

from parser import parse_pdf

DB_PATH = os.path.join(os.path.dirname(__file__), 'momo_book.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schema.sql')
IMAGES_DIR = os.path.join(os.path.dirname(__file__), 'extracted_images')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def init_db():
    conn = get_conn()
    with open(SCHEMA_PATH, encoding='utf-8') as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()


def file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def _delete_document_children(conn, doc_id):
    essay_ids = [r[0] for r in conn.execute('SELECT id FROM essay_prompt WHERE doc_id=?', (doc_id,))]
    for eid in essay_ids:
        conn.execute('DELETE FROM essay_outline_question WHERE essay_id=?', (eid,))
    conn.execute('DELETE FROM essay_prompt WHERE doc_id=?', (doc_id,))
    conn.execute('DELETE FROM vocabulary WHERE doc_id=?', (doc_id,))
    conn.execute('DELETE FROM ox_quiz WHERE doc_id=?', (doc_id,))
    conn.execute('DELETE FROM discussion_qa WHERE doc_id=?', (doc_id,))
    conn.execute('DELETE FROM document_image WHERE doc_id=?', (doc_id,))
    conn.execute('DELETE FROM extraction_log WHERE doc_id=?', (doc_id,))
    doc_image_dir = os.path.join(IMAGES_DIR, doc_id)
    if os.path.isdir(doc_image_dir):
        for fname in os.listdir(doc_image_dir):
            os.remove(os.path.join(doc_image_dir, fname))


def save_document(conn, doc_meta: dict, parsed: dict) -> dict:
    """doc_meta: documents 테이블에 넣을 값들(doc_id, curriculum_id, level, quarter, week,
    book_title, book_author, isbn, source_file, source_format, source_hash).
    반환: {'skipped': bool, 'stats': {...}}"""
    doc_id = doc_meta['doc_id']
    existing = conn.execute('SELECT source_hash, version FROM documents WHERE doc_id=?', (doc_id,)).fetchone()

    if existing and existing[0] == doc_meta['source_hash']:
        return {'skipped': True, 'stats': {}}

    version = (existing[1] + 1) if existing else 1
    parsed_at = datetime.now(timezone.utc).isoformat()

    cover_message = parsed.get('cover_message')
    background_text = parsed.get('background_text')

    if existing:
        _delete_document_children(conn, doc_id)
        conn.execute('''UPDATE documents SET curriculum_id=?, level=?, quarter=?, week=?,
                         book_title=?, book_author=?, isbn=?, cover_message=?, background_text=?, source_file=?, source_format=?,
                         source_hash=?, version=?, parsed_at=?, review_status='pending'
                         WHERE doc_id=?''', (
            doc_meta.get('curriculum_id'), doc_meta.get('level'), doc_meta.get('quarter'), doc_meta.get('week'),
            doc_meta['book_title'], doc_meta.get('book_author'), doc_meta.get('isbn'), cover_message, background_text,
            doc_meta['source_file'], doc_meta.get('source_format'), doc_meta['source_hash'],
            version, parsed_at, doc_id,
        ))
    else:
        conn.execute('''INSERT INTO documents
            (doc_id, curriculum_id, level, quarter, week, book_title, book_author, isbn, cover_message, background_text,
             source_file, source_format, source_hash, version, parsed_at, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''', (
            doc_id, doc_meta.get('curriculum_id'), doc_meta.get('level'), doc_meta.get('quarter'), doc_meta.get('week'),
            doc_meta['book_title'], doc_meta.get('book_author'), doc_meta.get('isbn'), cover_message, background_text,
            doc_meta['source_file'], doc_meta.get('source_format'), doc_meta['source_hash'],
            version, parsed_at,
        ))

    for v in parsed['vocabulary']:
        conn.execute('''INSERT INTO vocabulary
            (doc_id, order_no, word, definition, book_page, example_sentence, source_page,
             raw_text, extraction_confidence, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''', (
            doc_id, v['order_no'], v['word'], v.get('definition'), v.get('book_page'),
            v.get('example_sentence'), v.get('source_page'), v.get('raw_text'), v.get('extraction_confidence', 0.8),
        ))

    for o in parsed['ox_quiz']:
        conn.execute('''INSERT INTO ox_quiz
            (doc_id, order_no, question, answer, evidence_page, explanation, source_page,
             raw_text, extraction_confidence, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''', (
            doc_id, o['order_no'], o['question'], None, o.get('evidence_page'), None,
            o.get('source_page'), o.get('raw_text'), o.get('extraction_confidence', 0.8),
        ))

    for d in parsed['discussion_qa']:
        conn.execute('''INSERT INTO discussion_qa
            (doc_id, order_no, order_label, reading_type, excerpt_text, excerpt_page,
             question_text, ui_type, ui_config, model_answer, source_page, raw_text,
             extraction_confidence, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''', (
            doc_id, d['order_no'], d.get('order_label'), d.get('reading_type'),
            d.get('excerpt_text'), d.get('excerpt_page'), d['question_text'], d.get('ui_type'),
            d.get('ui_config'), None, d.get('source_page'), d.get('raw_text'), d.get('extraction_confidence', 0.6),
        ))

    essay = parsed.get('essay_prompt') or {}
    if essay.get('main_topic'):
        cur = conn.execute('''INSERT INTO essay_prompt
            (doc_id, main_topic, writing_guide, writing_format, min_length, closing_instruction, source_page, raw_text,
             extraction_confidence, review_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')''', (
            doc_id, essay['main_topic'], essay.get('writing_guide'), essay.get('writing_format'), essay.get('min_length'),
            essay.get('closing_instruction'), essay.get('source_page'), essay.get('raw_text'), 0.7,
        ))
        essay_id = cur.lastrowid
        for oq in essay.get('outline_questions', []):
            conn.execute('''INSERT INTO essay_outline_question (essay_id, order_no, question_text, role)
                VALUES (?, ?, ?, ?)''', (essay_id, oq['order_no'], oq['question_text'], oq.get('role')))

    images = parsed.get('images') or []
    if images:
        doc_image_dir = os.path.join(IMAGES_DIR, doc_id)
        os.makedirs(doc_image_dir, exist_ok=True)
        for img in images:
            fname = f"{img['image_type']}_p{img['source_page']}.{img['ext']}"
            with open(os.path.join(doc_image_dir, fname), 'wb') as f:
                f.write(img['image_bytes'])
            conn.execute('''INSERT INTO document_image (doc_id, image_type, source_page, file_path, extraction_confidence)
                VALUES (?, ?, ?, ?, ?)''', (
                doc_id, img['image_type'], img['source_page'], f'{doc_id}/{fname}', 0.7,
            ))

    for log in parsed.get('extraction_log', []):
        conn.execute('''INSERT INTO extraction_log (doc_id, level, stage, message, created_at)
            VALUES (?, ?, ?, ?, ?)''', (doc_id, log.get('level'), log.get('stage'), log.get('message'), parsed_at))

    conn.commit()

    return {'skipped': False, 'stats': {
        'vocabulary': len(parsed['vocabulary']),
        'ox_quiz': len(parsed['ox_quiz']),
        'discussion_qa': len(parsed['discussion_qa']),
        'essay_prompt': 1 if essay.get('main_topic') else 0,
        'images': len(images),
        'has_background_text': bool(background_text),
        'version': version,
    }}


def load_pdf_into_db(pdf_path: str, doc_meta: dict, use_llm: bool = False) -> dict:
    doc_meta = dict(doc_meta)
    doc_meta['source_file'] = pdf_path
    doc_meta.setdefault('source_format', 'pdf')
    doc_meta['source_hash'] = file_hash(pdf_path)

    parsed = parse_pdf(pdf_path, use_llm=use_llm)
    conn = get_conn()
    try:
        result = save_document(conn, doc_meta, parsed)
    finally:
        conn.close()
    return result


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('pdf_path')
    ap.add_argument('--doc-id', required=True)
    ap.add_argument('--curriculum-id')
    ap.add_argument('--level')
    ap.add_argument('--quarter')
    ap.add_argument('--week', type=int)
    ap.add_argument('--book-title', required=True)
    ap.add_argument('--book-author')
    ap.add_argument('--isbn')
    ap.add_argument('--llm', action='store_true')
    args = ap.parse_args()

    if not os.path.exists(DB_PATH):
        init_db()
        print(f'DB 새로 생성: {DB_PATH}')

    if args.llm:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

    doc_meta = {
        'doc_id': args.doc_id,
        'curriculum_id': args.curriculum_id,
        'level': args.level,
        'quarter': args.quarter,
        'week': args.week,
        'book_title': args.book_title,
        'book_author': args.book_author,
        'isbn': args.isbn,
    }
    result = load_pdf_into_db(args.pdf_path, doc_meta, use_llm=args.llm)
    if result['skipped']:
        print(f'{args.doc_id}: 이미 처리된 파일(source_hash 동일) - 건너뜀')
    else:
        print(f'{args.doc_id}: 저장 완료 - {result["stats"]}')
