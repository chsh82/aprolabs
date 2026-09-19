#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""momo_book.db의 doc_id 1건을 학습지 생성기 입력 JSON으로 변환한다.

DESIGN_GUIDE.md §4.1 스키마를 뼈대로 하되, 렌더러(worksheet/scripts/*.js)가 바로 쓰기
편하도록 DB 필드명·ui_type을 그대로 살려서 내보낸다(값 변환은 여기서 전부 끝냄 -
Node 쪽은 매핑 없이 그대로 클래스/텍스트에 꽂기만 하면 되게).

사용법: python extract_worksheet_json.py <doc_id> [출력경로]
기본 출력: momo_book_db/generated/<doc_id>/data.json

2026-09-18 1차 안정화: 입력 검사(issues[])와 schema_version 추가, background_text
포함, DB 연결을 읽기전용으로 열고 with문으로 확실히 닫음. review_status/ui_type
분포 조사 결과(momo_book.db 실측) review_status는 documents/vocabulary/ox_quiz/
discussion_qa/essay_prompt 전부 {'pending','approved'} 뿐이고 discussion_qa.ui_type도
blocks.js가 처리하는 7종 밖의 값은 없었음 - 그래도 미래 데이터 대비 검사는 유지한다.
"""
import json
import re
import sqlite3
import sys
from pathlib import Path

SCHEMA_VERSION = 1
# 호환 방식: 기존 필드(doc_id/meta/step1/step2/step3)는 이름·형태를 바꾸지 않는다.
# 추가된 필드(schema_version, issues, meta.background_text)는 렌더러가 몰라도 되는
# 값이라 하위 호환에 영향 없음 - generate.js/blocks.js는 새 필드를 무시해도 동작한다.

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'momo_book.db'
EXTRACTED_IMAGES_DIR = BASE_DIR / 'extracted_images'

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

REVIEW_STATUS_ALLOWED = {'pending', 'approved'}
# blocks.js의 answerSlotHtml() switch문과 반드시 일치시킨다 - 여기 없는 값이 오면
# blocks.js는 default: unknown 경로(bodyFallback)로 빠지므로 크래시는 안 하지만,
# 검수화면에서 놓친 매핑일 수 있어 issues에 남긴다.
UI_TYPE_ALLOWED = {
    'choice_ab', 'choice_multi', 'text_short_multi', 'table_compare',
    'text_short', 'text_long', 'unknown',
}
# extracted_images/<doc_id>/<파일명>.<확장자> 형태만 허용 - '..'/절대경로/드라이브
# 문자는 전부 거부(허용 디렉터리 밖 접근 차단).
IMAGE_PATH_RE = re.compile(r'^[A-Za-z0-9_\-]+/[A-Za-z0-9_.\-]+\.(?:jpe?g|png|webp|gif)$', re.IGNORECASE)


class Issue(dict):
    """doc_id/table/row_id/field/reason을 갖춘 구조화된 검증 이슈. dict를 상속해
    json.dumps에 그대로 들어간다."""
    def __init__(self, doc_id, table, row_id, field, reason):
        super().__init__(doc_id=doc_id, table=table, row_id=row_id, field=field, reason=reason)


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


def _check_review_status(issues, doc_id, table, row_id, value):
    if value not in REVIEW_STATUS_ALLOWED:
        issues.append(Issue(doc_id, table, row_id, 'review_status',
                             f'허용되지 않은 값: {value!r} (허용: {sorted(REVIEW_STATUS_ALLOWED)})'))


def _check_source_page(issues, doc_id, table, row_id, value):
    if value is not None and not isinstance(value, int):
        issues.append(Issue(doc_id, table, row_id, 'source_page',
                             f'정수가 아님: {value!r} ({type(value).__name__})'))


def _check_image_path(issues, doc_id, table, row_id, field, rel_path):
    """형식(허용 디렉터리 안, 상대경로, 이미지 확장자)과 실제 파일 존재 여부를 검사한다.
    둘 다 issues에만 남기고 렌더링 자체를 막지는 않는다(렌더 단계 QA가 별도로
    로딩 실패를 잡음) - 여기서는 "입력이 의심스럽다"는 신호만 보고한다."""
    if not rel_path:
        return
    if '..' in Path(rel_path).parts or rel_path.startswith(('/', '\\')) or ':' in rel_path:
        issues.append(Issue(doc_id, table, row_id, field,
                             f'허용 디렉터리를 벗어난 경로: {rel_path!r}'))
        return
    if not IMAGE_PATH_RE.match(rel_path.replace('\\', '/')):
        issues.append(Issue(doc_id, table, row_id, field,
                             f'예상 형식(<폴더>/<파일명>.<확장자>)에 맞지 않음: {rel_path!r}'))
        return
    if not (EXTRACTED_IMAGES_DIR / rel_path).is_file():
        issues.append(Issue(doc_id, table, row_id, field, f'파일이 존재하지 않음: {rel_path!r}'))


def _check_semantic_placeholder(issues, doc_id, table, row_id, field, text):
    """질문/발췌문 등 텍스트 칸에 ui_type 토큰만 덩그러니 들어있는 경우(추출 플레이스홀더가
    검수 없이 그대로 남은 흔적) 발견용. 흔치 않은 이상 패턴이라 issues로만 남긴다."""
    if text and text.strip() in UI_TYPE_ALLOWED:
        issues.append(Issue(doc_id, table, row_id, field,
                             f'질문/텍스트 칸에 ui_type 토큰만 있음: {text!r}'))


def _check_duplicate_order_no(issues, doc_id, table, rows, label_field=None):
    """order_no(또는 order_label - discussion_qa는 "4-1"/"4-2" 하위번호가 있어 order_no만
    보면 정상적으로도 겹치므로, 있으면 order_label을 기준으로 본다) 중복을 찾는다.
    2026-09-18 실측: discussion_qa 다수 문서에서 같은 order_label에 ui_type='unknown'
    행(도표/목차 항목으로 보이는 짧은 구절)이 실제 문항과 나란히 붙어 나옴 - 상위 추출
    파이프라인의 알려진 아티팩트로 보이며, 검수 단계에서 걸러내야 할 대상."""
    seen = {}
    for r in rows:
        key = r[label_field] if label_field and r.get(label_field) else r['order_no']
        seen.setdefault(key, []).append(r['id'])
    dups = {k: v for k, v in seen.items() if len(v) > 1}
    field_name = label_field or 'order_no'
    for key, ids in dups.items():
        issues.append(Issue(doc_id, table, ids, field_name,
                             f'{field_name}={key!r} 중복(행 id={ids}) - 원본 순서를 임의로 재정렬하지 않음, 원본 확인 필요'))


def extract(doc_id):
    issues = []
    # 읽기전용 연결(mode=ro) - 이 스크립트는 절대 DB에 쓰지 않는다는 걸 연결 단계에서부터 보장.
    uri = f'file:{DB_PATH.as_posix()}?mode=ro'
    with sqlite3.connect(uri, uri=True) as con:
        con.row_factory = sqlite3.Row

        doc = row_to_dict(con.execute('SELECT * FROM documents WHERE doc_id=?', (doc_id,)).fetchone())
        if not doc:
            raise SystemExit(f'문서를 찾을 수 없음: {doc_id}')

        _check_review_status(issues, doc_id, 'documents', doc_id, doc['review_status'])

        level = doc['level']
        tone_class = LEVEL_TO_TONE.get(level, 'tone-elem56')
        level_num = level[1:] if level and level.startswith('L') else level

        images = [row_to_dict(r) for r in con.execute(
            'SELECT * FROM document_image WHERE doc_id=? ORDER BY id', (doc_id,))]
        for im in images:
            _check_image_path(issues, doc_id, 'document_image', im['id'], 'file_path', im['file_path'])
        cover_image = next((im['file_path'] for im in images if im['image_type'] == 'cover'), None)

        vocab = [row_to_dict(r) for r in con.execute(
            'SELECT * FROM vocabulary WHERE doc_id=? ORDER BY order_no', (doc_id,))]
        for v in vocab:
            _check_review_status(issues, doc_id, 'vocabulary', v['id'], v['review_status'])
            _check_source_page(issues, doc_id, 'vocabulary', v['id'], v['source_page'])
            if not v['word'] or not str(v['word']).strip():
                issues.append(Issue(doc_id, 'vocabulary', v['id'], 'word', '단어 칸이 비어 있음'))
        _check_duplicate_order_no(issues, doc_id, 'vocabulary', vocab)

        ox = [row_to_dict(r) for r in con.execute(
            'SELECT * FROM ox_quiz WHERE doc_id=? ORDER BY order_no', (doc_id,))]
        for o in ox:
            _check_review_status(issues, doc_id, 'ox_quiz', o['id'], o['review_status'])
            _check_source_page(issues, doc_id, 'ox_quiz', o['id'], o['source_page'])
        _check_duplicate_order_no(issues, doc_id, 'ox_quiz', ox)

        qa_rows = [row_to_dict(r) for r in con.execute(
            'SELECT * FROM discussion_qa WHERE doc_id=? ORDER BY order_no, order_label', (doc_id,))]
        _check_duplicate_order_no(issues, doc_id, 'discussion_qa', qa_rows, label_field='order_label')
        for r in qa_rows:
            raw_cfg = r['ui_config']
            if raw_cfg:
                try:
                    r['ui_config'] = json.loads(raw_cfg)
                except (json.JSONDecodeError, TypeError) as e:
                    issues.append(Issue(doc_id, 'discussion_qa', r['id'], 'ui_config',
                                         f'JSON 파싱 실패: {e} (원본 앞 60자: {str(raw_cfg)[:60]!r})'))
                    r['ui_config'] = None  # {}(정상·비어있음)과 구분 - degraded 경로로 가되 issues에 남음
            else:
                r['ui_config'] = {}
            r['reading_type_label'] = reading_type_label(r['reading_type'])

            _check_review_status(issues, doc_id, 'discussion_qa', r['id'], r['review_status'])
            _check_source_page(issues, doc_id, 'discussion_qa', r['id'], r['source_page'])
            if r['ui_type'] not in UI_TYPE_ALLOWED:
                issues.append(Issue(doc_id, 'discussion_qa', r['id'], 'ui_type',
                                     f'허용되지 않은 값: {r["ui_type"]!r} (허용: {sorted(UI_TYPE_ALLOWED)})'))
            if not r['question_text'] or not str(r['question_text']).strip():
                issues.append(Issue(doc_id, 'discussion_qa', r['id'], 'question_text', '질문 칸이 비어 있음'))
            else:
                _check_semantic_placeholder(issues, doc_id, 'discussion_qa', r['id'], 'question_text', r['question_text'])
            _check_image_path(issues, doc_id, 'discussion_qa', r['id'], 'excerpt_image_path', r.get('excerpt_image_path'))
            _check_image_path(issues, doc_id, 'discussion_qa', r['id'], 'reference_image_path', r.get('reference_image_path'))
            # ui_type별 필수 구조 확인 - 없어도 blocks.js가 degraded로 낮춰서 렌더는 되지만,
            # "승인됐는데 조판 못 할 데이터"를 미리 알 수 있게 issues로 표시.
            cfg = r['ui_config'] or {}
            if r['ui_type'] in ('choice_ab', 'choice_multi') and len(cfg.get('options') or []) < 2:
                issues.append(Issue(doc_id, 'discussion_qa', r['id'], 'ui_config.options',
                                     f'{r["ui_type"]}인데 options가 {len(cfg.get("options") or [])}개(최소 2개 필요)'))
            if r['ui_type'] == 'text_short_multi' and len(cfg.get('blanks') or []) < 1:
                issues.append(Issue(doc_id, 'discussion_qa', r['id'], 'ui_config.blanks',
                                     'text_short_multi인데 blanks가 없음'))
            if r['ui_type'] == 'table_compare' and len(cfg.get('columns') or []) < 1:
                issues.append(Issue(doc_id, 'discussion_qa', r['id'], 'ui_config.columns',
                                     'table_compare인데 columns가 없음'))
            if r.get('review_status') != 'approved':
                print(f'[경고] {doc_id} discussion_qa#{r["id"]} (order {r["order_label"]}) '
                      f'review_status={r["review_status"]!r} - 승인 안 된 행 포함됨', file=sys.stderr)

        essay = row_to_dict(con.execute('SELECT * FROM essay_prompt WHERE doc_id=?', (doc_id,)).fetchone())
        outline = []
        if essay:
            _check_review_status(issues, doc_id, 'essay_prompt', essay['id'], essay['review_status'])
            _check_source_page(issues, doc_id, 'essay_prompt', essay['id'], essay['source_page'])
            _check_image_path(issues, doc_id, 'essay_prompt', essay['id'], 'image_path', essay.get('image_path'))
            if not essay['main_topic'] or not str(essay['main_topic']).strip():
                issues.append(Issue(doc_id, 'essay_prompt', essay['id'], 'main_topic', '글감 주제가 비어 있음'))
            outline = [row_to_dict(r) for r in con.execute(
                'SELECT * FROM essay_outline_question WHERE essay_id=? ORDER BY order_no', (essay['id'],))]
            for q in outline:
                if not q['question_text'] or not str(q['question_text']).strip():
                    issues.append(Issue(doc_id, 'essay_outline_question', q['id'], 'question_text', '질문 칸이 비어 있음'))

        has_background_only = bool((doc['background_text'] or '').strip()) and not vocab and not ox
        if not vocab and not ox and not (doc['background_text'] or '').strip():
            issues.append(Issue(doc_id, 'documents', doc_id, 'step1',
                                 '어휘·OX·background_text가 전부 비어 있음 - 1단계에 내용이 없음'))

        result = {
            'schema_version': SCHEMA_VERSION,
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
                'background_text': doc['background_text'],
            },
            'step1': {
                'vocab': vocab,
                'ox': ox,
                'background_only': has_background_only,
            },
            'step2': qa_rows,
            'step3': {
                'essay': essay,
                'outline_questions': outline,
            } if essay else None,
            'issues': issues,
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
          f'discussion_qa {len(data["step2"])}건 / step3 {"있음" if data["step3"] else "없음"} / '
          f'issues {len(data["issues"])}건')
    for issue in data['issues']:
        print(f'  [issue] {issue["table"]}#{issue["row_id"]} {issue["field"]}: {issue["reason"]}', file=sys.stderr)


if __name__ == '__main__':
    main()
