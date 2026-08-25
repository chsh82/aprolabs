# -*- coding: utf-8 -*-
"""파서 출력과 골든 샘플을 비교해서 필드별 일치율 계산"""
import json
import sys


_QUOTE_MAP = str.maketrans({'‘': "'", '’': "'", '“': '"', '”': '"'})


def norm(s):
    if s is None:
        return None
    return ' '.join(str(s).translate(_QUOTE_MAP).split())


def compare_list(golden_list, parsed_list, key_field, fields, label):
    results = []
    parsed_by_key = {str(p.get(key_field)): p for p in parsed_list}
    for g in golden_list:
        key = str(g.get(key_field))
        p = parsed_by_key.get(key)
        if p is None:
            results.append((key, None, 'MISSING'))
            continue
        mismatches = [f for f in fields if norm(g.get(f)) != norm(p.get(f))]
        results.append((key, mismatches, 'OK' if not mismatches else 'DIFF'))
    return results


def report(golden_path, parsed_path):
    with open(golden_path, encoding='utf-8') as f:
        golden = json.load(f)
    with open(parsed_path, encoding='utf-8') as f:
        parsed = json.load(f)

    print(f'\n===== {golden_path} vs {parsed_path} =====')

    vocab_r = compare_list(golden['vocabulary'], parsed['vocabulary'], 'order_no', ['word', 'definition', 'book_page'], 'vocab')
    ox_r = compare_list(golden['ox_quiz'], parsed['ox_quiz'], 'order_no', ['question', 'evidence_page'], 'ox')
    disc_r = compare_list(golden['discussion_qa'], parsed['discussion_qa'], 'order_label',
                           ['reading_type', 'question_text', 'ui_type'], 'discussion')

    for name, results, total_fields in [('vocabulary', vocab_r, 3), ('ox_quiz', ox_r, 2), ('discussion_qa', disc_r, 3)]:
        ok = sum(1 for _, _, status in results if status == 'OK')
        print(f'\n[{name}] {ok}/{len(results)} 완전일치')
        for key, mismatches, status in results:
            if status != 'OK':
                print(f'  - {key}: {status} {mismatches}')

    # discussion_qa 필드별 일치율(질문 텍스트는 공백만 다르면 일치로 간주됨)
    field_ok = {'reading_type': 0, 'question_text': 0, 'ui_type': 0}
    total = len(disc_r)
    parsed_by_key = {str(p.get('order_label')): p for p in parsed['discussion_qa']}
    for g in golden['discussion_qa']:
        p = parsed_by_key.get(str(g.get('order_label')))
        if not p:
            continue
        for f in field_ok:
            if norm(g.get(f)) == norm(p.get(f)):
                field_ok[f] += 1
    print(f'\n[discussion_qa 필드별 일치율] (전체 {total}건)')
    for f, ok in field_ok.items():
        print(f'  {f}: {ok}/{total} ({ok/total*100:.0f}%)')


if __name__ == '__main__':
    report(sys.argv[1], sys.argv[2])
