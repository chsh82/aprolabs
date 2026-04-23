"""
create_qa_baseline.py — auto_qa_agent 기반 QA 상태 baseline 생성
=================================================================
최신 QA 리포트(golden_tests/qa_report_*.json)를 집계하여:
  1. golden_tests/qa_baseline_YYYYMMDD.json  (JSON 스냅샷)
  2. baseline_report_YYYYMMDD.md             (사람이 읽는 요약)

사용법:
  python3 create_qa_baseline.py [--force-rerun]

  --force-rerun: 오래된 리포트(N시간 초과)가 있으면 경고 출력
"""
import json, glob, os, subprocess, sys, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

GOLDEN_DIR  = '/home/chsh82/aprolabs/golden_tests'
REPORT_DIR  = '/home/chsh82/aprolabs'
STALE_HOURS = 24   # 이보다 오래된 리포트는 '오래됨' 경고

# ── 알려진 이슈 (수동 기록) ───────────────────────────────────────────────
KNOWN_ISSUES = {
    '2026 수능 국어(언매) 문제.pdf': [
        {'q': 16, 'field': 'choices', 'type': '구조적한계',
         'desc': '학습 활동지 표 형식 → segmenter가 선택지 번호만 추출, 내용 없음 (4/5건)'},
        {'q': 40, 'field': 'stem',    'type': 'Gemini비결정성',
         'desc': 'Gemini가 stem 빈 값 반환 (오탐), DB 정상'},
    ],
}

GLOBAL_LIMITATIONS = [
    '**Gemini Vision 비결정성**: 발문불일치 metric이 실행마다 0~16건으로 변동. 신뢰 불가.',
    '**segmenter `<보기>` 잘림 버그**: 2026-04-20 fix 시도 후 당일 revert (오매칭 위험). 패치 스크립트로 개별 처리.',
    '**학습 활동지 표 형식 선택지**: segmenter가 선택지 번호만 추출, 내용 없음 (2026 언매 Q16류).',
    '**bracket 텍스트 불일치**: auto_review.py가 PDF bracket 원문을 AI에 미전달 → 자동 판정 불가.',
    '**Reset→Start 재처리 시 패치 소실**: patch_empty_stem / patch_bogi_hwajak 결과가 segments 재생성으로 사라짐.',
]

TODAY_CHANGES = [
    {'no': 1, 'what': '`auto_qa_agent.py` `_normalize_stem()` 추가 (문항번호 prefix 제거)',
     'scope': '전체', 'effect': '발문불일치 오탐 제거'},
    {'no': 2, 'what': '`patch_bogi_hwajak.py` 범용화 (`--file`, `--targets`)',
     'scope': '2024/2026 화작, 2026 언매 Q16/21/41/42', 'effect': '보기불일치 해소'},
    {'no': 3, 'what': '`patch_empty_stem.py` 신규 작성 (bogi→stem 재구성)',
     'scope': '2024 화작 12건, 2026 언매 11건', 'effect': '빈 stem → 정상화'},
    {'no': 4, 'what': '`apply_structure_to_text()` used_labels 추적 로직 추가',
     'scope': 'vision_analyzer.py', 'effect': '레이블 충돌 방지'},
    {'no': 5, 'what': 'STRUCTURE_PROMPT "시각적 박스" 조항 추가 → **롤백**',
     'scope': 'vision_analyzer.py', 'effect': '발문불일치 악화 방지 (3→17건 악화 확인 후 롤백)'},
]

# ── 유틸 ──────────────────────────────────────────────────────────────────
def get_git_info():
    try:
        cwd = '/home/chsh82/aprolabs'
        commit = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'], cwd=cwd).decode().strip()
        msg = subprocess.check_output(
            ['git', 'log', '-1', '--format=%s'], cwd=cwd).decode().strip()
        date = subprocess.check_output(
            ['git', 'log', '-1', '--format=%ci'], cwd=cwd).decode().strip()
        return {'commit': commit, 'message': msg, 'date': date}
    except Exception as e:
        return {'commit': 'unknown', 'message': str(e), 'date': ''}


def get_latest_reports():
    """파일명 기준 최신 QA 리포트 1개씩 수집."""
    paths = sorted(glob.glob(f'{GOLDEN_DIR}/qa_report_*.json'))
    latest = {}   # filename → {'ts': str, 'path': str, 'data': dict}

    for path in paths:
        try:
            with open(path, encoding='utf-8') as f:
                reports = json.load(f)
            ts = os.path.basename(path).replace('qa_report_', '').replace('.json', '')
            for file_result in reports:
                fname = file_result.get('filename', '')
                if not fname:
                    continue
                if fname not in latest or ts > latest[fname]['ts']:
                    latest[fname] = {'ts': ts, 'path': path, 'data': file_result}
        except Exception as e:
            print(f"  SKIP {os.path.basename(path)}: {e}")

    return latest


def ts_to_dt(ts: str):
    """'20260423_040500' → datetime"""
    try:
        return datetime.strptime(ts, '%Y%m%d_%H%M%S')
    except Exception:
        return None


def summarize(file_result: dict):
    """mismatches → 유형별 집계 + 상세 목록."""
    counts = defaultdict(int)
    details = []
    for m in file_result.get('mismatches', []):
        q = m.get('question_number')
        for issue in m.get('issues', []):
            field = issue.get('field', '')
            counts[field] += 1
            details.append({
                'q': q,
                'field': field,
                'vision': str(issue.get('vision', ''))[:80],
                'db':     str(issue.get('db',     ''))[:80],
            })
    return dict(counts), details


# ── 메인 ─────────────────────────────────────────────────────────────────
def main():
    git  = get_git_info()
    rpts = get_latest_reports()
    now  = datetime.now()

    print(f"Git commit : {git['commit']}  ({git['message']})")
    print(f"리포트 파일: {len(rpts)}개\n")

    baseline = {
        'created_at':  now.isoformat(timespec='seconds'),
        'git_commit':  git['commit'],
        'git_message': git['message'],
        'git_date':    git['date'],
        'files':       [],
        'global_known_limitations': GLOBAL_LIMITATIONS,
        'today_changes': TODAY_CHANGES,
    }

    md_rows   = []
    stale     = []
    totals    = defaultdict(int)

    for fname, info in sorted(rpts.items()):
        ts      = info['ts']
        data    = info['data']
        counts, details = summarize(data)

        bogi   = counts.get('bogi',           0)
        img    = counts.get('image_location', 0)
        stem   = counts.get('stem',           0)
        choices= counts.get('choices',        0)
        q_miss = counts.get('question',       0)

        totals['bogi']    += bogi
        totals['img']     += img
        totals['stem']    += stem
        totals['choices'] += choices
        totals['q_miss']  += q_miss

        # 오래된 리포트 체크
        dt = ts_to_dt(ts)
        age_mark = ''
        if dt and (now - dt) > timedelta(hours=STALE_HOURS):
            stale.append(fname)
            age_mark = ' ⚠오래됨'

        known = KNOWN_ISSUES.get(fname, [])
        baseline['files'].append({
            'filename':        fname,
            'report_ts':       ts,
            'report_path':     info['path'],
            'summary': {
                'bogi_mismatch':     bogi,
                'image_missing':     img,
                'stem_mismatch':     stem,
                'choices_mismatch':  choices,
                'question_missing':  q_miss,
            },
            'mismatch_details': details,
            'known_issues':    known,
        })

        status = '✅' if (bogi + img + q_miss) == 0 else '⚠'
        short  = fname[-50:]
        md_rows.append(
            f"| {status} | `{short}` | {bogi} | {img} | {stem} | {choices} | {ts}{age_mark} |"
        )

        print(f"  {status} {short:<52} 보기={bogi} 이미지={img} 발문={stem} 선택지={choices}{age_mark}")

    baseline['totals'] = dict(totals)

    # ── JSON 저장 ──────────────────────────────────────────────────────────
    date_str = now.strftime('%Y%m%d')
    json_path = f'{GOLDEN_DIR}/qa_baseline_{date_str}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(baseline, f, ensure_ascii=False, indent=2)
    print(f"\nQA baseline → {json_path}")

    # ── Markdown 생성 ──────────────────────────────────────────────────────
    md_path = f'{REPORT_DIR}/baseline_report_{date_str}.md'

    known_file_section = ''
    for fname, issues in KNOWN_ISSUES.items():
        known_file_section += f'\n**`{fname}`**\n'
        for iss in issues:
            known_file_section += f"- Q{iss['q']} `[{iss['field']}]` {iss['desc']} — *{iss['type']}*\n"

    changes_rows = '\n'.join(
        f"| {c['no']} | {c['what']} | {c['scope']} | {c['effect']} |"
        for c in TODAY_CHANGES
    )
    limits_list = '\n'.join(f'{i+1}. {lim}' for i, lim in enumerate(GLOBAL_LIMITATIONS))

    stale_note = ''
    if stale:
        stale_note = f'\n> ⚠ 오래된 리포트({STALE_HOURS}시간 초과): {", ".join(s[-30:] for s in stale)}\n'

    md = f"""# 파이프라인 QA Baseline — {now.strftime('%Y-%m-%d')}

## 메타데이터

| 항목 | 값 |
|------|----|
| 생성 시점 | {now.strftime('%Y-%m-%d %H:%M')} KST |
| Git commit | `{git['commit']}` |
| Commit 메시지 | {git['message']} |
| Commit 날짜 | {git['date']} |
| 대상 파일 수 | {len(rpts)} |

---

## 파일별 QA 결과
{stale_note}
| 상태 | 파일명 | 보기불일치 | 이미지누락 | 발문불일치 | 선택지이슈 | 리포트 시각 |
|------|--------|:----------:|:----------:|:----------:|:----------:|-------------|
{chr(10).join(md_rows)}

### 전체 합계

| 보기불일치 | 이미지누락 | 발문불일치 | 문항누락 |
|:----------:|:----------:|:----------:|:--------:|
| **{totals['bogi']}** | **{totals['img']}** | **{totals['stem']}** | **{totals['q_miss']}** |

---

## 오늘 수행한 주요 수정 (2026-04-23)

| # | 변경 내용 | 대상 | 효과 |
|---|-----------|------|------|
{changes_rows}

---

## 알려진 이슈

### 파일별 이슈
{known_file_section}

### 구조적 한계

{limits_list}

---

## 재현성 주의사항

- **Reset→Start 재처리 시 패치 소실**: `patch_empty_stem.py`, `patch_bogi_hwajak.py` 결과는
  segments 재생성으로 사라짐 → 재처리 후 패치 스크립트 재실행 필요
- **Gemini 비결정성**: QA 수치가 실행마다 ±3건 수준으로 변동 가능 (발문불일치 특히 불안정)
- **DB 백업 필수**: Reset 전 `cp aprolabs.db aprolabs_backup_$(date +%Y%m%d).db`

---

## 참고 명령

```bash
# 이 baseline과 현재 DB 상태 비교 (verify_corrections 기반)
python3 compare_baseline.py golden_tests/baseline_{date_str}.json

# 특정 파일 QA 재실행
python3 auto_qa_agent.py --file "파일명.pdf"
```
"""

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md)
    print(f"Markdown    → {md_path}")

    print(f"\n=== 전체 합계 ===")
    print(f"  보기불일치: {totals['bogi']}")
    print(f"  이미지누락: {totals['img']}")
    print(f"  발문불일치: {totals['stem']}")
    print(f"  문항누락:   {totals['q_miss']}")
    if stale:
        print(f"\n  ⚠ 오래된 리포트 {len(stale)}개 — QA 재실행 권장")


if __name__ == '__main__':
    main()
