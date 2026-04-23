"""
Vision QA Agent: PDF 페이지 이미지 → Claude Vision → DB 비교 검수

사용법:
  python3 auto_qa_agent.py --file "2024학년도 수능_국어(화작) 문제.pdf"
  python3 auto_qa_agent.py --file "..." --pages 13-16
  python3 auto_qa_agent.py --all
  python3 auto_qa_agent.py --file "..." --dry-run
"""
import os, sys, json, time, argparse, sqlite3, base64, re, difflib
from pathlib import Path
from datetime import datetime

sys.path.insert(0, '/home/chsh82/aprolabs')

DB_PATH    = '/home/chsh82/aprolabs/aprolabs.db'
BASE_DIR   = '/home/chsh82/aprolabs'   # file_path는 이미 'uploads/suneung/...' 포함
DPI        = 200
MODEL      = 'claude-sonnet-4-6'

# ─── Claude 분석 프롬프트 ─────────────────────────────────────────────────────

PROMPT = """이 수능 국어 PDF 페이지를 분석해줘.

각 문항에 대해 아래 정보를 추출해:
1. 문항 번호 (int)
2. 발문(stem) 전문 — "것은?", "것으로 적절하지 않은 것은?" 등 물음 어구 끝까지
3. 보기 유무 — <보기> 레이블 유무와 무관하게, 테두리 박스/글상자가 있으면 true
4. 보기 내용 요약 (있으면 50자 이내, 없으면 null)
5. 선택지 개수 (①~⑤ 중 실제 있는 개수)
6. 이미지/표/그림 유무 (true/false)
7. 이미지 위치 (발문안/보기안/지문안 중 하나, 없으면 null)

이 페이지에 문항이 전혀 없으면 (지문만 있거나 빈 페이지) questions를 빈 배열로 반환.

반드시 아래 JSON 형식으로만 출력 (설명 없이):
{
  "page": <페이지번호>,
  "questions": [
    {
      "number": <int>,
      "stem": "<발문 전문>",
      "has_bogi": <true|false>,
      "bogi_summary": "<50자 이내 요약 or null>",
      "choice_count": <int>,
      "has_image": <true|false>,
      "image_location": "<발문안|보기안|지문안|null>"
    }
  ]
}"""


# ─── DB ───────────────────────────────────────────────────────────────────────

_COMBINE_RE = re.compile(r'국어\s*\(\s*\)')


def is_qa_target(filename: str) -> bool:
    """QA 측정 대상 여부. 합본(국어()) 및 정답해설 제외."""
    if '정답' in filename or '해설' in filename:
        return False
    if _COMBINE_RE.search(filename):
        return False
    return True


def get_jobs(conn, identifier=None):
    cur = conn.cursor()
    if identifier:
        cur.execute(
            "SELECT id, filename, file_path, segments FROM pipeline_jobs "
            "WHERE filename LIKE ? AND segments IS NOT NULL",
            (f'%{identifier}%',)
        )
        rows = cur.fetchall()
        if not rows:
            cur.execute(
                "SELECT id, filename, file_path, segments FROM pipeline_jobs "
                "WHERE id LIKE ? AND segments IS NOT NULL",
                (f'{identifier}%',)
            )
            rows = cur.fetchall()
    else:
        cur.execute(
            "SELECT id, filename, file_path, segments FROM pipeline_jobs "
            "WHERE segments IS NOT NULL ORDER BY source_year, exam_type"
        )
        rows = cur.fetchall()
    return rows


def load_db_segments(segments_json):
    """segments JSON → (questions_dict, passages_list)"""
    if not segments_json:
        return {}, []
    try:
        segs = json.loads(segments_json) if isinstance(segments_json, str) else segments_json
    except Exception:
        return {}, []
    if not segs:
        return {}, []
    if isinstance(segs, dict):
        qs_raw = segs.get('questions', [])
        ps_raw = segs.get('passages', [])
        questions = json.loads(qs_raw) if isinstance(qs_raw, str) else qs_raw
        passages  = json.loads(ps_raw) if isinstance(ps_raw, str) else ps_raw
    else:
        questions, passages = segs, []
    if not questions:
        return {}, passages or []
    return {q['number']: q for q in questions if isinstance(q, dict) and q.get('number')}, passages or []


def load_db_questions(segments_json):
    """segments JSON → {number: question_dict}  (backward compat)"""
    qs, _ = load_db_segments(segments_json)
    return qs


# ─── PDF 렌더링 ───────────────────────────────────────────────────────────────

def render_page_b64(pdf_path: str, page_num: int, dpi: int = DPI) -> str:
    """PDF 1-based 페이지 → base64 PNG"""
    import fitz
    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    data = pix.tobytes("png")
    doc.close()
    return base64.standard_b64encode(data).decode()


def total_pages(pdf_path: str) -> int:
    import fitz
    doc = fitz.open(pdf_path)
    n = len(doc)
    doc.close()
    return n


# ─── Claude 호출 ─────────────────────────────────────────────────────────────

def analyze_page(client, img_b64: str, page_num: int) -> tuple[dict, object]:
    """페이지 이미지 → Vision 분석 결과 dict"""
    resp = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": img_b64,
                    }
                },
                {"type": "text", "text": PROMPT.replace("<페이지번호>", str(page_num))}
            ]
        }]
    )
    raw = resp.content[0].text.strip()
    m = re.search(r'\{[\s\S]+\}', raw)
    if not m:
        raise ValueError(f"JSON 파싱 실패 (p{page_num}): {raw[:300]}")
    return json.loads(m.group(0)), resp.usage


# ─── 비교 ─────────────────────────────────────────────────────────────────────

_QNUM_PREFIX_RE = re.compile(r'^\d+\.\s*[\u3000\s]*')
_HTML_FORMAT_RE = re.compile(r'</?(u|b|i|em|strong|span|s)\b[^>]*>', re.IGNORECASE)

def _normalize_stem(text: str) -> str:
    """공백 정규화 + 문항번호 prefix 제거 + HTML 서식 태그 제거"""
    t = re.sub(r'\s+', ' ', str(text or '')).strip()
    t = _QNUM_PREFIX_RE.sub('', t).strip()
    t = _HTML_FORMAT_RE.sub('', t)      # <u>/<b>/<i> 등 제거 (내용 마커 <보기>·<img> 유지)
    t = re.sub(r'\s+', ' ', t).strip()  # 태그 제거 후 공백 재정규화
    return t

def sim(a, b) -> float:
    a = _normalize_stem(a)
    b = _normalize_stem(b)
    if not a and not b:
        return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def compare(vision_q: dict, db_q: dict | None, db_passages: list = None) -> list[dict]:
    issues = []
    num = vision_q.get('number')

    if db_q is None:
        return [{'field': 'missing', 'issue': 'DB에 문항 없음', 'vision': num, 'db': None}]

    # 발문 유사도
    stem_sim = sim(vision_q.get('stem', ''), db_q.get('stem', ''))
    if stem_sim < 0.6:
        issues.append({
            'field': 'stem',
            'issue': f'발문 불일치 (sim={stem_sim:.2f})',
            'vision': (vision_q.get('stem') or '')[:80],
            'db':     (db_q.get('stem') or '')[:80],
        })

    # 보기 유무
    v_bogi = bool(vision_q.get('has_bogi'))
    d_bogi = bool(db_q.get('bogi'))
    if v_bogi != d_bogi:
        issues.append({
            'field': 'bogi',
            'issue': '보기 유무 불일치',
            'vision': v_bogi,
            'db':     d_bogi,
        })

    # 이미지 유무 (DB stem/bogi/content 내 <img> 태그, 또는 연결된 지문 내 <img>)
    db_has_img = any('<img' in (db_q.get(f) or '') for f in ('stem', 'bogi', 'content'))
    v_has_img  = bool(vision_q.get('has_image'))
    if v_has_img and not db_has_img:
        # 이미지가 지문 안에 있는 경우: 해당 문항의 passage도 확인
        img_location = vision_q.get('image_location', '')
        passage_has_img = False
        if db_passages and img_location in ('지문안', '지문', '지문 내'):
            p_idx = db_q.get('passage_idx')
            if p_idx is not None and 0 <= p_idx < len(db_passages):
                p_content = db_passages[p_idx].get('content', '') or ''
                passage_has_img = '<img' in p_content
            else:
                # passage_idx 없으면 전체 passages에서 <img> 있는지 확인
                passage_has_img = any('<img' in (p.get('content', '') or '') for p in db_passages)
        if not passage_has_img:
            issues.append({
                'field': 'image',
                'issue': 'PDF에 이미지 있으나 DB에 <img> 없음',
                'vision': img_location,
                'db':     False,
            })

    # 선택지 수
    db_choices    = db_q.get('choices') or {}
    db_choice_cnt = len(db_choices) if isinstance(db_choices, dict) else 0
    v_choice_cnt  = vision_q.get('choice_count', 0)
    if db_choice_cnt > 0 and v_choice_cnt > 0 and db_choice_cnt != v_choice_cnt:
        issues.append({
            'field': 'choices',
            'issue': f'선택지 수 불일치',
            'vision': v_choice_cnt,
            'db':     db_choice_cnt,
        })

    return issues


# ─── 메인 로직 ───────────────────────────────────────────────────────────────

def run_job(job_row, page_range=None, dry_run=False, client=None) -> dict:
    job_id, filename, file_path, segments_json = job_row
    pdf_path = os.path.join(BASE_DIR, file_path)

    if not os.path.exists(pdf_path):
        return {'error': f'PDF 없음: {pdf_path}', 'job_id': job_id, 'filename': filename}

    n_pages = total_pages(pdf_path)
    if page_range:
        pages = list(range(page_range[0], min(page_range[1] + 1, n_pages + 1)))
    else:
        pages = list(range(1, n_pages + 1))

    if dry_run:
        est_tokens = len(pages) * 1_300   # ~1300 input tokens per 200dpi page
        est_cost   = est_tokens / 1e6 * 3.0 + len(pages) * 400 / 1e6 * 15.0
        return {
            'dry_run':          True,
            'job_id':           job_id,
            'filename':         filename,
            'total_pdf_pages':  n_pages,
            'pages_to_analyze': len(pages),
            'est_input_tokens': est_tokens,
            'est_cost_usd':     round(est_cost, 3),
        }

    db_qs, db_passages = load_db_segments(segments_json)

    mismatches  = []
    usage_total = {'input_tokens': 0, 'output_tokens': 0}
    t0          = time.time()

    print(f"\n{'='*62}")
    print(f"  {filename}")
    print(f"  페이지 {pages[0]}~{pages[-1]}  ({len(pages)}장) / DB 문항 {len(db_qs)}개")
    print(f"{'='*62}")

    for page_num in pages:
        print(f"  p{page_num:02d}  ", end='', flush=True)
        try:
            img_b64 = render_page_b64(pdf_path, page_num)
            vision, usage = analyze_page(client, img_b64, page_num)
            usage_total['input_tokens']  += usage.input_tokens
            usage_total['output_tokens'] += usage.output_tokens

            page_qs = vision.get('questions', [])
            page_issues = []
            for vq in page_qs:
                num = vq.get('number')
                if not num:
                    continue
                issues = compare(vq, db_qs.get(num), db_passages)
                if issues:
                    page_issues.append({
                        'question_number': num,
                        'page':            page_num,
                        'issues':          issues,
                        'vision_raw':      vq,
                    })

            nums = [q.get('number') for q in page_qs]
            flag = '⚠' if page_issues else '✓'
            print(f"{flag}  문항 {nums or '없음'}  →  {len(page_issues)}건 불일치"
                  f"  (in:{usage.input_tokens} out:{usage.output_tokens})")
            mismatches.extend(page_issues)

        except Exception as e:
            print(f"✗  ERROR: {e}")
            mismatches.append({'page': page_num, 'error': str(e)})

        time.sleep(0.3)

    elapsed = round(time.time() - t0, 1)

    # 카테고리별 집계
    bogi_n  = sum(1 for m in mismatches if any(i['field'] == 'bogi'    for i in m.get('issues', [])))
    img_n   = sum(1 for m in mismatches if any(i['field'] == 'image'   for i in m.get('issues', [])))
    stem_n  = sum(1 for m in mismatches if any(i['field'] == 'stem'    for i in m.get('issues', [])))
    miss_n  = sum(1 for m in mismatches if any(i['field'] == 'missing' for i in m.get('issues', [])))
    cost_usd = round(
        usage_total['input_tokens']  / 1e6 * 3.0 +
        usage_total['output_tokens'] / 1e6 * 15.0,
        4
    )

    return {
        'job_id':    job_id,
        'filename':  filename,
        'pages_analyzed': len(pages),
        'summary': {
            'total_issues':  len(mismatches),
            'bogi_mismatch': bogi_n,
            'image_missing': img_n,
            'stem_mismatch': stem_n,
            'question_missing': miss_n,
        },
        'cost': {
            'input_tokens':  usage_total['input_tokens'],
            'output_tokens': usage_total['output_tokens'],
            'est_usd':       cost_usd,
        },
        'elapsed_sec': elapsed,
        'mismatches':  mismatches,
        'timestamp':   datetime.now().isoformat(),
    }


# ─── CLI ─────────────────────────────────────────────────────────────────────

def load_api_key():
    key = os.environ.get('ANTHROPIC_API_KEY')
    if key:
        return key
    env_file = '/home/chsh82/aprolabs/.env'
    if os.path.exists(env_file):
        for line in open(env_file):
            if line.startswith('ANTHROPIC_API_KEY='):
                return line.strip().split('=', 1)[1]
    raise RuntimeError("ANTHROPIC_API_KEY 없음")


def main():
    parser = argparse.ArgumentParser(description='Vision QA Agent — PDF vs DB 자동 비교')
    parser.add_argument('--file',    help='파일명 (부분 일치 검색)')
    parser.add_argument('--pages',   help='페이지 범위 예: 13-16')
    parser.add_argument('--all',     action='store_true', help='전체 파일 검사')
    parser.add_argument('--dry-run', action='store_true', help='API 호출 없이 비용만 추정')
    args = parser.parse_args()

    if not args.file and not getattr(args, 'all'):
        parser.print_help()
        sys.exit(1)

    page_range = None
    if args.pages:
        parts = args.pages.split('-')
        page_range = (int(parts[0]), int(parts[-1]))

    conn = sqlite3.connect(DB_PATH)

    client = None
    if not args.dry_run:
        import anthropic
        client = anthropic.Anthropic(api_key=load_api_key())

    jobs = get_jobs(conn, args.file if args.file else None)
    if not jobs:
        print(f"매칭되는 파일 없음: {args.file}")
        sys.exit(1)

    if args.file and len(jobs) > 1:
        print("여러 파일 매칭됨:")
        for r in jobs:
            print(f"  {r[0][:8]}  {r[1]}")
        print("파일명을 더 구체적으로 입력하세요.")
        sys.exit(1)

    report_dir = '/home/chsh82/aprolabs/golden_tests'
    os.makedirs(report_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = f'{report_dir}/qa_report_{ts}.json'

    all_results = []
    for job_row in jobs:
        if not is_qa_target(job_row[1]):
            print(f"\n[SKIP] {job_row[1]} (합본/정답해설)")
            continue
        try:
            result = run_job(job_row, page_range=page_range, dry_run=args.dry_run, client=client)
        except Exception as e:
            result = {'job_id': job_row[0], 'filename': job_row[1], 'error': str(e)}
            print(f"\n  ✗ 오류: {e}")

        all_results.append(result)

        if args.dry_run:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            if 'summary' in result:
                s = result['summary']
                print(f"\n  ▶ 요약: 보기불일치={s['bogi_mismatch']}  이미지누락={s['image_missing']}"
                      f"  발문불일치={s['stem_mismatch']}  문항누락={s['question_missing']}")
                print(f"  ▶ 비용: ${result['cost']['est_usd']}"
                      f"  (in:{result['cost']['input_tokens']} out:{result['cost']['output_tokens']})")
                print(f"  ▶ 소요: {result['elapsed_sec']}초")
            # 파일별 즉시 중간 저장 (크래시 대비)
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)

    conn.close()

    if not args.dry_run:
        print(f"\n리포트 저장: {report_path}")


if __name__ == '__main__':
    main()
