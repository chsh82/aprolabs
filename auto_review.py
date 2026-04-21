"""
auto_review.py — Claude Haiku를 이용한 경고 AI 사전 판정

사용법:
  python3 auto_review.py [--dry-run] [--force] [--cat 카테고리명]

옵션:
  --dry-run   API 호출 없이 프롬프트만 출력
  --force     이미 판정된 항목도 재판정
  --cat CAT   특정 카테고리만 처리 (예: --cat 텍스트불일치)
  --limit N   최대 N건만 처리

결과: raw_result["ai_reviews"][key] = {"judgment": "오탐"|"실제오류"|"보류", "reason": "..."}
"""
import sys
import os
import re
import json
import time
import sqlite3
import argparse
from collections import Counter
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import anthropic

# ──────────────────────────────────────────
# 설정
# ──────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "aprolabs.db")
MODEL   = "claude-haiku-4-5-20251001"

# Haiku 가격 (2025-04 기준, per 1M tokens)
INPUT_PRICE_PER_M  = 0.80
OUTPUT_PRICE_PER_M = 4.00

CALL_DELAY = 0.3   # API 호출 간격 (초)


# ──────────────────────────────────────────
# 카테고리 분류
# ──────────────────────────────────────────
def categorize(msg: str) -> str:
    if "PDF 밑줄 텍스트" in msg and ("못" in msg or "찾" in msg):
        return "밑줄못찾음"
    if "텍스트 불일치" in msg and re.search(r"\[[A-E]\]", msg):
        return "bracket텍스트불일치"
    if re.search(r"\[[A-E]\]", msg) and ("범위 내 텍스트" in msg or "텍스트 미확인" in msg):
        return "bracket텍스트불일치"
    if "끝 위치 특정 불가" in msg or "시작 위치는 찾았" in msg:
        return "bracket텍스트불일치"
    if "선택지" in msg and "불일치" in msg:
        return "텍스트불일치"
    if "텍스트 불일치" in msg:
        return "텍스트불일치"
    if "지문을 PDF에서 찾지 못" in msg or "대응하는 PDF 지문" in msg:
        return "지문못찾음"
    if "문항을 PDF에서 찾지 못" in msg or "PDF에서 해당 문항을" in msg:
        return "문항못찾음"
    return "기타"


# ──────────────────────────────────────────
# 경고별 컨텍스트 추출
# ──────────────────────────────────────────
def _plain(html: str, limit: int = 600) -> str:
    """HTML 태그 제거 후 텍스트 반환."""
    text = re.sub(r"<[^>]+>", "", html or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def build_context(warning: dict, passages: dict, questions: dict) -> tuple[str, str]:
    """(json_text, pdf_text) 반환."""
    loc  = warning["location"]
    msg  = warning["message"]
    cat  = warning["category"]

    # ── 지문 content ──
    p = passages.get(loc, {})
    content_html = p.get("content", "")
    content_plain = _plain(content_html)

    # ── 문항 데이터 ──
    q_m = re.search(r"(\d+)", loc)
    q   = questions.get(q_m.group(1), {}) if q_m else {}
    stem    = (q.get("stem") or "")[:300]
    choices = q.get("choices") or []

    # ── pdf_text: 메시지에서 추출 ──
    pdf_text = ""

    if cat == "텍스트불일치":
        # 메시지에서 페어 추출: 'A' → 'B'
        pairs = re.findall(r"'([^']{1,80})' → '([^']{0,80})'", msg)
        if not pairs:
            pairs = re.findall(r"'([^']{1,80})' -> '([^']{0,80})'", msg)
        if pairs:
            pdf_text = "PDF에서 추출된 텍스트 (변경 전→후):\n"
            for a, b in pairs[:5]:
                pdf_text += f"  · '{a}' → '{b}'\n"
        sim_m = re.search(r"유사도 (\d+)%", msg)
        if sim_m:
            pdf_text = f"유사도: {sim_m.group(1)}%\n" + pdf_text
        json_text = f"JSON 지문 내용 (앞 600자):\n{content_plain}"

    elif cat == "bracket텍스트불일치":
        # [A:START]...[A:END] 마커 포함 구간 추출
        bracket_m = re.search(r"\[([A-E])\]", msg)
        label = bracket_m.group(1) if bracket_m else "?"
        bracket_re = re.compile(
            rf"\[{label}:START\](.*?)\[{label}:END\]", re.DOTALL
        )
        b_match = bracket_re.search(content_html)
        if b_match:
            bracket_text = _plain(b_match.group(1), 400)
            json_text = f"JSON에서 [{label}] 범위 텍스트:\n{bracket_text}"
        else:
            json_text = f"JSON 지문 내용 (앞 600자):\n{content_plain}"
        pdf_text = f"경고 내용: {msg}"

    elif cat == "지문못찾음":
        # content가 비어있거나 이미지만 있을 경우 특이
        has_img  = "<img" in content_html
        is_empty = len(content_plain.strip()) < 20
        json_text = (
            f"JSON 지문 내용 요약:\n"
            f"- 텍스트 길이: {len(content_plain)}자\n"
            f"- 이미지 포함: {'예' if has_img else '아니오'}\n"
        )
        if content_plain and not is_empty:
            json_text += f"- 내용 앞부분: {content_plain[:300]}"
        pdf_text = (
            "PDF에서 해당 위치의 지문을 찾지 못함.\n"
            "가능한 원인: 지문 번호 불일치, 레이아웃 비표준, 이미지 전용 지문"
        )

    elif cat == "문항못찾음":
        choices_str = ""
        if isinstance(choices, list):
            choices_str = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(choices[:5]))
        elif isinstance(choices, dict):
            choices_str = "\n".join(f"  {k}. {v}" for k, v in list(choices.items())[:5])
        json_text = (
            f"JSON 문항 데이터:\n"
            f"stem: {stem or '(없음)'}\n"
            f"선택지:\n{choices_str or '(없음)'}"
        )
        pdf_text = (
            "PDF에서 해당 문항 번호를 찾지 못함.\n"
            "가능한 원인: stem이 너무 짧음, 문제 번호 레이아웃 차이"
        )

    elif cat == "밑줄못찾음":
        # 메시지에서 밑줄 텍스트 추출
        ul_m = re.search(r"못함: '(.+)'$", msg)
        u_text = ul_m.group(1) if ul_m else ""
        json_text = f"JSON 지문 내용 (앞 600자):\n{content_plain}"
        pdf_text = f"PDF 밑줄 텍스트 (JSON에서 못 찾음):\n'{u_text}'"

    else:  # 기타
        json_text = f"JSON 내용:\n{content_plain or stem or '(없음)'}"
        pdf_text  = f"경고 메시지: {msg}"

    return json_text, pdf_text


# ──────────────────────────────────────────
# 프롬프트 생성
# ──────────────────────────────────────────
CATEGORY_GUIDE = {
    "텍스트불일치": (
        "JSON 지문/선택지 텍스트가 PDF와 다름. "
        "전각공백(　), 개행 차이만 있으면 '오탐'. "
        "실제 단어/문장이 다르면 '실제오류'."
    ),
    "bracket텍스트불일치": (
        "[A]~[E] bracket 범위 텍스트가 PDF와 다름. "
        "bracket 마커가 JSON에 없거나 위치가 잘못된 경우 '실제오류'. "
        "JSON 내용이 PDF와 동일하면 '오탐'."
    ),
    "지문못찾음": (
        "PDF에서 지문을 찾지 못함. "
        "JSON 지문이 이미지 전용(텍스트 없음)이면 '오탐'(PDF도 이미지일 가능성). "
        "JSON에 텍스트가 있는데 못 찾으면 '실제오류' 가능."
    ),
    "문항못찾음": (
        "PDF에서 문항을 찾지 못함. "
        "stem이 5자 이하이면 '오탐'(비교 불가 INFO 처리 대상). "
        "stem이 충분한데 못 찾으면 '보류'."
    ),
    "밑줄못찾음": (
        "PDF 밑줄 텍스트를 JSON 지문에서 찾지 못함. "
        "공백/특수문자 차이로 못 찾는 경우가 많음 → '오탐'. "
        "JSON에 해당 텍스트가 완전히 없으면 '실제오류'."
    ),
    "기타": "분류되지 않은 경고. 메시지 내용을 기반으로 판단.",
}


def build_prompt(w: dict, json_text: str, pdf_text: str) -> str:
    guide = CATEGORY_GUIDE.get(w["category"], "")
    return f"""수능 국어 문제 PDF와 JSON 데이터 검증 경고를 판정해줘.

카테고리: {w['category']}
위치: {w['location']}
경고 메시지: {w['message'][:200]}

판정 기준:
{guide}

JSON 데이터:
{json_text}

PDF 추출 데이터:
{pdf_text}

다음 중 하나만 답해 (반드시 이 형식 유지):
판정: 오탐
이유: (한 줄 설명)

또는

판정: 실제오류
이유: (한 줄 설명)

또는

판정: 보류
이유: (한 줄 설명)"""


# ──────────────────────────────────────────
# 응답 파싱
# ──────────────────────────────────────────
def parse_response(text: str) -> tuple[str, str]:
    """(judgment, reason) 반환. judgment: 오탐|실제오류|보류"""
    j_m = re.search(r"판정\s*[:：]\s*(오탐|실제오류|보류)", text)
    r_m = re.search(r"이유\s*[:：]\s*(.+)", text)
    judgment = j_m.group(1) if j_m else "보류"
    reason   = r_m.group(1).strip() if r_m else text.strip()[:100]
    return judgment, reason


# ──────────────────────────────────────────
# 메인
# ──────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="경고 AI 자동 판정")
    parser.add_argument("--dry-run", action="store_true", help="API 호출 없이 프롬프트 출력")
    parser.add_argument("--force",   action="store_true", help="이미 판정된 항목도 재판정")
    parser.add_argument("--cat",     type=str, default="", help="특정 카테고리만 처리")
    parser.add_argument("--limit",   type=int, default=0,  help="최대 처리 건수")
    args = parser.parse_args()

    client = None if args.dry_run else anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"]
    )

    # ── DB 로드 ──
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT id, filename, raw_result, segments
        FROM pipeline_jobs
        WHERE raw_result IS NOT NULL
        ORDER BY filename
    """)
    rows = cur.fetchall()

    # ── 경고 목록 구성 ──
    all_warnings = []
    job_data = {}  # job_id → (raw, segs)

    for job_id, fname, raw_str, segs_str in rows:
        raw  = json.loads(raw_str)
        if not isinstance(raw, dict):
            continue
        segs = json.loads(segs_str) if segs_str else {}
        if not isinstance(segs, dict):
            segs = {}

        # 지문 인덱스 (지문N, pN 양방향)
        passages: dict = {}
        for p in segs.get("passages", []):
            pid = p.get("id", "")
            passages[pid] = p
            if pid.startswith("p") and pid[1:].isdigit():
                passages["지문" + pid[1:]] = p
            if pid.startswith("지문"):
                n = pid[2:]
                if n.isdigit():
                    passages["p" + n] = p

        # 문항 인덱스
        questions: dict = {}
        for q in segs.get("questions", []):
            n = q.get("number")
            if n is not None:
                questions[str(n)] = q

        for c in raw.get("verify_corrections", []):
            if not isinstance(c, dict):
                continue
            if c.get("kind", "").lower() != "warning":
                continue
            loc = c.get("location", "")
            msg = c.get("message", "")
            cat = categorize(msg)
            key = f"{loc}|||{msg[:80]}"

            all_warnings.append({
                "job_id":   job_id,
                "filename": fname,
                "location": loc,
                "message":  msg,
                "category": cat,
                "key":      key,
                "passages": passages,
                "questions": questions,
            })
        job_data[job_id] = (raw, segs)

    # ── 필터 ──
    if args.cat:
        all_warnings = [w for w in all_warnings if w["category"] == args.cat]
    if not args.force:
        filtered = []
        for w in all_warnings:
            raw, _ = job_data[w["job_id"]]
            ai_reviews = raw.get("ai_reviews", {})
            if w["key"] not in ai_reviews:
                filtered.append(w)
        skipped = len(all_warnings) - len(filtered)
        all_warnings = filtered
        if skipped:
            print(f"이미 판정된 {skipped}건 스킵 (--force로 재판정 가능)")
    if args.limit:
        all_warnings = all_warnings[:args.limit]

    total = len(all_warnings)
    print(f"\n처리 대상: {total}건")
    if total == 0:
        print("처리할 경고가 없습니다.")
        return

    # ── 처리 루프 ──
    results: list[dict] = []
    total_input_tokens  = 0
    total_output_tokens = 0
    start_time = time.time()

    for i, w in enumerate(all_warnings, 1):
        json_text, pdf_text = build_context(w, w["passages"], w["questions"])
        prompt = build_prompt(w, json_text, pdf_text)

        prefix = f"[{i:3d}/{total}] {w['category']} | {w['location']} | {w['filename'][-30:]}"
        print(f"\n{prefix}")

        if args.dry_run:
            print("── 프롬프트 ──")
            print(prompt[:600])
            print("── (dry-run: API 호출 생략) ──")
            continue

        try:
            resp = client.messages.create(
                model=MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = resp.content[0].text.strip()
            judgment, reason = parse_response(raw_text)

            total_input_tokens  += resp.usage.input_tokens
            total_output_tokens += resp.usage.output_tokens

            print(f"  → 판정: {judgment} | {reason[:80]}")

            # DB 저장 (job별로 raw_result 업데이트)
            job_id = w["job_id"]
            raw, segs = job_data[job_id]
            ai_reviews = dict(raw.get("ai_reviews", {}))
            ai_reviews[w["key"]] = {"judgment": judgment, "reason": reason}
            raw["ai_reviews"] = ai_reviews
            job_data[job_id] = (raw, segs)

            cur.execute(
                "UPDATE pipeline_jobs SET raw_result = ? WHERE id = ?",
                (json.dumps(raw, ensure_ascii=False), job_id)
            )
            conn.commit()

            results.append({**w, "judgment": judgment, "reason": reason})
            time.sleep(CALL_DELAY)

        except anthropic.RateLimitError:
            print("  ⚠ Rate limit — 10초 대기 후 재시도")
            time.sleep(10)
            # 재시도 1회
            try:
                resp = client.messages.create(
                    model=MODEL, max_tokens=200,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_text = resp.content[0].text.strip()
                judgment, reason = parse_response(raw_text)
                total_input_tokens  += resp.usage.input_tokens
                total_output_tokens += resp.usage.output_tokens
                print(f"  → 판정: {judgment} | {reason[:80]}")
                job_id = w["job_id"]
                raw, segs = job_data[job_id]
                ai_reviews = dict(raw.get("ai_reviews", {}))
                ai_reviews[w["key"]] = {"judgment": judgment, "reason": reason}
                raw["ai_reviews"] = ai_reviews
                job_data[job_id] = (raw, segs)
                cur.execute(
                    "UPDATE pipeline_jobs SET raw_result = ? WHERE id = ?",
                    (json.dumps(raw, ensure_ascii=False), job_id)
                )
                conn.commit()
                results.append({**w, "judgment": judgment, "reason": reason})
            except Exception as e2:
                print(f"  ✗ 재시도 실패: {e2}")
                results.append({**w, "judgment": "보류", "reason": f"API 오류: {e2}"})

        except Exception as e:
            print(f"  ✗ 오류: {e}")
            results.append({**w, "judgment": "보류", "reason": f"오류: {e}"})

    conn.close()

    if args.dry_run:
        print(f"\n[dry-run 완료] {total}건 프롬프트 생성됨. API는 호출하지 않았습니다.")
        return

    # ── 최종 요약 ──
    elapsed = time.time() - start_time
    cost_in  = total_input_tokens  / 1_000_000 * INPUT_PRICE_PER_M
    cost_out = total_output_tokens / 1_000_000 * OUTPUT_PRICE_PER_M
    total_cost = cost_in + cost_out

    print("\n" + "=" * 65)
    print("최종 요약")
    print("=" * 65)

    # 카테고리별 집계
    cat_result: dict[str, Counter] = {}
    for r in results:
        cat = r["category"]
        if cat not in cat_result:
            cat_result[cat] = Counter()
        cat_result[cat][r["judgment"]] += 1

    for cat, ctr in sorted(cat_result.items()):
        total_cat = sum(ctr.values())
        parts = " | ".join(f"{k}: {v}" for k, v in sorted(ctr.items()))
        print(f"  {cat:20s} ({total_cat:2d}건)  {parts}")

    print()
    overall = Counter(r["judgment"] for r in results)
    print(f"  전체 판정:  오탐 {overall.get('오탐',0)}건 | 실제오류 {overall.get('실제오류',0)}건 | 보류 {overall.get('보류',0)}건")
    print()
    print(f"  입력 토큰:  {total_input_tokens:,}")
    print(f"  출력 토큰:  {total_output_tokens:,}")
    print(f"  총 비용:    ${total_cost:.4f}")
    print(f"  소요 시간:  {elapsed:.1f}초")
    print("=" * 65)


if __name__ == "__main__":
    main()
