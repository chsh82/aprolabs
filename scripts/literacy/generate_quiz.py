"""퀴즈 문항 생성 파일럿. docs/literacy/05-퀴즈생성.md 참고.

terms/examples는 읽기만 한다. quiz_items에 저장한다. LLM 호출은 auto_review.py의
anthropic 패턴을 따르되, 모델명은 .env의 LITERACY_QUIZ_MODEL에서 읽는다.

실행:
    python scripts/literacy/generate_quiz.py --pilot      # 10문항만 생성(9-1절)
    python scripts/literacy/generate_quiz.py               # 전체 약 260문항 생성(9-2절)
    python scripts/literacy/generate_quiz.py --dry-run     # API 호출 없이 대상 선정만 확인
"""
from __future__ import annotations

import argparse
import io
import json
import os
import random
import re
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from quiz_prompt import build_batch_prompt  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

SEED = 20260831
BATCH_SIZE = 10
MAX_RETRIES = 3
CALL_DELAY = 1.0
MAX_ANSWER_LEN = 100
QUIZ_TYPE = "뜻풀이선택"
SOURCE = "quiz-gen-pilot"
VOCAB_PER_LEVEL = 30
PROVERB_TARGET = 50
MAX_TOKENS_PER_BATCH = 4000


@dataclass
class SkipLog:
    reason: str
    detail: str


@dataclass
class GenResult:
    inserted: list[dict] = field(default_factory=list)
    skipped: list[SkipLog] = field(default_factory=list)
    batch_failures: list[dict] = field(default_factory=list)
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    prompts_used: list[str] = field(default_factory=list)


# ── 대상 선정 (4절) ──

def select_vocab_targets(conn: sqlite3.Connection, per_level: int, seed: int) -> tuple[list[dict], dict[int, int]]:
    targets = []
    shortfall = {}
    for level in range(0, 7):
        rows = conn.execute(
            """
            SELECT id, headword, definition, level, sense_category, source
            FROM terms
            WHERE category = '어휘' AND level = ?
              AND definition IS NOT NULL AND definition != ''
              AND (review_status IS NULL OR review_status != '보류')
            ORDER BY id
            """,
            (level,),
        ).fetchall()
        rng = random.Random(seed + level)
        pool = list(rows)
        rng.shuffle(pool)
        picked = pool[:per_level]
        if len(picked) < per_level:
            shortfall[level] = per_level - len(picked)
        for r in picked:
            targets.append({
                "term_id": r[0], "headword": r[1], "definition": r[2],
                "level": r[3], "sense_category": r[4], "source": r[5],
                "category": "어휘",
            })
    return targets, shortfall


def select_proverb_targets(conn: sqlite3.Connection, limit: int, seed: int) -> tuple[list[dict], int]:
    rows = conn.execute(
        """
        SELECT id, headword, definition, level, sense_category, source
        FROM terms
        WHERE category = '속담'
          AND definition IS NOT NULL AND definition != ''
          AND (review_status IS NULL OR review_status != '보류')
        ORDER BY id
        """
    ).fetchall()
    rng = random.Random(seed + 100)
    pool = list(rows)
    rng.shuffle(pool)
    picked = pool[:limit]
    shortfall = max(0, limit - len(picked))
    targets = [{
        "term_id": r[0], "headword": r[1], "definition": r[2],
        "level": r[3], "sense_category": r[4], "source": r[5],
        "category": "속담",
    } for r in picked]
    return targets, shortfall


def filter_long_definitions(targets: list[dict], max_len: int) -> tuple[list[dict], list[SkipLog]]:
    kept, skipped = [], []
    for t in targets:
        if len(t["definition"]) > max_len:
            skipped.append(SkipLog(
                reason="정답 60자 초과",
                detail=f"{t['headword']} ({len(t['definition'])}자): {t['definition'][:40]}...",
            ))
        else:
            kept.append(t)
    return kept, skipped


def exclude_already_generated(conn: sqlite3.Connection, targets: list[dict]) -> list[dict]:
    existing = {row[0] for row in conn.execute(
        "SELECT term_id FROM quiz_items WHERE quiz_type = ?", (QUIZ_TYPE,)
    ).fetchall()}
    return [t for t in targets if t["term_id"] not in existing]


# ── 배치 구성 (5-5절: 같은 sense_category 안에서 묶는다) ──

def make_batches(targets: list[dict]) -> list[tuple[str | None, list[dict]]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in targets:
        if t["category"] == "속담":
            key = "속담"
        elif t["sense_category"]:
            key = f"sense:{t['sense_category']}"
        else:
            key = f"level:{t['level']}"
        groups[key].append(t)

    batches = []
    for key, items in groups.items():
        for i in range(0, len(items), BATCH_SIZE):
            chunk = items[i:i + BATCH_SIZE]
            label = None
            if key.startswith("sense:"):
                label = f"sense_category={key[6:]}"
            elif key.startswith("level:"):
                label = f"레벨 {key[6:]} (공통 sense_category 없음)"
            batches.append((label, chunk))
    return batches


# ── LLM 호출 ──

def _extract_json(text: str) -> dict:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if m:
        text = m.group(1).strip()
    return json.loads(text)


def call_batch(client, model: str, label: str | None, batch: list[dict]) -> tuple[dict[int, list[str]], int, int, str]:
    prompt = build_batch_prompt(batch, context_label=label)
    resp = client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS_PER_BATCH,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_text = resp.content[0].text
    parsed = _extract_json(raw_text)
    items = {int(it["term_id"]): it["distractors"] for it in parsed["items"]}

    expected_ids = {t["term_id"] for t in batch}
    got_ids = set(items.keys())
    if got_ids != expected_ids:
        raise ValueError(f"term_id 불일치: 기대={expected_ids}, 실제={got_ids}")
    for tid, distractors in items.items():
        if not isinstance(distractors, list) or len(distractors) != 3:
            raise ValueError(f"term_id {tid}: 오답이 3개가 아님 ({distractors})")
        for d in distractors:
            if not d or not str(d).strip():
                raise ValueError(f"term_id {tid}: 빈 오답")

    return items, resp.usage.input_tokens, resp.usage.output_tokens, prompt


def generate_with_retry(client, model: str, label: str | None, batch: list[dict], result: GenResult) -> dict[int, list[str]] | None:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            items, in_tok, out_tok, prompt = call_batch(client, model, label, batch)
            result.total_input_tokens += in_tok
            result.total_output_tokens += out_tok
            result.prompts_used.append(prompt)
            return items
        except Exception as e:  # noqa: BLE001
            last_err = e
            print(f"    [시도 {attempt}/{MAX_RETRIES} 실패] {e}")
            if attempt < MAX_RETRIES:
                time.sleep(CALL_DELAY * 2)
    result.batch_failures.append({
        "label": label,
        "term_ids": [t["term_id"] for t in batch],
        "error": str(last_err),
    })
    return None


# ── 문항 구성/저장 ──

def build_question(headword: str, category: str) -> str:
    noun = "낱말" if category == "어휘" else "속담"
    return f"다음 {noun}의 뜻으로 알맞은 것을 고르세요: '{headword}'"


_shuffle_rng = random.Random()  # 시드 없음(OS 엔트로피) - 대상 추출용 SEED와 절대 공유하지 않는다


def compute_position(correct: str, distractors: list[str]) -> tuple[list[str], int]:
    """엑셀 출력·품질 검사를 위한 위치를 계산한다. DB에는 순서를 저장하지 않으므로
    이 결과는 이번 실행에서만 쓰인다 - 재현성이 필요 없고, 오히려 대상 추출 SEED와
    분리된 독립적인 무작위성이 필요하다(위치가 term_id·추출 시드에 종속되면 안 됨)."""
    options = [correct] + list(distractors)
    _shuffle_rng.shuffle(options)
    return options, options.index(correct) + 1


def insert_quiz_item(conn: sqlite3.Connection, item: dict, distractors: list[str], model: str, now: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO quiz_items
            (term_id, quiz_type, question, correct_answer, distractors,
             difficulty, generated_by, model, review_status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'api', ?, '검수전', ?)
        """,
        (
            item["term_id"], QUIZ_TYPE, build_question(item["headword"], item["category"]),
            item["definition"], json.dumps(distractors, ensure_ascii=False),
            item["level"], model, now,
        ),
    )
    return cur.lastrowid


# ── 실행 ──

def run(pilot: bool, dry_run: bool) -> GenResult:
    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA foreign_keys=ON")

    vocab_targets, vocab_shortfall = select_vocab_targets(conn, VOCAB_PER_LEVEL, SEED)
    proverb_targets, proverb_shortfall = select_proverb_targets(conn, PROVERB_TARGET, SEED)

    if pilot:
        vocab_targets = vocab_targets[:BATCH_SIZE]
        proverb_targets = []

    all_targets = vocab_targets + proverb_targets
    all_targets, too_long = filter_long_definitions(all_targets, MAX_ANSWER_LEN)
    all_targets = exclude_already_generated(conn, all_targets)

    result = GenResult()
    result.skipped.extend(too_long)
    for level, n in vocab_shortfall.items():
        result.skipped.append(SkipLog("레벨별 30건 부족", f"level {level}: {n}건 부족"))
    if proverb_shortfall:
        result.skipped.append(SkipLog("속담 50건 부족", f"{proverb_shortfall}건 부족"))

    if dry_run:
        print(f"대상: 어휘 {len([t for t in all_targets if t['category']=='어휘'])}건, "
              f"속담 {len([t for t in all_targets if t['category']=='속담'])}건 "
              f"(이미 생성된 term_id 제외 후)")
        for s in result.skipped:
            print(f"  [스킵] {s.reason}: {s.detail}")
        conn.close()
        return result

    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    model = os.environ["LITERACY_QUIZ_MODEL"]

    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    cur = conn.execute(
        "INSERT INTO collection_runs (source, started_at, status) VALUES (?, ?, 'running')",
        (SOURCE, now),
    )
    run_id = cur.lastrowid
    conn.commit()

    batches = make_batches(all_targets)
    try:
        for bi, (label, batch) in enumerate(batches, 1):
            print(f"[배치 {bi}/{len(batches)}] {label or '(레이블 없음)'} - {len(batch)}건")
            items = generate_with_retry(client, model, label, batch, result)
            if items is None:
                print("    3회 실패 - 배치 건너뜀")
                continue

            by_id = {t["term_id"]: t for t in batch}
            insert_now = datetime.now().isoformat(sep=" ", timespec="seconds")
            for term_id, distractors in items.items():
                item = by_id[term_id]
                insert_quiz_item(conn, item, distractors, model, insert_now)
                _, position = compute_position(item["definition"], distractors)
                result.inserted.append({**item, "distractors": distractors, "position": position, "model": model})
            conn.commit()
            time.sleep(CALL_DELAY)

        conn.execute(
            "UPDATE collection_runs SET finished_at = ?, fetched_count = ?, inserted_count = ?, status = 'success' WHERE id = ?",
            (datetime.now().isoformat(sep=" ", timespec="seconds"), len(all_targets), len(result.inserted), run_id),
        )
        conn.commit()
    except Exception as e:
        conn.execute(
            "UPDATE collection_runs SET finished_at = ?, status = 'failed', error = ? WHERE id = ?",
            (datetime.now().isoformat(sep=" ", timespec="seconds"), str(e), run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()

    return result


# ── 8절 품질 검사 ──

def _ends_with_period(s: str) -> bool:
    return s.rstrip().endswith((".", "。"))


def quality_report(inserted: list[dict]) -> dict:
    if not inserted:
        return {}

    n = len(inserted)
    pos_counts = defaultdict(int)
    longest_count = 0
    period_match_count = 0
    ending_match_count = 0
    length_ratios = []
    dup_count = 0

    for item in inserted:
        correct = item["definition"]
        distractors = item["distractors"]
        options = [correct] + distractors
        pos_counts[item["position"]] += 1

        lens = [len(o) for o in options]
        if lens.count(max(lens)) == 1 and lens.index(max(lens)) == 0:
            longest_count += 1

        periods = [_ends_with_period(o) for o in options]
        if len(set(periods)) == 1:
            period_match_count += 1

        endings = [o.rstrip()[-1] if o.rstrip() else "" for o in options]
        if len(set(endings)) == 1:
            ending_match_count += 1

        other_lens = lens[1:]
        avg_other = sum(other_lens) / len(other_lens)
        if avg_other:
            length_ratios.append(avg_other / lens[0])

        norm = [d.strip() for d in distractors]
        if len(set(norm)) < len(norm):
            dup_count += 1

    return {
        "n": n,
        "position_distribution": {k: (v, v / n * 100) for k, v in sorted(pos_counts.items())},
        "longest_ratio": longest_count / n * 100,
        "ending_match_rate": ending_match_count / n * 100,
        "period_match_rate": period_match_count / n * 100,
        "avg_length_ratio": sum(length_ratios) / len(length_ratios) if length_ratios else 0,
        "duplicate_distractor_count": dup_count,
    }


def print_quality_report(qr: dict, batch_failures: list[dict], skipped: list[SkipLog]) -> None:
    if not qr:
        print("생성된 문항이 없음 - 품질 검사 생략")
        return
    print(f"\n=== 8절 품질 검사 (n={qr['n']}) ===")
    print("정답 위치 분포:")
    for pos, (cnt, pct) in qr["position_distribution"].items():
        print(f"  {pos}번: {cnt}건 ({pct:.1f}%)")
    print(f"정답 최장 비율: {qr['longest_ratio']:.1f}% (40% 초과면 단서 노출)")
    print(f"종결형 일치율: {qr['ending_match_rate']:.1f}%")
    print(f"마침표 일치율: {qr['period_match_rate']:.1f}%")
    print(f"길이 편차(오답평균/정답): {qr['avg_length_ratio']:.2f}")
    print(f"오답 중복 문항 수: {qr['duplicate_distractor_count']}")
    print(f"\n생성 실패 배치: {len(batch_failures)}건")
    for bf in batch_failures:
        print(f"  {bf['label']}: term_ids={bf['term_ids']} - {bf['error']}")
    print(f"\n스킵된 항목: {len(skipped)}건")
    for s in skipped:
        print(f"  [{s.reason}] {s.detail}")

    if qr["longest_ratio"] > 40:
        print("\n*** 경고: 정답 최장 비율이 40%를 초과했습니다. 지시서 8절에 따라 "
              "적재를 재검토해야 합니다(이미 삽입된 항목은 review_status='검수전'으로 "
              "남아 있으니 검수 단계에서 걸러야 합니다). ***")


# ── 7절 검수용 엑셀 ──

def export_review_excel(inserted: list[dict]) -> Path:
    import openpyxl

    wb = openpyxl.Workbook()
    ws_vocab = wb.active
    ws_vocab.title = "어휘"
    ws_vocab.append(["문항ID", "레벨", "카테고리", "출처", "단어", "정답", "오답1", "오답2", "오답3", "정답위치", "판정", "메모"])
    ws_proverb = wb.create_sheet("속담")
    ws_proverb.append(["문항ID", "레벨", "카테고리", "출처", "단어", "정답", "오답1", "오답2", "오답3", "정답위치", "판정", "메모"])

    ordered = sorted(inserted, key=lambda x: ((x["level"] is None, x["level"]), x["category"]))
    for item in ordered:
        row = [
            item["term_id"], item["level"], item["category"], item["source"],
            item["headword"], item["definition"], *item["distractors"],
            item["position"], "", "",
        ]
        (ws_vocab if item["category"] == "어휘" else ws_proverb).append(row)

    out_dir = REPO_ROOT / "data" / "literacy"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"quiz_review_{datetime.now().strftime('%Y%m%d')}.xlsx"
    wb.save(out_path)
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="퀴즈 문항 생성")
    parser.add_argument("--pilot", action="store_true", help="10문항만 생성(9-1절 소량 테스트)")
    parser.add_argument("--dry-run", action="store_true", help="API 호출 없이 대상 선정만 확인")
    args = parser.parse_args()

    result = run(pilot=args.pilot, dry_run=args.dry_run)

    if args.dry_run:
        return 0

    if args.pilot:
        print(f"\n=== 파일럿 {len(result.inserted)}문항 전문 ===")
        for item in result.inserted:
            print(f"\n단어: {item['headword']} (level={item['level']})")
            print(f"  정답: {item['definition']}")
            for i, d in enumerate(item["distractors"], 1):
                print(f"  오답{i}: {d}")
            print(f"  정답위치: {item['position']}")
        if result.prompts_used:
            print("\n=== 사용한 프롬프트(첫 배치) ===")
            print(result.prompts_used[0])
        print(f"\n모델: {os.environ.get('LITERACY_QUIZ_MODEL')}")
        print(f"입력 토큰 합계: {result.total_input_tokens}, 출력 토큰 합계: {result.total_output_tokens}")
        if result.inserted:
            print(f"호출당 평균 입력 토큰: {result.total_input_tokens / max(1, len(result.prompts_used)):.0f}")

    qr = quality_report(result.inserted)
    print_quality_report(qr, result.batch_failures, result.skipped)

    if not args.pilot and result.inserted:
        out_path = export_review_excel(result.inserted)
        print(f"\n검수용 엑셀: {out_path}")

    print(f"\n총 생성: {len(result.inserted)}건 "
          f"(어휘 {sum(1 for i in result.inserted if i['category']=='어휘')}, "
          f"속담 {sum(1 for i in result.inserted if i['category']=='속담')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
