"""어휘 뜻풀이 sup 플래그 일괄 보충 - 2026-09-29 사용자 지시 [B].

edition 722(L1-Q1-W01) 검수 중 "낱말의 뜻을 찾아 선으로 이어 보세요" 형식의
원본 페이지는 파싱 단계에서 뜻풀이 텍스트를 못 뽑아내는 것으로 확인됐다
(원본이 "단어 목록"과 "뜻 목록"을 줄로 잇게만 되어 있어, 단어와 뜻이 텍스트로
직접 붙어있지 않음). 이 형식이 momo_book.db 전체에서 78개 문서·381개 단어에
걸쳐 있음(L1 38, L2 36, L6 1, L8 3) - 한 문서씩 원본 스캔을 다시 봐서 손으로
채우기엔 너무 많아, LLM(초등 어휘 수준 표준 사전식 뜻풀이)로 채운 뒤 검수자가
기존 sup 배지("뜻 보충: 검수 필요")로 확인하게 한다.

주의: 여기서 만든 뜻은 원본 스캔을 대조해서 만든 게 아니라 LLM 추정이다.
그래서:
  - momo_book.db(원천)에는 definition을 실제로 채운다 - 이후 재정규화해도
    다시 비지 않게.
  - 이미 만들어진 edition의 layout_json vocab.d도 같이 채우되, sup 플래그는
    True로 남겨둔다(검수 화면 "뜻 보충: 검수 필요" 배지 유지) - edition 722의
    5개 단어(사용자가 원본 스캔으로 직접 확인한 값)와 달리 이건 검수자가 한 번
    보고 승인해야 하는 값이라서 자동으로 resolve하지 않는다.

실행: python scripts/autofill_vocab_definitions.py [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import anthropic

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from edition import store  # noqa: E402

MOMO_BOOK_DB = ROOT.parent / "momo_book_db" / "momo_book.db"

_MODEL = "claude-sonnet-5"

_PROMPT_TMPL = """너는 초등학교 국어 어휘 학습지를 만드는 편집자다. 아래 단어들에
대해 초등학생이 이해할 수 있는 표준 국어사전 스타일의 짧은 뜻풀이를 한 문장으로
만들어라(교과서/사전에 실제로 쓰이는 뜻과 다르게 창작하지 말 것 - 표준적인 뜻을
간결하게 쓸 것). 문서 식별자 "{doc_id}"의 앞자리 숫자(L 다음 숫자)가 학년이다
(예: L1=1학년, L2=2학년) - 학년에 맞는 쉬운 설명을 써라.

단어 목록: {words}

반드시 아래 형식의 JSON 객체 하나만 출력해라(설명·코드블록 없이):
{{"단어1": "뜻풀이", "단어2": "뜻풀이", ...}}"""


def _client() -> anthropic.Anthropic:
    return anthropic.Anthropic()


def _gen_definitions(client: anthropic.Anthropic, doc_id: str, words: list[str]) -> dict[str, str]:
    prompt = _PROMPT_TMPL.format(doc_id=doc_id, words=", ".join(words))
    res = client.messages.create(
        model=os.environ.get("FREEFORM_EDIT_MODEL", _MODEL),
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = "".join(b.text for b in res.content if getattr(b, "text", None)).strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def _patch_editions_for_doc(doc_id: str, defs: dict[str, str], dry_run: bool) -> int:
    """이미 생성된 edition들의 layout_json vocab.d를 채운다(sup는 유지). 패치한 edition 수 반환."""
    conn = store.db.get_connection()
    try:
        rows = conn.execute("SELECT id, layout_json FROM edition WHERE doc_id = ?", (doc_id,)).fetchall()
    finally:
        conn.close()

    patched = 0
    for row in rows:
        layout = json.loads(row["layout_json"])
        ops = []
        for pi, page in enumerate(layout.get("pages", [])):
            if page.get("type") != "vocab":
                continue
            for vi, v in enumerate(page.get("vocab", [])):
                if not v.get("d") and v.get("w") in defs:
                    ops.append({"op": "replace", "path": f"/pages/{pi}/vocab/{vi}/d", "value": defs[v["w"]]})
        if not ops:
            continue
        if dry_run:
            print(f"    [dry-run] edition {row['id']}: {len(ops)}개 op 적용 예정")
            patched += 1
            continue
        store.patch_edition(
            row["id"], ops, editor="vocab-autofill-script",
            reason="선잇기형 원본에서 못 뽑힌 뜻풀이를 LLM으로 보충 - 검수자 확인 필요(sup 유지)",
        )
        patched += 1
    return patched


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="처리할 문서 수 제한(테스트용)")
    args = ap.parse_args()

    conn = sqlite3.connect(MOMO_BOOK_DB)
    conn.row_factory = sqlite3.Row
    doc_rows = conn.execute(
        "SELECT DISTINCT doc_id FROM vocabulary WHERE definition IS NULL OR TRIM(definition) = '' ORDER BY doc_id"
    ).fetchall()
    doc_ids = [r["doc_id"] for r in doc_rows]
    if args.limit:
        doc_ids = doc_ids[: args.limit]

    print(f"대상 문서 {len(doc_ids)}개")
    client = _client()

    total_words = 0
    total_editions = 0
    for doc_id in doc_ids:
        word_rows = conn.execute(
            "SELECT id, word FROM vocabulary WHERE doc_id = ? AND (definition IS NULL OR TRIM(definition) = '')",
            (doc_id,),
        ).fetchall()
        words = [r["word"] for r in word_rows]
        try:
            defs = _gen_definitions(client, doc_id, words)
        except Exception as e:  # noqa: BLE001 - 한 문서 실패해도 나머지는 계속
            print(f"  [실패] {doc_id}: {e}")
            continue

        missing = [w for w in words if w not in defs]
        if missing:
            print(f"  [경고] {doc_id}: LLM이 빠뜨린 단어 {missing}")

        if not args.dry_run:
            for r in word_rows:
                if r["word"] in defs:
                    conn.execute("UPDATE vocabulary SET definition = ? WHERE id = ?", (defs[r["word"]], r["id"]))
            conn.commit()

        n_edited = _patch_editions_for_doc(doc_id, defs, args.dry_run)
        total_words += len(defs)
        total_editions += n_edited
        print(f"  {doc_id}: 단어 {len(defs)}개 보충, edition {n_edited}개 패치")
        time.sleep(0.3)  # API rate 여유

    conn.close()
    print(f"\n완료: 문서 {len(doc_ids)}개, 단어 {total_words}개, edition {total_editions}개"
          f"{' (dry-run - 실제 반영 안 됨)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
