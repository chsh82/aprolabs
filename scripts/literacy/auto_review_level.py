"""속담·관용구·사자성어(level IS NULL) 레벨 자동 부여 - 원칙적으로 완전자동
(사람 확인 없음), 단 학년 근거가 부족한 판정만 예외적으로 보류.

사용자 결정(2026-09-02): 검수 UI(수동)와 별개로, Gemini가 판정하면 사람
확인 없이 바로 확정한다. 다만 "누가/무엇이 판정했는지"는 note에 남겨서
나중에 구분할 수 있게 한다 - review_status='검수완료'가 사람 검수인지
AI 자동 판정인지 note로만 구분 가능(스키마는 안 바꿈).

정책 갱신(2026-09-24, `docs/literacy/07-학년경계정책-L5L6.md` 참고): 학년
경계를 L5=고1/L6=고2~3로 통일하면서, Gemini에게 판정마다 `grounded`(학년
근거가 실제로 있는지)도 같이 표시하게 했다. `grounded=false`(뜻과 난이도로
대략 짐작만 한 경우)면 level 값은 채우되 review_status를 '검수완료'가 아니라
'보류'로 남긴다 - "AI가 레벨만 추측해도 곧바로 검수완료로 넘어가지 않게"
하는 안전장치다(`save_result()` 참고). grounded=true(또는 level=null인
"해당없음")는 기존과 동일하게 즉시 확정한다. **이 정책 갱신은 앞으로 이
스크립트를 실행할 때만 적용된다 - 과거에 이미 '검수완료'로 저장된 판정
(2,944건)은 소급 재분류하지 않는다.**

대상: terms WHERE category IN ('속담','관용구','사자성어') AND level IS NULL
      AND definition IS NOT NULL AND review_status != '제외'
      (사자성어 225건 중 정의가 없는 6건은 review_status='보류'라 여기서
      같이 걸릴 뻔했는데, definition IS NOT NULL 조건으로 제외했다 - 정의가
      생기기 전에는 레벨을 판단할 근거가 없다)

레벨 0~6 중 하나 또는 "해당없음"(교육과정에 낼 만하지 않음 - review_UI의
"해당없음" 버튼과 같은 의미) 판정. "해당없음"이면 review_status='제외'.
레벨이 있으면 grounded 여부에 따라 review_status가 '검수완료' 또는 '보류'로
갈린다(위 정책 갱신 참고).

실행:
    python auto_review_level.py --dry-run          # 10건만 판정해서 출력, 저장 안 함
    python auto_review_level.py --limit 50          # 50건만 실제 저장
    python auto_review_level.py                      # 전체 실행
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gemini_client import call_gemini_json, get_client  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

BATCH_SIZE = 20

LEVEL_TABLE = """| level | 학년 |
|---|---|
| 0 | 초1~2 |
| 1 | 초3~4 |
| 2 | 초5~6 |
| 3 | 중1~2 |
| 4 | 중3 |
| 5 | 고1 |
| 6 | 고2~3 |"""


def select_targets(conn: sqlite3.Connection, limit: int | None) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, headword, definition, category
        FROM terms
        WHERE category IN ('속담','관용구','사자성어') AND level IS NULL
          AND definition IS NOT NULL AND review_status != '제외'
        ORDER BY id
        """
    ).fetchall()
    targets = [{"id": r[0], "headword": r[1], "definition": r[2], "category": r[3]} for r in rows]
    return targets[:limit] if limit else targets


def build_prompt(batch: list[dict]) -> str:
    parts = [
        "너는 한국 초중고 국어 교육과정 관점에서 속담·관용구에 학습 레벨을 부여한다.",
        "",
        "레벨 대응표:",
        LEVEL_TABLE,
        "",
        "판단 기준:",
        "- 얼마나 널리 쓰이고 익숙한 표현인지, 학교 교과서/국어 학습에 등장할 만한지로 판단한다",
        "- 뜻이 어렵거나 한자어가 많이 섞여 있으면 레벨이 높다",
        "- 초중고 교육과정에 낼 만한 표현이 아니면(너무 저속하거나, 사장된 표현이거나, "
        "교육적으로 부적절하면) \"해당없음\"으로 판정한다",
        "",
        "아래 각 항목에 대해 레벨(0~6) 또는 \"해당없음\"을 판정하고, 한 줄 근거를 달아라. "
        "그리고 그 판정이 실제로 학년 근거가 있는 추정인지 스스로 표시하라(grounded):",
        "- grounded=true: 이 표현이 특정 학년군에서 흔히 쓰이거나 교과서/교육과정에 등장한다고 "
        "구체적으로 알고 있어서 레벨을 확정할 수 있다",
        "- grounded=false: 뜻과 난이도로 대략 짐작만 할 뿐, 어느 학년군에서 실제로 쓰이는지 "
        "확신할 근거가 부족하다(단어가 생소하거나, 여러 학년에 걸쳐 쓰일 법하거나, 판단이 애매한 "
        "경우) — 이 경우에도 level은 최선의 추정값을 채우되 grounded는 반드시 false로 표시한다",
        "",
    ]
    for item in batch:
        parts.append(f"- id: {item['id']}")
        parts.append(f"  {item['category']}: {item['headword']}")
        parts.append(f"  뜻: {item['definition']}")
        parts.append("")

    parts.append(
        "출력은 아래 JSON 형식만 반환하라:\n"
        '{"items": [{"id": 123, "level": 3, "grounded": true, "reason": "..."}, '
        '{"id": 456, "level": null, "grounded": true, "reason": "..."}, '
        '{"id": 789, "level": 2, "grounded": false, "reason": "..."}, ...]}\n'
        "level은 0~6 정수 또는 해당없음이면 null이다. level이 null이면 grounded는 true로 채운다"
        "(해당없음 자체는 근거 부족이 아니라 교육과정 부적합 판정이다). "
        f"배열에는 위 {len(batch)}개 id가 전부, 그리고 그것만 있어야 한다."
    )
    return "\n".join(parts)


def make_batches(targets: list[dict]) -> list[list[dict]]:
    return [targets[i:i + BATCH_SIZE] for i in range(0, len(targets), BATCH_SIZE)]


def save_result(
    conn: sqlite3.Connection,
    term_id: int,
    level: int | None,
    reason: str,
    now: str,
    grounded: bool = True,
) -> None:
    """AI 판정 저장.

    level이 None이면(해당없음) 기존과 동일하게 review_status='제외'로 확정한다.
    level이 있어도 grounded=False(학년 근거 부족을 AI 스스로 표시)면
    review_status를 '검수완료'로 바로 올리지 않고 '보류'로 남긴다 - literacy.db
    전반에서 '보류'는 이미 "사람 확인 전까지는 확정 아님"을 뜻하는 기존 상태값이다
    (예: krdict 폴백 동음이의/다의어 보류, S(schema) 정의 없는 행 보류와 동일 계열).
    grounded=True인 기존 경로는 회귀 없이 그대로 '검수완료'로 확정한다.
    """
    if level is None:
        conn.execute(
            "UPDATE terms SET review_status='제외', reviewed_at=?, "
            "note = COALESCE(note || ' / ', '') || ? WHERE id=?",
            (now, f"[AI 자동 판정: 해당없음] {reason}", term_id),
        )
    elif grounded:
        conn.execute(
            "UPDATE terms SET level=?, grade_source='auto', review_status='검수완료', reviewed_at=?, "
            "note = COALESCE(note || ' / ', '') || ? WHERE id=?",
            (level, now, f"[AI 자동 레벨 부여: {level}] {reason}", term_id),
        )
    else:
        conn.execute(
            "UPDATE terms SET level=?, grade_source='auto', review_status='보류', reviewed_at=?, "
            "note = COALESCE(note || ' / ', '') || ? WHERE id=?",
            (level, now, f"[AI 자동 레벨 부여-근거부족(보류): {level}] {reason}", term_id),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="속담·관용구 레벨 완전자동 판정")
    parser.add_argument("--dry-run", action="store_true", help="10건만 판정해서 출력, DB 저장 안 함")
    parser.add_argument("--limit", type=int, default=None, help="처리할 최대 건수")
    args = parser.parse_args()

    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA foreign_keys=ON")

    limit = 10 if args.dry_run else args.limit
    targets = select_targets(conn, limit)
    if not targets:
        print("대상 없음")
        conn.close()
        return 0

    print(f"대상 {len(targets)}건")
    client = get_client()
    batches = make_batches(targets)

    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    done = failed = 0
    for bi, batch in enumerate(batches, 1):
        print(f"[배치 {bi}/{len(batches)}] {len(batch)}건")
        prompt = build_prompt(batch)
        try:
            parsed = call_gemini_json(client, prompt)
        except Exception as e:  # noqa: BLE001
            print(f"  실패: {e}", file=sys.stderr)
            failed += len(batch)
            continue

        by_id = {item["id"]: item for item in batch}
        items = {int(it["id"]): it for it in parsed.get("items", [])}
        for term_id, item in by_id.items():
            result = items.get(term_id)
            if not result:
                print(f"  누락: id={term_id}", file=sys.stderr)
                failed += 1
                continue
            level = result.get("level")
            reason = result.get("reason", "")
            # grounded 필드가 없으면(AI 응답 누락/구버전 프롬프트) 안전 쪽으로 기본값을
            # False(보류)로 둔다 - "필드가 없다고 검수완료로 확정"하는 쪽이 더 위험하다.
            grounded = bool(result.get("grounded", False)) if level is not None else True
            if args.dry_run:
                hold_note = "" if grounded else " [보류 예정-근거부족]"
                print(f"  {item['headword']}: level={level}{hold_note} - {reason}")
            else:
                save_result(conn, term_id, level, reason, now, grounded=grounded)
                done += 1
        if not args.dry_run:
            conn.commit()

    conn.close()
    print(f"\n완료 {done}건, 실패 {failed}건" + (" (dry-run - 저장 안 함)" if args.dry_run else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
