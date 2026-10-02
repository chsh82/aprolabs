# -*- coding: utf-8 -*-
"""최소 정보 기반 Gemini 중등 어휘 레벨 분류기 v3.

v2(scripts/vocab/nikl_grade5_gemini_classifier_v2.py)를 포크했다 - 입력·출력·
캐싱·오류처리·배치 크기 전부 동일. **달라진 건 프롬프트뿐이다.**

## v3 프롬프트를 바꾼 근거 - 대표 12건에 사용자가 직접 쓴 판단 이유

2026-10-02, 관리자가 `/vocab-grade5-candidate-review/?filter=v2_rep12` 화면에서
v1/v2가 사람과 갈렸던 대표 12건에 "이 레벨로 판단한 이유"를 append-only로
추가했다(연구 DB `vocabulary_grade5_candidate_judgments` id 203~214, 판정값은
기존과 동일하게 유지 - 아래는 그 12건의 **사용자 원문 그대로**다):

| 표제어 | 사람 판정 | 사용자가 쓴 이유(원문) |
|---|---|---|
| 개축 | L3 | "중학생에게 어렵지 않은 단어임" |
| 경제인 | L3 | "경제 개념 자체는 초등학교 때부터 배움" |
| 보국안민 | L3 | "쉬운 사자성어임" |
| 소송비 | L3 | "중학생에게 어렵지 않은 단어임" |
| 성싶다 | L4 | "일반적으로 잘 사용하지 않음" |
| 외성 | L4 | "자주 사용하지 않는 단어임" |
| 동판화 | L4 | "자주 사용하지 않음" |
| 재담 | L4 | "자주 사용되지 않음" |
| 이다 | L4 | "동의이의어에 비해 자주 사용되지 않음" |
| 어진 | L4 | "자주 사용되지 않음" |
| 수사법 | L4 | "어려운 어휘임" |
| 이래 | L4 | "자주사용되지 않음" |

**분석자 해석(사용자 원문이 아님 - 구분해서 읽을 것)**:
- L4 쪼 8건 중 7건이 공통으로 "자주/일반적으로 사용하지 않음"을 이유로 들었다 -
  v1/v2가 썼던 "교과서에서 다룬다"·"한자어=전문용어" 기준과는 다른 축이다.
  사용자의 L4 판정 근거는 **교과서 수록 여부가 아니라 실생활 사용 빈도**로
  보인다.
- L3 쪼 4건 전부 "어렵지 않다/쉽다"로만 적었고 빈도를 직접 언급하지 않았다 -
  "경제인"의 "초등학교 때부터 배움"은 빈도와 조기 노출이 겹쳐 있어 완전히
  분리하긴 어렵다. L3 쪽 근거가 "안 어렵다"는 것만으로는 v2가 이미 반영한
  "한자어=전문용어 아님" 원칙과 겹칠 뿐, 빈도 기준을 L3 쪼에서 직접 뒷받침하진
  않는다 - 이 비대칭은 숨기지 않고 그대로 남긴다.
- **모순/예외 사례(그대로 제시, 뭉개지 않음)**: "수사법"은 다른 7건과 달리
  빈도를 언급하지 않고 "어려운 어휘임"이라고만 적었다 - 빈도만으로는 설명되지
  않는 L4 판정이 적어도 1건 있다는 뜻이다. "이다"는 "동의이의어에 비해"라는
  단서를 달았다 - 흔한 동음이의어(서술격 조사 '이다')와 비교해 이 뜻(지붕을
  잇다)만 드문 것이라, 표제어 단위가 아니라 **그 의미/용법 단위**의 빈도라는
  뜻이다. 이 두 예외는 "자주 안 쓰이면 L4"라는 단일 규칙으로 깔끔하게
  정리되지 않는다는 걸 보여준다 - 그래서 v3 프롬프트는 빈도를 **유일한 결정
  신호가 아니라 (B)를 보여주는 신호 중 하나**로만 추가하고, "빈도 추정 근거가
  부족하면 억지로 판정하지 말라"는 보류 지시를 같이 넣었다(아래 프롬프트 참고).
- n=12인 소표본에서 나온 패턴이라, 이걸 "전체 어휘에 적용하는 절대 규칙"으로
  만들지 않았다 - 특정 표제어를 프롬프트에 few-shot으로 박아넣지 않은 것도
  v2와 같은 이유(교차오염 방지, 특정 사례에 대한 과적합 방지).

## 입력 - v1/v2와 완전히 동일

candidate_id/lemma/pos/official_grade/official_meaning_short/
specialized_domain_flag 5개뿐. 빈도 데이터를 새 입력 필드로 추가하지 않았다 -
실사용 빈도는 모델이 자신의 사전 지식으로 추정하게 하는 것이고, 그 추정이
부실하면 아래 프롬프트의 보류 지시가 적용된다.

## 고정 설정

model=gemini-3.6-flash(v1/v2와 동일), generation_config={'max_output_tokens': 8192},
배치 크기 10(v1/v2와 동일).

실행:
    python scripts/vocab/nikl_grade5_gemini_classifier_v3.py --input <csv> --output <csv> [--cache <json>] [--limit N]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

from gemini_client import call_gemini_json, get_client, MODEL  # noqa: E402

BATCH_SIZE = 10
VALID_JUDGMENTS = {"L3", "L4", "경계 유지", "검토 필요"}

GENERATION_CONFIG = {"max_output_tokens": 8192}

PROMPT_INSTRUCTIONS = """너는 한국 중학교 국어 교육과정 관점에서, 아래 각 단어의 "그 뜻(의미)"이
한국어를 모국어로 쓰는 학생이 보통 몇 학년 즈음 "기본적으로 이해하고 사용"하는지를 판정한다.

레벨 대응:
- L3 = 중학교 1~2학년 수준에서 기본적으로 이해·사용하는 뜻
- L4 = 중학교 3학년 수준에서 기본적으로 이해·사용하는 뜻

## 가장 중요한 판단 기준 — 두 가지를 반드시 구분하라

(A) "뜻을 설명하면 그 학년 학생이 이해할 수 있다"
(B) "그 학년 학생이 설명 없이도 이미 알고/익숙하게 쓰는 어휘다"

**(A)는 L3/L4 판정의 근거가 될 수 없다.** 뜻을 설명하면 이해할 수 있는 단어는
무수히 많다 - 그건 "이해 가능"이지 "기본 습득"이 아니다. 오직 (B)만이 올바른
판단 기준이다.

이 구분이 특히 중요한 세 가지 흔한 착오:

1. **"교과서/교육과정에서 다룬다/배운다"는 것은 (A)에 가깝지 (B)가 아니다.**
   어떤 개념을 특정 학년 교과서가 "핵심적으로 다룬다"는 것은 오히려 그 학년
   학생들이 **아직 모르기 때문에 가르친다**는 신호인 경우가 많다. "중학교
   1~2학년 국어 교육과정에서 다루는 개념어다"라는 이유만으로 L3를 주지 마라
   - 그 개념을 배우는 학년과, 그 단어를 이미 알고 쓰는 학년은 다를 수 있다.
2. **한자어·격식체라고 자동으로 어렵거나 전문적인 것은 아니다.** 뉴스·생활
   문서에서 흔히 쓰이는 한자어(예: 행정·경제·법률 관련 일상 어휘)를 "학술적
   개념어"로 과대 해석해 무조건 높은 레벨로 올리지 마라. 실제로 그 학년
   학생의 일상·미디어 노출에서 흔한 단어인지를 기준으로 삼아라.
3. **그 학년 학생이 실생활(대화·독서·미디어)에서 그 단어(그 뜻)를 실제로
   얼마나 자주 접하거나 쓰는지가 (B)를 가장 직접적으로 보여주는 신호다.**
   자주 쓰이지 않는 단어는 뜻을 설명하면 바로 이해되더라도 "기본 습득"이
   아닐 수 있다 - 반대로 한자어라도 뉴스·일상에서 자주 쓰이면 기본 습득으로
   볼 수 있다. 동음이의어가 있는 단어는 **그 표제어 전체**가 아니라 **지금
   주어진 그 뜻(의미)** 자체가 얼마나 자주 쓰이는지를 봐야 한다(예: 흔한
   동음이의어 때문에 표제어는 익숙해 보여도, 지금 판정 대상인 그 뜻만 유독
   드물게 쓰일 수 있다).
   **주의: 빈도가 유일한 어려움의 원인은 아니다.** 개념 자체가 생소하거나
   추상적이어서 어렵게 느껴지는 경우도 있으므로, 빈도 하나로만 기계적으로
   판정하지 마라. 그 단어(그 뜻)의 실사용 빈도를 추정할 근거가 부족하면
   억지로 추정해 단정하지 말고, 아래 "판단 근거가 부족한 경우"로 분류하라.

다음 신호만으로 기계적으로 결정하지도 마라(약한 상관일 뿐 결정 근거가 아니다):
- 뜻풀이 문장이 길다/짧다
- 전문어 분야로 표시돼 있다/아니다
- 특정 교재에 등장하는 학년

각 항목에 대해 L3 또는 L4로 판정하되, 다음 두 경우에는 억지로 L3/L4를 찍지 말고
grounded를 false로 표시하라(이 경우 level 필드에는 그래도 더 가까운 쪽을
최선으로 채우되, grounded=false로 "확정 아님"을 분명히 표시한다):
- 판단은 섰지만 L3와 L4 경계에 걸쳐 있어 확신이 약한 경우(경계) -> borderline=true
- 판단할 근거 자체가 부족한 경우(단어가 매우 생소하거나 정보가 불충분, 또는
  실사용 빈도를 추정할 근거가 부족한 경우) -> borderline=false

**주의: "애매하면 무조건 L4로 판정하라"는 규칙이 아니다.** 위 (A)/(B) 구분과
착오 세 가지를 기준으로 정직하게 판단한 결과가 L3일 수도 L4일 수도, 경계/
근거부족일 수도 있다 - 방향을 미리 정하지 마라.

새로운 뜻풀이를 만들어내거나 외부 자료를 조사하지 말고, 아래 주어진 정보만으로
판단하라.

출력은 반드시 아래 JSON 형식만 반환하라:
{"items": [
  {"id": "G5-xxxx", "level": "L3", "grounded": true, "borderline": false, "reason": "..."},
  {"id": "G5-yyyy", "level": "L4", "grounded": false, "borderline": true, "reason": "..."},
  ...
]}
level은 "L3" 또는 "L4" 둘 중 하나만 쓴다(문자열). grounded=false이고
borderline=true면 경계로, grounded=false이고 borderline=false면 근거 부족으로
나중에 분류한다. 배열에는 아래 제시된 id가 전부, 그리고 그것만 있어야 한다."""


@dataclass
class Item:
    candidate_id: str
    lemma: str
    pos: str
    official_grade: str
    official_meaning_short: str
    specialized_domain_flag: str


@dataclass
class Prediction:
    candidate_id: str
    judgment: str | None
    reason: str = ""
    grounded: bool | None = None
    borderline: bool | None = None
    api_error: bool = False
    error_detail: str = ""


def prompt_template_hash() -> str:
    return hashlib.sha256(PROMPT_INSTRUCTIONS.encode("utf-8")).hexdigest()[:16]


def build_prompt(batch: list[Item]) -> str:
    parts = [PROMPT_INSTRUCTIONS, "", "항목:"]
    for it in batch:
        domain = "전문어" if str(it.specialized_domain_flag).strip() in ("1", "True", "true") else "일반어"
        parts.append(f"- id: {it.candidate_id}")
        parts.append(f"  표제어: {it.lemma} / 품사: {it.pos} / 분야: {domain} / 공식등급: {it.official_grade}등급")
        parts.append(f"  뜻풀이: {it.official_meaning_short or '(뜻풀이 없음)'}")
        parts.append("")
    return "\n".join(parts)


def make_batches(items: list[Item], size: int = BATCH_SIZE) -> list[list[Item]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def classify_judgment(level: str | None, grounded: bool, borderline: bool) -> str | None:
    if level not in ("L3", "L4"):
        return None
    if grounded:
        return level
    return "경계 유지" if borderline else "검토 필요"


def load_cache(path: Path) -> dict:
    if path.is_file():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(path: Path, cache: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def cache_key(candidate_id: str, template_hash: str) -> str:
    return f"{candidate_id}:{template_hash}:{MODEL}:v3"


def run_classifier(items: list[Item], cache_path: Path, *, limit: int | None = None) -> tuple[list[Prediction], dict]:
    if limit:
        items = items[:limit]

    cache = load_cache(cache_path)
    thash = prompt_template_hash()

    to_call: list[Item] = []
    predictions: dict[str, Prediction] = {}
    for it in items:
        key = cache_key(it.candidate_id, thash)
        if key in cache:
            c = cache[key]
            predictions[it.candidate_id] = Prediction(
                candidate_id=it.candidate_id, judgment=c.get("judgment"), reason=c.get("reason", ""),
                grounded=c.get("grounded"), borderline=c.get("borderline"),
                api_error=c.get("api_error", False), error_detail=c.get("error_detail", ""),
            )
        else:
            to_call.append(it)

    print(f"캐시 적중 {len(items) - len(to_call)}건, 신규 호출 필요 {len(to_call)}건 "
          f"(prompt_template_hash={thash}, model={MODEL})")

    if to_call:
        client = get_client()
        batches = make_batches(to_call)
        for bi, batch in enumerate(batches, 1):
            print(f"[배치 {bi}/{len(batches)}] {len(batch)}건 호출 중...")
            prompt = build_prompt(batch)
            by_id = {it.candidate_id: it for it in batch}
            try:
                parsed = call_gemini_json(client, prompt, max_output_tokens=GENERATION_CONFIG["max_output_tokens"])
            except Exception as e:  # noqa: BLE001
                print(f"  배치 호출 실패: {e}", file=sys.stderr)
                for cid in by_id:
                    predictions[cid] = Prediction(candidate_id=cid, judgment=None, api_error=True, error_detail=str(e))
                continue

            resp_items = {str(it_["id"]): it_ for it_ in parsed.get("items", [])}
            for cid in by_id:
                r = resp_items.get(cid)
                if not r:
                    print(f"  누락: id={cid}", file=sys.stderr)
                    predictions[cid] = Prediction(candidate_id=cid, judgment=None, api_error=True, error_detail="응답에 항목 없음")
                    continue
                level = r.get("level")
                grounded = bool(r.get("grounded", False))
                borderline = bool(r.get("borderline", False))
                reason = r.get("reason", "")
                judgment = classify_judgment(level, grounded, borderline)
                if judgment is None:
                    pred = Prediction(candidate_id=cid, judgment=None, api_error=True, error_detail=f"level 값 비정상: {level!r}")
                else:
                    pred = Prediction(candidate_id=cid, judgment=judgment, reason=reason, grounded=grounded, borderline=borderline)
                predictions[cid] = pred
                cache[cache_key(cid, thash)] = {
                    "judgment": pred.judgment, "reason": pred.reason, "grounded": pred.grounded,
                    "borderline": pred.borderline, "api_error": pred.api_error, "error_detail": pred.error_detail,
                }
        save_cache(cache_path, cache)

    ordered = [predictions[it.candidate_id] for it in items]
    meta = {"prompt_template_hash": thash, "model": MODEL, "generation_config": GENERATION_CONFIG, "version": "v3"}
    return ordered, meta


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="candidate_id,lemma,pos,official_grade,official_meaning_short,specialized_domain_flag 컬럼을 가진 CSV")
    parser.add_argument("--output", required=True)
    parser.add_argument("--cache", default=str(REPO_ROOT / "data" / "import" / "nikl_grade5_gemini_v3_cache_20261006.json"))
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    items = []
    with open(args.input, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            items.append(Item(
                candidate_id=r["candidate_id"], lemma=r["lemma"], pos=r["pos"],
                official_grade=r.get("official_grade") or "5",
                official_meaning_short=r.get("official_meaning_short") or "",
                specialized_domain_flag=r.get("specialized_domain_flag", "0"),
            ))
    print(f"입력 {len(items)}건")

    predictions, meta = run_classifier(items, Path(args.cache), limit=args.limit)

    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["candidate_id", "lemma", "pos", "v3_predicted_judgment", "v3_predicted_reason",
                    "grounded", "borderline", "api_error", "error_detail", "model", "prompt_template_hash"])
        by_id = {it.candidate_id: it for it in items}
        for p in predictions:
            it = by_id[p.candidate_id]
            w.writerow([p.candidate_id, it.lemma, it.pos, p.judgment or "", p.reason,
                        p.grounded, p.borderline, p.api_error, p.error_detail,
                        meta["model"], meta["prompt_template_hash"]])

    n_error = sum(1 for p in predictions if p.api_error)
    n_hold = sum(1 for p in predictions if p.judgment in ("경계 유지", "검토 필요"))
    n_forced = sum(1 for p in predictions if p.judgment in ("L3", "L4"))
    print(f"완료: {len(predictions)}건 중 오류 {n_error}건, 보류 {n_hold}건, 확정 {n_forced}건 -> {args.output}")
    print(f"고정 설정: model={meta['model']}, prompt_template_hash={meta['prompt_template_hash']}, "
          f"generation_config={meta['generation_config']}")
    return 1 if n_error else 0


if __name__ == "__main__":
    sys.exit(main())
