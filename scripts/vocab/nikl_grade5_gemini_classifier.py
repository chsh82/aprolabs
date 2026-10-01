# -*- coding: utf-8 -*-
"""최소 정보 기반 Gemini 중등 어휘 레벨 분류기(실험).

입력은 엄격히 5개로 제한한다: 표제어(lemma), 품사(pos), 공식등급(official_grade,
이 배치 전체 '5' 상수), 공식 자료의 짧은 원문 뜻풀이(official_meaning_short),
전문어 분야(specialized_domain_flag). 기존 DB vocab_level·사람 판정·동형이의/
고유명사 위험 플래그는 모델 입력으로 전혀 넘기지 않는다(동형이의 위험은
참고 정보로만 프롬프트에 노출 — 결정 입력은 아님, 독립 플래그로 과거
다의어_위험/고유명사_위험은 아예 넘기지 않는다 - "최소 정보" 그대로 지킴).

판단 기준은 "그 뜻(의미)의 한국어 모국어 화자 기준 기본 습득 시기"로
고정한다 - 뜻풀이 길이·전문어 여부·교재 등장 학년만으로 결정하지 말라고
프롬프트에 명시한다(규칙 기반 분류기가 뜻풀이 길이에 과적합해 다수결보다
못한 성능을 낸 실패를 교정하기 위함, reports/nikl_grade5_batch1_analysis_
and_classifier_20261002.md 참고).

출력은 L3(중1~2)/L4(중3)/경계 유지/검토 필요 중 하나 + 짧은 근거.
"grounded"(실제로 그 학년군에서 쓰이는지 구체적 근거가 있는지)를 모델이
스스로 표시하게 하고, grounded=false면 L3/L4를 강제로 확정하지 않고
"경계 유지"(판단은 섰으나 확신이 약함) 또는 "검토 필요"(판단 근거 자체가
부족함)로 내리게 한다 - scripts/literacy/auto_review_level.py의 grounded
패턴을 그대로 가져왔다.

캐싱: (candidate_id, prompt_template_hash, model_version) 키로 로컬 JSON
캐시를 쓴다 - 같은 조합이 캐시에 있으면 API를 다시 호출하지 않는다(재실행
시 불필요한 호출을 줄이라는 지시).

오류/누락 처리: API 호출 실패나 응답에서 빠진 id는 judgment=None,
api_error=True로 명시적으로 남긴다 - 정상 판정으로 치환하지 않는다.

실행:
    python scripts/vocab/nikl_grade5_gemini_classifier.py --input <csv> --output <csv> [--cache <json>] [--limit N]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sys
from dataclasses import dataclass, field
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

BATCH_SIZE = 10  # 20이었으나 실측 결과 일부 배치가 max_output_tokens(4096)에서
# JSON이 중간에 잘려(Unterminated string) 파싱 실패하는 경우가 발견돼(100건 중
# 37건) 10으로 줄이고 아래 토큰 한도도 함께 늘렸다 - 응답 길이를 안전하게 확보.
VALID_JUDGMENTS = {"L3", "L4", "경계 유지", "검토 필요"}

# 생성 설정(고정, 재현성을 위해 명시) - call_gemini_json은 temperature를 받지 않으므로
# (gemini_client.py 공용 모듈 변경 없이 재사용) max_output_tokens만 고정값으로 넘긴다.
GENERATION_CONFIG = {"max_output_tokens": 8192}

PROMPT_INSTRUCTIONS = """너는 한국 중학교 국어 교육과정 관점에서, 아래 각 단어의 "그 뜻(의미)"이
한국어를 모국어로 쓰는 학생이 보통 몇 학년 즈음 처음 습득/학습하는지를 판정한다.

레벨 대응:
- L3 = 중학교 1~2학년 수준에서 기본적으로 습득하는 뜻
- L4 = 중학교 3학년 수준에서 기본적으로 습득하는 뜻

판단 기준은 반드시 "그 뜻의 기본 습득 시기"다. 다음 신호만으로 기계적으로
결정하지 마라(약한 상관일 뿐 결정 근거가 아니다):
- 뜻풀이 문장이 길다/짧다
- 전문어 분야로 표시돼 있다/아니다
- 특정 교재에 등장하는 학년

위 신호를 참고는 하되, 실제로 그 뜻을 중1~2 학생이 이미 아는 게 자연스러운지
아니면 중3은 돼야 익히는 게 자연스러운지를 단어 자체의 쓰임·친숙도·개념
난이도로 직접 판단하라.

각 항목에 대해 L3 또는 L4로 판정하되, 다음 두 경우에는 억지로 L3/L4를 찍지 말고
grounded를 false로 표시하라(이 경우 level 필드에는 그래도 더 가까운 쪽을
최선으로 채우되, grounded=false로 "확정 아님"을 분명히 표시한다):
- 판단은 섰지만 L3와 L4 경계에 걸쳐 있어 확신이 약한 경우(경계)
- 판단할 근거 자체가 부족한 경우(단어가 매우 생소하거나 정보가 불충분)

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
    judgment: str | None  # L3 / L4 / 경계 유지 / 검토 필요 / None(오류)
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
    return f"{candidate_id}:{template_hash}:{MODEL}"


def run_classifier(
    items: list[Item],
    cache_path: Path,
    *,
    limit: int | None = None,
) -> tuple[list[Prediction], dict]:
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
                candidate_id=it.candidate_id,
                judgment=c.get("judgment"),
                reason=c.get("reason", ""),
                grounded=c.get("grounded"),
                borderline=c.get("borderline"),
                api_error=c.get("api_error", False),
                error_detail=c.get("error_detail", ""),
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
                    pred = Prediction(candidate_id=cid, judgment=None, api_error=True, error_detail=str(e))
                    predictions[cid] = pred
                continue

            resp_items = {str(it_["id"]): it_ for it_ in parsed.get("items", [])}
            for cid in by_id:
                r = resp_items.get(cid)
                if not r:
                    print(f"  누락: id={cid}", file=sys.stderr)
                    pred = Prediction(candidate_id=cid, judgment=None, api_error=True, error_detail="응답에 항목 없음")
                    predictions[cid] = pred
                    continue
                level = r.get("level")
                grounded = bool(r.get("grounded", False))
                borderline = bool(r.get("borderline", False))
                reason = r.get("reason", "")
                judgment = classify_judgment(level, grounded, borderline)
                if judgment is None:
                    pred = Prediction(candidate_id=cid, judgment=None, api_error=True,
                                       error_detail=f"level 값 비정상: {level!r}")
                else:
                    pred = Prediction(candidate_id=cid, judgment=judgment, reason=reason,
                                       grounded=grounded, borderline=borderline)
                predictions[cid] = pred
                cache[cache_key(cid, thash)] = {
                    "judgment": pred.judgment, "reason": pred.reason,
                    "grounded": pred.grounded, "borderline": pred.borderline,
                    "api_error": pred.api_error, "error_detail": pred.error_detail,
                }
        save_cache(cache_path, cache)

    ordered = [predictions[it.candidate_id] for it in items]
    meta = {"prompt_template_hash": thash, "model": MODEL, "generation_config": GENERATION_CONFIG}
    return ordered, meta


def read_items_generic(csv_path: Path, *, id_col: str | None, lemma_col: str, pos_col: str,
                        hom_col: str | None, meaning_col: str, domain_col: str,
                        grade_const: str, source_file_sha256: str | None) -> list[Item]:
    import importlib
    apply_mod = None
    if id_col is None:
        spec_path = REPO_ROOT / "scripts" / "vocab" / "apply_grade5_candidate_batch.py"
        spec = importlib.util.spec_from_file_location("apply_grade5_candidate_batch", spec_path)
        apply_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(apply_mod)

    items = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if id_col:
                cid = r[id_col]
            else:
                hom = r.get(hom_col) if hom_col else None
                cid = apply_mod.candidate_id_for(r[lemma_col], r[pos_col], hom)
            items.append(Item(
                candidate_id=cid,
                lemma=r[lemma_col],
                pos=r[pos_col],
                official_grade=grade_const,
                official_meaning_short=r.get(meaning_col) or "",
                specialized_domain_flag=r.get(domain_col, "0"),
            ))
    return items


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cache", default=str(REPO_ROOT / "data" / "import" / "nikl_grade5_gemini_cache_20261003.json"))
    parser.add_argument("--id-col", default=None, help="candidate_id 컬럼명(없으면 lemma/pos/hom으로 파생)")
    parser.add_argument("--lemma-col", default="lemma")
    parser.add_argument("--pos-col", default="pos")
    parser.add_argument("--hom-col", default="homonym_number")
    parser.add_argument("--meaning-col", default="official_meaning_short")
    parser.add_argument("--domain-col", default="specialized_domain_flag")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    items = read_items_generic(
        Path(args.input), id_col=args.id_col, lemma_col=args.lemma_col, pos_col=args.pos_col,
        hom_col=args.hom_col, meaning_col=args.meaning_col, domain_col=args.domain_col,
        grade_const="5", source_file_sha256=None,
    )
    print(f"입력 {len(items)}건")

    predictions, meta = run_classifier(items, Path(args.cache), limit=args.limit)

    out_path = Path(args.output)
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["candidate_id", "lemma", "pos", "predicted_judgment", "predicted_reason",
                    "grounded", "borderline", "api_error", "error_detail",
                    "model", "prompt_template_hash"])
        by_id = {it.candidate_id: it for it in items}
        for p in predictions:
            it = by_id[p.candidate_id]
            w.writerow([p.candidate_id, it.lemma, it.pos, p.judgment or "", p.reason,
                        p.grounded, p.borderline, p.api_error, p.error_detail,
                        meta["model"], meta["prompt_template_hash"]])

    n_error = sum(1 for p in predictions if p.api_error)
    n_hold = sum(1 for p in predictions if p.judgment in ("경계 유지", "검토 필요"))
    n_forced = sum(1 for p in predictions if p.judgment in ("L3", "L4"))
    print(f"완료: {len(predictions)}건 중 오류 {n_error}건, 보류(경계유지/검토필요) {n_hold}건, "
          f"확정(L3/L4) {n_forced}건 -> {out_path}")
    print(f"고정 설정: model={meta['model']}, prompt_template_hash={meta['prompt_template_hash']}, "
          f"generation_config={meta['generation_config']}")
    return 1 if n_error else 0


if __name__ == "__main__":
    sys.exit(main())
