"""
verify_overlay.py  —  --llm 로 구조화된 정답의 무결성 자동 검증.

목적: LLM(sonnet 등)이 정답을 '지어내거나 왜곡'하지 않았는지 기계적으로 잡아낸다.
학부모 리포트로 나갈 데이터이므로, 원문 대조로 신뢰를 보장한다.

검사 항목(문항별):
  1) JSON 유효성        — model_answer 가 dict 로 파싱되는가
  2) 빈/과소 정답        — 내용이 비었거나 너무 짧은가
  3) scaffold 일치       — 표 라벨이 있으면 각 라벨이 key 로 채워졌는가
  4) 발문 에코           — 정답이 발문을 되풀이했는가
  5) 원문 일치(지어냄)   — [텍스트 출처] 정답의 글자 n-gram 이 교사용 원문에 실제로 있는가
                           [비전 출처]  원문 텍스트가 없으므로 대조 불가 → '수동 확인' 분류

사용:
  python verify_overlay.py                # momo_kb.db + materials.json 기준
  python verify_overlay.py --min-overlap 0.5
종료코드: 하드 오류(JSON깨짐/지어냄의심/빈정답)가 있으면 1 (CI 게이트용).
"""
import os, re, json, sqlite3, argparse
from extract_teacher import pdftext, extract_answers
from parser import parse_skeleton

HERE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(HERE, "momo_kb.db")
DATA = os.environ.get("MOMOAI_DATA", os.path.join(HERE, "data"))
SHINGLE = 8   # 글자 n-gram 길이


def _norm(s):
    return re.sub(r"\s+", "", s or "")


def _shingles(s, n=SHINGLE):
    s = _norm(s)
    return {s[i:i+n] for i in range(0, max(0, len(s) - n + 1))} or ({s} if s else set())


def overlap(answer, source):
    """정답 글자 n-gram 중 원문에 실제 존재하는 비율(=충실도). 1.0=완전 포함."""
    a = _shingles(answer)
    if not a:
        return 1.0
    src = _norm(source)
    hit = sum(1 for sh in a if sh in src)
    return hit / len(a)


def flatten(ma):
    """model_answer(dict) → 값들을 이은 텍스트 + key 목록."""
    if not isinstance(ma, dict):
        return "", []
    vals, keys = [], []
    for k, v in ma.items():
        if k.startswith("_"):
            continue
        keys.append(k)
        if isinstance(v, str):
            vals.append(v)
        elif isinstance(v, list):
            vals.append(" ".join(map(str, v)))
        else:
            vals.append(json.dumps(v, ensure_ascii=False))
    return " ".join(vals), keys


def load_materials():
    cfg = os.path.join(HERE, "materials.json")
    with open(cfg, encoding="utf-8") as f:
        mats = json.load(f)
    for m in mats:
        for k in ("student", "teacher"):
            if not os.path.isabs(m[k]):
                m[k] = os.path.join(DATA, m[k])
    return mats


def key_of(seq, sub):
    return f"{seq}" + (f"-{sub}" if sub else "")


def verify(min_overlap):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    mats = {m["work_title"]: m for m in load_materials()}

    stats = dict(ok=0, empty=0, json_err=0, scaffold=0, echo=0,
                 fabricate=0, vision_review=0, total=0)
    flagged = []

    for mrow in con.execute("SELECT id, work_title FROM materials"):
        meta = mats.get(mrow["work_title"])
        raw_by_key = {}
        if meta and os.path.exists(meta["teacher"]):
            idx = {(it["seq"], it["sub_seq"]): it["prompt"]
                   for it in parse_skeleton(pdftext(meta["student"]))} \
                  if os.path.exists(meta["student"]) else {}
            for a in extract_answers(pdftext(meta["teacher"]), idx):
                raw_by_key[key_of(a["seq"], a["sub_seq"])] = a

        for it in con.execute(
                "SELECT seq, sub_seq, prompt, model_answer, scaffold_labels, "
                "answer_confidence, answer_flags FROM items WHERE material_id=? "
                "AND model_answer IS NOT NULL", (mrow["id"],)):
            stats["total"] += 1
            tag = f"{mrow['work_title']} {key_of(it['seq'], it['sub_seq'])}"
            problems = []

            # 1) JSON 유효성
            try:
                ma = json.loads(it["model_answer"])
                assert isinstance(ma, dict)
            except Exception:
                stats["json_err"] += 1
                flagged.append((tag, "JSON깨짐", ""))
                continue

            ans_text, keys = flatten(ma)

            # 2) 빈/과소
            if len(_norm(ans_text)) < 6:
                stats["empty"] += 1
                flagged.append((tag, "빈정답", ""))
                continue

            # 3) scaffold 일치
            if it["scaffold_labels"]:
                labels = json.loads(it["scaffold_labels"])
                missing = [L for L in labels if L not in ma or not str(ma.get(L, "")).strip()]
                if missing:
                    problems.append(f"scaffold누락:{'/'.join(missing)}")
                    stats["scaffold"] += 1

            # 4) 발문 에코
            if _norm(ans_text)[:16] and _norm(ans_text)[:16] in _norm(it["prompt"]):
                problems.append("발문에코")
                stats["echo"] += 1

            # 5) 원문 일치 (출처 구분)
            raw = raw_by_key.get(key_of(it["seq"], it["sub_seq"]))
            vision_sourced = (it["answer_confidence"] == "low")
            if vision_sourced or not raw or raw["marker"] not in ("답:", "anchor") \
                    or len(_norm(raw["raw_answer"])) < 12:
                problems.append("비전판독→수동확인")
                stats["vision_review"] += 1
            else:
                ov = overlap(ans_text, raw["raw_answer"])
                if ov < min_overlap:
                    problems.append(f"원문일치 {ov:.0%}(지어냄의심)")
                    stats["fabricate"] += 1

            if problems:
                flagged.append((tag, ", ".join(problems), ""))
            else:
                stats["ok"] += 1

    con.close()

    print(f"검증 대상 정답 {stats['total']}개")
    print(f"  ✓ 정상               {stats['ok']}")
    print(f"  ▲ 비전판독(수동확인)  {stats['vision_review']}")
    print(f"  ⚠ scaffold 누락       {stats['scaffold']}")
    print(f"  ⚠ 발문 에코           {stats['echo']}")
    print(f"  ✕ 원문불일치(지어냄)  {stats['fabricate']}")
    print(f"  ✕ 빈 정답             {stats['empty']}")
    print(f"  ✕ JSON 깨짐           {stats['json_err']}")
    if flagged:
        print("\n플래그 목록:")
        for tag, why, _ in flagged:
            mark = "✕" if any(w in why for w in ("지어냄", "빈정답", "JSON")) else "▲"
            print(f"  {mark} [{tag}] {why}")

    hard = stats["fabricate"] + stats["empty"] + stats["json_err"]
    return 0 if hard == 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-overlap", type=float, default=0.5,
                    help="원문 일치 최소 비율(이하면 지어냄 의심, 기본 0.5)")
    args = ap.parse_args()
    raise SystemExit(verify(args.min_overlap))
