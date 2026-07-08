"""
gen_overlay.py  —  교사용 PDF → model_answer(JSON) 오버레이 자동 생성기 (MOMOAI 패스)

설계 원칙(사용자 요청): '정확한 텍스트 추출'이 최우선, 표 기하 재현은 후순위.

파이프라인 (문항별):
  1) 학생용 골격에서 발문·scaffold 확보
  2) 교사용에서 정답 원문 추출 (extract_teacher: 답:/앵커)  ← 결정적, 고신뢰
  3) 신뢰도 판정:
       high  = 답:/anchor 이고 길이 충분    → 텍스트만 LLM에 전달
       low   = 표/빈칸/짧음/빈값             → 해당 교사용 '페이지 이미지'를 함께 전달(비전)
  4) LLM이 {prompt, scaffold, raw_answer(, image)} → model_answer JSON 으로 구조화
  5) overlay.json 저장 + (원하면) DB items.model_answer 갱신

LLM 호출은 Anthropic API(=MOMOAI 키)를 사용. 키가 없으면 --no-llm 폴백으로
raw_answer 를 그대로 {"answer": ...} 로 감싸 파이프라인 전체가 돌아가게 한다.
"""
import os, re, json, base64, subprocess, argparse
from parser import pdftext, parse_skeleton
from extract_teacher import extract_answers

MODEL = os.environ.get("MOMOAI_MODEL", "claude-haiku-4-5-20251001")           # 텍스트 구조화
VISION_MODEL = os.environ.get("MOMOAI_VISION_MODEL", "claude-sonnet-5")        # 표/OX 비전 판독
MIN_LEN = 12   # 이보다 짧은 정답 원문은 저신뢰로 간주

RE_TYPEHDR_START = re.compile(r"^\s*(사실적|추론적|분석적|비판적|적용적|적용|창의적|감상)\s*독해")


def build_prompt_index(student_pdf):
    idx, meta = {}, []
    for it in parse_skeleton(pdftext(student_pdf)):
        idx[(it["seq"], it["sub_seq"])] = it["prompt"]
        meta.append(it)
    return idx, meta


def _norm(s):
    return re.sub(r"\s+", "", s or "")


def _echo(ans, ref, win=16):
    """ans 앞부분이 ref(발문/지문) 안에 그대로 나타나면 True (에코/유입 탐지)."""
    a, r = _norm(ans), _norm(ref)
    return len(a) >= win and bool(r) and a[:win] in r


def confidence(ans, prompt="", next_passage=""):
    """
    고신뢰 = (답:/anchor) + 충분한 길이 + 거짓 고신뢰 신호 없음.
    거짓 고신뢰 신호:
      · 유형헤더시작   : 정답이 '비판적 독해…'로 시작 → 다음 문항 지문을 잡음
      · 발문에코       : 정답 앞부분이 발문에 그대로 → 발문 되풀이
      · 다음지문유입   : 정답 앞부분이 다음 문항 지문에 포함
    """
    raw = ans["raw_answer"]; flags = []
    if ans["marker"] not in ("답:", "anchor"):
        flags.append("무마커")
    if len(raw) < MIN_LEN:
        flags.append("짧음")
    if RE_TYPEHDR_START.match(raw):
        flags.append("유형헤더시작")
    if _echo(raw, prompt):
        flags.append("발문에코")
    if next_passage and _echo(raw, next_passage):
        flags.append("다음지문유입")
    return ("high" if not flags else "low"), flags


def teacher_page_png(teacher_pdf, page, out_dir="/tmp/momo_pages"):
    """저신뢰 문항용: 교사용 특정 페이지를 PNG로 렌더(비전 입력)."""
    os.makedirs(out_dir, exist_ok=True)
    prefix = os.path.join(out_dir, f"p{page}")
    subprocess.run(["pdftoppm", "-png", "-r", "150", "-f", str(page), "-l", str(page),
                    teacher_pdf, prefix], check=True)
    for f in os.listdir(out_dir):
        if f.startswith(f"p{page}") and f.endswith(".png"):
            return os.path.join(out_dir, f)
    return None


def llm_structure(prompt, scaffold, raw_answer, image_path=None):
    """Anthropic API로 정답을 model_answer JSON 으로 구조화."""
    import anthropic  # pip install anthropic
    client = anthropic.Anthropic()  # ANTHROPIC_API_KEY 환경변수 사용
    sys_msg = (
        "너는 논술 교재의 '교사용 모범답안'을 구조화하는 도우미다. "
        "주어진 발문과 정답 원문(때로는 교사용 페이지 이미지)을 보고, "
        "정답을 JSON 하나로만 출력하라. 마크다운/설명 없이 JSON 만. "
        "scaffold(표 라벨)가 있으면 각 라벨을 key 로, 없으면 {\"answer\": ...} 로. "
        "원문의 표현·수치·인용을 왜곡하지 말고 충실히 옮겨라. 없는 내용을 지어내지 마라. "
        "만약 [정답 원문]이 이 발문의 답이 아니라고 판단되면(예: 다른 문항의 지문이거나 "
        "발문을 되풀이한 것) 답을 지어내지 말고 {\"_needs_vision\": true} 만 출력하라."
    )
    user_content = [{
        "type": "text",
        "text": f"[발문]\n{prompt}\n\n[scaffold]\n{scaffold or '없음'}\n\n"
                f"[정답 원문]\n{raw_answer or '(원문 추출 실패 — 이미지에서 읽어라)'}"
    }]
    if image_path:
        with open(image_path, "rb") as f:
            b64 = base64.standard_b64encode(f.read()).decode()
        user_content.append({"type": "image", "source": {
            "type": "base64", "media_type": "image/png", "data": b64}})
    msg = client.messages.create(
        model=(VISION_MODEL if image_path else MODEL), max_tokens=1500,
        system=sys_msg, messages=[{"role": "user", "content": user_content}])
    text = "".join(b.text for b in msg.content if b.type == "text")
    text = re.sub(r"^```json|```$", "", text.strip()).strip()
    return json.loads(text)


def generate(student_pdf, teacher_pdf, use_llm=True):
    idx, meta = build_prompt_index(student_pdf)
    scaffold_of = {(m["seq"], m["sub_seq"]): m.get("scaffold_labels") for m in meta}
    # 다음 문항 지문(유입 탐지용): 문서 순서상 바로 다음 item 의 passage_quote
    nextpass = {}
    for i, m in enumerate(meta):
        nxt = meta[i + 1]["passage_quote"] if i + 1 < len(meta) else ""
        nextpass[(m["seq"], m["sub_seq"])] = nxt
    answers = extract_answers(pdftext(teacher_pdf), idx)

    overlay, report = {}, []
    for a in answers:
        key = f"{a['seq']}" + (f"-{a['sub_seq']}" if a['sub_seq'] else "")
        prompt = idx.get((a["seq"], a["sub_seq"])) or idx.get((a["seq"], None)) or ""
        scaffold = scaffold_of.get((a["seq"], a["sub_seq"]))
        np = nextpass.get((a["seq"], a["sub_seq"])) or nextpass.get((a["seq"], None)) or ""
        conf, flags = confidence(a, prompt, np)
        # 표형(scaffold 존재)은 텍스트 흩어짐 위험 → 무조건 비전 강제
        if scaffold and conf == "high":
            conf = "low"; flags.append("표형→비전강제")

        rec = {"seq": a["seq"], "sub_seq": a["sub_seq"],
               "excluded": a["excluded"], "is_starred": a["starred"],
               "confidence": conf, "marker": a["marker"], "flags": flags}
        if use_llm:
            img = None
            if conf == "low":
                pg = find_teacher_page(teacher_pdf, prompt, a["raw_answer"])
                img = teacher_page_png(teacher_pdf, pg) if pg else None
            ma = llm_structure(prompt, scaffold, a["raw_answer"], img)
            # LLM 자기검증: 원문이 발문과 안 맞다고 판단하면 비전으로 재시도
            if isinstance(ma, dict) and ma.get("_needs_vision") and img is None:
                pg = find_teacher_page(teacher_pdf, prompt, a["raw_answer"])
                img = teacher_page_png(teacher_pdf, pg) if pg else None
                if img:
                    rec["flags"] = flags + ["LLM재시도(비전)"]
                    ma = llm_structure(prompt, scaffold, a["raw_answer"], img)
            rec["model_answer"] = ma
        else:
            rec["model_answer"] = {"answer": a["raw_answer"]}
        overlay[key] = rec
        report.append((key, conf, a["marker"], len(a["raw_answer"]), flags))
    return overlay, report


def read_ox_vision(teacher_pdf, page):
    """O/X 정답은 음영 마크라 텍스트로 못 읽음 → 교사용 페이지 이미지를 LLM 비전으로 판독."""
    import anthropic
    client = anthropic.Anthropic()
    img = teacher_page_png(teacher_pdf, page)
    if not img:
        return None
    with open(img, "rb") as f:
        b64 = base64.standard_b64encode(f.read()).decode()
    msg = client.messages.create(
        model=VISION_MODEL, max_tokens=800,
        system="교사용 페이지의 O/X 퀴즈에서 각 문항의 정답(색칠/음영된 O 또는 X)을 읽어라. "
               "JSON 배열 [{\"q\":\"문항요지\",\"answer\":\"O\"|\"X\"}] 만 출력. 마크다운 금지.",
        messages=[{"role": "user", "content": [
            {"type": "text", "text": "이 페이지의 O/X 퀴즈 정답을 판독하라."},
            {"type": "image", "source": {"type": "base64",
             "media_type": "image/png", "data": b64}}]}])
    text = "".join(b.text for b in msg.content if b.type == "text")
    return json.loads(re.sub(r"^```json|```$", "", text.strip()).strip())


def find_teacher_page(teacher_pdf, prompt, raw_answer):
    """정답(또는 발문) 텍스트가 등장하는 교사용 페이지 번호 (pdftotext 페이지 스캔)."""
    needle = re.sub(r"\s+", "", (raw_answer or prompt))[:14]
    if not needle:
        return 1
    info = subprocess.run(["pdfinfo", teacher_pdf], capture_output=True, text=True).stdout
    m = re.search(r"Pages:\s*(\d+)", info)
    pages = int(m.group(1)) if m else 1
    for i in range(1, pages + 1):
        txt = subprocess.run(["pdftotext", "-f", str(i), "-l", str(i), teacher_pdf, "-"],
                             capture_output=True, text=True).stdout
        if re.sub(r"\s+", "", txt).find(needle) >= 0:
            return i
    return 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("teacher"); ap.add_argument("student")
    ap.add_argument("--no-llm", action="store_true", help="LLM 없이 원문 폴백")
    ap.add_argument("--out", default="overlay.json")
    args = ap.parse_args()

    overlay, report = generate(args.student, args.teacher, use_llm=not args.no_llm)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(overlay, f, ensure_ascii=False, indent=2)

    hi = sum(1 for r in report if r[1] == "high")
    print(f"오버레이 {len(report)}문항 → {args.out}")
    print(f"  고신뢰(텍스트만) {hi} | 저신뢰(비전 권장) {len(report)-hi}")
    for k, c, mk, ln, flags in report:
        tag = "✓" if c == "high" else "▲비전"
        fl = ("  ⚠ " + ", ".join(flags)) if flags else ""
        print(f"  {tag:5} [{k:>4}] marker={mk:<11} len={ln}{fl}")
