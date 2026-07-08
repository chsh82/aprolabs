"""
extract_teacher.py  —  교사용 PDF에서 '문항별 정답 텍스트'를 정확히 잘라낸다.

우선순위: 프레임(표 기하) 재현이 아니라 '정답 텍스트의 정확한 추출'.
  · 서술형 : '답:' / '답 :' 마커 뒤 텍스트를 그대로 확보 (골드 시그널)
  · 표/무마커 : 발문 이후 텍스트를 통째로 확보 (구조화는 LLM 패스가 담당)

교사용은 학생용과 문항 표기가 다를 수 있으므로(예: 금오신화 교사용은
"N. \u201c인용문\u201d"), 학생용 파서와 별개로 '문항 번호'만으로 세그먼트한다.
반환 키 (stage_hint, seq, sub_seq) 는 학생용 골격과 조인된다.
"""
import re, subprocess

# 문항 시작:  (★)? ([모모제외])? N. ...   /  N. (1) ... (금오신화식 하위표기)
RE_QHEAD = re.compile(r"^\s*(\u2605\s*)?(\[모모제외\]\s*)?(\d+)\.\s*(?:\((\d+)\)\s*)?")
RE_ANS   = re.compile(r"^\s*답\s*[:：]\s*(.*)$")
RE_STAGE = re.compile(r"^\s*(\d)\s*(?:단계|단어|이해|질문|토론|글쓰기)")
RE_STOP  = re.compile(r"(글쓰기\s*주제|주제\s*글쓰기|<\s*글쓰기|^\s*\[?글쓰기|^\s*3\s*글쓰기|내\s*글로\s*엮기|^Step\s*\d)")
# β: 다음 문항의 지문은 유형 헤더로 시작 → 정답 꼬리에서 잘라낼 신호
RE_NEXTHDR = re.compile(r"(사실적|추론적|분석적|비판적|적용|창의적|감상)\s*독해")
RE_PAGE  = re.compile(r"\((\d{1,3}(?:-\d{1,3})?)\s*쪽?\)|[-\u2013]?\s*p\.?\s*\d{1,3}", re.I)
RE_QMARK = re.compile(r"[?\uff1f]")
# 정답 끝단의 PDF 페이지 하단 마커(예: "- 4 -", 가운데정렬 페이지번호)만 제거.
# 앞에 공백이 있어야 매치되므로 본문 중 대화 줄표/목록 번호("1-2-3" 등)는 건드리지 않고,
# 반드시 문자열 맨 끝(anchor $)에서 dash-숫자-dash로 끝나는 경우만 잡음.
# RE_PAGE("p.NNN"/"(NNN쪽)" 인용 표기)와는 표기 형태가 달라 하나로 합치지 않음.
RE_FOOTER_PAGENUM = re.compile(r"(?:\s+-\s*\d{1,3}\s*-\s*)+$")


def pdftext(path):
    return subprocess.run(["pdftotext", "-layout", path, "-"],
                          capture_output=True, text=True, check=True).stdout


def _clean(s):
    return re.sub(r"[ \t]+", " ", re.sub(r"\n+", " ", s)).strip()


def extract_answers(text, prompt_index=None):
    """교사용 텍스트 → [{stage, seq, sub_seq, excluded, starred, raw_answer, marker}]

    prompt_index: {(seq,sub_seq): student_prompt}  주어지면 학생용 발문을
    앵커로 삼아 교사용 블록에서 '발문 뒤부터'를 정답으로 정확히 잘라낸다.
    (발문 오염 제거 + 표 여러 칸 전체 확보)
    """
    prompt_index = prompt_index or {}
    lines = text.splitlines()
    blocks, cur = [], None
    stage = None
    stop = False

    def flush():
        nonlocal cur
        if cur:
            cur["_lines"] = cur.get("_lines", [])
            blocks.append(cur); cur = None

    for raw in lines:
        s = raw.rstrip("\n")
        stripped = s.strip()
        if not stripped:
            continue
        if RE_STOP.search(stripped):
            stop = True; flush(); continue
        if stop:
            continue

        mst = RE_STAGE.match(stripped)
        if mst and (len(stripped) < 30 or "단계" in stripped):
            stage = int(mst.group(1)); continue

        mh = RE_QHEAD.match(stripped)
        if mh:
            flush()
            cur = {"stage": stage, "seq": int(mh.group(3)),
                   "sub_seq": int(mh.group(4)) if mh.group(4) else None,
                   "excluded": 1 if mh.group(2) else 0,
                   "starred": 1 if mh.group(1) else 0,
                   "_lines": [s]}
            continue
        if cur is not None:
            cur["_lines"].append(s)
    flush()

    # 문항 블록 → 정답 텍스트 추출
    out = []
    for b in blocks:
        block = "\n".join(b.pop("_lines"))
        flat = re.sub(r"\s+", " ", block)
        marker = None; raw_answer = None

        # 1) '답:' 마커 (가장 신뢰도 높음)
        m = re.search(r"답\s*[:：]\s*", block)
        if m:
            raw_answer = block[m.end():]; marker = "답:"
        else:
            # 2) 학생용 발문을 앵커로. 발문 꼬리를 '글자 사이 공백 허용' 정규식으로
            #    찾아 줄바꿈 분절(예: '해 석')에도 매칭. 매치 끝~블록끝 = 정답 전체.
            cands = [prompt_index.get((b["seq"], b["sub_seq"])),
                     prompt_index.get((b["seq"], None)),
                     prompt_index.get((b["seq"], 1))]
            best = None
            for sp in filter(None, cands):
                tail = re.sub(r"\s+", "", sp)[-20:]
                if len(tail) < 5:
                    continue
                pat = re.compile(r"\s*".join(map(re.escape, tail)))
                mm = pat.search(block)
                if mm and (best is None or mm.end() < best):
                    best = mm.end()
            if best is not None:
                raw_answer = block[best:]; marker = "anchor"
            else:
                qs = list(RE_QMARK.finditer(block)); pg = list(RE_PAGE.finditer(block))
                cut = qs[-1].end() if qs else (pg[-1].end() if pg else 0)
                raw_answer = block[cut:]; marker = "post-prompt"

        # 다음 섹션/문항 오염 제거: 작품·단계 헤더 또는 다음 문항의 유형 헤더 앞에서 컷
        raw_answer = re.split(r"<[가-힣]+>\s*:|\uff0a|^\s*\d단계", raw_answer)[0]
        mnx = RE_NEXTHDR.search(raw_answer, 10)      # 앞 10자 이후의 유형 헤더 = 다음 문항 지문
        if mnx:
            raw_answer = raw_answer[:mnx.start()]
        raw_answer = RE_FOOTER_PAGENUM.sub("", _clean(raw_answer)).strip()
        b["raw_answer"] = raw_answer; b["marker"] = marker
        out.append(b)
    return out


if __name__ == "__main__":
    import sys
    from parser import parse_skeleton, pdftext as _pt
    teacher = sys.argv[1]
    pidx = {}
    if len(sys.argv) > 2:                 # 학생용 경로가 있으면 앵커로 사용
        for it in parse_skeleton(_pt(sys.argv[2])):
            pidx[(it["seq"], it["sub_seq"])] = it["prompt"]
    for b in extract_answers(pdftext(teacher), pidx):
        key = f"{b['seq']}" + (f"-{b['sub_seq']}" if b['sub_seq'] else "")
        flag = ("★" if b["starred"] else " ") + ("✕" if b["excluded"] else " ")
        print(f"[{key:>4}] {flag} <{b['marker']:>11}> {b['raw_answer'][:74]}")
