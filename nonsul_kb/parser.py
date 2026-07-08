"""
parser.py  —  논술교재 PDF에서 '문항 골격'을 자동 추출한다.

두 가지 포맷 프로파일을 모두 처리:
  α (금오신화·세계사추리반) : "2. [사실적/추론적 독해]"  ← 발문 줄에 유형 인라인
  β (어느날이런미래)        : "사실적 독해 / 분석적 독해" (별도 헤더 줄) + "1. ..."

자동 추출(Tier 1): 단계·단계명·작품·문항번호·하위번호·유형태그·지문/페이지·★
overlay/LLM(Tier 3): 표 라벨 정답, 참고박스 본문, 글쓰기 모범답, 복합표 정답
"""
import re
import subprocess

WORKS = {"만복사저포기", "이생규장전", "남염부주지"}
TYPES = "사실적|추론적|분석적|비판적|적용적|적용|창의적|감상"

RE_Q       = re.compile(r"^\s*(\d+)\.\s*(?:\[(.+?)\])?")
RE_TYPEHDR = re.compile(rf"^\s*((?:{TYPES})\s*(?:독해)?(?:\s*[/·]\s*(?:{TYPES})\s*(?:독해)?)*)\s*(?:\||$)")
RE_SUB     = re.compile(r"^\s*\((\d+)\)\s*\S")          # (1) 뒤 내용 필수 → 순수 (49) 페이지 배제
RE_STAGE   = re.compile(rf"^\s*(\d)\s*(?:단계|단어|이해|질문|토론|글쓰기)")
RE_QUOTE   = re.compile(r"[\u201c\"](.+?)[\u201d\"]", re.S)
RE_TYPE    = re.compile(rf"({TYPES})")
RE_STOP    = re.compile(r"(3\s*단계|글쓰기\s*주제|주제\s*글쓰기|<\s*글쓰기|^Step\s*\d)")
RE_PAGE    = re.compile(
    r"\((\d{1,3}(?:-\d{1,3})?)\s*쪽?\)"
    r"|[-\u2013]?\s*p\.?\s*(\d{1,3})(?:\s*[~\u301c\uff5e\u2013-]\s*\d{1,3})?",
    re.I)  # "p.146~148" 뒷부분(~148)까지 통째로 소비 (잔여 텍스트로 안 새게)


def pdftext(path: str) -> str:
    out = subprocess.run(["pdftotext", "-layout", path, "-"],
                         capture_output=True, text=True, check=True)
    return out.stdout


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _types(s):
    """유형 태그 추출 + 정규화(적용적→적용) + 순서보존 dedup."""
    raw = ["적용" if t == "적용적" else t for t in RE_TYPE.findall(s)]
    return list(dict.fromkeys(raw))


def parse_skeleton(text: str):
    lines = text.splitlines()
    items, cur = [], None
    stage = stage_title = work = None
    ordn = 0
    pending_types = []

    passage_buf = []      # β: 발문(N.) 앞에 나오는 지문 줄들을 임시 수집
    collecting = False    # β 지문 수집 모드

    def flush():
        nonlocal cur
        if cur:
            items.append(cur); cur = None

    def new_item(seq, sub, tags, starred, prebuf=None):
        nonlocal ordn
        ordn += 1
        return {"stage": stage, "stage_title": stage_title, "work": work,
                "seq": seq, "sub_seq": sub, "reading_types": tags,
                "prompt": "", "passage_quote": "", "passage_page": None,
                "answer_area_type": "box", "scaffold_labels": None,
                "is_starred": starred, "ord": ordn, "_buf": list(prebuf or [])}

    stop = False
    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        if RE_STOP.search(s):
            stop = True; flush(); collecting = False; pending_types = []; continue
        if stop:
            continue

        # 단계 헤더
        m = RE_STAGE.match(s)
        if m and (len(s) < 30 or "단계" in s or "퀴즈" in s or "토론" in s or "글쓰기" in s):
            stage = int(m.group(1)); stage_title = _clean(s)
            pending_types = []; collecting = False; passage_buf = []
            continue

        # 작품 헤더
        bare = s.strip("<>\u300c\u300d ")
        if bare in WORKS and len(s) <= len(bare) + 4:
            work = bare; continue

        # 유형 헤더 줄 (β) → 다음 N. 발문의 태그 + 지문 수집 시작
        mth = RE_TYPEHDR.match(s)
        if mth and "독해" in s and not re.match(r"^\s*\d+\.", s):
            flush()
            pending_types = _types(mth.group(1))
            collecting = True; passage_buf = []
            continue

        # 문항 시작
        mq = RE_Q.match(s)
        if mq:
            inline = mq.group(2)
            tags = (_types(inline) if inline else list(pending_types))
            if inline is not None or tags:
                residual = re.sub(r"^\s*\d+\.\s*(?:\[.+?\]\s*)?", "", s)  # 발문 잔여
                flush()
                cur = new_item(int(mq.group(1)), None, tags, 1 if "\u2605" in s else 0,
                               prebuf=passage_buf)          # β: 수집한 지문을 앞에 붙임
                if residual.strip():
                    cur["_buf"].append(residual.strip())    # β: 발문 텍스트
                # pending_types는 여기서 초기화하지 않음(sticky) — 같은 유형 헤더를
                # 공유하는 연속 문항(예: 헤더 하나에 3., 4. 두 문항)이 다음 헤더/단계
                # 전환 전까지 계속 태그를 이어받도록 함.
                collecting = False; passage_buf = []
                continue

        # 지문 수집 모드(β)면 지문 버퍼로, 아니면 현재 문항 버퍼로
        if collecting:
            passage_buf.append(s); continue
        if cur is None:
            continue

        # 하위 문항 (1)(2)
        msub = RE_SUB.match(s)
        if msub and int(msub.group(1)) <= 9:
            if cur["sub_seq"] is None and not cur["_buf"]:
                cur["sub_seq"] = int(msub.group(1))
            else:
                seq0, tags0 = cur["seq"], list(cur["reading_types"])
                flush()
                cur = new_item(seq0, int(msub.group(1)), tags0, 0)

        cur["_buf"].append(s)

    flush()

    # 후처리: 지문/발문 분리 (마지막 페이지 마커 기준)
    for it in items:
        block = "\n".join(it.pop("_buf"))
        marks = list(RE_PAGE.finditer(block))
        has_quote_before = lambda pos: RE_QUOTE.search(block[:pos]) is not None
        good = []
        for mk in marks:
            val = mk.group(1) or mk.group(2)
            n = int(re.match(r"\d+", val).group())
            if has_quote_before(mk.start()) or "-" in val or n >= 10 or mk.group(2):
                good.append((mk, n))
        if good:
            mk, n = good[-1]
            passage, prompt = block[:mk.start()], block[mk.end():]
            it["passage_page"] = n
        else:
            passage, prompt = "", block
        qm = RE_QUOTE.search(passage) or RE_QUOTE.search(block)
        it["passage_quote"] = _clean(qm.group(1)) if qm else ""
        prompt = re.sub(r"^\s*\(\d+\)\s*", "", prompt.strip())
        prompt = _clean(prompt)
        qpos = [mm.end() for mm in re.finditer(r"[?\uff1f]", prompt)]
        if qpos:
            prompt = prompt[:qpos[-1]]
        it["prompt"] = prompt
    return items


if __name__ == "__main__":
    import sys, json
    sk = parse_skeleton(pdftext(sys.argv[1]))
    print(f"[자동 추출] 문항 {len(sk)}개")
    for it in sk:
        print(json.dumps(it, ensure_ascii=False))
