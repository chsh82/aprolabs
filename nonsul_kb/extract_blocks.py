"""
extract_blocks.py  —  교재의 '비문항 영역'을 content_blocks 로 확보.

문항(2단계 발문)은 parser 가 담당. 그 외 전부가 여기 대상:
  cover(표지·제사) / vocab_quiz(어휘) / ox_quiz(O/X) / keyword_def(키워드 정의)
  / background(배경지식·빈칸) / illustration_pick(삽화 고르기) / writing(글쓰기)

원칙(사용자 요청): 블록 구조를 완벽 모델링하기보다 '텍스트를 정확히' 담는다.
교사용에서 같은 타입 블록의 텍스트도 함께 잡아 빈칸/정의 정답을 확보한다.
"""
import re, subprocess

RE_STAGE = re.compile(r"^\s*(\d)\s*(?:단계|단어|이해|질문|토론|글쓰기)")
RE_QSTART = re.compile(r"^\s*\d+\.\s")          # 2단계 발문 시작
RE_WRITING = re.compile(r"글쓰기\s*주제|주제\s*글쓰기|<\s*글쓰기|내\s*글로\s*엮기|Step\s*\.?\s*\d")
RE_OX = re.compile(r"O\s*[/.]\s*X")
RE_VOCAB = re.compile(r"어휘|단어의\s*뜻|뜻\s*유추")
RE_KEYWORD = re.compile(r"키워드.*정의|정의를\s*살펴")
RE_ILLUST = re.compile(r"삽화|인상적이었던\s*그림")
RE_MORE = re.compile(r"더\s*알아보기|작가\s*소개|글쓴이\s*소개")


def pdftext(path):
    return subprocess.run(["pdftotext", "-layout", path, "-"],
                          capture_output=True, text=True, check=True).stdout


def _clean(s):
    return re.sub(r"[ \t]+", " ", re.sub(r"\n{2,}", "\n", s)).strip()


def _classify(text, is_before_first_stage, in_question_stage):
    if is_before_first_stage:
        return "cover"
    if RE_WRITING.search(text):
        return "writing"
    if RE_KEYWORD.search(text):
        return "keyword_def"
    if RE_OX.search(text) and RE_VOCAB.search(text):
        return "vocab+ox"        # 뒤에서 분리
    if RE_OX.search(text):
        return "ox_quiz"
    if RE_VOCAB.search(text):
        return "vocab_quiz"
    if RE_ILLUST.search(text):
        return "illustration_pick"
    if in_question_stage:
        return "keyword_def"     # 문항 스테이지의 발문 前 영역(정의표 등)
    return "background"


def _flex(s):
    """공백 유연 정규식: 글자 사이 줄바꿈 허용."""
    return re.compile(r"\s*".join(map(re.escape, re.sub(r"\s+", "", s))))


def teacher_region(teacher_full, block_text, title=""):
    """블록의 시작·끝 텍스트를 교사용 전문에서 앵커로 찾아 그 구간(=정답 포함)을 반환.
    교사용 블록 구조가 학생용과 달라도 동작(타입 매칭 대신 앵커)."""
    body = block_text
    if title:
        body = body.replace(title, "", 1)
    norm = re.sub(r"\s+", "", body)
    if len(norm) < 30:
        return None
    start_key, end_key = norm[:18], norm[-18:]
    sm = _flex(start_key).search(teacher_full)
    if not sm:
        return None
    em = _flex(end_key).search(teacher_full, sm.end())
    region = teacher_full[sm.start(): em.end()] if em \
        else teacher_full[sm.start(): sm.start() + int(len(norm) * 2.2)]
    return _clean(region)


def extract_blocks(text):
    lines = text.splitlines()
    # 경계: 스테이지 헤더 위치
    stages = []
    for i, ln in enumerate(lines):
        s = ln.strip()
        m = RE_STAGE.match(s)
        if m and (len(s) < 40 or "단계" in s or "퀴즈" in s or "토론" in s or "글쓰기" in s):
            stages.append((i, int(m.group(1)), s))
    if not stages:
        return []

    blocks = []
    # 표지: 첫 스테이지 이전
    cover = _clean("\n".join(lines[:stages[0][0]]))
    if cover:
        blocks.append(("cover", "표지/제사", cover))

    for si, (idx, num, title) in enumerate(stages):
        end = stages[si + 1][0] if si + 1 < len(stages) else len(lines)
        region = lines[idx:end]
        rtext = "\n".join(region)
        is_qstage = ("심화" in title or "질문" in title or "토론" in title)

        if is_qstage:
            # 발문 前 영역(키워드 정의/삽화 등) + 발문 後 영역(글쓰기 주제)만 블록화.
            qidx = next((j for j, ln in enumerate(region) if RE_QSTART.match(ln.strip())
                         or re.match(r"^\s*(사실적|추론적|분석적|비판적|적용적|적용)\s*독해\s*/", ln.strip())), len(region))
            pre = _clean("\n".join(region[:qidx]))
            if pre and (RE_KEYWORD.search(pre) or RE_ILLUST.search(pre)) and len(pre) > 30:
                bt = "illustration_pick" if RE_ILLUST.search(pre) else "keyword_def"
                blocks.append((bt, _clean(title), pre))
            # 스테이지 끝에 글쓰기 주제가 붙는 경우(예: 세계사)
            widx = next((j for j, ln in enumerate(region) if RE_WRITING.search(ln)), None)
            if widx is not None and widx > qidx:
                wtext = _clean("\n".join(region[widx:]))
                if len(wtext) > 30:
                    blocks.append(("writing", "글쓰기 주제", wtext))
            # 스테이지 끝의 '더 알아보기'(작가 소개 등)
            midx = next((j for j, ln in enumerate(region) if RE_MORE.search(ln)), None)
            if midx is not None and midx > qidx:
                mend = widx if (widx is not None and widx > midx) else len(region)
                mtext = _clean("\n".join(region[midx:mend]))
                if len(mtext) > 30:
                    blocks.append(("author_bio", "더 알아보기", mtext))
            continue

        if "글쓰기" in title or RE_WRITING.search(rtext):
            blocks.append(("writing", _clean(title), _clean(rtext)))
            continue
        bt = _classify(rtext, False, False)
        if bt == "vocab+ox":
            # O/X 퀴즈 시작 = '...O.X 퀴즈를 풀어...' 지시문 줄 (제목의 O.X 언급 제외)
            ox_j = next((j for j, ln in enumerate(region)
                         if j > 0 and re.search(r"O\s*[/.]\s*X\s*퀴즈.{0,8}풀", ln)), None)
            if ox_j is None:
                ox_j = next((j for j, ln in enumerate(region)
                             if j > 0 and RE_OX.search(ln)), None)
            if ox_j:
                blocks.append(("vocab_quiz", _clean(title),
                               _clean("\n".join(region[:ox_j]))))
                blocks.append(("ox_quiz", "O/X 퀴즈",
                               _clean("\n".join(region[ox_j:]))))
                continue
            bt = "vocab_quiz"
        blocks.append((bt, _clean(title), _clean(rtext)))
    return blocks


if __name__ == "__main__":
    import sys
    for bt, title, body in extract_blocks(pdftext(sys.argv[1])):
        print(f"[{bt:17}] {title[:24]:24} | {len(body):4}자 | {body[:46].replace(chr(10),' ')}")
