"""krdict(한국어기초사전) 전체 덤프 다운로드 + XML 파싱.

docs/literacy/02-속담수집.md에서 검색 API(`part=ip`)가 한 글자 검색어에 대해
0건을 반환하는 문제를 실측으로 확인한 뒤, API 순회를 버리고 이 방식(전체
내려받기 파싱)으로 전환했다. 이 모듈은 Phase 2(속담·관용구)뿐 아니라 이후
Phase 3(어휘) 수집에서도 재사용한다.

원본 덤프는 raw/krdict/에 보관하고 git에는 커밋하지 않는다.
"""
from __future__ import annotations

import re
import urllib.request
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree import ElementTree as ET

DUMP_URL = "https://krdict.korean.go.kr/dicBatchDownload?seq=213"  # XML 전체 내려받기
RAW_DIR = Path(__file__).resolve().parents[2] / "raw" / "krdict"

# XML 1.0 규격상 허용 안 되는 제어문자. 실제 덤프(11개 파일 중 4개)에 0x08(백스페이스)이
# <Equivalent>(외국어 번역) 안에 섞여 있어 ET.parse가 전체 파일을 못 읽는 문제가 있었다.
# 우리가 읽는 필드(Lemma/lexicalUnit/Sense>definition)에는 안 나타나는 걸 확인했으므로
# 파싱 전에 제거한다 - 우리가 쓰는 데이터에는 영향 없음.
_ILLEGAL_XML_CHARS_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f]")


@dataclass
class Entry:
    external_id: str
    headword: str
    lexical_unit: str  # 단어/구/관용구/속담/문법‧표현
    definitions: list[str] = field(default_factory=list)  # Sense 순서대로


def download_dump(force: bool = False) -> Path:
    """전체 XML 덤프 zip을 raw/krdict/에 받는다. 이미 있으면 재사용(재다운로드 안 함).

    파일명은 서버가 Content-Disposition으로 주는 이름을 쓰지 않고 고정 이름을
    쓴다 - 이 서버가 그 헤더를 UTF-8 그대로(RFC 7230 위반) 보내서 urllib가
    latin-1로 잘못 디코딩해 파일명이 깨지는 문제가 있었다. 고정 이름이면 이
    문제 자체가 발생하지 않고, 재다운로드 시 덮어쓰기도 더 간단해진다.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = RAW_DIR / "krdict_dump.zip"

    if not force and zip_path.exists():
        return zip_path

    req = urllib.request.Request(DUMP_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()

    zip_path.write_bytes(data)
    return zip_path


def extract_dump(zip_path: Path) -> list[Path]:
    """zip 안의 XML 파일들을 raw/krdict/{zip 이름}/에 풀고 경로 목록을 반환한다.

    이미 풀려 있으면 재사용한다(재실행 시 압축 해제를 다시 안 함).
    """
    extract_dir = RAW_DIR / zip_path.stem
    if extract_dir.exists():
        existing = sorted(extract_dir.glob("*.xml"))
        if existing:
            return existing

    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    return sorted(extract_dir.glob("*.xml"))


def iter_entries(xml_paths: list[Path]):
    """모든 XML 파일의 LexicalEntry를 순서대로 yield한다."""
    for path in xml_paths:
        text = _ILLEGAL_XML_CHARS_RE.sub("", path.read_text(encoding="utf-8"))
        root = ET.fromstring(text)
        for lex_entry in root.iter("LexicalEntry"):
            external_id = lex_entry.get("val")
            headword_feat = lex_entry.find("Lemma/feat[@att='writtenForm']")
            unit_feat = lex_entry.find("feat[@att='lexicalUnit']")
            if external_id is None or headword_feat is None or unit_feat is None:
                continue

            definitions = []
            for sense in lex_entry.findall("Sense"):
                dfeat = sense.find("feat[@att='definition']")
                definitions.append(dfeat.get("val") if dfeat is not None else "")

            yield Entry(
                external_id=external_id,
                headword=headword_feat.get("val"),
                lexical_unit=unit_feat.get("val"),
                definitions=definitions,
            )
