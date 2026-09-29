# -*- coding: utf-8 -*-
"""KRDict(한국어기초사전) XML 덤프 파싱 - lemma -> [{homonym, pos, level, defs}] 딕셔너리로
캐시(json)한다. 읽기 전용, 원본 XML은 건드리지 않는다."""
import glob
import json
import re
from pathlib import Path
from xml.etree import ElementTree as ET

_ILLEGAL_XML_CHARS_RE = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f]")

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRATCH_DIR = Path.home() / "l0l6_scratch"  # 실행 환경에 맞게 조정
DUMP_DIR = str(REPO_ROOT / "raw" / "krdict" / "krdict_dump")  # git 미포함 - scripts/literacy/krdict_dump.py로 다운로드
OUT_PATH = str(SCRATCH_DIR / "krdict_lemma_index.json")

lemma_index = {}
total_entries = 0
files = sorted(glob.glob(DUMP_DIR + "/*.xml"))
print(f"파일 {len(files)}개")

for fp in files:
    with open(fp, encoding="utf-8") as f:
        raw = f.read()
    raw = _ILLEGAL_XML_CHARS_RE.sub("", raw)
    root = ET.fromstring(raw)
    for entry in root.iter("LexicalEntry"):
        feats = {}
        for feat in entry.findall("feat"):
            att = feat.get("att")
            val = feat.get("val")
            feats.setdefault(att, val)
        lemma_el = entry.find("Lemma")
        lemma = None
        if lemma_el is not None:
            wf = lemma_el.find("feat[@att='writtenForm']")
            if wf is not None:
                lemma = wf.get("val")
        if not lemma:
            continue
        homonym = feats.get("homonym_number") or feats.get("homonymNumber")
        pos = feats.get("partOfSpeech")
        level = feats.get("vocabularyLevel")
        defs = []
        for sense in entry.findall("Sense"):
            for feat in sense.findall("feat"):
                if feat.get("att") == "definition":
                    v = feat.get("val")
                    if v:
                        defs.append(v)
        lemma_index.setdefault(lemma, []).append({
            "homonym": homonym, "pos": pos, "level": level, "defs": defs,
        })
        total_entries += 1
    print(f"  {fp}: 누적 entries={total_entries}, 누적 lemma={len(lemma_index)}")

with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(lemma_index, f, ensure_ascii=False)
print(f"완료: entries={total_entries}, distinct lemma={len(lemma_index)}")
