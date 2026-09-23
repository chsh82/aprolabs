"""원본 template.html + data_*.js로 3개 시안 HTML을 만든다(비교용 기준).
prototype/build.py는 클로드 샌드박스 경로(/home/claude/proto/...)를 참조해서 이 저장소에서는
그대로 못 돌린다 - 로컬 assets/ 폴더를 쓰도록 다시 쓴 버전. renderer/의 새 렌더러가 시안과
동일하게 그리는지 확인하는 tests/verify_stage2.js가 이 스크립트의 출력물을 기준으로 삼는다.
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO = ROOT / "prototype"
ASSETS = ROOT / "assets"
OUT = PROTO / "out"
OUT.mkdir(exist_ok=True)

manifest = json.loads((ASSETS / "manifest.json").read_text(encoding="utf-8"))
T = (PROTO / "template.html").read_text(encoding="utf-8")


def build(data_file, imgs, title, brand, out_name):
    data_js = (PROTO / data_file).read_text(encoding="utf-8")
    t = T.replace("__DATA__", data_js)
    t = t.replace("__LOGO__", f"../../assets/{manifest['logoIvory']}")
    t = t.replace("__TITLE__", title)
    t = t.replace("__BRAND__", brand)
    img_map = {k: f"../../assets/{manifest[v]}" for k, v in imgs.items()}
    t = t.replace("__IMGS__", json.dumps(img_map, ensure_ascii=False))
    out_path = OUT / out_name
    out_path.write_text(t, encoding="utf-8")
    print(out_path, len(t))


build(
    "data_ginginbam.js",
    {k: k for k in ["holmes", "jekyll", "aronnax", "anne", "cover", "logoIvory"]},
    "긴긴밤 10주차 · 태블릿 학습지 시안", "긴긴밤 · 초등 고학년 시안",
    "L5-Q3-W10.html",
)
build(
    "data_yeolha.js",
    {"cover": "cover_yh", "samjeondo": "samjeondo", "logoIvory": "logoIvory", "hanja5": "hanja5"},
    "열하일기 7주차 · 중등 태블릿 학습지 시안", "열하일기 · 중학생 시안",
    "L9-Q3-W07.html",
)
build(
    "data_yaong.js",
    {"cover": "cover_ya", "logoIvory": "logoIvory", "dorothy": "dorothy", "fogg": "fogg",
     "anne": "anne", "holmes": "holmes", "jekyll": "jekyll", "aronnax": "aronnax",
     "ill4": "ill4", "ill6": "ill6", "ill7": "ill7", "ill8": "ill8"},
    "야옹아, 가족이 되어 줄게 8주차 · 저학년 태블릿 학습지 시안", "야옹아 · 초등 저학년 시안",
    "L2-Q2-W08.html",
)
