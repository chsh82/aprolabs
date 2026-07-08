"""
build_all.py  —  학생용+교사용 PDF 쌍들을 정답까지 자동으로 momo_kb.db 에 적재.

수작업 OVERLAY dict 제거:  gen_overlay.generate() 가 교사용에서 뽑은 정답을
학생용 골격과 (seq, sub_seq) 키로 조인하여 items.model_answer 에 바로 채운다.

  parse_skeleton(학생용)  ─┐
                          ├─(seq,sub_seq)─►  items (+model_answer, excluded, ★)
  generate(교사용)        ─┘

use_llm=True 면 MOMOAI(Anthropic API)로 구조화, False 면 원문 폴백.
"""
import os, json, sqlite3, argparse
from parser import pdftext, parse_skeleton
from gen_overlay import generate, find_teacher_page, read_ox_vision
from extract_blocks import extract_blocks, teacher_region

HERE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(HERE, "momo_kb.db")
DATA = os.environ.get("MOMOAI_DATA", os.path.join(HERE, "data"))


def load_materials():
    """materials.json 이 있으면 그걸, 없으면 data/ 내 *_학생용/_교사용 쌍 자동 탐색.
    각 항목: {work_title, author, grade, quarter, week, subject, student, teacher}
    student/teacher 는 DATA 기준 상대경로(또는 절대경로)."""
    cfg = os.path.join(HERE, "materials.json")
    if os.path.exists(cfg):
        with open(cfg, encoding="utf-8") as f:
            mats = json.load(f)
        for m in mats:
            for k in ("student", "teacher"):
                if not os.path.isabs(m[k]):
                    m[k] = os.path.join(DATA, m[k])
        return mats
    raise SystemExit(f"materials.json 이 없습니다. {cfg} 를 만들어 교재 쌍을 등록하세요.\n"
                     "예시는 README.md 참고.")


MATERIALS = None   # main()에서 load_materials()로 채움


def key_of(seq, sub):
    return f"{seq}" + (f"-{sub}" if sub else "")


def load_material(con, meta, use_llm):
    mid = con.execute(
        "INSERT INTO materials(work_title,author,grade,quarter,week,subject,"
        "src_student,src_teacher) VALUES(?,?,?,?,?,?,?,?)",
        (meta["work_title"], meta["author"], meta["grade"], meta["quarter"],
         meta["week"], meta["subject"],
         os.path.basename(meta["student"]), os.path.basename(meta["teacher"]))).lastrowid

    skeleton = parse_skeleton(pdftext(meta["student"]))
    overlay, _ = generate(meta["student"], meta["teacher"], use_llm=use_llm)

    matched = hi = 0
    for it in skeleton:
        ov = overlay.get(key_of(it["seq"], it["sub_seq"])) \
             or overlay.get(key_of(it["seq"], None))
        ma = ov["model_answer"] if ov else None
        if ov:
            matched += 1
            if ov.get("confidence") == "high":
                hi += 1
        iid = con.execute(
            "INSERT INTO items(material_id,stage,stage_title,work,seq,sub_seq,excluded,"
            "prompt,passage_quote,passage_page,answer_area_type,scaffold_labels,"
            "model_answer,answer_confidence,answer_flags,is_starred,ord) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (mid, it["stage"], it.get("stage_title"), it["work"], it["seq"], it["sub_seq"],
             ov.get("excluded", 0) if ov else 0,
             it["prompt"], it["passage_quote"], it["passage_page"],
             it["answer_area_type"],
             json.dumps(it["scaffold_labels"], ensure_ascii=False) if it["scaffold_labels"] else None,
             json.dumps(ma, ensure_ascii=False) if ma else None,
             ov.get("confidence") if ov else None,
             json.dumps(ov.get("flags"), ensure_ascii=False) if ov and ov.get("flags") else None,
             ov.get("is_starred", 0) if ov else it["is_starred"], it["ord"])).lastrowid
        for rt in it["reading_types"]:
            con.execute("INSERT OR IGNORE INTO item_tags(item_id,reading_type) VALUES(?,?)",
                        (iid, rt))

    # 비문항 영역 → content_blocks (앵커로 교사용 정답 확보, O/X는 비전 표시)
    s_blocks = extract_blocks(pdftext(meta["student"]))
    teacher_full = pdftext(meta["teacher"])
    for i, (bt, ti, s_body) in enumerate(s_blocks):
        payload = {"student_text": s_body}
        if bt == "ox_quiz":
            # O/X 정답은 음영 마크 → 텍스트 불가. 교사용 페이지를 비전으로.
            pg = find_teacher_page(meta["teacher"], "", s_body)
            payload["needs_vision"] = True
            payload["teacher_page"] = pg
            if use_llm:
                try:
                    payload["ox_answers"] = read_ox_vision(meta["teacher"], pg)
                    payload["needs_vision"] = False
                except Exception as e:
                    payload["vision_error"] = str(e)
        elif bt not in ("cover",):
            # 배경지식·키워드정의·어휘·글쓰기: 앵커로 교사용 정답 구간 확보
            t_body = teacher_region(teacher_full, s_body, ti)
            if t_body and t_body != s_body:
                payload["teacher_text"] = t_body
        con.execute("INSERT INTO content_blocks(material_id,block_type,title,body,ord) "
                    "VALUES(?,?,?,?,?)",
                    (mid, bt, ti, json.dumps(payload, ensure_ascii=False), i))

    return mid, len(skeleton), matched, hi, len(s_blocks)


def preflight(use_llm):
    """--llm 실행 전 환경 점검: 키·패키지·pdftoppm."""
    import shutil
    problems = []
    if use_llm:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            problems.append("ANTHROPIC_API_KEY 환경변수가 없습니다.")
        try:
            import anthropic  # noqa
        except ImportError:
            problems.append("anthropic 패키지 미설치 (pip install anthropic).")
        if not shutil.which("pdftoppm"):
            problems.append("pdftoppm 없음 (비전 렌더 불가). poppler-utils 설치 필요.")
    if not shutil.which("pdftotext"):
        problems.append("pdftotext 없음. poppler-utils 설치 필요.")
    if problems:
        print("⚠ 실행 전 점검 실패:")
        for p in problems:
            print("   -", p)
        raise SystemExit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--llm", action="store_true", help="MOMOAI로 정답 구조화(키 필요)")
    args = ap.parse_args()
    preflight(args.llm)
    global MATERIALS
    MATERIALS = load_materials()

    if os.path.exists(DB):
        os.remove(DB)
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys=ON")
    with open(os.path.join(HERE, "schema.sql"), encoding="utf-8") as f:
        con.executescript(f.read())

    print(f"자동 적재 (LLM={'ON' if args.llm else 'OFF/폴백'})")
    tot_i = tot_m = tot_h = tot_b = 0
    for meta in MATERIALS:
        _, ni, nm, nh, nb = load_material(con, meta, use_llm=args.llm)
        tot_i += ni; tot_m += nm; tot_h += nh; tot_b += nb
        print(f"  {meta['work_title']:18s} {meta['grade']:4s} | "
              f"문항 {ni:2d} | 정답 {nm:2d} | 고신뢰 {nh:2d} | 블록 {nb}")
    con.commit()
    print(f"  {'합계':18s}      | 문항 {tot_i:2d} | 정답 {tot_m:2d} | 고신뢰 {tot_h:2d} | 블록 {tot_b}")

    # 검증 쿼리: 정답이 실제로 들어갔는지
    print("\n정답 적재 확인 — ★ 중요문항 & model_answer 존재율")
    for r in con.execute("""
        SELECT m.work_title,
               SUM(CASE WHEN i.model_answer IS NOT NULL THEN 1 ELSE 0 END) ans,
               COUNT(*) tot,
               SUM(i.is_starred) star, SUM(i.excluded) excl
        FROM items i JOIN materials m ON m.id=i.material_id
        GROUP BY m.id ORDER BY m.id"""):
        print(f"  {r[0]:18s} 정답 {r[1]}/{r[2]}  ★{r[3]}  제외{r[4]}")

    print("\n검토 필요 목록 — 저신뢰(비전 권장) 문항과 사유")
    for r in con.execute("""
        SELECT m.work_title, i.seq, i.sub_seq, i.answer_flags
        FROM items i JOIN materials m ON m.id=i.material_id
        WHERE i.answer_confidence='low' ORDER BY m.id, i.ord"""):
        sub = f"-{r[2]}" if r[2] else ""
        flags = ", ".join(json.loads(r[3])) if r[3] else "-"
        print(f"  {r[0]:18s} {r[1]}{sub}  ⚠ {flags}")

    print("\n블록 적재 확인 — 교재 × 블록유형 (전체 내용 커버리지)")
    for r in con.execute("""
        SELECT m.work_title, GROUP_CONCAT(b.block_type, ', ') types, COUNT(*) n
        FROM content_blocks b JOIN materials m ON m.id=b.material_id
        GROUP BY m.id ORDER BY m.id"""):
        print(f"  {r[0]:18s} ({r[2]}) {r[1]}")
    tt = con.execute("SELECT COUNT(*) FROM content_blocks WHERE body LIKE '%teacher_text%'").fetchone()[0]
    ov = con.execute("SELECT COUNT(*) FROM content_blocks WHERE body LIKE '%needs_vision%'").fetchone()[0]
    print(f"  · 교사용 정답 포함 블록: {tt}   · O/X 비전 대기 블록: {ov}")

    print("\n샘플 — 적재된 정답 하나")
    row = con.execute("""SELECT m.work_title, i.seq, i.prompt, i.model_answer
        FROM items i JOIN materials m ON m.id=i.material_id
        WHERE i.model_answer IS NOT NULL ORDER BY i.is_starred DESC, i.id LIMIT 1""").fetchone()
    if row:
        ans = json.loads(row[3])
        first = ans.get("answer") or next(iter(ans.values()), "")
        print(f"  [{row[0]} {row[1]}번] {row[2][:40]}…")
        print(f"  정답: {first[:90]}…")
    con.close()
    print(f"\n✓ {os.path.basename(DB)} — {len(MATERIALS)}개 교재를 정답까지 자동 적재 완료")


if __name__ == "__main__":
    main()
