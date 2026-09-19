# -*- coding: utf-8 -*-
"""
모모의책장 교재DB — A4 학습지 자동 생성 가능 여부 진단 스크립트 (읽기 전용).

momo_book_db/momo_book.db 를 읽기 전용으로 열어 documents 305건 각각을
BLOCKER(치명적 구조 결함 - 등급 unusable) / WARNING(손봐야 함 - 등급
needs_review) 규칙으로 검사하고, momo_book_db/reports/ 아래에
doc_diagnosis.csv 와 DIAGNOSIS_SUMMARY.md 를 만든다.

이 스크립트는 DB나 다른 코드를 전혀 수정하지 않는다(전부 SELECT).

사용법:
    python momo_book_db/diagnose_docs.py                 # 전체 305건 진단 + 리포트 생성
    python momo_book_db/diagnose_docs.py --doc-id L5-Q4-W10  # 한 문서만 상세 출력(리포트 파일은 안 만듦)
"""
import argparse
import csv
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict

# ============================================================
# 판정 임계치/정규식 상수 (나중에 조정할 것을 고려해 전부 여기 모음)
# ============================================================

DB_PATH = os.path.join(os.path.dirname(__file__), "momo_book.db")
IMAGES_DIR = os.path.join(os.path.dirname(__file__), "extracted_images")
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
CSV_PATH = os.path.join(REPORTS_DIR, "doc_diagnosis.csv")
SUMMARY_PATH = os.path.join(REPORTS_DIR, "DIAGNOSIS_SUMMARY.md")

# B3. 정규화 후 문항이 독해유형 라벨과 같은지/너무 짧은지 판정
#   - 정규화: 공백 전부 + 문장부호/기호 전부 제거(한글·영문·숫자만 남김)
#   - \w 는 유니코드 모드에서 한글도 "단어 문자"로 취급하므로, [^\w] 를 지우면
#     공백/문장부호/슬래시 등이 전부 사라지고 한글+영문+숫자만 남는다.
NORMALIZE_RE = re.compile(r"[^\w]", re.UNICODE)
B3_MIN_LEN = 10  # 정규화 후 이 길이 미만이면 "문항이라 하기엔 너무 짧음"으로 판정

# B5. 폰트 깨짐 판정
#   - question_text + excerpt_text 를 이어붙인 문자열에서, 아래 SUSPECT_CHARS
#     중 하나가 "한글 글자 바로 앞 또는 바로 뒤"(공백 없이 붙어서)에 나타나면
#     "잡문자 낀 글자"로 센다. 정상적인 한국어 문장에서는 이 문자들이 한글에
#     공백 없이 바로 붙어 나오는 경우가 거의 없고(반점/온점 등 정상 문장부호는
#     이 집합에 없음), 폰트가 깨진 PDF에서는 단어 사이에 이런 문자가 흔히
#     끼어 나온다(예: "능력주의적) 대입이) 갖는)", "행운! 평등주의에! 대한!").
#   - 잡문자 수 / 전체 글자 수 비율이 FONT_BROKEN_RATIO_THRESHOLD 를 넘으면
#     BLOCKER(B5)로 판정한다.
#   - 실측 보정: L9-Q4-W07(7.88%), L9-Q1-W03(5.77%), L9-Q4-W08(5.29%) 세 문서가
#     이 규칙으로 잡히고, 나머지 302개 문서는 전부 1.82% 이하였다(직접 실행해
#     확인함). 2%를 기준으로 하면 알려진 폰트 깨짐 문서만 깨끗하게 갈린다.
SUSPECT_CHARS = set("$#!)")
FONT_BROKEN_RATIO_THRESHOLD = 0.02
HANGUL_RE = re.compile(r"[가-힣]")

# W3. 독해유형(reading_type) 정상값 판정 - "/"로 이어붙인 조합까지 허용
READING_TYPE_BASE = {"사실적", "추론적", "비판적", "분석적", "적용적"}

# W1/W2 판정에 쓰는 표 형태(ui_type) 값
TABLE_UI_TYPE = "table_compare"


def normalize_text(text):
    """공백/문장부호를 지우고 한글·영문·숫자만 남김(B3 판정용)."""
    return NORMALIZE_RE.sub("", text or "")


def is_font_broken(combined_text):
    """(잡문자 비율이 임계치를 넘는지, 잡문자 개수, 전체 글자 수) 반환."""
    if not combined_text:
        return False, 0, 0
    polluted = 0
    n = len(combined_text)
    for i, ch in enumerate(combined_text):
        if ch in SUSPECT_CHARS:
            prev_hangul = i > 0 and bool(HANGUL_RE.match(combined_text[i - 1]))
            next_hangul = i < n - 1 and bool(HANGUL_RE.match(combined_text[i + 1]))
            if prev_hangul or next_hangul:
                polluted += 1
    ratio = polluted / n
    return ratio > FONT_BROKEN_RATIO_THRESHOLD, polluted, n


def is_reading_type_odd(reading_type):
    """알려진 독해유형 조합("사실적/추론적" 등)이 아니면 True."""
    tokens = [t.strip() for t in reading_type.split("/")]
    return not all(t in READING_TYPE_BASE for t in tokens if t != "") or any(t == "" for t in tokens)


def load_db(db_path):
    """읽기 전용으로 DB를 열고, 필요한 테이블을 doc_id 기준으로 묶어서 반환."""
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row

    documents = {r["doc_id"]: dict(r) for r in conn.execute("SELECT * FROM documents ORDER BY doc_id")}

    by_doc = lambda rows: _group_by(rows, "doc_id")
    vocabulary = by_doc([dict(r) for r in conn.execute("SELECT * FROM vocabulary")])
    ox_quiz = by_doc([dict(r) for r in conn.execute("SELECT * FROM ox_quiz")])
    discussion_qa = by_doc([dict(r) for r in conn.execute("SELECT * FROM discussion_qa")])
    essay_prompt = by_doc([dict(r) for r in conn.execute("SELECT * FROM essay_prompt")])
    document_image = by_doc([dict(r) for r in conn.execute("SELECT * FROM document_image")])

    conn.close()
    return documents, vocabulary, ox_quiz, discussion_qa, essay_prompt, document_image


def _group_by(rows, key):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r[key]].append(r)
    return grouped


# ============================================================
# 문서 1건 진단
# ============================================================

def diagnose_document(doc_id, doc, vocab_rows, ox_rows, qa_rows, essay_rows, image_rows):
    result = {
        "doc_id": doc_id,
        "level": doc.get("level"),
        "quarter": doc.get("quarter"),
        "week": doc.get("week"),
        "book_title": doc.get("book_title"),
        "review_status": doc.get("review_status"),
        "n_vocab": len(vocab_rows),
        "n_ox": len(ox_rows),
        "n_qa": len(qa_rows),
        "n_essay": len(essay_rows),
        "b1_unknown": 0,
        "b2_empty": 0,
        "b3_label_as_question": 0,
        "b4_dup_order": 0,
        "b5_font_broken": 0,
        "w1_table_noconfig": 0,
        "w2_rows_int": 0,
        "w3_reading_type_odd": 0,
        "w4_reading_type_null": 0,
        "w5_missing_parts": 0,
        "w6_no_cover_message": 0,
        "n_images": len(image_rows),
        "n_images_linked": 0,
    }

    # --- B1/B2/B3: discussion_qa 행 단위 검사 ---
    for qa in qa_rows:
        if qa.get("ui_type") == "unknown":
            result["b1_unknown"] += 1

        question_text = qa.get("question_text")
        if question_text is None or question_text.strip() == "":
            result["b2_empty"] += 1

        norm_q = normalize_text(question_text)
        norm_r = normalize_text(qa.get("reading_type"))
        if (qa.get("reading_type") and norm_q == norm_r) or len(norm_q) < B3_MIN_LEN:
            result["b3_label_as_question"] += 1

        rt = qa.get("reading_type")
        if rt is None or rt.strip() == "":
            result["w4_reading_type_null"] += 1
        elif is_reading_type_odd(rt):
            result["w3_reading_type_odd"] += 1

        if qa.get("ui_type") == TABLE_UI_TYPE:
            cfg_raw = qa.get("ui_config")
            cfg = None
            if cfg_raw:
                try:
                    cfg = json.loads(cfg_raw)
                except (json.JSONDecodeError, TypeError):
                    cfg = None
            if not cfg:  # None(파싱실패/NULL) 또는 빈 dict("{}")
                result["w1_table_noconfig"] += 1
            elif isinstance(cfg.get("rows"), int):
                result["w2_rows_int"] += 1

    # --- B4: 같은 (doc_id, order_no) 안에 하이픈 없는 order_label이 2개 이상 ---
    by_order_no = defaultdict(list)
    for qa in qa_rows:
        by_order_no[qa.get("order_no")].append(qa.get("order_label") or "")
    for order_no, labels in by_order_no.items():
        no_hyphen = [lbl for lbl in labels if "-" not in lbl]
        if len(no_hyphen) >= 2:
            result["b4_dup_order"] += 1

    # --- B5: 폰트 깨짐 (문서 전체의 question_text + excerpt_text 합본 기준) ---
    combined = "".join((qa.get("question_text") or "") + (qa.get("excerpt_text") or "") for qa in qa_rows)
    font_broken, polluted_count, _ = is_font_broken(combined)
    result["b5_font_broken"] = polluted_count

    # --- W5: 필수 구성요소 누락 개수(0~4) ---
    missing = 0
    if len(vocab_rows) == 0:
        missing += 1
    if len(ox_rows) == 0:
        missing += 1
    if len(qa_rows) == 0:
        missing += 1
    if len(essay_rows) == 0:
        missing += 1
    result["w5_missing_parts"] = missing

    # --- W6: 표지 인용문 없음 ---
    cover = doc.get("cover_message")
    result["w6_no_cover_message"] = 1 if (cover is None or cover.strip() == "") else 0

    # --- 이미지 연결 개수 ---
    linked_paths = set()
    for qa in qa_rows:
        if qa.get("reference_image_path"):
            linked_paths.add(qa["reference_image_path"])
        if qa.get("excerpt_image_path"):
            linked_paths.add(qa["excerpt_image_path"])
    for e in essay_rows:
        if e.get("image_path"):
            linked_paths.add(e["image_path"])
    result["n_images_linked"] = sum(1 for img in image_rows if img["file_path"] in linked_paths)

    # --- 등급 산정 ---
    blockers = []
    if result["b1_unknown"] > 0:
        blockers.append("B1")
    if result["b2_empty"] > 0:
        blockers.append("B2")
    if result["b3_label_as_question"] > 0:
        blockers.append("B3")
    if result["b4_dup_order"] > 0:
        blockers.append("B4")
    if font_broken:
        blockers.append("B5")

    warnings = [
        result["w1_table_noconfig"] > 0,
        result["w2_rows_int"] > 0,
        result["w3_reading_type_odd"] > 0,
        result["w4_reading_type_null"] > 0,
        result["w5_missing_parts"] > 0,
        result["w6_no_cover_message"] > 0,
    ]

    if blockers:
        grade = "unusable"
    elif any(warnings):
        grade = "needs_review"
    else:
        grade = "ready"

    result["grade"] = grade
    result["blockers"] = ";".join(blockers)
    return result


# ============================================================
# 이미지 연결 상태 조사 (전체 DB 기준, 문서 단위가 아님)
# ============================================================

def analyze_images(document_image_by_doc, discussion_qa_by_doc, essay_prompt_by_doc):
    all_images = [img for imgs in document_image_by_doc.values() for img in imgs]
    total_images = len(all_images)

    linked_paths = set()
    for qa_rows in discussion_qa_by_doc.values():
        for qa in qa_rows:
            if qa.get("reference_image_path"):
                linked_paths.add((qa["doc_id"], qa["reference_image_path"]))
            if qa.get("excerpt_image_path"):
                linked_paths.add((qa["doc_id"], qa["excerpt_image_path"]))
    for essay_rows in essay_prompt_by_doc.values():
        for e in essay_rows:
            if e.get("image_path"):
                linked_paths.add((e["doc_id"], e["image_path"]))

    linked_count = sum(1 for img in all_images if (img["doc_id"], img["file_path"]) in linked_paths)

    image_type_counts = Counter(img["image_type"] for img in all_images)

    # (doc_id, source_page) 별 이미지 개수 분포 -> 재연결 모호성 판단
    page_groups = defaultdict(list)
    for img in all_images:
        page_groups[(img["doc_id"], img["source_page"])].append(img)

    unlinked_images = [img for img in all_images if (img["doc_id"], img["file_path"]) not in linked_paths]
    unlinked_unambiguous = 0
    unlinked_ambiguous = 0
    for img in unlinked_images:
        group = page_groups[(img["doc_id"], img["source_page"])]
        if len(group) == 1:
            unlinked_unambiguous += 1
        else:
            unlinked_ambiguous += 1

    missing_files = []
    for img in all_images:
        abs_path = os.path.join(IMAGES_DIR, img["file_path"])
        if not os.path.isfile(abs_path):
            missing_files.append((img["doc_id"], img["id"], img["file_path"]))

    return {
        "total_images": total_images,
        "linked_count": linked_count,
        "image_type_counts": image_type_counts,
        "unlinked_total": len(unlinked_images),
        "unlinked_unambiguous": unlinked_unambiguous,
        "unlinked_ambiguous": unlinked_ambiguous,
        "missing_files": missing_files,
    }


# ============================================================
# 리포트 출력
# ============================================================

CSV_COLUMNS = [
    "doc_id", "level", "quarter", "week", "book_title", "review_status",
    "n_vocab", "n_ox", "n_qa", "n_essay",
    "b1_unknown", "b2_empty", "b3_label_as_question", "b4_dup_order", "b5_font_broken",
    "w1_table_noconfig", "w2_rows_int", "w3_reading_type_odd", "w4_reading_type_null",
    "w5_missing_parts", "w6_no_cover_message",
    "n_images", "n_images_linked",
    "grade", "blockers",
]

BLOCKER_CODES = ["B1", "B2", "B3", "B4", "B5"]
BLOCKER_LABELS = {
    "B1": "ui_type='unknown' 문항 존재",
    "B2": "question_text 비어있음",
    "B3": "독해유형 라벨이 문항으로 잘못 잡힘/너무 짧음",
    "B4": "같은 order_no에 하이픈 없는 행 2개 이상(중복 의심)",
    "B5": "폰트 깨짐(잡문자 비율 2% 초과)",
}


def write_csv(results, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r[k] for k in CSV_COLUMNS})


def _recompute_grade_without(result, blocker_code):
    """특정 BLOCKER 하나가 없었다면(=값이 0이었다면) 등급이 뭐가 됐을지 재계산.
    B1~B5 모두 result['blockers'](세미콜론 목록)에 이미 최종 판정이 담겨있으므로,
    그 목록에서 blocker_code 하나만 뺀 나머지가 남아있는지만 보면 된다."""
    remaining_blockers = [c for c in BLOCKER_CODES if c != blocker_code and c in result["blockers"].split(";")]
    if remaining_blockers:
        return "unusable"
    warnings = [
        result["w1_table_noconfig"] > 0, result["w2_rows_int"] > 0,
        result["w3_reading_type_odd"] > 0, result["w4_reading_type_null"] > 0,
        result["w5_missing_parts"] > 0, result["w6_no_cover_message"] > 0,
    ]
    return "needs_review" if any(warnings) else "ready"


def find_unclassified_risks(discussion_qa_by_doc, essay_prompt_by_doc):
    """규칙(B1~B5/W1~W6)에는 없지만 조판에 문제가 될 수 있는 패턴을 실제로 세어봄."""
    table_missing_columns = 0
    for qa_rows in discussion_qa_by_doc.values():
        for qa in qa_rows:
            if qa.get("ui_type") != TABLE_UI_TYPE:
                continue
            cfg_raw = qa.get("ui_config")
            if not cfg_raw:
                continue
            try:
                cfg = json.loads(cfg_raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if cfg and "rows" in cfg and "columns" not in cfg:
                table_missing_columns += 1

    multi_essay_docs = [doc_id for doc_id, rows in essay_prompt_by_doc.items() if len(rows) > 1]

    order_no_gap_docs = []
    for doc_id, qa_rows in discussion_qa_by_doc.items():
        nos = sorted({qa["order_no"] for qa in qa_rows})
        if nos and nos != list(range(nos[0], nos[-1] + 1)):
            order_no_gap_docs.append(doc_id)

    return {
        "table_missing_columns": table_missing_columns,
        "multi_essay_docs": multi_essay_docs,
        "order_no_gap_docs": order_no_gap_docs,
    }


def write_summary(results, image_stats, risk_stats, path):
    total = len(results)
    grade_counts = Counter(r["grade"] for r in results)

    lines = []
    lines.append("# 모모의책장 교재DB — A4 학습지 자동 생성 가능 여부 진단 결과\n")
    lines.append(f"진단 대상: 문서 {total}건 (`momo_book_db/momo_book.db` 기준, 읽기 전용 조회)\n")

    # 1. 등급 분포
    lines.append("## 1. 등급 분포\n")
    lines.append("| 등급 | 건수 | 비율 |")
    lines.append("|---|---|---|")
    for grade in ["ready", "needs_review", "unusable"]:
        c = grade_counts.get(grade, 0)
        lines.append(f"| {grade} | {c} | {c/total*100:.1f}% |")
    lines.append("")

    # 2. 등급 x level / quarter 교차표
    lines.append("## 2. 등급 × 레벨(level) 교차표\n")
    levels = sorted({r["level"] for r in results if r["level"]})
    lines.append("| level | ready | needs_review | unusable | 합계 |")
    lines.append("|---|---|---|---|---|")
    for lv in levels:
        sub = [r for r in results if r["level"] == lv]
        c = Counter(r["grade"] for r in sub)
        lines.append(f"| {lv} | {c.get('ready',0)} | {c.get('needs_review',0)} | {c.get('unusable',0)} | {len(sub)} |")
    lines.append("")

    lines.append("## 2-1. 등급 × 분기(quarter) 교차표\n")
    quarters = sorted({r["quarter"] for r in results if r["quarter"]})
    lines.append("| quarter | ready | needs_review | unusable | 합계 |")
    lines.append("|---|---|---|---|---|")
    for q in quarters:
        sub = [r for r in results if r["quarter"] == q]
        c = Counter(r["grade"] for r in sub)
        lines.append(f"| {q} | {c.get('ready',0)} | {c.get('needs_review',0)} | {c.get('unusable',0)} | {len(sub)} |")
    lines.append("")

    # 3. BLOCKER 코드별 영향 문서 수 + 단독 해결 시 효과
    lines.append("## 3. BLOCKER 코드별 영향 문서 수\n")
    lines.append("| 코드 | 설명 | 걸린 문서 수 | 이 코드만 해결하면 새로 살아나는 문서 수 |")
    lines.append("|---|---|---|---|")
    for code in BLOCKER_CODES:
        affected = [r for r in results if code in r["blockers"].split(";")]
        rescued = 0
        for r in affected:
            new_grade = _recompute_grade_without(r, code)
            if new_grade != "unusable":
                rescued += 1
        lines.append(f"| {code} | {BLOCKER_LABELS[code]} | {len(affected)} | {rescued} |")
    lines.append("")
    lines.append(
        "(\"새로 살아나는 문서 수\"는 그 문서의 BLOCKER가 이 코드 하나뿐이었다는 뜻. "
        "여러 BLOCKER가 겹친 문서는 그 코드만 고쳐도 여전히 unusable이라 0으로 집계됨.)\n"
    )

    # 4. ready 문서 목록 전체
    ready_docs = sorted([r for r in results if r["grade"] == "ready"], key=lambda r: r["doc_id"])
    lines.append(f"## 4. `ready` 문서 목록 (전체 {len(ready_docs)}건)\n")
    lines.append("| doc_id | book_title | level |")
    lines.append("|---|---|---|")
    for r in ready_docs:
        lines.append(f"| {r['doc_id']} | {r['book_title']} | {r['level']} |")
    lines.append("")

    # 5. unusable 중 결함이 가장 적은 20건
    unusable_docs = [r for r in results if r["grade"] == "unusable"]
    def defect_count(r):
        return (r["b1_unknown"] + r["b2_empty"] + r["b3_label_as_question"] + r["b4_dup_order"]
                + (1 if "B5" in r["blockers"].split(";") else 0))
    unusable_sorted = sorted(unusable_docs, key=defect_count)[:20]
    lines.append("## 5. `unusable` 중 결함이 가장 적은 20건 (손보면 바로 쓸 수 있는 후보)\n")
    lines.append("| doc_id | book_title | level | blockers | 결함 수(대략) |")
    lines.append("|---|---|---|---|---|")
    for r in unusable_sorted:
        lines.append(f"| {r['doc_id']} | {r['book_title']} | {r['level']} | {r['blockers']} | {defect_count(r)} |")
    lines.append("")

    # 6. 이미지 연결 상태
    lines.append("## 6. 이미지 연결 상태\n")
    total_images = image_stats["total_images"]
    linked = image_stats["linked_count"]
    lines.append(f"- 추출된 이미지(`document_image`) 총 {total_images}건 중 문항/글쓰기에 실제로 연결된 이미지: "
                 f"**{linked}건 ({linked/total_images*100:.1f}%)**")
    lines.append(f"- 연결 안 된 이미지: {image_stats['unlinked_total']}건")
    lines.append("")
    lines.append("### image_type 종류와 건수")
    lines.append("| image_type | 건수 |")
    lines.append("|---|---|")
    for t, c in image_stats["image_type_counts"].most_common():
        lines.append(f"| {t} | {c} |")
    lines.append("")
    lines.append("### 연결 끊긴 이미지를 (doc_id, source_page)로 재연결할 수 있는지")
    ua = image_stats["unlinked_unambiguous"]
    am = image_stats["unlinked_ambiguous"]
    unlinked_total = image_stats["unlinked_total"] or 1
    lines.append(f"- 같은 문서·같은 페이지에 이미지가 **1장뿐**이라 재연결이 모호하지 않은 경우: "
                 f"{ua}건 ({ua/unlinked_total*100:.1f}%)")
    lines.append(f"- 같은 문서·같은 페이지에 이미지가 **2장 이상**이라 어느 문항에 붙일지 모호한 경우: "
                 f"{am}건 ({am/unlinked_total*100:.1f}%)")
    lines.append("")
    lines.append("### 파일 실존 여부")
    missing = image_stats["missing_files"]
    lines.append(f"- `document_image.file_path`가 가리키는 파일이 디스크에 없는 건수: **{len(missing)}건** / {total_images}건")
    if missing:
        lines.append("")
        lines.append("| doc_id | image_id | file_path |")
        lines.append("|---|---|---|")
        for doc_id, img_id, fp in missing[:30]:
            lines.append(f"| {doc_id} | {img_id} | {fp} |")
        if len(missing) > 30:
            lines.append(f"| ... | ... | (외 {len(missing)-30}건) |")
    lines.append("")

    # 7. 미분류 위험 (전부 실제로 세어본 값 - 추측 아님)
    lines.append("## 7. 미분류 위험 (위 규칙으로는 안 잡히지만 조판에 문제가 될 수 있는 패턴)\n")
    lines.append(
        f"- **`table_compare`인데 `ui_config`에 `columns`가 없이 `rows`만 있는 항목: {risk_stats['table_missing_columns']}건 확인됨.** "
        "표의 열 제목 없이 렌더링될 위험이 있음(W1/W2와는 다른 결함이라 별도로 셈).\n"
    )
    if risk_stats["multi_essay_docs"]:
        lines.append(
            f"- **`essay_prompt`가 문서 안에 2건 이상 있는 문서: {len(risk_stats['multi_essay_docs'])}건** "
            f"({', '.join(risk_stats['multi_essay_docs'][:10])}) — 1문서 1글쓰기를 가정한 조판이면 확인 필요.\n"
        )
    else:
        lines.append(
            "- `essay_prompt`가 문서 안에 2건 이상 있는 경우는 **0건**(직접 확인함) — 1문서 1글쓰기 가정은 지금 데이터로는 안전함.\n"
        )
    lines.append(
        "- **`document_image.file_path`가 존재는 하지만 손상된 이미지 파일(0바이트, 깨진 헤더 등)일 가능성**은 "
        "이번 진단에서 파일 존재 여부만 확인했고 이미지 내용 자체(디코딩 가능 여부, 해상도)는 검사하지 않았음(미검증).\n"
    )
    lines.append(
        "- **`vocabulary.definition`이 NULL인 항목이 다수 존재**(별도 조사 보고서 `docs/DB_REPORT.md` 기준 약 41%) — 어휘 카드에 "
        "단어만 있고 뜻이 비어있는 블록이 그려질 수 있는데, 이번 진단은 이를 W5(테이블 전체가 0건인지)로만 보고 "
        "행 단위 결측은 별도로 세지 않았음.\n"
    )
    lines.append(
        f"- **`order_no`가 1부터 연속되지 않고 건너뛰는 문서: {len(risk_stats['order_no_gap_docs'])}건 확인됨** "
        f"({', '.join(risk_stats['order_no_gap_docs'][:10])}). "
        "고정 조판이 순번을 그대로 페이지 번호처럼 쓴다면 확인이 필요함.\n"
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def print_one_doc(result, qa_rows):
    print(f"=== {result['doc_id']} ({result['book_title']}) ===")
    print(f"등급: {result['grade']}  (blockers: {result['blockers'] or '없음'})")
    print(f"level={result['level']} quarter={result['quarter']} week={result['week']} "
          f"review_status={result['review_status']}")
    print(f"n_vocab={result['n_vocab']} n_ox={result['n_ox']} n_qa={result['n_qa']} n_essay={result['n_essay']}")
    print("-- BLOCKER --")
    for code, field in [("B1", "b1_unknown"), ("B2", "b2_empty"), ("B3", "b3_label_as_question"),
                        ("B4", "b4_dup_order")]:
        print(f"  {code} ({BLOCKER_LABELS[code]}): {result[field]}건")
    font_broken, polluted, total_len = is_font_broken(
        "".join((qa.get("question_text") or "") + (qa.get("excerpt_text") or "") for qa in qa_rows)
    )
    ratio = (polluted / total_len * 100) if total_len else 0.0
    print(f"  B5 (폰트 깨짐): 잡문자 {polluted}자 / 전체 {total_len}자 = {ratio:.2f}% "
          f"({'초과 -> BLOCKER' if font_broken else '기준 이하'})")
    print("-- WARNING --")
    for code, field in [("W1", "w1_table_noconfig"), ("W2", "w2_rows_int"), ("W3", "w3_reading_type_odd"),
                        ("W4", "w4_reading_type_null"), ("W5", "w5_missing_parts"), ("W6", "w6_no_cover_message")]:
        print(f"  {code}: {result[field]}건")
    print(f"이미지: {result['n_images']}건 중 {result['n_images_linked']}건 연결됨")


def main():
    ap = argparse.ArgumentParser(description="교재DB A4 학습지 자동 생성 가능 여부 진단")
    ap.add_argument("--doc-id", help="이 문서 하나만 상세 출력(리포트 파일은 생성하지 않음)")
    args = ap.parse_args()

    documents, vocabulary, ox_quiz, discussion_qa, essay_prompt, document_image = load_db(DB_PATH)

    if args.doc_id:
        doc_id = args.doc_id
        if doc_id not in documents:
            print(f"'{doc_id}' 문서를 찾을 수 없습니다.")
            return
        result = diagnose_document(
            doc_id, documents[doc_id],
            vocabulary.get(doc_id, []), ox_quiz.get(doc_id, []), discussion_qa.get(doc_id, []),
            essay_prompt.get(doc_id, []), document_image.get(doc_id, []),
        )
        print_one_doc(result, discussion_qa.get(doc_id, []))
        return

    results = []
    for doc_id, doc in documents.items():
        result = diagnose_document(
            doc_id, doc,
            vocabulary.get(doc_id, []), ox_quiz.get(doc_id, []), discussion_qa.get(doc_id, []),
            essay_prompt.get(doc_id, []), document_image.get(doc_id, []),
        )
        results.append(result)

    image_stats = analyze_images(document_image, discussion_qa, essay_prompt)
    risk_stats = find_unclassified_risks(discussion_qa, essay_prompt)

    write_csv(results, CSV_PATH)
    write_summary(results, image_stats, risk_stats, SUMMARY_PATH)

    grade_counts = Counter(r["grade"] for r in results)
    total = len(results)
    print(
        f"진단 완료: 전체 {total}건 - ready {grade_counts.get('ready',0)}건 / "
        f"needs_review {grade_counts.get('needs_review',0)}건 / "
        f"unusable {grade_counts.get('unusable',0)}건"
    )
    print(f"CSV: {CSV_PATH}")
    print(f"요약: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
