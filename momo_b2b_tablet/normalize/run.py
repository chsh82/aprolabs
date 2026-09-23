"""① 정규화 CLI - doc_id 하나를 momo_book_db에서 읽어 NormalizedDoc으로 낸다.

완료 기준(SPEC §9 1단계): "샘플 전부 이름 기준으로 올바르게 매핑,
unknown 행 0". discussion_qa 재조립은 normalize/discussion_qa.py가 맡고
(그 파일 docstring에 실제 데이터로 확인한 3가지 패턴을 적어 뒀다), 여기서는
테이블별로 조회해 이름 기준으로 넘기고 결과를 합친다.

실행:
    python -m normalize.run L5-Q3-W10
    python -m normalize.run L5-Q3-W10 --json  # 정규화 레코드를 JSON으로 출력
"""
from __future__ import annotations

import argparse
import io
import json
import sys
from dataclasses import asdict
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
PKG_ROOT = Path(__file__).resolve().parent.parent
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from normalize.db import get_connection  # noqa: E402
from normalize.discussion_qa import normalize_discussion_qa  # noqa: E402
from normalize.essay import normalize_essay  # noqa: E402
from normalize.models import Flag, NormalizedDoc, NormalizedImage, NormalizedOx, NormalizedVocab  # noqa: E402
from normalize.text_repair import repair_text  # noqa: E402
from normalize.tone import level_to_band, quarter_to_key  # noqa: E402

IMAGES_DIR = REPO_ROOT / "momo_book_db" / "extracted_images"
_LOWRES_MIN_SIDE = 300  # SPEC §3.5.4 인쇄 300dpi 기준 + §4 "열하일기 표지 204×299" 예시


def _normalize_vocab(rows) -> list[NormalizedVocab]:
    out = []
    for r in rows:
        word, _ = repair_text(r["word"])
        definition, def_flags = repair_text(r["definition"])
        example, ex_flags = repair_text(r["example_sentence"])
        v = NormalizedVocab(order_no=r["order_no"], word=word or "", definition=definition,
                             example_sentence=example, book_page=r["book_page"])
        v.flags.extend(def_flags)
        v.flags.extend(ex_flags)
        if not definition:
            # sup: LLM/사전DB가 자동으로 채울 수 있는 종류의 누락(SPEC §4 "초등
            # 어휘 DB 조회 또는 LLM 보충"). 반대로 OX 정답 누락처럼 사람이 교사용
            # 자료를 찾아와야 하는 건 missing으로 구분한다(_normalize_ox 참고).
            v.flags.append(Flag(kind="sup", order_no=r["order_no"],
                                 message=f"'{word}' 뜻풀이 누락 - 초등 어휘 DB 조회 또는 LLM 보충 필요"))
        out.append(v)
    return out


def _normalize_ox(rows) -> list[NormalizedOx]:
    out = []
    for r in rows:
        question, q_flags = repair_text(r["question"])
        o = NormalizedOx(order_no=r["order_no"], question=question or "",
                          answer=bool(r["answer"]) if r["answer"] is not None else None,
                          explanation=r["explanation"], evidence_page=r["evidence_page"])
        o.flags.extend(q_flags)
        if r["answer"] is None:
            # sup가 아니라 missing - LLM이 보충하는 게 아니라 사람이 교사용 교재에서
            # 찾아와야 하는 값이다(SPEC §4). sup는 어휘 뜻처럼 LLM/사전DB가 채우는
            # 경우로만 쓴다.
            o.flags.append(Flag(kind="missing", order_no=r["order_no"],
                                 message="O·X 정답이 비어있음 - 교사용 교재에서 보충 필요(LLM 대상 아님)"))
        out.append(o)
    return out


def _normalize_ox_from_background(background_text: str | None) -> list[NormalizedOx]:
    """SPEC §4: ox_quiz가 비어 있으면 background_text에서 O·X 문장을 복원한다
    (저학년 실제 사례, L2-Q2-W08). 원문에 정답 표시가 안 남아 있어 answer는
    항상 None - 골든 샘플(oxp 페이지 스키마)도 애초에 answer 필드가 없다
    (정답은 레이아웃과 별개로 관리됨)."""
    from normalize.ox_from_background import extract_ox_from_background

    out = []
    for order_no, (sentence, page) in enumerate(extract_ox_from_background(background_text), start=1):
        question, q_flags = repair_text(sentence)
        o = NormalizedOx(order_no=order_no, question=question or "", answer=None, explanation=None,
                          evidence_page=page)
        o.flags.extend(q_flags)
        o.flags.append(Flag(
            kind="split", order_no=order_no,
            message=f"ox_quiz가 비어있어 background_text에서 복원함 (원본 쪽번호 {page})",
        ))
        out.append(o)
    return out


def normalize_document(doc_id: str) -> NormalizedDoc:
    conn = get_connection()
    try:
        doc_row = conn.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
        if doc_row is None:
            raise ValueError(f"documents에 doc_id={doc_id!r} 가 없습니다")

        qa_rows = conn.execute(
            "SELECT * FROM discussion_qa WHERE doc_id = ? ORDER BY order_no", (doc_id,)
        ).fetchall()
        vocab_rows = conn.execute(
            "SELECT * FROM vocabulary WHERE doc_id = ? ORDER BY order_no", (doc_id,)
        ).fetchall()
        ox_rows = conn.execute(
            "SELECT * FROM ox_quiz WHERE doc_id = ? ORDER BY order_no", (doc_id,)
        ).fetchall()
        essay_row = conn.execute(
            "SELECT * FROM essay_prompt WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        outline_rows = []
        if essay_row is not None:
            outline_rows = conn.execute(
                "SELECT * FROM essay_outline_question WHERE essay_id = ? ORDER BY order_no",
                (essay_row["id"],),
            ).fetchall()
        image_rows = conn.execute(
            "SELECT * FROM document_image WHERE doc_id = ?", (doc_id,)
        ).fetchall()
    finally:
        conn.close()

    qa, hanja, qa_doc_flags = normalize_discussion_qa(qa_rows)
    vocab = _normalize_vocab(vocab_rows)
    ox = _normalize_ox(ox_rows)
    if not ox:
        ox = _normalize_ox_from_background(doc_row["background_text"])
    essay = normalize_essay(essay_row, outline_rows)
    images = []
    for r in image_rows:
        img = NormalizedImage(image_type=r["image_type"], source_page=r["source_page"],
                               file_path=r["file_path"], extraction_confidence=r["extraction_confidence"])
        images.append(img)

    image_flags: list[Flag] = []
    for img in images:
        full_path = IMAGES_DIR / img.file_path
        if not full_path.exists():
            image_flags.append(Flag(kind="lowres", message=f"이미지 파일을 찾을 수 없음: {img.file_path}"))
            continue
        try:
            from PIL import Image as PILImage
            with PILImage.open(full_path) as im:
                w, h = im.size
            if w < _LOWRES_MIN_SIDE or h < _LOWRES_MIN_SIDE:
                image_flags.append(Flag(
                    kind="lowres",
                    message=f"{img.file_path} 저해상도 {w}x{h} (최소 {_LOWRES_MIN_SIDE}px 권장, SPEC §4)",
                ))
        except Exception as e:  # noqa: BLE001 - 이미지 못 열어도 정규화 자체는 계속
            image_flags.append(Flag(kind="lowres", message=f"{img.file_path} 이미지 열기 실패: {e}"))

    doc = NormalizedDoc(
        doc_id=doc_id,
        book_title=doc_row["book_title"],
        book_author=doc_row["book_author"],
        level=doc_row["level"],
        band=level_to_band(doc_row["level"]),
        quarter=quarter_to_key(doc_row["quarter"]),
        week=doc_row["week"],
        cover_message=doc_row["cover_message"],
        background_text=doc_row["background_text"],
        qa=qa,
        vocab=vocab,
        ox=ox,
        hanja_glossary=hanja,
        essay=essay,
        images=images,
    )
    doc.flags.extend(qa_doc_flags)
    doc.flags.extend(image_flags)
    return doc


def _to_jsonable(obj):
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _to_jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [_to_jsonable(x) for x in obj]
    return obj


def main() -> int:
    parser = argparse.ArgumentParser(description="momo_book.db 문서 정규화(① 단계)")
    parser.add_argument("doc_id")
    parser.add_argument("--json", action="store_true", help="정규화 레코드 전체를 JSON으로 출력")
    args = parser.parse_args()

    doc = normalize_document(args.doc_id)

    if args.json:
        print(json.dumps(_to_jsonable(doc), ensure_ascii=False, indent=2))
        return 0

    print(f"=== {doc.doc_id} ({doc.book_title}) band={doc.band} quarter={doc.quarter} ===")
    print(f"QA {len(doc.qa)}건 (unknown 남은 것: {doc.unresolved_unknown_count()}건)")
    print(f"vocab {len(doc.vocab)}건, ox {len(doc.ox)}건, hanja_glossary {len(doc.hanja_glossary)}건")
    print(f"essay: {'있음' if doc.essay else '없음'}, images {len(doc.images)}건")
    flags = doc.all_flags()
    print(f"\n플래그 {len(flags)}건:")
    for f in flags:
        loc = f"order_no={f.order_no} " if f.order_no is not None else ""
        print(f"  [{f.kind}] {loc}{f.message}")

    return 0 if doc.unresolved_unknown_count() == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
