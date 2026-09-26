"""④ edition 저장·버전·승인 - SPEC §5/§9 4단계, 사용자 지시(2026-09-23) 4단계.

원칙(전부 사용자 지시 그대로):
  - 초안은 문항을 빼지 않는다. 페이지의 included(기본 true)로 검수가 제외를 표시한다.
  - form을 바꾼 이력은 correction_log에 kind='form_change'로 남는다.
  - placeholder 플래그가 하나라도 안 풀리면 approve를 막는다.
  - 확정(approved/published)된 edition은 그 자리에서 못 고친다 - PATCH하면 새 version.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import jsonpatch
import jsonpointer

from layout.generate import generate_layout
from normalize.models import Flag

from . import db

ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
EXTRACTED_DIR = REPO_ROOT / "momo_book_db" / "extracted_images"


class NotFound(Exception):
    pass


class PatchError(Exception):
    pass


class ApprovalBlocked(Exception):
    pass


class ConflictError(Exception):
    """낙관적 잠금 충돌 - 두 사람이 같은 draft를 동시에 열고 고칠 때(5단계 요구사항)."""
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify(flag: Flag) -> str:
    kind = flag.category or flag.kind
    return kind if kind in db.EDITION_FLAG_KINDS else flag.kind


def _find_page_idx(layout: dict, order_no: int | None) -> int | None:
    """Flag.order_no(정규화 단계 문항 번호)로 layout['pages']에서 그 문항의 페이지를
    찾는다. order_label이 "5-1"/"5-2"처럼 갈라졌으면 둘 다 매칭되므로(모호함) 그때는
    포기하고 None을 돌려준다 - 문서 단위 플래그는 애초에 특정 페이지가 없다."""
    if order_no is None:
        return None
    matches = [i for i, p in enumerate(layout.get("pages", []))
               if "q" in p and (p["q"]["id"] == str(order_no) or p["q"]["id"].startswith(f"{order_no}-"))]
    return matches[0] if len(matches) == 1 else None


def _insert_flags(conn, edition_id: int, layout: dict, flags: list[Flag]) -> None:
    for f in flags:
        page_idx = _find_page_idx(layout, f.order_no)
        path = f"/pages/{page_idx}" if page_idx is not None else None
        conn.execute(
            "INSERT INTO edition_flag (edition_id, page_idx, path, kind, message) VALUES (?,?,?,?,?)",
            (edition_id, page_idx, path, _classify(f), f.message),
        )


def _insert_draft(doc_id: str, layout: dict, flags: list[Flag], created_by: str | None = None) -> int:
    """이미 만들어진 layout/flags를 edition으로 저장 - create_draft()와
    vision_parse.generate로 만든 초안(2026-09-24 지시 [5]) 둘 다 이 함수를 쓴다."""
    source_hash = hashlib.sha256(
        json.dumps(layout, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    conn = db.get_connection()
    try:
        version = conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM edition WHERE doc_id = ?", (doc_id,)
        ).fetchone()["v"]
        cur = conn.execute(
            "INSERT INTO edition (doc_id, version, status, layout_json, band, quarter, "
            "created_by, created_at, source_hash) VALUES (?,?,?,?,?,?,?,?,?)",
            (doc_id, version, "draft", json.dumps(layout, ensure_ascii=False),
             layout["tone"]["band"], layout["quarter"], created_by, _now(), source_hash),
        )
        edition_id = cur.lastrowid
        _insert_flags(conn, edition_id, layout, flags)
        conn.commit()
        return edition_id
    finally:
        conn.close()


def create_draft(doc_id: str, created_by: str | None = None) -> int:
    """POST /api/editions/draft - 정규화 + 조판 초안 생성(②/③단계를 그대로 호출)."""
    try:
        layout, flags = generate_layout(doc_id)
    except ValueError as e:
        raise NotFound(str(e)) from e
    return _insert_draft(doc_id, layout, flags, created_by)


def get_edition_row(edition_id: int):
    conn = db.get_connection()
    try:
        return conn.execute("SELECT * FROM edition WHERE id = ?", (edition_id,)).fetchone()
    finally:
        conn.close()


def _form_change_before_after(before_layout: dict, op: dict):
    """path가 .../q/form이면 (before, after) 값을 'single' 기본값까지 채워서 돌려준다
    (사용자 지시 예시: before='single', after='pledge'). form이 아니면 None."""
    path = op.get("path", "")
    if not path.endswith("/form"):
        return None
    before_val = jsonpointer.resolve_pointer(before_layout, path, default=None)
    before_display = before_val if before_val is not None else "single"
    after_display = op.get("value") if op.get("op") != "remove" else "single"
    return before_display, after_display


def _order_label_for_path(layout: dict, path: str) -> str | None:
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "pages":
        try:
            page = layout["pages"][int(parts[1])]
        except (ValueError, IndexError):
            return None
        return page.get("q", {}).get("id")
    return None


def patch_edition(edition_id: int, patch_ops: list[dict], editor: str | None = None,
                   reason: str | None = None, expected_rev: int | None = None) -> int:
    """PATCH /api/editions/{id} - JSON Patch 적용 + correction_log 기록.

    SPEC §5: "확정된 edition의 layout_json은 수정 불가. 고치면 새 version." -
    status가 approved/published면 이 edition은 그대로 두고 새 edition row(version+1,
    status=draft)를 만들어 거기에 patch를 적용한다.

    expected_rev: 낙관적 잠금(5단계 요구사항) - 두 사람이 같은 draft를 열어 놓고
    있다가 한쪽이 먼저 저장하면, 다른 쪽이 들고 있던 rev는 낡은 값이 된다. 클라이언트가
    화면을 처음 불러왔을 때의 rev를 그대로 실어 보내면, 그 사이 다른 사람이 먼저
    고쳐서 rev가 바뀌어 있을 경우 ConflictError로 막고 최신 내용을 다시 불러오게 한다."""
    row = get_edition_row(edition_id)
    if row is None:
        raise NotFound(f"edition {edition_id} 없음")
    if expected_rev is not None and row["status"] not in ("approved", "published") and row["rev"] != expected_rev:
        raise ConflictError(
            f"edition {edition_id}이 그 사이 다른 사람이 고쳐서 rev가 바뀜 "
            f"(가지고 있던 rev={expected_rev}, 현재 rev={row['rev']}) - 최신 내용을 다시 불러오세요"
        )

    before_layout = json.loads(row["layout_json"])
    try:
        after_layout = jsonpatch.JsonPatch(patch_ops).apply(before_layout)
    except jsonpatch.JsonPatchException as e:
        raise PatchError(str(e)) from e

    conn = db.get_connection()
    try:
        if row["status"] in ("approved", "published"):
            new_version = conn.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM edition WHERE doc_id = ?", (row["doc_id"],),
            ).fetchone()["v"]
            cur = conn.execute(
                "INSERT INTO edition (doc_id, version, status, layout_json, rev, band, quarter, "
                "created_by, created_at, source_hash) VALUES (?,?,?,?,1,?,?,?,?,?)",
                (row["doc_id"], new_version, "draft", json.dumps(after_layout, ensure_ascii=False),
                 after_layout["tone"]["band"], after_layout["quarter"], editor, _now(), row["source_hash"]),
            )
            target_id = cur.lastrowid
        else:
            conn.execute("UPDATE edition SET layout_json = ?, rev = rev + 1 WHERE id = ?",
                         (json.dumps(after_layout, ensure_ascii=False), edition_id))
            target_id = edition_id

        for op in patch_ops:
            path = op.get("path", "")
            form_change = _form_change_before_after(before_layout, op)
            if form_change is not None:
                before_display, after_display = form_change
                order_label = _order_label_for_path(before_layout, path)
                field_path = f"{path} (문항 {order_label})" if order_label else path
                conn.execute(
                    "INSERT INTO correction_log (doc_id, edition_id, field_path, before, after, "
                    "kind, reason, editor, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (row["doc_id"], target_id, field_path, str(before_display), str(after_display),
                     "form_change", reason, editor, _now()),
                )
            else:
                before_val = jsonpointer.resolve_pointer(before_layout, path, default=None)
                after_val = op.get("value")
                conn.execute(
                    "INSERT INTO correction_log (doc_id, edition_id, field_path, before, after, "
                    "kind, reason, editor, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (row["doc_id"], target_id, path,
                     json.dumps(before_val, ensure_ascii=False) if before_val is not None else None,
                     json.dumps(after_val, ensure_ascii=False) if after_val is not None else None,
                     "text_correction", reason, editor, _now()),
                )
        conn.commit()
        return target_id
    finally:
        conn.close()


def list_flags(edition_id: int) -> list[dict]:
    conn = db.get_connection()
    try:
        rows = conn.execute("SELECT * FROM edition_flag WHERE edition_id = ? ORDER BY id", (edition_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def resolve_flag(flag_id: int, resolved_by: str | None) -> None:
    conn = db.get_connection()
    try:
        conn.execute("UPDATE edition_flag SET resolved_by = ?, resolved_at = ? WHERE id = ?",
                     (resolved_by, _now(), flag_id))
        conn.commit()
    finally:
        conn.close()


def approve(edition_id: int, approved_by: str | None) -> None:
    """POST /api/editions/{id}/approve - 사용자 지시 4단계 3번: placeholder 플래그가
    안 풀렸으면 승인을 막는다(중등 배경지식처럼 DB에 원문이 없는 자리표시자 콘텐츠가
    그대로 공개되는 걸 막기 위함)."""
    row = get_edition_row(edition_id)
    if row is None:
        raise NotFound(f"edition {edition_id} 없음")

    conn = db.get_connection()
    try:
        unresolved = conn.execute(
            "SELECT id, message FROM edition_flag WHERE edition_id = ? AND kind = 'placeholder' "
            "AND resolved_at IS NULL", (edition_id,),
        ).fetchall()
        if unresolved:
            names = "; ".join(r["message"] for r in unresolved[:3])
            raise ApprovalBlocked(
                f"placeholder 플래그 {len(unresolved)}건이 안 풀려서 승인할 수 없음: {names}"
                + (" 등" if len(unresolved) > 3 else "")
            )
        conn.execute("UPDATE edition SET status = 'approved', approved_by = ?, approved_at = ? WHERE id = ?",
                     (approved_by, _now(), edition_id))
        conn.commit()
    finally:
        conn.close()


def publish(edition_id: int) -> None:
    row = get_edition_row(edition_id)
    if row is None:
        raise NotFound(f"edition {edition_id} 없음")
    if row["status"] != "approved":
        raise PatchError(f"edition {edition_id}는 approved 상태가 아님(현재 {row['status']}) - publish 불가")
    conn = db.get_connection()
    try:
        conn.execute("UPDATE edition SET status = 'published' WHERE id = ?", (edition_id,))
        conn.commit()
    finally:
        conn.close()


def _collect_image_keys(layout: dict) -> set[str]:
    keys: set[str] = set()
    cover = layout.get("book", {}).get("cover")
    if cover:
        keys.add(cover)
    for p in layout.get("pages", []):
        guide = p.get("guide") or {}
        if guide.get("img"):
            keys.add(guide["img"])
        image = p.get("image")
        if image and image.get("key"):
            keys.add(image["key"])
        slot = p.get("slot")
        if slot and slot.get("img"):
            keys.add(slot["img"])
        ref = p.get("ref")
        if ref and ref.get("img"):
            keys.add(ref["img"])
        q = p.get("q")
        if q and q.get("img"):
            keys.add(q["img"])
    keys.add("logo")
    keys.add("logoIvory")
    return keys


_MANIFEST_CACHE: dict | None = None


def _asset_manifest() -> dict:
    global _MANIFEST_CACHE
    if _MANIFEST_CACHE is None:
        manifest_path = ASSETS_DIR / "manifest.json"
        _MANIFEST_CACHE = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    return _MANIFEST_CACHE


def resolve_image_urls(layout: dict) -> dict[str, str]:
    """자산 키 -> URL. "/"가 있으면 momo_book_db/extracted_images의 문서별 원본
    이미지(Stage3가 NormalizedImage.file_path를 그대로 키로 쓴다), 없으면 시안용
    평면 자산(assets/manifest.json - 캐릭터·로고)로 본다."""
    images: dict[str, str] = {}
    manifest = _asset_manifest()
    for key in _collect_image_keys(layout):
        if "/" in key:
            images[key] = f"/static/extracted/{key}"
        elif key in manifest:
            images[key] = f"/static/assets/{manifest[key]}"
    return images


def runtime_view(edition_id: int) -> dict | None:
    """GET /api/runtime/{edition_id} - 학생용 layout JSON + 이미지 URL(SPEC §6).
    included=false인 페이지는 여기서 거른다(사용자 지시 4단계 2번)."""
    row = get_edition_row(edition_id)
    if row is None:
        return None
    layout = json.loads(row["layout_json"])
    filtered_pages = [p for p in layout["pages"] if p.get("included", True) is not False]
    filtered_layout = {**layout, "pages": filtered_pages}
    return {"layout": filtered_layout, "images": resolve_image_urls(filtered_layout)}


def print_view(edition_id: int) -> dict | None:
    """⑥단계 인쇄 PDF용 - runtime_view와 같은 included 필터를 쓰되(사용자 지시:
    "included=false 페이지 제외"), 쪽수가 홀수면 마지막에 빈 면을 하나 덧붙인다
    (2-up 인쇄에서 짝을 맞추려고 - {"type":"blank"}은 renderer.js가 빈 프레임으로
    처리한다). 실제 edition에는 절대 저장하지 않는 임시 페이지다."""
    row = get_edition_row(edition_id)
    if row is None:
        return None
    layout = json.loads(row["layout_json"])
    pages = [p for p in layout["pages"] if p.get("included", True) is not False]
    if len(pages) % 2 == 1:
        pages = pages + [{"type": "blank"}]
    filtered_layout = {**layout, "pages": pages}
    return {"layout": filtered_layout, "images": resolve_image_urls(filtered_layout)}


def load_answers(edition_id: int, student_id: str) -> dict:
    """⑥단계 "학생 필기 포함" 인쇄용 - student_answer에 흩어져 있는 행들을
    renderer.js의 상태 모양({ink,text,ox,choice})으로 되돌린다(save_answer가
    저장한 그대로 역으로 풂 - ox#N/  #choice 접미사로 구분)."""
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT part_id, ink_json, text FROM student_answer WHERE edition_id = ? AND student_id = ?",
            (edition_id, student_id),
        ).fetchall()
    finally:
        conn.close()

    ink: dict = {}
    text: dict = {}
    ox: dict = {}
    choice: dict = {}
    for r in rows:
        part_id = r["part_id"]
        if r["ink_json"]:
            ink[part_id] = json.loads(r["ink_json"])
        if r["text"] is None:
            continue
        if part_id.startswith("ox#"):
            ox[part_id[3:]] = r["text"]
        elif part_id.endswith("#choice"):
            try:
                choice[part_id] = json.loads(r["text"]) if r["text"].startswith("[") else r["text"]
            except (ValueError, AttributeError):
                choice[part_id] = r["text"]
        else:
            try:
                text[part_id] = json.loads(r["text"])
            except (ValueError, TypeError):
                text[part_id] = {"text": r["text"], "saved": True, "stale": False}
    return {"ink": ink, "text": text, "ox": ox, "choice": choice}


def review_images(edition_id: int) -> dict[str, str] | None:
    """검수 화면 전용 - runtime_view와 달리 included=false 페이지도 이미지가
    필요하다(검수자는 뺀 문항도 봐야 다시 포함시킬지 판단할 수 있다)."""
    row = get_edition_row(edition_id)
    if row is None:
        return None
    return resolve_image_urls(json.loads(row["layout_json"]))


def source_text(doc_id: str, order_no: int) -> dict | None:
    """검수 화면의 "원문 대조"용 - momo_book_db(읽기 전용)에서 이 문항의 DB 원문을
    그대로 가져온다. layout JSON에 들어간 값은 ①②단계에서 이미 정제·재구성된
    것이라 diff 기준은 항상 DB 원문이어야 한다."""
    import sys
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from normalize.db import get_connection as get_source_connection

    conn = get_source_connection()
    try:
        row = conn.execute(
            "SELECT question_text, excerpt_text FROM discussion_qa WHERE doc_id = ? AND order_no = ?",
            (doc_id, order_no),
        ).fetchone()
        if row is None:
            return None
        return {"question_text": row["question_text"], "excerpt_text": row["excerpt_text"]}
    finally:
        conn.close()


def source_text_by_page(doc_id: str, source_page: int) -> list[dict]:
    """비전(방식 B) 문항 전용 원문 대조 - q.id가 momo_book.db의 order_no와
    1:1로 대응하지 않아(비전은 페이지 항목 순서로 새로 번호를 매김) source_text()의
    order_no 직접 대조는 잘못된 행을 짚을 수 있다(2026-09-26 검수 중 발견 - 야옹아
    9쪽에서 q.id=5를 order_no=5로 착각해 실제로는 order_no=4에 대응하는 문항인데
    엉뚱한 다음 문항의 DB 원문을 보여준 사례). 대신 같은 source_page의 모든 행을
    후보로 반환해 검수자가 직접 눈으로 대조하게 한다."""
    import sys
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from normalize.db import get_connection as get_source_connection

    conn = get_source_connection()
    try:
        rows = conn.execute(
            "SELECT order_no, order_label, question_text, excerpt_text FROM discussion_qa "
            "WHERE doc_id = ? AND source_page = ? ORDER BY order_no",
            (doc_id, source_page),
        ).fetchall()
        return [
            {
                "order_no": r["order_no"], "order_label": r["order_label"],
                "question_text": r["question_text"], "excerpt_text": r["excerpt_text"],
            }
            for r in rows
        ]
    finally:
        conn.close()


def available_images(doc_id: str) -> list[dict]:
    """검수 화면의 "이미지 자리 추가" 기능용 - 이 문서의 momo_book.db
    document_image(원본 삽화·표지·배경) 전부를 후보로 준다(2026-09-26,
    검수자가 직접 원본 이미지를 고르거나 생성 지시문을 쓸 수 있게 하라는
    사용자 지시 [5순위])."""
    import sys
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from normalize.db import get_connection as get_source_connection

    conn = get_source_connection()
    try:
        rows = conn.execute(
            "SELECT image_type, source_page, file_path FROM document_image "
            "WHERE doc_id = ? ORDER BY source_page", (doc_id,),
        ).fetchall()
        return [
            {
                "image_type": r["image_type"], "source_page": r["source_page"],
                "file_path": r["file_path"], "url": f"/static/extracted/{r['file_path']}",
            }
            for r in rows
        ]
    finally:
        conn.close()


def save_answer(edition_id: int, part_id: str, student_id: str,
                 ink: list | None = None, text: dict | None = None, ox: str | None = None,
                 choice: str | list[str] | None = None) -> None:
    conn = db.get_connection()
    try:
        existing = conn.execute(
            "SELECT id, rev FROM student_answer WHERE student_id=? AND edition_id=? AND part_id=?",
            (student_id, edition_id, part_id),
        ).fetchone()
        ink_json = json.dumps(ink, ensure_ascii=False) if ink is not None else None
        # text 칼럼 하나를 text/ox/choice가 같이 쓴다 - 셋 다 part_id가 서로 다른 별도
        # 행이라(#reason vs ox#N vs #choice) 섞일 일은 없다. choice가 배열이면(choiceList의
        # single:false, 복수 선택) JSON으로 직렬화한다 - load_answers가 "["로 시작하는지
        # 보고 다시 배열로 되돌린다.
        choice_val = json.dumps(choice, ensure_ascii=False) if isinstance(choice, list) else choice
        text_val = (json.dumps(text, ensure_ascii=False) if text is not None
                    else ox if ox is not None else choice_val if choice_val is not None else None)
        # choice는 탭 하나가 곧 확정 답이라(별도 "저장" 버튼이 없음) 들어오면 바로 confirmed.
        confirmed = 1 if (text is not None and text.get("saved")) or choice is not None else 0
        if existing:
            conn.execute(
                "UPDATE student_answer SET ink_json = COALESCE(?, ink_json), "
                "text = COALESCE(?, text), confirmed = ?, updated_at = ?, rev = rev + 1 WHERE id = ?",
                (ink_json, text_val, confirmed, _now(), existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO student_answer (student_id, edition_id, part_id, ink_json, text, "
                "confirmed, updated_at, rev) VALUES (?,?,?,?,?,?,?,1)",
                (student_id, edition_id, part_id, ink_json, text_val, confirmed, _now()),
            )
        conn.commit()
    finally:
        conn.close()


def log_recognition(edition_id: int, student_id: str, part_id: str, model: str, prompt: str,
                     text: str, unclear: int, latency_ms: int) -> None:
    """⑦단계: recognition_log에 모델·프롬프트 해시·결과·unclear 수·지연 시간을 남긴다
    (사용자 지시 2026-09-23). recognition_log.answer_id는 student_answer를 참조하므로,
    아직 그 part의 답안 행이 없으면(필기 저장 전에 인식부터 부른 경우) 먼저 빈 행을
    만들어 둔다."""
    conn = db.get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM student_answer WHERE student_id=? AND edition_id=? AND part_id=?",
            (student_id, edition_id, part_id),
        ).fetchone()
        if row is None:
            cur = conn.execute(
                "INSERT INTO student_answer (student_id, edition_id, part_id, updated_at, rev) "
                "VALUES (?,?,?,?,1)",
                (student_id, edition_id, part_id, _now()),
            )
            answer_id = cur.lastrowid
        else:
            answer_id = row["id"]
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        conn.execute(
            "INSERT INTO recognition_log (answer_id, model, prompt_hash, text, unclear_count, "
            "latency_ms, created_at) VALUES (?,?,?,?,?,?,?)",
            (answer_id, model, prompt_hash, text, unclear, latency_ms, _now()),
        )
        conn.commit()
    finally:
        conn.close()
