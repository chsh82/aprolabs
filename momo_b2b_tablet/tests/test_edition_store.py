"""④ edition 저장소 테스트 - 사용자 지시(2026-09-23) 4단계 완료 기준 1~3번.

실행: python tests/test_edition_store.py
edition/edition_store.db를 이 테스트 전용으로 초기화하고 쓴다(momo_book.db는 안 건드림).
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from edition import db, store  # noqa: E402

_PASS, _FAIL = "[PASS]", "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def run() -> bool:
    db.reset_db()

    # ---------- 1. 초안 생성: 3종 전부, 플래그 kind 분류 ----------
    edition_ids = {}
    for doc_id in ("L2-Q2-W08", "L5-Q3-W10", "L9-Q3-W07"):
        eid = store.create_draft(doc_id, created_by="tester")
        edition_ids[doc_id] = eid
        row = store.get_edition_row(eid)
        check(row["status"] == "draft" and row["version"] == 1, f"{doc_id}: 초안 생성 version=1, status=draft")

    l9_id = edition_ids["L9-Q3-W07"]
    flags = store.list_flags(l9_id)
    check(len(flags) > 0, f"L9-Q3-W07: 플래그 {len(flags)}건 저장됨")
    placeholder_flags = [f for f in flags if f["kind"] == "placeholder"]
    widget_flags = [f for f in flags if f["kind"] == "widget_unavailable"]
    check(len(placeholder_flags) > 0, f"L9-Q3-W07: placeholder 플래그 {len(placeholder_flags)}건 (중등 배경지식 등)")
    check(len(widget_flags) > 0, f"L9-Q3-W07: widget_unavailable 플래그 {len(widget_flags)}건 (choice_multi 제외 등)")
    check(all(f["kind"] in db.EDITION_FLAG_KINDS for f in flags), "모든 플래그 kind가 edition_flag 허용 목록 안에 있음")

    # 같은 doc_id로 다시 초안을 만들면 version이 올라간다
    eid2 = store.create_draft("L9-Q3-W07", created_by="tester")
    check(store.get_edition_row(eid2)["version"] == 2, "같은 doc_id로 재생성하면 version=2")

    # ---------- 2. 승인 차단: placeholder 플래그가 남아있으면 approve 실패 ----------
    try:
        store.approve(l9_id, approved_by="reviewer")
        check(False, "placeholder 플래그가 있는데 approve가 성공해버림(막혀야 함)")
    except store.ApprovalBlocked:
        check(True, "placeholder 플래그가 안 풀려 있으면 approve가 막힘")

    for f in placeholder_flags:
        store.resolve_flag(f["id"], resolved_by="reviewer")
    remaining = [f for f in store.list_flags(l9_id) if f["kind"] == "placeholder" and f["resolved_at"] is None]
    check(len(remaining) == 0, "placeholder 플래그를 전부 resolve하면 안 풀린 게 0건")

    store.approve(l9_id, approved_by="reviewer")
    check(store.get_edition_row(l9_id)["status"] == "approved", "placeholder를 다 풀면 approve 성공")

    try:
        store.publish(edition_ids["L2-Q2-W08"])
        check(False, "approve 안 한 edition이 publish 돼버림(막혀야 함)")
    except store.PatchError:
        check(True, "approve 전에는 publish가 막힘")

    store.publish(l9_id)
    check(store.get_edition_row(l9_id)["status"] == "published", "approve 후 publish 성공")

    # ---------- 3. PATCH: 일반 텍스트 수정 -> correction_log(text_correction) ----------
    l2_id = edition_ids["L2-Q2-W08"]
    row = store.get_edition_row(l2_id)
    import json
    layout = json.loads(row["layout_json"])
    vocab_page_idx = next(i for i, p in enumerate(layout["pages"]) if p["type"] == "vocab")
    before_word = layout["pages"][vocab_page_idx]["vocab"][0]["w"]
    patch = [{"op": "replace", "path": f"/pages/{vocab_page_idx}/vocab/0/w", "value": "심통(고침)"}]
    new_id = store.patch_edition(l2_id, patch, editor="reviewer", reason="오타 수정 테스트")
    check(new_id == l2_id, "draft 상태 edition은 PATCH해도 같은 id(새 버전 안 만듦)")
    patched_layout = json.loads(store.get_edition_row(l2_id)["layout_json"])
    check(patched_layout["pages"][vocab_page_idx]["vocab"][0]["w"] == "심통(고침)", "PATCH가 layout_json에 실제로 반영됨")

    conn = db.get_connection()
    logs = conn.execute("SELECT * FROM correction_log WHERE doc_id=? ORDER BY id DESC LIMIT 1",
                         ("L2-Q2-W08",)).fetchall()
    conn.close()
    check(len(logs) == 1 and logs[0]["kind"] == "text_correction", "일반 텍스트 PATCH가 correction_log에 kind=text_correction으로 기록됨")
    check(json.loads(logs[0]["before"]) == before_word, f"correction_log.before가 실제 이전 값과 일치 ({before_word!r})")

    # ---------- 4. PATCH: form 변경 -> correction_log(form_change) ----------
    qa_page_idx = next(i for i, p in enumerate(layout["pages"]) if p.get("q", {}).get("id") == "4")
    form_patch = [{"op": "add", "path": f"/pages/{qa_page_idx}/q/form", "value": "pledge"}]
    store.patch_edition(l2_id, form_patch, editor="reviewer", reason="서약형으로 바꿈")
    conn = db.get_connection()
    fc = conn.execute("SELECT * FROM correction_log WHERE doc_id=? AND kind='form_change' ORDER BY id DESC LIMIT 1",
                       ("L2-Q2-W08",)).fetchall()
    conn.close()
    check(len(fc) == 1, "form 변경 PATCH가 correction_log에 kind=form_change로 기록됨")
    check(fc[0]["before"] == "single" and fc[0]["after"] == "pledge",
          f"form_change before/after가 사용자 예시대로 'single'->'pledge' (실제 {fc[0]['before']!r}->{fc[0]['after']!r})")
    check("문항 4" in fc[0]["field_path"], f"field_path에 문항 order_label이 들어감 ({fc[0]['field_path']!r})")

    # ---------- 5. 확정 후 PATCH -> 새 version ----------
    l9_after_publish = store.get_edition_row(l9_id)
    row2 = json.loads(l9_after_publish["layout_json"])
    bgline_idx = next(i for i, p in enumerate(row2["pages"]) if p["type"] == "bgline")
    patch2 = [{"op": "replace", "path": f"/pages/{bgline_idx}/inst", "value": "수정된 안내문"}]
    new_version_id = store.patch_edition(l9_id, patch2, editor="reviewer")
    check(new_version_id != l9_id, "published edition을 PATCH하면 다른 id(새 version)가 생김")
    new_row = store.get_edition_row(new_version_id)
    # L9-Q3-W07은 위에서 이미 한 번 재생성(version=2)했으니 새 version은 그보다 커야 한다
    # (doc_id 전체에서 MAX(version)+1 - published 시점 버전+1이 아니라).
    check(new_row["version"] > l9_after_publish["version"] and new_row["status"] == "draft",
          f"새 version={new_row['version']}(기존 최대보다 큼), status={new_row['status']}")
    check(store.get_edition_row(l9_id)["status"] == "published", "원래 published edition은 그대로 published로 남아있음(수정 안 됨)")

    # ---------- 6. included: 검수에서 문항 제외 -> runtime에서 안 보임 ----------
    l5_id = edition_ids["L5-Q3-W10"]
    row5 = json.loads(store.get_edition_row(l5_id)["layout_json"])
    total_pages = len(row5["pages"])
    exclude_idx = next(i for i, p in enumerate(row5["pages"]) if p.get("q", {}).get("id") == "2")
    store.patch_edition(l5_id, [{"op": "replace", "path": f"/pages/{exclude_idx}/included", "value": False}],
                         editor="reviewer", reason="검수: 이 문항 싣지 않음")
    # publish 전이라도 runtime_view 자체 필터 동작만 확인(승인 흐름은 이미 위에서 별도로 검증)
    view = store.runtime_view(l5_id)
    check(len(view["layout"]["pages"]) == total_pages - 1,
          f"included=false로 표시한 페이지가 runtime_view에서 빠짐 ({total_pages}->{len(view['layout']['pages'])})")
    check(all(p.get("q", {}).get("id") != "2" for p in view["layout"]["pages"]), "제외한 문항(2)이 실제로 runtime 목록에 없음")

    # ---------- 7. GET /api/runtime 이미지 URL 해석 ----------
    l9_view = store.runtime_view(new_version_id)
    check(l9_view["layout"]["book"]["cover"] in l9_view["images"], "book.cover 키가 images 맵에 있음")
    check(l9_view["images"][l9_view["layout"]["book"]["cover"]].startswith("/static/extracted/"),
          "L9 표지는 doc-scoped 원본 이미지라 /static/extracted/ 로 매핑됨")

    # ---------- 8. 5단계: 낙관적 잠금(rev), 검수 이미지, 원문 대조 ----------
    l2_row = store.get_edition_row(l2_id)
    stale_rev = l2_row["rev"]
    layout_now = json.loads(l2_row["layout_json"])
    idx = next(i for i, p in enumerate(layout_now["pages"]) if p["type"] == "vocab")
    # 다른 사람이 먼저 저장(rev가 올라감)
    store.patch_edition(l2_id, [{"op": "add", "path": f"/pages/{idx}/inst", "value": "다른 사람이 먼저 고침"}],
                         editor="other-reviewer")
    try:
        store.patch_edition(l2_id, [{"op": "add", "path": f"/pages/{idx}/inst", "value": "내가 고침"}],
                             editor="reviewer", expected_rev=stale_rev)
        check(False, "낡은 rev로 PATCH했는데 성공해버림(충돌 감지가 안 됨)")
    except store.ConflictError:
        check(True, "낡은 rev로 PATCH하면 ConflictError(다른 사람이 먼저 고친 걸 덮어쓰지 않음)")
    fresh_rev = store.get_edition_row(l2_id)["rev"]
    store.patch_edition(l2_id, [{"op": "add", "path": f"/pages/{idx}/inst", "value": "내가 고침(최신 rev로)"}],
                         editor="reviewer", expected_rev=fresh_rev)
    check(True, "최신 rev로 다시 보내면 정상 적용됨")

    l5_images = store.review_images(l5_id)
    check(l5_images is not None and len(l5_images) > 0,
          f"included=false인 페이지가 있어도 review_images는 에러 없이 이미지 맵을 돌려줌 ({len(l5_images or {})}개)")

    src = store.source_text("L9-Q3-W07", 5)
    check(src is not None and src["question_text"], f"원문 대조용 DB 원문을 가져옴(order_no=5, {len(src['question_text'])}자)")
    check(store.source_text("L9-Q3-W07", 9999) is None, "존재하지 않는 order_no는 None")

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
