# -*- coding: utf-8 -*-
"""페이지 고정 편집(momo_page_editor_plan.md P1)의 저장소. project_store.py(기존 문항
단위 편집)는 건드리지 않고, 그 위에 "페이지 구성표(page_manifest)"와 "후보
(page_proposal)" 개념만 얹는다.

핵심 불변 조건: 선택한 페이지 1개만 다시 렌더링하고, 나머지 페이지는 얼려둔 HTML
문자열을 그대로 재사용한다(momo_book_db/worksheet/scripts/page_manifest.js/
page_candidate.js가 이미 "조각 하나만 다시 만들고 나머지는 그대로 잇는다"는 걸 보장 -
이 파일은 그 스크립트를 호출하고 결과를 파일로 관리하는 역할만 한다).

저장 구조 (momo_book_db/worksheet/edit_projects/<project_id>/ 아래):
- page_manifest.json: 현재 페이지 구성표(가장 최근 리비전).
- page_manifest_history/<revision_id>.json: 모든 리비전의 스냅샷(복원용).
- page_proposals/<proposal_id>/: spec.json(요청), result.json(후보 생성 결과),
  preview/index.html(적용 전 미리보기 - 정상 버전/PDF 다운로드 경로와 분리됨).
"""
import io
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
import project_store as ps  # noqa: E402

_SCRIPTS_DIR = os.path.join(ps.MOMO_DIR, "worksheet", "scripts")


class PageEditError(Exception):
    """사용자에게 그대로 보여줄 수 있는 오류(검증 실패·차단 등)."""


class ConflictError(PageEditError):
    """base_revision이 최신 페이지 구성표와 다름 - 다른 탭 저장이 먼저 반영됨."""


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_json_atomic(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _read_json_or(default, path):
    if not os.path.isfile(path):
        return default
    try:
        return _read_json(path)
    except (json.JSONDecodeError, OSError):
        return default


def _pdir(project_id):
    return ps.project_dir(project_id)


def _manifest_path(project_id):
    return os.path.join(_pdir(project_id), "page_manifest.json")


def _history_dir(project_id):
    return os.path.join(_pdir(project_id), "page_manifest_history")


def _proposals_dir(project_id):
    return os.path.join(_pdir(project_id), "page_proposals")


def has_manifest(project_id):
    return os.path.isfile(_manifest_path(project_id))


def load_manifest(project_id):
    p = _manifest_path(project_id)
    if not os.path.isfile(p):
        raise PageEditError("이 프로젝트는 아직 페이지 고정 편집을 시작하지 않았습니다.")
    return _read_json(p)


def _save_history(project_id, manifest):
    hdir = _history_dir(project_id)
    os.makedirs(hdir, exist_ok=True)
    _write_json_atomic(os.path.join(hdir, f"{manifest['revision_id']}.json"), manifest)


def list_history(project_id):
    hdir = _history_dir(project_id)
    if not os.path.isdir(hdir):
        return []
    out = []
    for fn in sorted(os.listdir(hdir)):
        if not fn.endswith(".json"):
            continue
        m = _read_json(os.path.join(hdir, fn))
        out.append({
            "revision_id": m["revision_id"], "parent_revision": m.get("parent_revision"),
            "created_at": m["created_at"], "page_count": len(m["pages"]),
        })
    out.sort(key=lambda r: r["created_at"])
    return out


def freeze_manifest(project_id):
    """명시적 전환 경로 - 사용자가 이 프로젝트의 "페이지별 편집" 화면에 실제로 들어왔을
    때만 호출된다(다른 프로젝트를 건드리지 않는 일괄 자동 변환이 아님). 이미 얼려져
    있으면 그대로 반환 - 재방문할 때마다 다시 얼리지 않는다(그러면 직접 편집한 페이지
    구성이 매번 초기화되어 버림)."""
    if has_manifest(project_id):
        return load_manifest(project_id)

    data, edit_doc_id, version_id = ps.build_effective_data(project_id)
    if not version_id:
        raise PageEditError("먼저 '문항 편집' 화면에서 한 번 저장을 해야 페이지 고정 편집을 시작할 수 있습니다.")

    tmp_data_path = os.path.join(_pdir(project_id), "_page_freeze_input.json")
    _write_json_atomic(tmp_data_path, data)
    out_new = _manifest_path(project_id) + ".new"
    result = subprocess.run(
        ["node", "page_manifest.js", "freeze", tmp_data_path, out_new],
        cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=180,
    )
    os.remove(tmp_data_path)
    if result.returncode != 0 or not os.path.isfile(out_new):
        raise PageEditError(f"페이지 구성표 생성 실패: {result.stderr or result.stdout}")
    manifest = _read_json(out_new)
    os.remove(out_new)
    manifest["frozen_from_version"] = version_id
    manifest["book_title"] = data["meta"]["book"]["title"]
    manifest["quarter_class"] = data["meta"]["quarter_class"]
    _write_json_atomic(_manifest_path(project_id), manifest)
    _save_history(project_id, manifest)
    return manifest


def _find_page(manifest, page_id):
    for p in manifest["pages"]:
        if p["page_id"] == page_id:
            return p
    raise PageEditError(f"페이지를 찾을 수 없음: {page_id}")


def page_thumbnails(manifest):
    """왼쪽 썸네일 목록용 - 표시 쪽수는 배열 순서에서 매길 뿐(page_id가 진짜 식별자)."""
    labels = {"cover": "표지", "step1": "1단계", "step2": "2단계", "step3": "3단계"}
    out = []
    for i, p in enumerate(manifest["pages"]):
        editable = p["layout_type"] in ("halves", "fullpage")
        item_ids = sorted({s["itemId"] for s in p["slots"] if s["itemId"] is not None})
        out.append({
            "page_id": p["page_id"], "index": i + 1, "role": p["role"],
            "role_index": p["role_index"], "layout_type": p["layout_type"],
            "label": f"{labels.get(p['role'], p['role'])}" + (f" {p['role_index']+1}" if p["role"] in ("step1", "step2") else ""),
            "editable": editable, "item_ids": item_ids,
        })
    return out


_FIELD_PART_PRIORITY = {
    "question_text": ("merged", "answer"),
    "answer_height_mm": ("merged", "answer"),
    "reference_image_path": ("merged", "answer"),
    "excerpt_image_path": ("merged", "lead"),
}


def _resolve_target_slot(page, item_id, field):
    candidates = [s for s in page["slots"] if str(s["itemId"]) == str(item_id)]
    if not candidates:
        raise PageEditError(f"이 페이지에서 문항 {item_id}의 편집 대상 조각을 찾을 수 없습니다.")
    for part in _FIELD_PART_PRIORITY.get(field, ("merged", "answer", "lead")):
        for s in candidates:
            if s["part"] == part:
                return s
    return candidates[0]


def _get_effective_item(project_id, item_id):
    data, edit_doc_id, _version_id = ps.build_effective_data(project_id)
    for item in data.get("step2") or []:
        if str(item.get("id")) == str(item_id):
            return item, data["meta"]["tone_class"], edit_doc_id
    raise PageEditError(f"문항을 찾을 수 없음(현재 이 편집기는 2단계 문항만 지원): {item_id}")


def page_items(project_id, page):
    """이 페이지에 실제로 등장하는 문항들의 현재(effective) 데이터 - 편집 대상 선택
    드롭다운/요소 클릭 선택에 쓴다."""
    item_ids = sorted({s["itemId"] for s in page["slots"] if s["itemId"] is not None})
    out = []
    for iid in item_ids:
        item, _tone, _doc = _get_effective_item(project_id, iid)
        out.append(item)
    return out


def validate_image(file_bytes):
    """실제 디코딩 + 최소 픽셀 검증(기획서 §9 "실제 이미지 디코딩, 형식·용량·픽셀 수
    검증"). project_store.add_asset()은 확장자만 보므로 여기서 먼저 막는다."""
    from PIL import Image
    try:
        im = Image.open(io.BytesIO(file_bytes))
        im.verify()
        im2 = Image.open(io.BytesIO(file_bytes))
        w, h = im2.size
        fmt = im2.format
    except Exception as e:
        raise PageEditError(f"이미지 파일을 열 수 없습니다(손상되었거나 지원하지 않는 형식): {e}")
    if w < 40 or h < 40:
        raise PageEditError(f"이미지 해상도가 너무 작습니다({w}x{h}px) - 40x40px 이상 필요합니다.")
    if len(file_bytes) > 15 * 1024 * 1024:
        raise PageEditError("이미지 용량이 너무 큽니다(15MB 초과).")
    return {"width": w, "height": h, "format": fmt}


def create_proposal(project_id, page_id, item_id, operation, base_revision, *,
                     question_text=None, answer_height_mm=None,
                     image_field=None, image_file_bytes=None, image_filename=None):
    """직접 편집 1건 -> 후보 1건. 원본(page_manifest.json)·현재 저장 버전은 전혀 안 바뀐다.
    반환: proposal dict(status: ok=적용 가능한 후보 생김 / blocked=넘침으로 차단 /
    error=요청 자체 문제)."""
    manifest = load_manifest(project_id)
    if manifest["revision_id"] != base_revision:
        raise ConflictError(
            "다른 저장(다른 탭 등)이 먼저 반영되어 이 화면이 보고 있던 페이지 구성이 오래되었습니다. "
            "화면을 새로고침한 뒤 다시 시도해주세요."
        )

    page = _find_page(manifest, page_id)
    if page["layout_type"] not in ("halves", "fullpage"):
        raise PageEditError("이 페이지는 이번 범위(2단계 문항 페이지)에서 직접 편집을 지원하지 않습니다.")

    item, tone, edit_doc_id = _get_effective_item(project_id, item_id)

    content_delta = {}
    design_delta = {}
    if operation == "set_text":
        field = "question_text"
        if not question_text or not question_text.strip():
            raise PageEditError("문장이 비어 있습니다.")
        item["question_text"] = question_text
        content_delta["question_text"] = question_text
    elif operation == "set_answer_area":
        field = "answer_height_mm"
        try:
            h = max(15.0, float(answer_height_mm))
        except (TypeError, ValueError):
            raise PageEditError("답란 높이는 숫자(mm)여야 합니다.")
        item["answer_height_mm"] = h
        design_delta["answer_height_mm"] = h
    elif operation == "replace_image":
        if image_field not in ("excerpt_image_path", "reference_image_path"):
            raise PageEditError(f"알 수 없는 그림 필드: {image_field}")
        field = image_field
        if not image_file_bytes:
            raise PageEditError("업로드된 이미지 파일이 없습니다.")
        validate_image(image_file_bytes)
        asset = ps.add_asset(project_id, image_file_bytes, image_filename or "upload.png")
        ps.sync_assets_to_extracted_images(project_id)
        new_path = f"{edit_doc_id}/{asset['asset_id']}{asset['ext']}"
        item[image_field] = new_path
        content_delta[image_field] = f"asset:{asset['asset_id']}{asset['ext']}"
    else:
        raise PageEditError(f"알 수 없는 연산: {operation}")

    target_slot = _resolve_target_slot(page, item_id, field)

    proposal_id = f"prop_{uuid.uuid4().hex[:10]}"
    pdir = os.path.join(_proposals_dir(project_id), proposal_id)
    os.makedirs(pdir, exist_ok=True)

    spec = {
        "operation": operation, "tone": tone,
        "page": {"layout_type": page["layout_type"], "html_raw": page["html_raw"], "slots": page["slots"]},
        "target": {"itemId": item_id, "part": target_slot["part"]},
        "item_effective": item,
    }
    spec_path = os.path.join(pdir, "spec.json")
    _write_json_atomic(spec_path, spec)

    result = subprocess.run(
        ["node", "page_candidate.js", spec_path],
        cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=120,
    )
    try:
        out = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise PageEditError(f"후보 생성 실패(스크립트 오류): {result.stderr or result.stdout}")

    proposal = {
        "proposal_id": proposal_id, "project_id": project_id,
        "page_id": page_id, "item_id": item_id, "operation": operation, "field": field,
        "base_revision": base_revision, "created_at": _now(),
        "content_delta": content_delta, "design_delta": design_delta,
        "status": out["status"],  # 'ok' | 'blocked' | 'error'
        "reason": out.get("reason"),
    }
    if out["status"] == "ok":
        # 미리보기 전체 문서를 조립해 둔다(정상 버전/PDF 다운로드 경로와 완전히 분리된
        # page_proposals/ 아래 - "적용 전 후보는 정상 PDF 다운로드에 섞이지 않는다").
        preview_dir = os.path.join(pdir, "preview")
        overrides_path = os.path.join(pdir, "_overrides.json")
        _write_json_atomic(overrides_path, {page_id: out["html_raw"]})
        preview_html_path = os.path.join(preview_dir, "index.html")
        asm = subprocess.run(
            ["node", "page_manifest.js", "assemble", _manifest_path(project_id), overrides_path,
             manifest.get("book_title", ""), manifest.get("quarter_class", ""), preview_html_path],
            cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=120,
        )
        if asm.returncode != 0:
            proposal["status"] = "error"
            proposal["reason"] = f"미리보기 조립 실패: {asm.stderr or asm.stdout}"
        else:
            proposal["candidate_page_html"] = out["html_raw"]
            proposal["changed_slot_html"] = out["changed_slot_html"]
            proposal["target_part"] = target_slot["part"]

    _write_json_atomic(os.path.join(pdir, "proposal.json"), proposal)
    return proposal


def load_proposal(project_id, proposal_id):
    p = os.path.join(_proposals_dir(project_id), proposal_id, "proposal.json")
    if not os.path.isfile(p):
        raise PageEditError(f"후보를 찾을 수 없음: {proposal_id}")
    return _read_json(p)


def list_active_proposal(project_id, page_id):
    """페이지당 후보 하나만 유지(기획서 §8) - 이 페이지의 가장 최근 후보를 보여준다.
    가장 최근 것이 취소·적용·오류로 이미 끝난 상태면 "활성 후보 없음"으로 취급한다 -
    그렇지 않으면 방금 취소한 후보 대신 그보다 오래된 차단 이력이 다시 떠서 "아직도
    차단된 상태"처럼 보이는 혼동이 생긴다(가장 최신 시도만 화면에 반영)."""
    pdir = _proposals_dir(project_id)
    if not os.path.isdir(pdir):
        return None
    cands = []
    for pid in os.listdir(pdir):
        try:
            pr = load_proposal(project_id, pid)
        except PageEditError:
            continue
        if pr.get("page_id") == page_id:
            cands.append(pr)
    if not cands:
        return None
    cands.sort(key=lambda r: r["created_at"])
    latest = cands[-1]
    return latest if latest.get("status") in ("ok", "blocked") else None


def cancel_proposal(project_id, proposal_id):
    pdir = os.path.join(_proposals_dir(project_id), proposal_id)
    proposal = load_proposal(project_id, proposal_id)
    proposal["status"] = "cancelled"
    _write_json_atomic(os.path.join(pdir, "proposal.json"), proposal)


def _run_check(html_path, data_path, report_path):
    result = subprocess.run(
        ["node", "check.js", html_path, data_path, report_path],
        cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=120,
    )
    log = (result.stdout or "") + (("\n--- stderr ---\n" + result.stderr) if result.stderr else "")
    status = "FAIL"
    if os.path.isfile(report_path):
        try:
            status = _read_json(report_path).get("status", "FAIL")
        except (json.JSONDecodeError, OSError):
            pass
    return status, log


def apply_proposal(project_id, proposal_id, base_revision):
    """후보 적용 - 원자적으로 새 페이지 리비전 + 새 편집 버전을 만든다. QA 실패 시
    page_manifest.json은 전혀 바뀌지 않는다("실패 시 마지막 정상본 유지")."""
    pdir = os.path.join(_proposals_dir(project_id), proposal_id)
    proposal = load_proposal(project_id, proposal_id)

    if proposal["status"] == "applied":
        # 중복 클릭 방지 - 이미 적용된 후보를 다시 눌러도 같은 결과만 보여주고 끝낸다.
        return proposal
    if proposal["status"] != "ok":
        raise PageEditError(f"이 후보는 적용할 수 없는 상태입니다: {proposal['status']} - {proposal.get('reason','')}")

    manifest = load_manifest(project_id)
    if manifest["revision_id"] != base_revision or manifest["revision_id"] != proposal["base_revision"]:
        raise ConflictError(
            "다른 저장이 먼저 반영되어 이 후보의 기준이 오래되었습니다. 후보를 다시 생성한 뒤 적용해주세요."
        )

    # 중복 적용 방지: 먼저 상태를 'applying'으로 바꿔 써서, 같은 후보에 대한 동시 적용
    # 요청이 있어도 두 번째 요청은 아래 재확인에서 걸린다.
    proposal["status"] = "applying"
    _write_json_atomic(os.path.join(pdir, "proposal.json"), proposal)

    try:
        # 1) 기존 편집기(문항 단위)와 같은 overrides 체계에 이번 변경만 얹어 새 버전을 만든다
        #    - 이렇게 하면 기존 "문항 편집" 화면·버전 이력·미리보기·PDF 다운로드가 전부
        #    그대로 이 새 버전을 인식한다(별도 저장 체계를 새로 만들지 않음).
        meta = ps.load_project(project_id)
        prev_content, prev_design = ({}, {})
        if meta.get("current_version"):
            prev_content, prev_design = ps.get_version_overrides(project_id, meta["current_version"])
        content_overrides = json.loads(json.dumps(prev_content))
        design_overrides = json.loads(json.dumps(prev_design))
        item_key = str(proposal["item_id"])
        if proposal["content_delta"]:
            content_overrides.setdefault(item_key, {}).update(proposal["content_delta"])
        if proposal["design_delta"]:
            design_overrides.setdefault(item_key, {}).update(proposal["design_delta"])

        new_version_id = ps.save_version(
            project_id, content_overrides, design_overrides,
            note=f"[페이지 편집] page={proposal['page_id']} item={proposal['item_id']} {proposal['field']}",
        )
        vdir = os.path.join(_pdir(project_id), "versions", new_version_id)

        data, _edit_doc_id, _vid = ps.build_effective_data(project_id)
        data_path = os.path.join(vdir, "effective_data.json")
        _write_json_atomic(data_path, data)

        overrides_path = os.path.join(pdir, "_overrides.json")
        html_path = os.path.join(vdir, "index.html")
        asm = subprocess.run(
            ["node", "page_manifest.js", "assemble", _manifest_path(project_id), overrides_path,
             manifest.get("book_title", ""), manifest.get("quarter_class", ""), html_path],
            cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=120,
        )
        if asm.returncode != 0 or not os.path.isfile(html_path):
            raise PageEditError(f"페이지 조립 실패: {asm.stderr or asm.stdout}")

        report_path = os.path.join(vdir, "check_report.json")
        qa_status, log = _run_check(html_path, data_path, report_path)
        with open(os.path.join(vdir, "build.log"), "w", encoding="utf-8") as f:
            f.write(log)
        manifest_path_v = os.path.join(vdir, "manifest.json")
        vmanifest = _read_json_or({}, manifest_path_v)
        vmanifest["qa_status"] = qa_status
        vmanifest["checked_at"] = _now()
        vmanifest["source"] = "page_editor"
        _write_json_atomic(manifest_path_v, vmanifest)
        ps.mark_qa_result(project_id, new_version_id, qa_status)

        if qa_status != "PASS":
            proposal["status"] = "qa_failed"
            proposal["applied_version_id"] = new_version_id
            proposal["qa_status"] = qa_status
            _write_json_atomic(os.path.join(pdir, "proposal.json"), proposal)
            raise PageEditError(
                f"후보를 저장 버전으로 만들었지만 최종 QA를 통과하지 못했습니다(status={qa_status}). "
                f"페이지 구성표(page_manifest)는 바뀌지 않았고 마지막 정상 페이지가 계속 사용됩니다. "
                f"버전 {new_version_id}에서 build.log/check_report.json을 확인해주세요."
            )

        # 2) QA PASS - 이제 새 페이지 리비전을 원자적으로 커밋한다.
        new_pages = []
        for p in manifest["pages"]:
            if p["page_id"] == proposal["page_id"]:
                new_pages.append({
                    **p,
                    "html_raw": proposal["candidate_page_html"],
                    "slots": [
                        ({**s, "html": proposal["changed_slot_html"]}
                         if str(s["itemId"]) == str(proposal["item_id"]) and s["part"] == proposal["target_part"]
                         else s)
                        for s in p["slots"]
                    ],
                })
            else:
                new_pages.append(p)

        new_revision = {
            **manifest,
            "revision_id": f"rev_{uuid.uuid4().hex[:12]}",
            "parent_revision": manifest["revision_id"],
            "created_at": _now(),
            "pages": new_pages,
            "applied_proposal_id": proposal_id,
            "applied_version_id": new_version_id,
        }
        _write_json_atomic(_manifest_path(project_id), new_revision)
        _save_history(project_id, new_revision)

        proposal["status"] = "applied"
        proposal["applied_version_id"] = new_version_id
        proposal["applied_revision_id"] = new_revision["revision_id"]
        proposal["qa_status"] = qa_status
        _write_json_atomic(os.path.join(pdir, "proposal.json"), proposal)
        return proposal
    except PageEditError:
        raise
    except Exception as e:
        proposal["status"] = "error"
        proposal["reason"] = f"적용 중 예외: {e}"
        _write_json_atomic(os.path.join(pdir, "proposal.json"), proposal)
        raise


def restore_revision(project_id, target_revision_id, base_revision):
    """이전 페이지 구성표로 복원 - 기획서 §7 "실행 취소는 새 리비전으로 남긴다"를
    그대로 따른다(target_revision_id를 삭제하고 되돌리는 게 아니라, 그 내용을 담은
    "새" 리비전을 만든다). 적용과 동일하게 새 편집 버전 + QA를 거친다."""
    manifest = load_manifest(project_id)
    if manifest["revision_id"] != base_revision:
        raise ConflictError("다른 저장이 먼저 반영되었습니다. 새로고침 후 다시 시도해주세요.")

    hist_path = os.path.join(_history_dir(project_id), f"{target_revision_id}.json")
    if not os.path.isfile(hist_path):
        raise PageEditError(f"복원할 리비전을 찾을 수 없음: {target_revision_id}")
    target = _read_json(hist_path)

    meta = ps.load_project(project_id)
    prev_content, prev_design = ({}, {})
    if meta.get("current_version"):
        prev_content, prev_design = ps.get_version_overrides(project_id, meta["current_version"])
    new_version_id = ps.save_version(
        project_id, json.loads(json.dumps(prev_content)), json.loads(json.dumps(prev_design)),
        note=f"[페이지 편집] {target_revision_id}로 복원",
    )
    vdir = os.path.join(_pdir(project_id), "versions", new_version_id)
    data, _edit_doc_id, _vid = ps.build_effective_data(project_id)
    data_path = os.path.join(vdir, "effective_data.json")
    _write_json_atomic(data_path, data)

    overrides_path = os.path.join(vdir, "_restore_overrides.json")
    overrides = {p["page_id"]: p["html_raw"] for p in target["pages"]}
    _write_json_atomic(overrides_path, overrides)
    html_path = os.path.join(vdir, "index.html")
    asm = subprocess.run(
        ["node", "page_manifest.js", "assemble", hist_path, overrides_path,
         manifest.get("book_title", ""), manifest.get("quarter_class", ""), html_path],
        cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=120,
    )
    if asm.returncode != 0 or not os.path.isfile(html_path):
        raise PageEditError(f"복원 조립 실패: {asm.stderr or asm.stdout}")

    report_path = os.path.join(vdir, "check_report.json")
    qa_status, log = _run_check(html_path, data_path, report_path)
    with open(os.path.join(vdir, "build.log"), "w", encoding="utf-8") as f:
        f.write(log)
    vmanifest_path = os.path.join(vdir, "manifest.json")
    vmanifest = _read_json_or({}, vmanifest_path)
    vmanifest["qa_status"] = qa_status
    vmanifest["checked_at"] = _now()
    vmanifest["source"] = "page_editor_restore"
    _write_json_atomic(vmanifest_path, vmanifest)
    ps.mark_qa_result(project_id, new_version_id, qa_status)

    if qa_status != "PASS":
        raise PageEditError(f"복원 버전이 QA를 통과하지 못했습니다(status={qa_status}) - 페이지 구성표는 바뀌지 않았습니다.")

    new_revision = {
        **target,
        "revision_id": f"rev_{uuid.uuid4().hex[:12]}",
        "parent_revision": manifest["revision_id"],
        "created_at": _now(),
        "restored_from": target_revision_id,
        "applied_version_id": new_version_id,
    }
    _write_json_atomic(_manifest_path(project_id), new_revision)
    _save_history(project_id, new_revision)
    return new_revision
