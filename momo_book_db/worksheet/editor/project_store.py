# -*- coding: utf-8 -*-
"""학습지 최소 편집 기능의 저장소. 운영 DB(momo_book.db)는 전혀 건드리지 않는다 -
편집 프로젝트는 momo_book_db/worksheet/edit_projects/<project_id>/ 아래 파일로만 존재한다.

설계:
- project.json: 프로젝트 메타(원본 doc_id, 데이터 출처 origin/복원 패치 여부, 현재 버전 포인터)
- base_data.json: 프로젝트 생성 시점에 얼린 원본 데이터 스냅샷(그 뒤 실제 DB가 바뀌어도
  이 프로젝트는 영향 안 받음 - 편집 프로젝트의 "원본 문항 ID"는 전부 이 스냅샷 기준).
- versions/<version_id>/: 명시적 저장마다 하나. content_overrides.json(문장·그림 등 콘텐츠
  변경)과 design_overrides.json(답란 높이 등 배치 변경)을 분리해서 저장 - "콘텐츠 변경과
  디자인·배치 변경을 분리" 요구사항. manifest.json에 이전 버전 포인터(based_on)를 남겨
  버전 체인을 이룬다 - "이전 저장 버전 복원"은 current_version 포인터만 옮기면 된다
  (버전 자체는 삭제 안 하므로 언제든 되돌아갈 수 있음).
- assets/: 업로드된 그림. 파일명은 고유 asset_id(내용 sha256 기반 - 같은 파일 재업로드해도
  중복 저장 안 함) 그대로, 원본 파일명·sha256·업로드 시각은 assets.json에 별도 기록.
  원본 교재 이미지(momo_book_db/extracted_images/<원본doc_id>/)는 절대 안 건드림.
"""
import hashlib
import json
import os
import shutil
import time
import uuid
import zipfile
from datetime import datetime

MOMO_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # momo_book_db/
PROJECTS_ROOT = os.path.join(MOMO_DIR, "worksheet", "edit_projects")
EXTRACTED_IMAGES_ROOT = os.path.join(MOMO_DIR, "extracted_images")


class ProjectError(Exception):
    """프로젝트 열기/저장 실패(손상·자산 누락 등) - 호출부가 사용자에게 보여줄 메시지를 담는다."""


def project_dir(project_id):
    """호출부(라우터)에서 버전 폴더 등에 직접 접근해야 할 때 쓰는 공개 버전."""
    return _project_dir(project_id)


def _project_dir(project_id):
    # project_id에 경로 조작 문자가 섞여 들어오는 걸 막는다(디렉터리 탈출 방지).
    if not project_id or "/" in project_id or "\\" in project_id or ".." in project_id:
        raise ProjectError(f"올바르지 않은 project_id: {project_id!r}")
    return os.path.join(PROJECTS_ROOT, project_id)


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_project(source_doc_id, base_data, data_origin="original", restoration_patch_ids=None, label=None):
    """base_data(이미 extract_worksheet_json.extract()나 복원 패치를 적용해서 만든 dict)를
    얼려서 새 편집 프로젝트를 만든다. 반환: project_id."""
    project_id = f"{source_doc_id}-{uuid.uuid4().hex[:8]}"
    pdir = _project_dir(project_id)
    os.makedirs(pdir, exist_ok=True)
    os.makedirs(os.path.join(pdir, "versions"), exist_ok=True)
    os.makedirs(os.path.join(pdir, "assets"), exist_ok=True)

    base_path = os.path.join(pdir, "base_data.json")
    base_text = json.dumps(base_data, ensure_ascii=False, indent=2)
    with open(base_path, "w", encoding="utf-8") as f:
        f.write(base_text)
    base_hash = hashlib.sha256(base_text.encode("utf-8")).hexdigest()[:16]

    meta = {
        "project_id": project_id,
        "source_doc_id": source_doc_id,
        "label": label or project_id,
        "data_origin": data_origin,  # 'original' | 'restoration_candidate'
        "restoration_patch_ids": restoration_patch_ids or [],
        "created_at": _now(),
        "current_version": None,  # 가장 최근 "저장"(편집 내용 자체) - QA 통과 여부 무관
        "last_good_version": None,  # QA PASS였던 가장 최근 버전 - "마지막 정상본"
        "base_data_sha256": base_hash,
    }
    _write_json(os.path.join(pdir, "project.json"), meta)
    _write_json(os.path.join(pdir, "assets", "assets.json"), [])
    return project_id


def load_project(project_id):
    pdir = _project_dir(project_id)
    meta_path = os.path.join(pdir, "project.json")
    if not os.path.isdir(pdir) or not os.path.isfile(meta_path):
        raise ProjectError(f"프로젝트를 찾을 수 없음: {project_id}")
    try:
        meta = _read_json(meta_path)
    except (json.JSONDecodeError, OSError) as e:
        raise ProjectError(f"project.json이 손상됨: {e}")
    return meta


def list_projects():
    if not os.path.isdir(PROJECTS_ROOT):
        return []
    out = []
    for pid in sorted(os.listdir(PROJECTS_ROOT)):
        try:
            out.append(load_project(pid))
        except ProjectError:
            continue
    return out


def list_versions(project_id):
    pdir = _project_dir(project_id)
    vdir = os.path.join(pdir, "versions")
    if not os.path.isdir(vdir):
        return []
    versions = []
    for vid in sorted(os.listdir(vdir)):
        mpath = os.path.join(vdir, vid, "manifest.json")
        if os.path.isfile(mpath):
            versions.append(_read_json(mpath))
    return versions


def get_version_overrides(project_id, version_id):
    pdir = _project_dir(project_id)
    vdir = os.path.join(pdir, "versions", version_id)
    content = _read_json_or({}, os.path.join(vdir, "content_overrides.json"))
    design = _read_json_or({}, os.path.join(vdir, "design_overrides.json"))
    return content, design


def save_version(project_id, content_overrides, design_overrides, note=""):
    """명시적 저장 - 새 버전 폴더를 만들고 current_version을 그 버전으로 옮긴다.
    직전 버전의 overrides를 이어받은 뒤 이번 변경분을 덮어쓰는 건 호출부(라우터) 책임 -
    여기서는 "지금 준 overrides 그대로"를 새 버전으로 확정 저장하기만 한다."""
    meta = load_project(project_id)
    pdir = _project_dir(project_id)
    version_id = f"v{int(time.time()*1000)}"
    vdir = os.path.join(pdir, "versions", version_id)
    os.makedirs(vdir, exist_ok=True)
    _write_json(os.path.join(vdir, "content_overrides.json"), content_overrides)
    _write_json(os.path.join(vdir, "design_overrides.json"), design_overrides)
    _write_json(os.path.join(vdir, "manifest.json"), {
        "version_id": version_id,
        "created_at": _now(),
        "based_on": meta.get("current_version"),
        "note": note,
    })
    meta["current_version"] = version_id
    _write_json(os.path.join(pdir, "project.json"), meta)
    return version_id


def restore_version(project_id, version_id):
    """이전 저장 버전 복원 - 버전 자체(파일)는 그대로 두고 current_version 포인터만 옮긴다.
    되돌리기 자체도 "저장"이 아니라 포인터 이동이라, 실수로 되돌려도 최신 버전이 사라지지
    않는다(버전 히스토리는 계속 남음)."""
    pdir = _project_dir(project_id)
    vdir = os.path.join(pdir, "versions", version_id)
    if not os.path.isdir(vdir):
        raise ProjectError(f"버전을 찾을 수 없음: {version_id}")
    meta = load_project(project_id)
    meta["current_version"] = version_id
    _write_json(os.path.join(pdir, "project.json"), meta)
    return version_id


def mark_qa_result(project_id, version_id, status):
    """_run_pipeline()이 QA를 실제로 돌린 뒤 호출 - PASS일 때만 last_good_version을
    옮긴다. FAIL/BLOCKED여도 current_version(방금 저장한 편집 내용)은 그대로 둬서
    사용자가 계속 고쳐 나갈 수 있게 하되, "정상본"(미리보기 기본값)은 안 흔들린다."""
    pdir = _project_dir(project_id)
    meta = load_project(project_id)
    if status == "PASS":
        meta["last_good_version"] = version_id
    _write_json(os.path.join(pdir, "project.json"), meta)


def add_asset(project_id, file_bytes, original_filename):
    """업로드 그림을 sha256 기반 asset_id로 저장(같은 내용 재업로드해도 중복 저장 안 함).
    원본 파일명·해시·업로드 시각을 assets.json에 남긴다. 반환: asset 딕셔너리."""
    pdir = _project_dir(project_id)
    sha = hashlib.sha256(file_bytes).hexdigest()
    ext = os.path.splitext(original_filename)[1].lower() or ".png"
    if ext not in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
        raise ProjectError(f"허용되지 않는 이미지 확장자: {ext}")
    asset_id = sha[:24]
    assets_path = os.path.join(pdir, "assets", "assets.json")
    assets = _read_json_or([], assets_path)
    existing = next((a for a in assets if a["asset_id"] == asset_id), None)
    file_path = os.path.join(pdir, "assets", f"{asset_id}{ext}")
    if not existing:
        with open(file_path, "wb") as f:
            f.write(file_bytes)
        entry = {
            "asset_id": asset_id, "ext": ext, "original_filename": original_filename,
            "sha256": sha, "uploaded_at": _now(), "bytes": len(file_bytes),
        }
        assets.append(entry)
        _write_json(assets_path, assets)
    else:
        entry = existing
    return entry


def list_assets(project_id):
    pdir = _project_dir(project_id)
    return _read_json_or([], os.path.join(pdir, "assets", "assets.json"))


def sync_assets_to_extracted_images(project_id):
    """content_overrides가 참조할 수 있게, 프로젝트 자산을
    momo_book_db/extracted_images/EDIT-<project_id>/ 아래로 복사한다(원본 교재 폴더와
    이름이 절대 안 겹치는 전용 서브폴더 - 원본 그림을 덮어쓰는 일이 구조적으로 불가능).
    기존 generate.js/blocks.js의 docImageSrc()가 '../../extracted_images/<경로>'를
    그대로 쓰는 걸 그대로 활용하기 위함(생성기 자체는 안 건드림)."""
    edit_doc_id = f"EDIT-{project_id}"
    dest_dir = os.path.join(EXTRACTED_IMAGES_ROOT, edit_doc_id)
    os.makedirs(dest_dir, exist_ok=True)
    for a in list_assets(project_id):
        src = os.path.join(_project_dir(project_id), "assets", f"{a['asset_id']}{a['ext']}")
        dst = os.path.join(dest_dir, f"{a['asset_id']}{a['ext']}")
        if os.path.isfile(src) and not os.path.isfile(dst):
            shutil.copyfile(src, dst)
    return edit_doc_id


def build_effective_data(project_id):
    """base_data + 현재 버전의 content_overrides(+ design_overrides)를 합쳐 generate.js에
    바로 먹일 수 있는 data.json 형태를 만든다. 콘텐츠 변경(문장/그림)과 디자인 변경(답란
    높이)을 최종 산출물 안에서는 각 문항에 함께 반영하되, 소스(overrides 파일)는 분리
    보관한 걸 여기서만 합친다."""
    meta = load_project(project_id)
    pdir = _project_dir(project_id)
    base = _read_json(os.path.join(pdir, "base_data.json"))
    version_id = meta.get("current_version")
    content_overrides, design_overrides = ({}, {})
    if version_id:
        content_overrides, design_overrides = get_version_overrides(project_id, version_id)

    edit_doc_id = sync_assets_to_extracted_images(project_id)
    data = json.loads(json.dumps(base))  # deep copy
    data["doc_id"] = edit_doc_id

    def resolve_image(value):
        if isinstance(value, str) and value.startswith("asset:"):
            asset_id_ext = value[len("asset:"):]
            return f"{edit_doc_id}/{asset_id_ext}"
        return value

    all_items = list(data.get("step2") or [])
    for v in (data.get("step1") or {}).get("vocab", []):
        all_items.append(v)
    for o in (data.get("step1") or {}).get("ox", []):
        all_items.append(o)
    if data.get("step3") and data["step3"].get("essay"):
        all_items.append(data["step3"]["essay"])

    by_id = {str(item.get("id")): item for item in all_items}

    for item_id, changes in content_overrides.items():
        item = by_id.get(str(item_id))
        if not item:
            continue
        for field, value in changes.items():
            if field in ("excerpt_image_path", "reference_image_path", "image_path"):
                item[field] = resolve_image(value)
            else:
                item[field] = value

    for item_id, changes in design_overrides.items():
        item = by_id.get(str(item_id))
        if not item:
            continue
        if "answer_height_mm" in changes:
            # QA 최소 답란 15mm 정책을 편집 화면에서도 원천적으로 지킨다 - 그 밑으로는
            # 아예 저장이 안 되게(라우터에서도 한 번 더 막지만 여기서도 방어).
            item["answer_height_mm"] = max(15, float(changes["answer_height_mm"]))

    return data, edit_doc_id, version_id


def export_project(project_id, out_zip_path):
    """프로젝트 데이터 + 자산 이미지를 zip 하나로 내보낸다(다른 데서 불러오기 가능)."""
    pdir = _project_dir(project_id)
    if not os.path.isdir(pdir):
        raise ProjectError(f"프로젝트를 찾을 수 없음: {project_id}")
    with zipfile.ZipFile(out_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(pdir):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, pdir)
                zf.write(full, rel)
    return out_zip_path


def import_project(zip_path):
    """zip을 새 프로젝트로 불러온다. project.json이 없거나, assets.json에 기록된 자산
    파일이 실제로 없으면(손상/누락) ProjectError를 던진다 - 호출부가 이걸 잡아서
    "현재 정상 작업"(다른 프로젝트)을 안 건드리고 사용자에게 오류만 보여줘야 한다."""
    tmp_id = f"_import_{uuid.uuid4().hex[:8]}"
    tmp_dir = _project_dir(tmp_id)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir)
    except zipfile.BadZipFile as e:
        _safe_rmtree(tmp_dir)
        raise ProjectError(f"zip 파일이 손상됨: {e}")

    meta_path = os.path.join(tmp_dir, "project.json")
    if not os.path.isfile(meta_path):
        _safe_rmtree(tmp_dir)
        raise ProjectError("project.json이 없음 - 올바른 편집 프로젝트 내보내기 파일이 아님")
    try:
        meta = _read_json(meta_path)
    except (json.JSONDecodeError, OSError) as e:
        _safe_rmtree(tmp_dir)
        raise ProjectError(f"project.json 파싱 실패(손상): {e}")

    if not os.path.isfile(os.path.join(tmp_dir, "base_data.json")):
        _safe_rmtree(tmp_dir)
        raise ProjectError("base_data.json이 없음(손상된 내보내기)")

    assets_path = os.path.join(tmp_dir, "assets", "assets.json")
    missing = []
    for a in _read_json_or([], assets_path):
        fpath = os.path.join(tmp_dir, "assets", f"{a['asset_id']}{a['ext']}")
        if not os.path.isfile(fpath):
            missing.append(a["original_filename"])
        else:
            actual_sha = hashlib.sha256(open(fpath, "rb").read()).hexdigest()
            if actual_sha != a["sha256"]:
                missing.append(f"{a['original_filename']}(해시 불일치)")
    if missing:
        _safe_rmtree(tmp_dir)
        raise ProjectError(f"자산 파일 누락/손상: {', '.join(missing)}")

    new_project_id = f"{meta['source_doc_id']}-{uuid.uuid4().hex[:8]}"
    final_dir = _project_dir(new_project_id)
    os.rename(tmp_dir, final_dir)
    meta["project_id"] = new_project_id
    _write_json(os.path.join(final_dir, "project.json"), meta)
    return new_project_id


def _safe_rmtree(path):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)


def _write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)  # 원자적 교체 - 쓰는 도중 프로세스가 죽어도 이전 파일이 남음


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
