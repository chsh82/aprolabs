"""④⑦ edition API - SPEC §6 중 4·7단계 완료 기준에 필요한 부분.

의도적으로 안 만든 것(다음 단계 몫):
  - /slots/{path}/candidates, /export - 5·6단계.

실행: uvicorn edition.api:app --port 8000  (momo_b2b_tablet/에서)
/api/runtime/recognize는 RECOGNITION_PROVIDER(기본 anthropic)에 맞는 API 키가
있어야 동작한다 - gemini는 GEMINI_API_KEY, anthropic은 ANTHROPIC_API_KEY.

파트너 세션 인증(2026-09-27, edition/auth.py): 학생용 런타임(/api/runtime/*)은
파트너 세션이 있어야 접근된다 - 파트너 서버가 API 키로 launch 토큰을 받아
학생 태블릿에 launch=토큰으로 넘기면, 태블릿이 그 토큰을 한 번만 세션(httpOnly
쿠키)으로 바꾼다. RUNTIME_AUTH_DISABLED=true면 이 검사를 건너뛴다(진행 중인
손글씨 인식 검증용 임시 우회 - 실서비스 전 반드시 꺼야 함).

검수 로그인(2026-09-28, edition/review_auth.py, 사용자 지시 [5]): 검수 화면
(/review/*)과 편집 API(/api/editions/* - 아래 review_router)는 인터넷에
그대로 열려 있으면 안 되어서 HTTP Basic Auth로 막는다(REVIEW_ACCOUNTS
환경변수). /api/editions/{id}/meta만 예외(momolib 등 파트너가 교재
메타데이터만 조회하는 공개 엔드포인트 - 전문·정답·이미지 지시문 등 민감한
내용은 없음). 인쇄(/print.pdf, /answers)는 review_router 안에 있어 같이 막힌다.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, FastAPI, File, Form, Header, HTTPException, Request, Response, UploadFile
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from pydantic import BaseModel

import jsonpatch

from . import auth, db, freeform_edit, image_gen, notify, presets, print_pdf as print_pdf_mod, recognize as recognize_mod, review_auth, store

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
EXTRACTED_DIR = REPO_ROOT / "momo_book_db" / "extracted_images"
RENDERER_DIR = Path(__file__).resolve().parent.parent / "renderer"
# 2026-09-27 사용자 지시 - 손글씨 인식 정확도 검증(실제 태블릿에서 여러 학년대
# 학생이 쓴 필기를 모아 모델별로 비교) 준비. recognize 호출 시 인식 결과
# 텍스트만 DB(recognition_log)에 남고 원본 PNG는 그냥 버려지고 있었는데,
# tests/compare_recognition_models.py로 나중에 haiku/sonnet/opus를 나란히
# 비교하려면 그 이미지 자체가 있어야 한다. 학생 콘텐츠라 git에는 안 올린다
# (.gitignore에 추가).
HANDWRITING_SAMPLES_DIR = Path(__file__).resolve().parent.parent / "handwriting_samples"

db.init_db()

app = FastAPI(title="momo_b2b_tablet edition API")

if ASSETS_DIR.exists():
    app.mount("/static/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
if EXTRACTED_DIR.exists():
    app.mount("/static/extracted", StaticFiles(directory=str(EXTRACTED_DIR)), name="extracted")
if RENDERER_DIR.exists():
    # 실제 배포에서도 aprolabs가 API와 학생용 웹뷰(/runtime/{edition_id})를 같은
    # 서버에서 직접 호스팅하기로 했었다(호스팅·인증 방향 1번) - 그 모양 그대로
    # viewer.html을 이 앱에 정적으로 얹는다. renderer/adapters/api.js가 실제
    # /api/runtime/{edition_id}에 붙는지 여기서 끝까지 확인한다.
    app.mount("/renderer", StaticFiles(directory=str(RENDERER_DIR), html=True), name="renderer")

REVIEW_DIR = Path(__file__).resolve().parent.parent / "review"
if REVIEW_DIR.exists():
    app.mount("/review", StaticFiles(directory=str(REVIEW_DIR), html=True), name="review")


@app.middleware("http")
async def _require_review_login(request: Request, call_next):
    """2026-09-28 [5] - /review/*는 StaticFiles 마운트라 APIRouter의
    dependencies=[Depends(...)]가 안 통한다(Starlette Mount는 의존성을 못
    받음) - 그래서 여기서 경로로 직접 걸러 Basic Auth를 요구한다."""
    if request.url.path.startswith("/review"):
        if review_auth.check_basic_auth(request) is None:
            return Response(status_code=401, headers={"WWW-Authenticate": "Basic"})
    return await call_next(request)


# 2026-09-28 [5] - 편집 API 전체에 로그인을 요구한다. /api/editions/{id}/meta는
# router 밖(app에 직접)에 둬서 momolib 등 파트너의 공개 메타데이터 조회는
# 그대로 인증 없이 열어둔다(전문·정답 등 민감한 내용은 안 들어있음).
review_router = APIRouter(dependencies=[Depends(review_auth.get_current_editor)])


@app.get("/api/editions/{edition_id}/meta")
def get_edition_meta(edition_id: int):
    """momolib 등 파트너용 공개 메타데이터 - review_router(로그인 필요)의
    get_edition()과 달리 인증이 필요 없다."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    layout = json.loads(row["layout_json"])
    book = layout.get("book", {})
    tone = layout.get("tone", {})
    return {"doc_id": row["doc_id"], "title": book.get("title", ""), "week": book.get("week", ""),
            "band": tone.get("band", ""), "status": row["status"]}


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    """2026-09-28 [5] - 검색엔진 색인 차단(검수 중인 미공개 교재 내용이라)."""
    return "User-agent: *\nDisallow: /\n"


class DraftRequest(BaseModel):
    doc_id: str
    created_by: str | None = None


class PatchRequest(BaseModel):
    patch: list[dict]
    editor: str | None = None
    reason: str | None = None
    expected_rev: int | None = None


class ApproveRequest(BaseModel):
    approved_by: str | None = None


class ResolveFlagRequest(BaseModel):
    resolved_by: str | None = None


class PresetRequest(BaseModel):
    preset: str
    page_idx: int
    params: dict | None = None
    editor: str | None = None
    expected_rev: int | None = None


class FreeformPreviewRequest(BaseModel):
    page_idx: int
    request_text: str


class FreeformApplyRequest(BaseModel):
    page_idx: int
    ops: list[dict]
    summary: str
    expected_rev: int | None = None


class GenerateImagesRequest(BaseModel):
    page_idx: int
    scene: str
    avoid: str = ""
    ratio: str = "1:1"
    count: int = 3
    editor: str | None = None


class ChooseImageRequest(BaseModel):
    candidate_id: int
    editor: str | None = None


class AnswerBody(BaseModel):
    ink: list | None = None
    text: dict | None = None
    ox: str | None = None
    choice: str | list[str] | None = None  # choiceList가 single:false면 배열(복수 선택)


class PartnerSessionRequest(BaseModel):
    partner_student_id: str
    edition_id: int
    # 2026-09-28: momolib 등 파트너가 "학습 목록으로" 버튼이 돌아갈 주소를
    # 여기서 넘긴다 - 없으면(직접 개발용 접근 등) 버튼 자체를 안 보여준다.
    return_url: str | None = None


class SessionExchangeRequest(BaseModel):
    edition_id: int
    launch: str


@review_router.post("/api/editions/draft")
def create_draft(req: DraftRequest, editor_name: str = Depends(review_auth.get_current_editor)):
    try:
        edition_id = store.create_draft(req.doc_id, created_by=editor_name)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    return {"edition_id": edition_id}


@review_router.get("/api/editions")
def list_editions(level: int | None = None, quarter: int | None = None, status: str | None = None,
                   search: str = "", sort: str = "default", page: int = 1, per_page: int = 50):
    """2026-09-28 - 검수 목록 화면(/review/index.html, edition 번호 없이 열면 목록)."""
    return store.list_editions_for_review(level=level, quarter=quarter, status=status,
                                           search=search, sort=sort, page=page, per_page=per_page)


@review_router.get("/api/editions/{edition_id}/neighbors")
def edition_neighbors(edition_id: int):
    """2026-09-28 - 검수 화면의 "같은 레벨·분기 안에서 이전/다음 문서" 버튼용."""
    return store.find_neighbors(edition_id)


@review_router.get("/api/editions/{edition_id}")
def get_edition(edition_id: int):
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    return {"id": row["id"], "doc_id": row["doc_id"], "version": row["version"], "rev": row["rev"],
            "status": row["status"], "layout": json.loads(row["layout_json"])}


@review_router.patch("/api/editions/{edition_id}")
def patch_edition(edition_id: int, req: PatchRequest, editor_name: str = Depends(review_auth.get_current_editor)):
    try:
        new_id = store.patch_edition(edition_id, req.patch, editor=editor_name, reason=req.reason,
                                      expected_rev=req.expected_rev)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    except store.ConflictError as e:
        raise HTTPException(409, str(e)) from e
    except store.PatchError as e:
        raise HTTPException(400, str(e)) from e
    row = store.get_edition_row(new_id)
    return {"id": new_id, "doc_id": row["doc_id"], "version": row["version"], "rev": row["rev"],
            "status": row["status"], "layout": json.loads(row["layout_json"])}


@review_router.post("/api/editions/{edition_id}/preset-preview")
def edition_preset_preview(edition_id: int, req: PresetRequest):
    """SPEC_프롬프트_편집_기능.md 1단계 - 프리셋 버튼 미리보기. 실제로 저장하지
    않고, 지금 layout에 프리셋을 적용하면 어떤 모습이 되는지(layout)와 무엇이
    바뀌는지(summary)만 계산해 돌려준다. 검수 화면이 이 layout으로 렌더러를
    다시 그려 원래 화면과 토글 비교하게 한다."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    layout = json.loads(row["layout_json"])
    try:
        result = presets.compute(layout, req.page_idx, req.preset, req.params)
    except presets.PresetNotApplicable as e:
        raise HTTPException(422, str(e)) from e
    try:
        preview_layout = jsonpatch.JsonPatch(result.ops).apply(layout)
    except jsonpatch.JsonPatchException as e:
        raise HTTPException(400, str(e)) from e
    return {"summary": result.summary, "layout": preview_layout}


@review_router.post("/api/editions/{edition_id}/preset-apply")
def edition_preset_apply(edition_id: int, req: PresetRequest,
                          editor_name: str = Depends(review_auth.get_current_editor)):
    """프리셋을 실제로 적용한다 - 미리보기와 같은 계산을 지금 상태 기준으로
    다시 해서(그 사이 다른 수정이 있었을 수 있으니 미리보기 결과를 그대로
    믿지 않는다) store.patch_edition에 kind="prompt_edit"로 남긴다."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    layout = json.loads(row["layout_json"])
    try:
        result = presets.compute(layout, req.page_idx, req.preset, req.params)
    except presets.PresetNotApplicable as e:
        raise HTTPException(422, str(e)) from e
    label = presets.PRESET_LABELS.get(req.preset, req.preset)
    try:
        new_id = store.patch_edition(edition_id, result.ops, editor=editor_name, reason=f"검수 프리셋: {label}",
                                      expected_rev=req.expected_rev, kind="prompt_edit", summary=result.summary)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    except store.ConflictError as e:
        raise HTTPException(409, str(e)) from e
    except store.PatchError as e:
        raise HTTPException(400, str(e)) from e
    new_row = store.get_edition_row(new_id)
    return {"id": new_id, "doc_id": new_row["doc_id"], "version": new_row["version"], "rev": new_row["rev"],
            "status": new_row["status"], "layout": json.loads(new_row["layout_json"]), "summary": result.summary}


@review_router.post("/api/editions/{edition_id}/freeform-preview")
async def edition_freeform_preview(edition_id: int, req: FreeformPreviewRequest):
    """2026-09-29 [3] - 프리셋으로 안 풀리는 편집을 자연어로 설명하면 LLM이
    JSON Patch를 만든다. preset-preview와 같은 모양(ops를 미리 계산해
    layout까지 적용한 미리보기를 돌려줌 - 실제 저장은 안 함)."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    layout = json.loads(row["layout_json"])
    try:
        page = layout["pages"][req.page_idx]
    except (IndexError, KeyError):
        raise HTTPException(404, f"페이지 {req.page_idx}가 없음") from None
    try:
        result = await freeform_edit.generate_patch(page, req.request_text, req.page_idx)
    except freeform_edit.FreeformEditError as e:
        raise HTTPException(422, str(e)) from e
    try:
        preview_layout = jsonpatch.JsonPatch(result["ops"]).apply(layout)
    except jsonpatch.JsonPatchException as e:
        raise HTTPException(400, str(e)) from e
    return {"ops": result["ops"], "summary": result["summary"], "layout": preview_layout}


@review_router.post("/api/editions/{edition_id}/freeform-apply")
def edition_freeform_apply(edition_id: int, req: FreeformApplyRequest,
                            editor_name: str = Depends(review_auth.get_current_editor)):
    """미리보기에서 이미 계산된 ops를 그대로 적용한다(재호출 시 LLM이 다른
    답을 줄 수 있으니 preset-apply처럼 재계산하지 않고 클라이언트가 미리본
    ops를 그대로 돌려받아 적용) - correction_log에 kind="prompt_edit"로 남음."""
    try:
        new_id = store.patch_edition(edition_id, req.ops, editor=editor_name,
                                      reason=f"검수 자유 편집: {req.summary}",
                                      expected_rev=req.expected_rev, kind="prompt_edit", summary=req.summary)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    except store.ConflictError as e:
        raise HTTPException(409, str(e)) from e
    except store.PatchError as e:
        raise HTTPException(400, str(e)) from e
    new_row = store.get_edition_row(new_id)
    return {"id": new_id, "doc_id": new_row["doc_id"], "version": new_row["version"], "rev": new_row["rev"],
            "status": new_row["status"], "layout": json.loads(new_row["layout_json"])}


@review_router.get("/api/editions/{edition_id}/pages/{page_idx}/image-candidates")
def list_image_candidates(edition_id: int, page_idx: int):
    """2026-09-28 [3] - 화면을 다시 열었을 때 이전에 만든 후보들을 보여준다."""
    slot_path = f"/pages/{page_idx}/slot"
    return {"candidates": store.list_image_candidates(edition_id, slot_path),
            "usage": store.image_usage_summary()}


@review_router.post("/api/editions/{edition_id}/pages/{page_idx}/generate-images")
async def generate_images(edition_id: int, page_idx: int, req: GenerateImagesRequest):
    """2026-09-28 [3] - "후보 만들기": scene+avoid+분기 하우스 스타일로 2~3장
    생성해서 미리보기용 후보로 저장한다(아직 슬롯에 반영 안 됨 - 선택은
    choose-image에서)."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    if not (1 <= req.count <= 3):
        raise HTTPException(400, "count는 1~3")
    if req.ratio not in image_gen.RATIO_TO_WH:
        raise HTTPException(400, f"지원하지 않는 ratio: {req.ratio}")
    try:
        results = await image_gen.generate_candidates(req.scene, req.avoid, row["quarter"], req.ratio, req.count)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except RuntimeError as e:
        raise HTTPException(502, str(e)) from e
    slot_path = f"/pages/{page_idx}/slot"
    candidates = store.save_image_candidates(edition_id, slot_path, results)
    return {"candidates": candidates, "usage": store.image_usage_summary()}


@review_router.post("/api/editions/{edition_id}/pages/{page_idx}/upload-image")
async def upload_page_image(edition_id: int, page_idx: int, file: UploadFile = File(...)):
    """2026-09-29 사용자 지시 - AI 생성/원본 선택 외에 검수자가 가진 이미지
    파일을 직접 올릴 수 있어야 한다는 요청. 저장만 하고 슬롯에 반영은 안
    한다(그대로 PATCH 경로를 태우는 게 choose-image와 같은 패턴) - 클라이언트가
    받은 file_path로 이어서 PATCH /api/editions/{id}를 호출해 슬롯을 바꾼다."""
    data = await file.read()
    try:
        result = image_gen.save_uploaded_image(data)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return result


@review_router.post("/api/editions/{edition_id}/choose-image")
def choose_image(edition_id: int, req: ChooseImageRequest,
                  editor_name: str = Depends(review_auth.get_current_editor)):
    """2026-09-28 [3] - 고른 후보를 실제 슬롯에 반영(PATCH 경로를 그대로 태움)."""
    try:
        new_id = store.choose_image_candidate(edition_id, req.candidate_id, editor=editor_name)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    new_row = store.get_edition_row(new_id)
    return {"id": new_id, "doc_id": new_row["doc_id"], "version": new_row["version"], "rev": new_row["rev"],
            "status": new_row["status"], "layout": json.loads(new_row["layout_json"])}


@review_router.get("/api/editions/{edition_id}/flags")
def list_flags(edition_id: int):
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    return {"flags": store.list_flags(edition_id)}


@review_router.patch("/api/editions/{edition_id}/flags/{flag_id}")
def resolve_flag(edition_id: int, flag_id: int, req: ResolveFlagRequest,
                  editor_name: str = Depends(review_auth.get_current_editor)):
    store.resolve_flag(flag_id, resolved_by=editor_name)
    return {"ok": True}


@review_router.post("/api/editions/{edition_id}/approve")
def approve_edition(edition_id: int, req: ApproveRequest,
                     editor_name: str = Depends(review_auth.get_current_editor)):
    try:
        store.approve(edition_id, approved_by=editor_name)
    except store.ApprovalBlocked as e:
        raise HTTPException(409, str(e)) from e
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    return {"ok": True}


@review_router.post("/api/editions/{edition_id}/publish")
def publish_edition(edition_id: int):
    try:
        store.publish(edition_id)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    except store.PatchError as e:
        raise HTTPException(409, str(e)) from e
    return {"ok": True}


@review_router.get("/api/editions/{edition_id}/images")
def edition_images(edition_id: int):
    """검수 미리보기 전용 - runtime과 달리 included=false 페이지 이미지도 필요하다."""
    images = store.review_images(edition_id)
    if images is None:
        raise HTTPException(404, "edition not found")
    return {"images": images}


@review_router.get("/api/editions/{edition_id}/source/{order_no}")
def edition_source_text(edition_id: int, order_no: int):
    """검수 화면의 "원문 대조" - momo_book_db(읽기 전용) 원문 그대로."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    text = store.source_text(row["doc_id"], order_no)
    if text is None:
        raise HTTPException(404, "source not found")
    return text


@review_router.get("/api/editions/{edition_id}/source-by-page/{page}")
def edition_source_by_page(edition_id: int, page: int):
    """비전(방식 B) 문항 전용 원문 대조 - order_no 직접 대조 대신 같은
    source_page의 DB 행 전부를 후보로 준다(store.source_text_by_page 참고)."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    return {"candidates": store.source_text_by_page(row["doc_id"], page)}


@review_router.get("/api/editions/{edition_id}/available-images")
def edition_available_images(edition_id: int):
    """검수 화면의 "이미지 자리 추가"/"원본 이미지로 바꾸기" - 이 문서의 원본
    이미지 후보 목록(store.available_images 참고). 2026-09-27: 재추출 v2
    이미지까지 합친 목록에 현재 layout에서 이미 쓰인 이미지는 used=true로
    표시해 검수자가 "미사용" 이미지를 먼저 찾을 수 있게 한다."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    layout = json.loads(row["layout_json"])
    used_keys = store._collect_image_keys(layout)
    return {"images": store.available_images(row["doc_id"], used_keys=used_keys)}


@review_router.get("/api/editions/{edition_id}/print-view")
def print_view(edition_id: int):
    """⑥단계 인쇄용 - included=false 페이지 제외 + 홀수 쪽수면 마지막에 빈 면(store.print_view)."""
    view = store.print_view(edition_id)
    if view is None:
        raise HTTPException(404, "edition not found")
    return view


@review_router.get("/api/editions/{edition_id}/answers")
def get_answers(edition_id: int, student_id: str = "dev-anonymous"):
    """⑥단계 "학생 필기 포함" 인쇄, renderer/adapters/print.js 전용."""
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    return store.load_answers(edition_id, student_id)


@review_router.get("/api/editions/{edition_id}/print.pdf")
async def print_pdf(edition_id: int, request: Request, ink: bool = False, student_id: str = "dev-anonymous"):
    """⑥단계: A4 2-up PDF. ?ink=true&student_id=... 로 특정 학생 필기 포함/미포함 선택."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    base = str(request.base_url).rstrip("/")
    url = (f"{base}/renderer/viewer.html?doc={row['doc_id']}&mode=student&adapter=print"
           f"&edition={edition_id}&ink={'true' if ink else 'false'}&student={student_id}")
    try:
        pdf_bytes = await print_pdf_mod.render_pdf(url)
    except PlaywrightTimeoutError as e:
        raise HTTPException(504, f"인쇄 렌더링 시간 초과: {e}") from e
    filename = f"{row['doc_id']}-edition{edition_id}{'-with-ink' if ink else ''}.pdf"
    return Response(content=pdf_bytes, media_type="application/pdf",
                     headers={"Content-Disposition": f'inline; filename="{filename}"'})


app.include_router(review_router)


@app.get("/api/partner/editions")
def list_partner_editions(x_partner_key: str | None = Header(default=None)):
    """2026-09-28 - momolib 등 파트너의 "교재 고르기" 관리 화면이 부른다.
    승인(approved)·발행(published) 된 것만 노출(draft는 아직 검수 중이라
    학생에게 배정할 대상이 아님)."""
    partner_id = auth.verify_partner_key(x_partner_key or "")
    if partner_id is None:
        raise HTTPException(401, "invalid partner key")
    return {"editions": store.list_editions_by_status(["approved", "published"])}


@app.post("/api/partner/sessions")
def create_partner_session(req: PartnerSessionRequest, x_partner_key: str | None = Header(default=None)):
    """파트너 서버가 부른다(태블릿 앱이 아니라) - API 키로 학생 1명·edition 1개
    짜리 일회용 launch 토큰을 받는다. 토큰은 몇 분 안에 /exchange로 바꿔야 하고,
    한 번 바꾸면 재사용 못 한다."""
    partner_id = auth.verify_partner_key(x_partner_key or "")
    if partner_id is None:
        raise HTTPException(401, "invalid partner key")
    if store.get_edition_row(req.edition_id) is None:
        raise HTTPException(404, "edition not found")
    token, expires_in = auth.create_launch_token(partner_id, req.partner_student_id, req.edition_id,
                                                  return_url=req.return_url)
    return {"launch_token": token, "expires_in": expires_in}


@app.post("/api/partner/sessions/exchange")
def exchange_partner_session(req: SessionExchangeRequest, response: Response):
    """태블릿 웹뷰가 launch 토큰을 세션(httpOnly 쿠키)으로 바꾼다 - 성공하면
    한 번만 성공하고, 재사용·만료·edition 불일치는 전부 401."""
    try:
        session_id, expires_in = auth.exchange_launch_token(req.launch, req.edition_id)
    except auth.TokenError as e:
        raise HTTPException(401, str(e)) from e
    # secure=False: 지금은 LAN 안에서 http로 태블릿 테스트를 하므로 - 실제
    # 배포에서 https 뒤에 서비스되면 secure=True로 바꿔야 한다.
    response.set_cookie(auth.SESSION_COOKIE_NAME, session_id, max_age=expires_in,
                         httponly=True, samesite="lax", secure=False)
    return {"ok": True}


def _require_session(request: Request, claimed_edition_id: int) -> str:
    """학생 런타임 엔드포인트 공통 인증 - partner_student_id를 돌려주거나
    401을 던진다. RUNTIME_AUTH_DISABLED=true면(진행 중인 손글씨 인식 검증용
    임시 우회) 예전처럼 쿼리 파라미터 student_id(없으면 dev-anonymous)만 보고
    통과시킨다 - 실서비스 전 반드시 꺼야 한다."""
    if auth.dev_auth_disabled():
        return request.query_params.get("student_id") or "dev-anonymous"
    session_id = request.cookies.get(auth.SESSION_COOKIE_NAME)
    if not session_id:
        raise HTTPException(401, "no session")
    info = auth.get_session(session_id)
    if info is None:
        raise HTTPException(401, "session invalid or expired")
    if info["edition_id"] != claimed_edition_id:
        raise HTTPException(401, "session not valid for this edition")
    return info["partner_student_id"]


def _session_return_url(request: Request) -> str | None:
    """2026-09-28 - "학습 목록으로" 버튼 주소. _require_session이 이미 세션
    유효성(edition 일치 등)을 확인했으니 여기서는 return_url 필드만 더
    조회한다. RUNTIME_AUTH_DISABLED(파트너 세션 자체가 없는 개발 경로)면
    None - 버튼이 renderer.js에서 자동으로 숨겨진다."""
    if auth.dev_auth_disabled():
        return None
    session_id = request.cookies.get(auth.SESSION_COOKIE_NAME)
    info = auth.get_session(session_id) if session_id else None
    return info["return_url"] if info else None


@app.get("/api/runtime/{edition_id}")
def runtime_view(edition_id: int, request: Request):
    _require_session(request, edition_id)
    view = store.runtime_view(edition_id)
    if view is None:
        raise HTTPException(404, "edition not found")
    view["return_url"] = _session_return_url(request)
    return view


@app.put("/api/runtime/{edition_id}/answers/{part_id}")
def save_answer(edition_id: int, part_id: str, body: AnswerBody, request: Request):
    student_id = _require_session(request, edition_id)
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    store.save_answer(edition_id, part_id, student_id, ink=body.ink, text=body.text, ox=body.ox, choice=body.choice)
    return {"ok": True}


@app.post("/api/runtime/{edition_id}/notify-progress")
async def notify_progress_endpoint(edition_id: int, request: Request):
    """2026-09-28 - 학생이 "학습 목록으로"를 누르는 순간 renderer.js가 부른다.
    파트너(momolib 등)에게 진행 요약만 서버 간으로 보낸다(브라우저는 이
    엔드포인트만 호출하고, 실제 파트너 웹훅 호출은 이 서버가 대신 함 -
    파트너 쪽 notify_secret이 브라우저에 노출되지 않게). 통지 실패해도
    학생은 그대로 return_url로 이동해야 하므로 항상 200을 준다(ok 필드로만
    성공 여부 표시)."""
    student_id = _require_session(request, edition_id)
    if auth.dev_auth_disabled():
        return {"ok": False, "reason": "dev_auth_disabled - 파트너 세션이 없어 통지 대상 없음"}
    session_id = request.cookies.get(auth.SESSION_COOKIE_NAME)
    info = auth.get_session(session_id)
    if info is None:
        raise HTTPException(401, "session invalid or expired")
    ok = await notify.notify_partner_progress(info["partner_id"], student_id, edition_id)
    return {"ok": ok}


@app.post("/api/runtime/recognize")
async def recognize_handwriting(
    request: Request,
    image: UploadFile = File(...),
    prompt: str = Form(...),
    part_id: str = Form(...),
    edition_id: int = Form(...),
    model: str | None = Form(None),
    provider: str | None = Form(None),
):
    student_id = _require_session(request, edition_id)
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    image_bytes = await image.read()
    try:
        result = await recognize_mod.recognize_handwriting(image_bytes, prompt, model=model, provider=provider)
    except recognize_mod.RecognitionError as e:
        status = {"rate_limited": 429, "upstream_error": 502}.get(e.code, 422)
        raise HTTPException(status, detail=e.code) from e
    store.log_recognition(edition_id, student_id, part_id, result["provider"], result["model"], prompt,
                           result["text"], result["unclear"], result["latency_ms"])
    _save_handwriting_sample(edition_id, part_id, student_id, result["provider"], result["model"], image_bytes)
    return {"text": result["text"], "unclear": result["unclear"]}


def _save_handwriting_sample(edition_id: int, part_id: str, student_id: str, provider: str, model: str,
                              image_bytes: bytes) -> None:
    safe_part = part_id.replace("/", "_")
    safe_student = student_id.replace("/", "_")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    out_dir = HANDWRITING_SAMPLES_DIR / str(edition_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{safe_student}_{safe_part}_{provider}-{model}_{ts}.png").write_bytes(image_bytes)


@app.on_event("shutdown")
async def _shutdown():
    await print_pdf_mod.shutdown()
