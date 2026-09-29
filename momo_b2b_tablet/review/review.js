/* 5단계 검수·편집 페이지 - 사용자 지시(2026-09-23) "3단: 좌 페이지 목록 / 중앙 렌더러
 * 미리보기(reviewer 모드) / 우 인스펙터". 우선순위: 플래그 큐(1) > 텍스트+원문대조(2) >
 * included 토글(3) > form 변경(4) > 줄 수·순서(5, 이번엔 자리만) > 이미지 슬롯 지시문(6) >
 * 분기색·인쇄 미리보기·승인(7).
 *
 * 편집은 전부 PATCH(JSON Patch)로 보내고, 성공하면 즉시 전체를 다시 불러와 미리보기
 * iframe을 새로고침한다. rev로 낙관적 잠금을 건다(다른 사람이 먼저 고치면 409).
 */

const qs = new URLSearchParams(location.search);
let editionId = qs.get("edition");

// 2026-09-29 발견한 문제 - approved/published된 edition을 계속 그 id로 편집하면
// store.patch_edition이 매번 새 버전(edition_id가 다른 새 row)을 만드는데(SPEC §5
// "확정된 edition의 layout_json은 수정 불가"), 화면은 URL의 옛 edition_id를 계속
// 써서 다음 편집이 "이번에 막 만든 새 버전"이 아니라 "원래(승인된) 버전"에서 또
// 새로 포크됐다 - 편집이 쌓이지 않고 매번 따로 버려지는 버전만 늘어났다. 응답의
// id가 지금 쓰던 것과 다르면 그 새 id를 이어서 쓰게 전환한다.
function adoptEditionId(newId) {
  if (newId == null || String(newId) === String(editionId)) return false;
  editionId = newId;
  state.editionId = newId;
  const url = new URL(location.href);
  url.searchParams.set("edition", String(editionId));
  history.replaceState(null, "", url);
  toast(`승인/공개된 버전이라 새 버전(edition ${editionId})을 만들어 이어서 편집합니다. 이 창을 계속 쓰세요.`, { sticky: true });
  return true;
}

const $ = sel => document.querySelector(sel);
const el = (tag, attrs = {}, children = []) => {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") e.className = v;
    else if (k === "html") e.innerHTML = v;
    else if (k.startsWith("on") && typeof v === "function") e.addEventListener(k.slice(2), v);
    else if (v !== false && v !== null && v !== undefined) e.setAttribute(k, v);
  }
  for (const c of [].concat(children)) if (c != null) e.append(c);
  return e;
};

const state = {
  editionId, docId: null, layout: null, rev: null, status: null,
  flags: [], flagFilter: "all", showResolved: false, selectedPageIdx: null,
  presetPreview: null,
};

let toastTimer = 0;
// sticky:true - 프리셋/자유 편집 실패 메시지처럼 LLM이 왜 안 되는지 한두 문장으로
// 설명하는 긴 안내문은 기존 2.6초 자동 소멸로는 읽기도 전에 사라졌다(2026-09-29
// 사용자 피드백) - 닫기 버튼으로 직접 닫을 때까지 남아있게 한다.
function toast(msg, { sticky = false } = {}) {
  const t = $("#toast");
  t.innerHTML = "";
  t.append(document.createTextNode(msg));
  t.classList.toggle("sticky", sticky);
  if (sticky) {
    const closeBtn = el("button", { class: "toast-close", type: "button", "aria-label": "닫기" }, "×");
    closeBtn.onclick = () => t.classList.remove("show");
    t.append(closeBtn);
  }
  t.classList.add("show");
  clearTimeout(toastTimer);
  if (!sticky) {
    toastTimer = setTimeout(() => t.classList.remove("show"), 2600);
  }
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
  });
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (e) {}
    const err = new Error(detail);
    err.status = res.status;
    throw err;
  }
  return res.status === 204 ? null : res.json();
}

async function loadEdition() {
  const data = await api(`/api/editions/${editionId}`);
  state.docId = data.doc_id; state.layout = data.layout; state.rev = data.rev; state.status = data.status;
}

async function loadFlags() {
  const data = await api(`/api/editions/${editionId}/flags`);
  state.flags = data.flags;
}

async function refreshAll({ keepPreview = false } = {}) {
  await Promise.all([loadEdition(), loadFlags()]);
  renderTopbar();
  renderFlagQueue();
  renderPageList();
  renderInspector();
  renderToneLines();
  if (!keepPreview) refreshPreviewFrame();
}

/* ============ PATCH ============ */
// 2026-09-28 [6] 외부 작업 환경 - 필드가 blur될 때만 저장되는데, 그 순간
// 연결이 끊기면 토스트만 뜨고 아무 경고 없이 화면을 닫으면 그 편집 내용이
// 그대로 사라졌다. unsavedFailures가 0보다 크면(=저장 시도가 실패한 채
// 남아있으면) 창을 닫거나 새로고침할 때 브라우저 기본 확인창으로 막는다.
let unsavedFailures = 0;
window.addEventListener("beforeunload", e => {
  if (unsavedFailures > 0) { e.preventDefault(); e.returnValue = ""; }
});

async function patch(ops, { reason } = {}) {
  if (!ops.length) return;
  try {
    const data = await api(`/api/editions/${editionId}`, {
      method: "PATCH",
      body: JSON.stringify({ patch: ops, editor: "reviewer", reason, expected_rev: state.rev }),
    });
    const forked = adoptEditionId(data.id);
    state.layout = data.layout; state.rev = data.rev; state.status = data.status;
    await refreshAll();
    if (!forked) toast("저장했습니다.");
    unsavedFailures = 0;
    return true;
  } catch (e) {
    if (e.status === 409) {
      $("#conflictBanner").hidden = false;
      await refreshAll();
      setTimeout(() => { $("#conflictBanner").hidden = true; }, 4000);
      return false;
    }
    unsavedFailures++;
    toast(`저장 실패: ${e.message} - 창을 닫지 말고 다시 시도하세요.`);
    throw e;
  }
}

/* ============ 상단바 ============ */
const QUARTER_NAME = { winter: "겨울 고전", spring: "봄 탐구", summer: "여름 문학", autumn: "가을 인문" };
const STATUS_LABEL = { draft: "초안", review: "검토중", approved: "승인됨", published: "공개됨" };

function renderTopbar() {
  const b = state.layout.book;
  $("#docTitle").textContent = `${b.title} · ${state.docId}`;
  const statusBadge = $("#statusBadge");
  statusBadge.textContent = STATUS_LABEL[state.status] || state.status;
  statusBadge.dataset.status = state.status;
  $("#revLabel").textContent = `rev ${state.rev}`;

  document.querySelectorAll("#quarterPreview [data-q]").forEach(b2 => {
    b2.setAttribute("aria-pressed", b2.dataset.q === state.layout.quarter);
  });

  const unresolvedPlaceholders = state.flags.filter(f => f.kind === "placeholder" && !f.resolved_at);
  const approveBtn = $("#btnApprove");
  const reasonBox = $("#approveReason");
  if (state.status === "draft" || state.status === "review") {
    if (unresolvedPlaceholders.length) {
      approveBtn.disabled = true;
      reasonBox.hidden = false;
      reasonBox.textContent = `placeholder 플래그 ${unresolvedPlaceholders.length}건이 안 풀려서 승인할 수 없습니다: `
        + unresolvedPlaceholders.slice(0, 3).map(f => f.message).join(" / ")
        + (unresolvedPlaceholders.length > 3 ? " 등" : "");
    } else {
      approveBtn.disabled = false;
      reasonBox.hidden = true;
    }
  } else {
    approveBtn.disabled = true;
    reasonBox.hidden = true;
  }
  $("#btnPublish").disabled = state.status !== "approved";
}

$("#quarterPreview").addEventListener("click", e => {
  const btn = e.target.closest("[data-q]");
  if (!btn) return;
  const frame = $("#previewFrame").contentWindow;
  if (frame && frame.__viewer) frame.__viewer.applyPalette(btn.dataset.q);
});

$("#btnPrintPreview").addEventListener("click", () => {
  const frame = $("#previewFrame").contentWindow;
  if (!frame || !frame.__viewer) return;
  frame.__viewer.printPrep();
  frame.print();
});

$("#btnApprove").addEventListener("click", async () => {
  try {
    await api(`/api/editions/${editionId}/approve`, { method: "POST", body: JSON.stringify({ approved_by: "reviewer" }) });
    toast("승인했습니다.");
    await refreshAll({ keepPreview: true });
  } catch (e) {
    toast(`승인 실패: ${e.message}`);
  }
});

$("#btnPublish").addEventListener("click", async () => {
  try {
    await api(`/api/editions/${editionId}/publish`, { method: "POST" });
    toast("공개했습니다.");
    await refreshAll({ keepPreview: true });
  } catch (e) {
    toast(`공개 실패: ${e.message}`);
  }
});

/* ============ 플래그 큐(1순위) ============ */
// page_split(2026-09-26): 페이지 경계로 갈라져 버려진/합쳐진 문항 - 내용
// 손실 가능성이 있어 placeholder보다도 먼저 확인해야 한다는 사용자 지시.
const FLAG_PRIORITY = ["page_split", "placeholder", "widget_unavailable"]; // 승인을 막는 것·품질에 영향 큰 것을 맨 위로

function flagKinds() {
  const ks = [...new Set(state.flags.map(f => f.kind))];
  ks.sort((a, b) => {
    const pa = FLAG_PRIORITY.indexOf(a), pb = FLAG_PRIORITY.indexOf(b);
    if (pa !== -1 || pb !== -1) return (pa === -1 ? 99 : pa) - (pb === -1 ? 99 : pb);
    return a.localeCompare(b);
  });
  return ks;
}

function visibleFlags() {
  return state.flags
    .filter(f => state.flagFilter === "all" || f.kind === state.flagFilter)
    .filter(f => state.showResolved || !f.resolved_at)
    .sort((a, b) => {
      const pa = FLAG_PRIORITY.indexOf(a.kind), pb = FLAG_PRIORITY.indexOf(b.kind);
      return (pa === -1 ? 99 : pa) - (pb === -1 ? 99 : pb);
    });
}

function orderLabelForPageIdx(idx) {
  const p = state.layout.pages[idx];
  return p && p.q ? p.q.id : null;
}

function renderFlagQueue() {
  const filters = $("#flagFilters");
  filters.innerHTML = "";
  const total = state.flags.length;
  const unresolved = state.flags.filter(f => !f.resolved_at).length;
  const allBtn = el("button", {
    class: "flag-filter", "data-kind": "all", "aria-pressed": String(state.flagFilter === "all"),
    onclick: () => { state.flagFilter = "all"; renderFlagQueue(); },
  }, `전체 ${total}`);
  filters.append(allBtn);
  for (const kind of flagKinds()) {
    const count = state.flags.filter(f => f.kind === kind && !f.resolved_at).length;
    filters.append(el("button", {
      class: "flag-filter", "data-kind": kind, "aria-pressed": String(state.flagFilter === kind),
      onclick: () => { state.flagFilter = kind; renderFlagQueue(); },
    }, `${kind} ${count}`));
  }

  $("#flagProgress").textContent = `해결 ${total - unresolved} / 전체 ${total}`;

  const list = $("#flagList");
  list.innerHTML = "";
  const items = visibleFlags();
  if (!items.length) {
    list.append(el("li", { class: "flag-empty" }, state.showResolved ? "표시할 플래그가 없습니다." : "미해결 플래그가 없습니다. 🎉"));
  }
  for (const f of items) {
    const label = f.page_idx != null ? orderLabelForPageIdx(f.page_idx) : null;
    const item = el("li", { class: "flag-item" + (f.resolved_at ? " resolved" : ""), "data-kind": f.kind }, [
      el("span", { class: "kind" }, f.kind + (label ? ` · 문항 ${label}` : "")),
      el("span", { class: "msg" }, f.message),
      el("div", { class: "row" }, [
        f.page_idx != null ? el("button", {
          class: "btn btn--small", onclick: () => selectPage(f.page_idx),
        }, "이 페이지 보기") : null,
        !f.resolved_at ? el("button", {
          class: "btn btn--small btn--primary", onclick: () => resolveFlag(f.id),
        }, "해결 표시") : el("span", { class: "hint" }, "해결됨"),
      ]),
    ]);
    list.append(item);
  }
}

async function resolveFlag(flagId) {
  await api(`/api/editions/${editionId}/flags/${flagId}`, {
    method: "PATCH", body: JSON.stringify({ resolved_by: "reviewer" }),
  });
  await refreshAll({ keepPreview: true });
  toast("플래그를 해결 표시했습니다.");
}

const showResolvedToggle = el("label", { class: "field-check", style: "margin:6px 0 0" }, [
  el("input", { type: "checkbox", onchange: e => { state.showResolved = e.target.checked; renderFlagQueue(); } }),
  " 해결된 항목도 보기",
]);
document.querySelector(".flag-queue").append(showResolvedToggle);

/* ============ 페이지 목록 ============ */
const TYPE_ABBR = {
  cover: "COV", vocab: "VOC", vocabMatch: "VOM", vocabFill: "VOF", oxp: "OXP", draw: "DRW", bgline: "BGL", bgtext: "BGT",
  qa: "QA", qaband: "QAB", qaref: "QAR", solo: "SOL", essay: "ESS", memos: "MEM", excerpt: "EXC",
};
// 렌더러가 slot을 실제로 그리는 페이지 유형만 "이미지 자리" UI를 보여준다(2026-09-26 [5순위]).
const SLOT_CAPABLE_TYPES = new Set(["qa", "qaband", "qaref", "solo", "oxp", "bgtext", "essay", "memos"]);

function renderPageList() {
  const list = $("#pageList");
  list.innerHTML = "";
  state.layout.pages.forEach((p, idx) => {
    const flagCount = state.flags.filter(f => f.page_idx === idx && !f.resolved_at).length;
    const critical = state.flags.some(f => f.page_idx === idx && !f.resolved_at && FLAG_PRIORITY.includes(f.kind));
    const item = el("li", {
      class: "page-item" + (idx === state.selectedPageIdx ? " selected" : "") + (p.included === false ? " excluded" : ""),
      onclick: () => selectPage(idx),
    }, [
      el("span", { class: "thumb" }, TYPE_ABBR[p.type] || p.type.slice(0, 3).toUpperCase()),
      el("span", { class: "info" }, [
        el("span", { class: "t" }, `${idx + 1}. ${p.title || p.type}${p.q ? ` (${p.q.id})` : ""}`),
        el("span", { class: "s" }, p.step || ""),
      ]),
      flagCount ? el("span", { class: "flagcount" + (critical ? " has-critical" : "") }, String(flagCount)) : null,
      el("span", { class: "reorder" }, [
        el("button", {
          class: "btn btn--small", title: "위로 이동", disabled: idx === 0 ? "disabled" : false,
          onclick: e => { e.stopPropagation(); movePage(idx, -1); },
        }, "▲"),
        el("button", {
          class: "btn btn--small", title: "아래로 이동", disabled: idx === state.layout.pages.length - 1 ? "disabled" : false,
          onclick: e => { e.stopPropagation(); movePage(idx, 1); },
        }, "▼"),
      ]),
      el("input", {
        type: "checkbox", title: "싣기(included)", checked: p.included !== false ? "checked" : false,
        onclick: e => e.stopPropagation(),
        onchange: e => patch([{ op: "replace", path: `/pages/${idx}/included`, value: e.target.checked }],
          { reason: e.target.checked ? "검수: 문항 다시 포함" : "검수: 이 문항 싣지 않음" }),
      }),
    ]);
    list.append(item);
  });
}

async function movePage(idx, dir) {
  const target = idx + dir;
  if (target < 0 || target >= state.layout.pages.length) return;
  const wasSelected = state.selectedPageIdx === idx;
  await patch([{ op: "move", from: `/pages/${idx}`, path: `/pages/${target}` }],
    { reason: `검수: 페이지 순서 이동(${idx + 1} -> ${target + 1})` });
  if (wasSelected) { state.selectedPageIdx = target; renderPageList(); renderInspector(); goToPreviewPage(target); }
}

function selectPage(idx) {
  if (state.presetPreview) { cancelPresetPreview(); }
  state.selectedPageIdx = idx;
  renderPageList();
  renderInspector();
  goToPreviewPage(idx);
}

/* ============ 중앙 미리보기(iframe) ============ */
function previewSrc(previewKey) {
  const base = `/renderer/viewer.html?doc=${encodeURIComponent(state.docId)}&mode=review&adapter=review&edition=${editionId}&_=${Date.now()}`;
  return previewKey ? `${base}&previewKey=${encodeURIComponent(previewKey)}` : base;
}

async function waitForViewer(frameEl, timeoutMs = 4000) {
  // frame.contentWindow는 src를 바꿔 새 문서로 내비게이션하면 새 Window 객체로
  // 바뀐다 - 한 번 읽어서 들고 있으면 옛 문서를 계속 들여다보게 되므로(끝내
  // __viewerReady가 안 됨) 매번 frameEl.contentWindow를 다시 읽어야 한다.
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const win = frameEl.contentWindow;
    if (win && win.__viewerReady && win.__viewer) return win.__viewer;
    await new Promise(r => setTimeout(r, 60));
  }
  return null;
}

async function refreshPreviewFrame() {
  const frame = $("#previewFrame");
  // loadFrameWithSrc와 같은 이유(경쟁 상태) - src 교체 직후엔 frame.contentWindow가
  // 잠깐 옛 문서를 가리켜 go()가 새 문서에 반영 안 될 수 있다. load를 먼저 기다린다.
  await new Promise(resolve => {
    frame.addEventListener("load", resolve, { once: true });
    frame.src = previewSrc();
  });
  const viewer = await waitForViewer(frame);
  if (viewer && state.selectedPageIdx != null) viewer.go(state.selectedPageIdx);
}

async function goToPreviewPage(idx) {
  const frame = $("#previewFrame");
  const win = frame.contentWindow;
  if (win && win.__viewer) { win.__viewer.go(idx); return; }
  const viewer = await waitForViewer(frame);
  if (viewer) viewer.go(idx);
}

/* ============ 검수 프리셋(SPEC_프롬프트_편집_기능.md 1단계, 2026-09-27) ============
 * 흐름: 버튼 클릭 -> preset-preview로 계산만 해서 미리보기 iframe에 즉시 보여줌
 * (원래 화면과 토글 비교 가능) -> 적용(실제 저장, kind=prompt_edit) 또는 취소
 * (sessionStorage만 지우고 아무것도 저장 안 함). */
const PRESET_DEFS = [
  { key: "mirror", label: "좌우 바꾸기", types: new Set(["qa", "qaband", "qaref", "solo", "essay"]) },
  { key: "title_top", label: "제목 중앙 상단", types: new Set(["qa", "qaband", "qaref", "solo"]) },
  { key: "three_tier", label: "3층 구조/전체 폭", types: new Set(["qa", "qaband", "qaref", "solo"]) },
  { key: "lines_more", label: "답란 늘리기" },
  { key: "lines_less", label: "답란 줄이기" },
  { key: "add_image_slot", label: "이미지 자리 추가" },
  { key: "table_header", label: "표 머리칸 넣기" },
  { key: "merge_pages", label: "페이지 합치기" },
  { key: "split_page", label: "페이지 나누기" },
];

const STEP_VALUES = ["STEP 1", "STEP 2", "STEP 3"];

function renderPresetPanel(body, page, idx) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "프리셋 버튼"));
  body.append(el("p", { class: "hint" }, "LLM 없이 바로 적용되는 고정 패치입니다. 눌러도 바로 저장되지 않고 먼저 미리보기로 보여줍니다."));

  if ("step" in page) {
    // 2026-09-27 사용자 지시 [A] - 값이 셋뿐이라 드롭다운(버튼형 선택)이 적합.
    const select = el("select", {}, STEP_VALUES.map(v =>
      el("option", { value: v, selected: v === page.step ? "selected" : false }, v)));
    body.append(field("STEP 단계 바꾸기", select));
    select.onchange = () => {
      if (select.value !== page.step) openPresetPreview(idx, "set_step", "STEP 단계 바꾸기", { step: select.value });
    };
  }

  const grid = el("div", { class: "preset-grid" });
  PRESET_DEFS.forEach(def => {
    if (def.types && !def.types.has(page.type)) return;
    const btn = el("button", { class: "btn btn--small" }, def.label);
    btn.onclick = () => openPresetPreview(idx, def.key, def.label);
    grid.append(btn);
  });
  body.append(grid);

  // 2026-09-29 [3] 사용자 지시 - 프리셋으로 안 풀리는 편집을 자연어로 설명하면
  // LLM(claude-sonnet-5)이 JSON Patch를 만든다. 프리셋과 같은 미리보기 흐름을 탄다.
  body.append(el("h3", { style: "margin-top:14px" }, "자유 편집 요청"));
  body.append(el("p", { class: "hint" }, "프리셋으로 안 되는 편집을 문장으로 설명해보세요. 미리보기로 먼저 보여줍니다."));
  const freeInput = el("textarea", { rows: "2", placeholder: "예: 답란을 조금 더 넓게 써줘" });
  const freeBtn = el("button", { class: "btn btn--small" }, "요청 반영해보기");
  freeBtn.onclick = async () => {
    const text = freeInput.value.trim();
    if (!text) return;
    freeBtn.disabled = true;
    freeBtn.textContent = "생각하는 중…";
    try {
      await openFreeformPreview(idx, text);
      freeInput.value = "";
    } finally {
      freeBtn.disabled = false;
      freeBtn.textContent = "요청 반영해보기";
    }
  };
  body.append(el("div", { class: "field" }, [freeInput, freeBtn]));
}

async function loadFrameWithSrc(src, pageIdx) {
  const frame = $("#previewFrame");
  // frame.src를 바꾼 직후엔 frame.contentWindow가 아직 "옛 문서"를 가리킬 수
  // 있어(내비게이션은 비동기) waitForViewer가 그 옛 문서의 __viewerReady를
  // 그대로 보고 즉시 반환해 버리는 경쟁 상태가 있었다(그 옛 문서는 곧 사라져
  // go(pageIdx)가 새 문서에 반영 안 됨 - 프리셋 미리보기가 항상 1쪽만 보이던
  // 원인). load 이벤트로 "새 문서로 내비게이션이 실제로 끝남"을 먼저 확인한
  // 뒤에 폴링을 시작한다.
  await new Promise(resolve => {
    frame.addEventListener("load", resolve, { once: true });
    frame.src = src;
  });
  const viewer = await waitForViewer(frame);
  if (viewer && pageIdx != null) viewer.go(pageIdx);
}

function renderPresetPreviewBar() {
  const bar = $("#presetPreviewBar");
  const pp = state.presetPreview;
  if (!pp) { bar.hidden = true; return; }
  bar.hidden = false;
  $("#presetPreviewSummary").textContent = `[${pp.label}] ${pp.summary}`;
  $("#presetPreviewToggle").textContent = pp.showingPreview ? "원래 화면 보기" : "미리보기 다시 보기";
}

async function openPresetPreview(pageIdx, presetKey, label, params) {
  try {
    const res = await api(`/api/editions/${editionId}/preset-preview`, {
      method: "POST",
      body: JSON.stringify({ preset: presetKey, page_idx: pageIdx, params: params || null }),
    });
    const key = `preset-preview:${editionId}`;
    sessionStorage.setItem(key, JSON.stringify(res.layout));
    state.presetPreview = { kind: "preset", pageIdx, preset: presetKey, label, params, summary: res.summary, key, showingPreview: true };
    renderPresetPreviewBar();
    await loadFrameWithSrc(previewSrc(key), pageIdx);
  } catch (e) {
    toast(`적용 불가: ${e.message}`, { sticky: true });
  }
}

// 2026-09-29 [3] - 프리셋과 같은 미리보기 흐름을 자유 편집 요청(LLM)에도 그대로 쓴다.
async function openFreeformPreview(pageIdx, requestText) {
  try {
    const res = await api(`/api/editions/${editionId}/freeform-preview`, {
      method: "POST",
      body: JSON.stringify({ page_idx: pageIdx, request_text: requestText }),
    });
    const key = `preset-preview:${editionId}`;
    sessionStorage.setItem(key, JSON.stringify(res.layout));
    state.presetPreview = { kind: "freeform", pageIdx, label: "자유 편집", ops: res.ops, summary: res.summary, key, showingPreview: true };
    renderPresetPreviewBar();
    await loadFrameWithSrc(previewSrc(key), pageIdx);
  } catch (e) {
    toast(`적용 불가: ${e.message}`, { sticky: true });
  }
}

async function togglePresetPreviewView() {
  const pp = state.presetPreview;
  if (!pp) return;
  pp.showingPreview = !pp.showingPreview;
  renderPresetPreviewBar();
  await loadFrameWithSrc(pp.showingPreview ? previewSrc(pp.key) : previewSrc(), pp.pageIdx);
}

async function applyPresetPreview() {
  const pp = state.presetPreview;
  if (!pp) return;
  try {
    const data = pp.kind === "freeform"
      ? await api(`/api/editions/${editionId}/freeform-apply`, {
          method: "POST",
          body: JSON.stringify({ page_idx: pp.pageIdx, ops: pp.ops, summary: pp.summary, expected_rev: state.rev }),
        })
      : await api(`/api/editions/${editionId}/preset-apply`, {
          method: "POST",
          body: JSON.stringify({ preset: pp.preset, page_idx: pp.pageIdx, params: pp.params || null, editor: "reviewer", expected_rev: state.rev }),
        });
    const forked = adoptEditionId(data.id);
    state.layout = data.layout; state.rev = data.rev; state.status = data.status;
    sessionStorage.removeItem(pp.key);
    state.presetPreview = null;
    renderPresetPreviewBar();
    await refreshAll();
    if (!forked) toast(`적용했습니다: ${pp.label}`);
  } catch (e) {
    if (e.status === 409) {
      sessionStorage.removeItem(pp.key);
      state.presetPreview = null;
      renderPresetPreviewBar();
      $("#conflictBanner").hidden = false;
      await refreshAll();
      setTimeout(() => { $("#conflictBanner").hidden = true; }, 4000);
      return;
    }
    toast(`적용 실패: ${e.message}`, { sticky: true });
  }
}

function cancelPresetPreview() {
  const pp = state.presetPreview;
  if (!pp) return;
  sessionStorage.removeItem(pp.key);
  state.presetPreview = null;
  renderPresetPreviewBar();
  refreshPreviewFrame();
  toast("취소했습니다. 저장된 것은 없습니다.");
}

$("#presetPreviewToggle").onclick = togglePresetPreviewView;
$("#presetPreviewApply").onclick = applyPresetPreview;
$("#presetPreviewCancel").onclick = cancelPresetPreview;

/* ============ 인스펙터(우) ============ */
const Q_FIELDS = ["form", "kind", "starter", "blanks", "rows", "items", "cards", "rowKind", "n", "img", "options", "single"];

function diffQPatch(oldQ, newQ, basePath) {
  const ops = [];
  for (const key of Q_FIELDS) {
    const had = Object.prototype.hasOwnProperty.call(oldQ, key);
    const rawHas = Object.prototype.hasOwnProperty.call(newQ, key);
    const has = rawHas && newQ[key] !== undefined && newQ[key] !== "";
    if (has && !had) ops.push({ op: "add", path: `${basePath}/${key}`, value: newQ[key] });
    else if (!has && had) ops.push({ op: "remove", path: `${basePath}/${key}` });
    else if (has && had && JSON.stringify(oldQ[key]) !== JSON.stringify(newQ[key])) {
      ops.push({ op: "replace", path: `${basePath}/${key}`, value: newQ[key] });
    }
  }
  return ops;
}

function currentFormValue(q) {
  if (q.form) return q.form;
  if (q.kind === "multi" && q.blanks) return "blanks";
  return "single";
}

async function fetchSourceText(orderNo) {
  try { return await api(`/api/editions/${editionId}/source/${orderNo}`); }
  catch (e) { return null; }
}

async function fetchSourceCandidates(page) {
  try { return await api(`/api/editions/${editionId}/source-by-page/${page}`); }
  catch (e) { return null; }
}

// 2026-09-27 사용자 지시 [B] - 안내 문구도 사람이 직접 고치는 경로가
// 원래 허용돼 있는데 입력창이 없었다. topic/inst/closing처럼 페이지 종류를
// 안 가리는 단순 문자열 필드는 키 존재 여부만으로 공통 처리한다.
const FREE_TEXT_FIELDS = [
  ["topic", "주제(topic)"],
  ["inst", "안내 문구(inst)"],
  ["closing", "마무리 문구(closing)"],
];

function renderFreeTextFields(body, page, idx, basePath) {
  FREE_TEXT_FIELDS.forEach(([key, label]) => {
    if (!(key in page)) return;
    const val = page[key] || "";
    const ta = el("textarea", { rows: "2" }, val);
    body.append(field(label, ta));
    ta.addEventListener("blur", () => {
      if (ta.value !== val) patch([{ op: "replace", path: `${basePath}/${key}`, value: ta.value }],
        { reason: `검수: ${label} 수정` });
    });
  });
}

function excerptParagraphs(page) {
  const text = (page.excerpt && page.excerpt.text && page.excerpt.text[0]) || "";
  return text.split("\n").filter(p => p.trim());
}

// 2026-09-27 사용자 지시 [B]-1 - 제시문 전문 편집 + 문단 단위 앞/뒤 페이지
// 이동(젊은 예술가 18·19쪽처럼 자동 반반 분할이 아니라 사람이 경계를 직접
// 조정해야 하는 경우). excerpt.text는 항상 원소 1개짜리 리스트라 "\n"으로
// 문단을 이어붙인다(edition/presets.py의 split/merge와 같은 데이터 모양).
function renderExcerptInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "제시문(excerpt)"));

  const fullText = (page.excerpt.text && page.excerpt.text[0]) || "";
  const ta = el("textarea", { rows: "6" }, fullText);
  body.append(field("전문 편집", ta,
    el("div", { class: "hint" }, "여러 문단이면 줄바꿈으로 구분됩니다.")));
  ta.addEventListener("blur", () => {
    if (ta.value !== fullText) {
      patch([{ op: "replace", path: `${basePath}/excerpt/text`, value: [ta.value] }],
        { reason: "검수: 제시문 전문 수정" });
    }
  });

  const pages = state.layout.pages;
  const prevPage = idx > 0 ? pages[idx - 1] : null;
  const nextPage = idx < pages.length - 1 ? pages[idx + 1] : null;
  const canSendPrev = !!(prevPage && prevPage.excerpt);
  const canSendNext = !!(nextPage && nextPage.excerpt);
  const paragraphs = excerptParagraphs(page);

  if ((canSendPrev || canSendNext) && paragraphs.length) {
    const moveHost = el("div", {});
    paragraphs.forEach((para, pi) => {
      const row = el("div", { class: "subfield-row", style: "align-items:flex-start" });
      row.append(el("div", { style: "flex:1 1 auto;font-size:11.5px;color:var(--sub);word-break:break-all" },
        para.length > 70 ? para.slice(0, 70) + "…" : para));
      if (canSendPrev) {
        const btn = el("button", { class: "btn btn--small" }, "◀ 앞 페이지로");
        btn.onclick = () => moveParagraph(idx, pi, -1);
        row.append(btn);
      }
      if (canSendNext) {
        const btn = el("button", { class: "btn btn--small" }, "뒤 페이지로 ▶");
        btn.onclick = () => moveParagraph(idx, pi, 1);
        row.append(btn);
      }
      moveHost.append(row);
    });
    body.append(field("문단 이동(분할 경계 조정)", moveHost,
      el("div", { class: "hint" }, "옮기면 두 페이지 미리보기가 함께 갱신됩니다.")));
  }
}

async function moveParagraph(idx, paraIdx, direction) {
  const pages = state.layout.pages;
  const page = pages[idx];
  const targetIdx = idx + direction;
  const targetPage = pages[targetIdx];
  if (!targetPage || !targetPage.excerpt) return;

  const paragraphs = excerptParagraphs(page);
  const [moved] = paragraphs.splice(paraIdx, 1);
  const targetParagraphs = excerptParagraphs(targetPage);
  if (direction < 0) targetParagraphs.push(moved);
  else targetParagraphs.unshift(moved);

  await patch([
    { op: "replace", path: `/pages/${idx}/excerpt/text`, value: [paragraphs.join("\n")] },
    { op: "replace", path: `/pages/${targetIdx}/excerpt/text`, value: [targetParagraphs.join("\n")] },
  ], { reason: "검수: 제시문 문단 이동" });
}

function renderInspector() {
  const body = $("#inspectorBody");
  const empty = $("#inspectorEmpty");
  const idx = state.selectedPageIdx;
  if (idx == null) { empty.hidden = false; body.hidden = true; body.innerHTML = ""; return; }
  empty.hidden = true; body.hidden = false;
  body.innerHTML = "";
  const page = state.layout.pages[idx];
  const basePath = `/pages/${idx}`;

  body.append(el("div", { class: "field-check" }, [
    el("input", {
      type: "checkbox", checked: page.included !== false ? "checked" : false,
      onchange: e => patch([{ op: "replace", path: `${basePath}/included`, value: e.target.checked }]),
    }),
    ` 이 페이지를 학생용에 싣는다(included)`,
  ]));

  if ("title" in page) {
    body.append(field("페이지 제목", el("input", {
      type: "text", value: page.title || "",
      onblur: e => { if (e.target.value !== (page.title || "")) patch([{ op: "replace", path: `${basePath}/title`, value: e.target.value }]); },
    })));
  }

  renderFreeTextFields(body, page, idx, basePath);
  if (page.type === "solo") renderSoloConvertInspector(body, page, idx, basePath);
  if (page.type === "qa") renderQaRevertInspector(body, page, idx, basePath);
  if (["qa", "qaband", "qaref", "solo"].includes(page.type)) renderReadingTypeInspector(body, page, idx, basePath);
  if (page.excerpt) renderExcerptInspector(body, page, idx, basePath);
  if (page.q) renderQuestionInspector(body, page, idx, basePath);
  if (page.ox) renderOxInspector(body, page, idx, basePath);
  if (page.vocab) renderVocabInspector(body, page, idx, basePath);
  if (page.qs) renderMemosInspector(body, page, idx, basePath);
  if (page.type === "cover") renderCoverInspector(body);
  if (page.type === "essay") {
    renderEssayDialogInspector(body, page, idx, basePath);
    renderEssayNoteInspector(body, page, idx, basePath);
  }
  if (SLOT_CAPABLE_TYPES.has(page.type)) renderSlotInspector(body, page, idx, basePath);
  renderPresetPanel(body, page, idx);

  body.append(el("hr", { class: "section-divider" }));
  body.append(el("div", { class: "todo-note" },
    "페이지 순서 이동은 페이지 목록의 ▲▼ 버튼으로 할 수 있습니다."));
}

// 2026-09-29 사용자 피드백 - 자유 편집 요청으로 OX 문제 텍스트(ox[].s)를
// 고치려 했는데 "허용된 필드(mirror/titleTop/wide/slot/q.kind)에 없어서
// 처리할 수 없다"는 안내를 받았다. freeform_edit.py는 원래 그 필드들만
// 다루도록 설계돼 있고(LLM이 아무 텍스트나 바꿔버리는 걸 막는 안전장치),
// 텍스트 내용 수정은 q.t처럼 여기 인스펙터에서 직접 편집하는 경로로 둔다.
function renderOxInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "O·X 문항 텍스트"));
  page.ox.forEach((item, k) => {
    const ta = el("textarea", { rows: "2" }, item.s || "");
    body.append(field(`${k + 1}번 문제`, ta));
    ta.addEventListener("blur", () => {
      if (ta.value !== (item.s || "")) {
        patch([{ op: "replace", path: `${basePath}/ox/${k}/s`, value: ta.value }],
          { reason: "검수: O·X 문항 텍스트 수정" });
      }
    });
  });
}

// 2026-09-29 사용자 피드백 - memos(STEP3 소질문) 페이지의 문항 텍스트를 고칠
// 방법이 없었다. ox와 같은 패턴(항목별 텍스트박스, blur 시 patch).
function renderMemosInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "소질문 텍스트"));
  page.qs.forEach((item, k) => {
    const ta = el("textarea", { rows: "3" }, item.t || "");
    body.append(field(`${item.no}번`, ta));
    ta.addEventListener("blur", () => {
      if (ta.value !== (item.t || "")) {
        patch([{ op: "replace", path: `${basePath}/qs/${k}/t`, value: ta.value }],
          { reason: "검수: 소질문 텍스트 수정" });
      }
    });
  });
}

// 2026-09-29 사용자 피드백 - 표지(cover) 페이지의 "책 속 한 문장"(book.quote)을
// 고칠 방법이 없었다. book.*는 page가 아니라 layout 최상위(state.layout.book)
// 필드라 basePath(/pages/{idx}) 기준이 아니라 /book/quote로 직접 patch한다.
function renderCoverInspector(body) {
  const book = state.layout.book || {};
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "표지 - 책 속 한 문장(quote)"));
  const ta = el("textarea", { rows: "3" }, book.quote || "");
  body.append(field("인용문(quote)", ta));
  ta.addEventListener("blur", () => {
    if (ta.value !== (book.quote || "")) {
      patch([{ op: "replace", path: "/book/quote", value: ta.value }],
        { reason: "검수: 표지 인용문 수정" });
    }
  });
}

// 2026-09-29 사용자 피드백 - solo 페이지(원본에 제시문이 없어 layout/step2.py가
// 만든 타입, qHead+widget만 있고 excerpt가 없음)에 인용문을 넣고 싶다는 요청.
// excerpt 칸 자체가 없어서 renderExcerptInspector가 안 뜬다 - type을 qa로
// 바꾸고 빈 excerpt를 추가해주면(ratio도 solo 기본값 그대로 넣어줌 - qa
// 렌더러는 solo와 달리 폴백이 없어서 안 넣으면 레이아웃이 깨짐) 그 아래
// 일반 excerpt 인스펙터가 그대로 뜬다.
function renderSoloConvertInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("p", { class: "hint" },
    "이 페이지는 원본에 제시문이 없어 solo 형식으로 만들어졌습니다. 인용문(제시문)을 넣으려면 qa 형식으로 바꿀 수 있습니다."));
  const btn = el("button", { class: "btn btn--small" }, "제시문 추가하기(qa로 전환)");
  btn.onclick = () => {
    patch([
      { op: "add", path: `${basePath}/excerpt`, value: { p: null, text: [""] } },
      { op: "add", path: `${basePath}/ratio`, value: page.ratio || "1fr 1.25fr" },
      { op: "replace", path: `${basePath}/type`, value: "qa" },
    ], { reason: "검수: solo를 qa로 전환(제시문 추가)" });
  };
  body.append(btn);
}

// 2026-09-29 사용자 피드백 - "독해 종류 선택 기능이 필요함. 어떤 독해인지
// 설명이 제대로 표시되지 않고 있음". layout/rules.py의 reading_type_label/
// character_for_reading_type과 layout/step2.py의 _STEP2_NM을 그대로 옮긴
// 목록이다(원본에 코드가 있으면 항상 이 표대로 정해지므로, 값이 바뀌면
// 여기도 같이 고쳐야 함). qa.reading_type이 원문에 없던 문항은 guide.rt가
// 그냥 "독해"로만 나가고(어떤 종류인지 안 보임) derived 플래그로만 표시돼
// 있었는데, 정작 검수자가 고를 UI가 없었다.
const READING_TYPES = [
  { key: "사실적", img: "holmes", nm: "셜록 홈즈와 근거 찾기" },
  { key: "분석적", img: "aronnax", nm: "아로낙스 박사와 나눠 보기" },
  { key: "추론적", img: "jekyll", nm: "지킬 박사와 숨은 뜻 찾기" },
  { key: "비판적", img: "tom_sawyer", nm: "톰 소여와 다르게 보기" },
  { key: "적용적", img: "fogg", nm: "필리어스 포그와 적용해 보기" },
  { key: "상상적", img: "alice", nm: "다빈치와 상상해 보기" },
  // 2026-09-29 사용자 지시 - 초1·2 원문 "논리적 읽기" 대응(7번째 유형).
  // 캐릭터 이미지(socrates)는 아직 assets에 없음(사용자가 나중에 지정
  // 예정) - 지정 전까지는 화면에 이미지가 안 뜨고 자리만 비어 보인다.
  { key: "논리적", img: "socrates", nm: "소크라테스와 논리 세우기" },
];

function renderReadingTypeInspector(body, page, idx, basePath) {
  const guide = page.guide || {};
  const current = READING_TYPES.find(t => (guide.rt || "").includes(t.key));
  body.append(el("hr", { class: "section-divider" }));
  const select = el("select", {}, [
    el("option", { value: "", selected: !current ? "selected" : false }, "(선택 안 됨 - 독해)"),
    ...READING_TYPES.map(t => el("option", {
      value: t.key, selected: current === t ? "selected" : false,
    }, `${t.key} 독해`)),
  ]);
  body.append(field("독해 종류", select,
    el("div", { class: "hint" }, !current
      ? "원문에 독해유형이 없어 지금은 그냥 \"독해\"로만 표시되고 있습니다 - 실제 내용을 보고 골라주세요."
      : "")));
  select.onchange = async () => {
    const picked = READING_TYPES.find(t => t.key === select.value);
    // 중학생(band=mid)은 캐릭터가 없는 라벨만 쓴다(layout/step2.py _guide_for
    // 참고) - guide.img/nm을 새로 만들어 붙이면 안 된다.
    const isMid = (state.layout.tone || {}).band === "mid";
    const newGuide = !picked ? { ...guide, rt: "독해" }
      : isMid ? { ...guide, rt: `${picked.key} 독해` }
      : { ...guide, img: picked.img, rt: `${picked.key} 독해`, nm: picked.nm };
    const ok = await patch([{ op: "replace", path: `${basePath}/guide`, value: newGuide }],
      { reason: "검수: 독해 종류 선택" });
    if (ok === false) return;
    const orderLabel = page.q && page.q.id;
    if (orderLabel != null) {
      state.flags
        .filter(f => !f.resolved_at && f.kind === "derived"
          && f.message.startsWith(`문항 ${orderLabel}:`) && f.message.includes("독해유형"))
        .forEach(f => resolveFlag(f.id));
    }
  };
}

// 2026-09-29 사용자 피드백 - solo->qa 전환 버튼의 반대 방향. 제시문(인용문)
// 자리가 필요 없어지면 지우고 solo로 되돌릴 수 있어야 한다는 요청 - excerpt
// 텍스트는 그대로 없어지니(patch()가 correction_log에 before/after는 남긴다)
// 버튼 문구로 미리 알린다.
function renderQaRevertInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  const btn = el("button", { class: "btn btn--small" }, "제시문 삭제하기(solo로 되돌리기)");
  body.append(field("제시문 없애기", btn,
    el("div", { class: "hint" }, "지금 있는 제시문 내용이 사라지고 이미지+문항만 있는 solo 형식으로 바뀝니다.")));
  btn.onclick = () => {
    const ops = [
      { op: "remove", path: `${basePath}/excerpt` },
      { op: "replace", path: `${basePath}/type`, value: "solo" },
    ];
    if (page.ratio !== undefined) ops.splice(1, 0, { op: "remove", path: `${basePath}/ratio` });
    patch(ops, { reason: "검수: qa를 solo로 전환(제시문 삭제)" });
  };
}

// 2026-09-29 사용자 피드백 - vocabMatch 페이지에서 "'뜻 보충: 검수 필요'
// 배지를 지워줘"를 자유 편집으로 요청했다가 거부당함(맞는 거부다 - 그 배지는
// 고정 텍스트가 아니라 vocab[k].sup === true일 때만 뜨는 표시라 LLM 패치
// 대상이 아니다). sup는 B단계 LLM 자동 보충 스크립트가 "검수자가 확인하기
// 전엔 켜 둔다"는 방침으로 일부러 안 지운 값(project 기억 참고) - 여기서
// 검수자가 뜻을 보고 확인하면 배지를 끌 수 있게 한다. resolveFlag는 위에서
// 이미 정의됨(호이스팅) - sup flag 메시지에 낱말이 들어있어 텍스트로 찾는다.
function renderVocabInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "낱말 뜻(vocab)"));
  page.vocab.forEach((item, k) => {
    const ta = el("textarea", { rows: "2" }, item.d || "");
    body.append(field(item.w, ta));
    ta.addEventListener("blur", () => {
      if (ta.value !== (item.d || "")) {
        patch([{ op: "replace", path: `${basePath}/vocab/${k}/d`, value: ta.value }], { reason: "검수: 낱말 뜻 수정" });
      }
    });
    if (item.sup) {
      const row = el("div", { class: "field" });
      row.append(el("span", { class: "hint" }, "뜻 보충: 검수 필요(자동 보충된 값 - 검수 전까지 표시)"));
      const confirmBtn = el("button", { class: "btn btn--small" }, "뜻 확인 완료(배지 끄기)");
      confirmBtn.onclick = async () => {
        const ok = await patch([{ op: "replace", path: `${basePath}/vocab/${k}/sup`, value: false }],
          { reason: "검수: 뜻풀이 확인 완료" });
        if (ok === false) return; // 409 충돌 - patch()가 이미 안내함
        const flag = state.flags.find(f => f.kind === "sup" && !f.resolved_at && f.message.includes(item.w));
        if (flag) await resolveFlag(flag.id);
      };
      row.append(confirmBtn);
      body.append(row);
    }
  });
}

// 2026-09-29 사용자 지시 - "이미지 왼쪽에 이미 텍스트 박스(.dialog)가 있는데
// 글을 쓸 수가 없다"는 피드백. dialog는 STEP3 도입 인용문을 원문에서 못
// 뽑았을 때 빈 배열로 남아(layout/step3.py 참고, split_dialog_lines가
// 못 찾으면 []) 그 박스가 그냥 빈 채로 나간다 - 검수자가 직접 채울 수 있게
// 인스펙터에 추가한다. 줄바꿈 하나가 배열 원소 하나(문단 하나)다.
function renderEssayDialogInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  const fullText = (page.dialog || []).join("\n");
  const ta = el("textarea", { rows: "4", placeholder: "이미지 왼쪽 인용문 박스에 넣을 문장(줄바꿈 = 문단 구분)" }, fullText);
  body.append(field("이미지 왼쪽 인용문(dialog)", ta,
    el("div", { class: "hint" }, "지금 이 박스가 비어 있으면 화면에 빈 박스만 나갑니다 - 원문에서 도입 인용문을 못 찾은 경우입니다.")));
  ta.addEventListener("blur", () => {
    if (ta.value !== fullText) {
      const lines = ta.value.split("\n").map(s => s.trim()).filter(Boolean);
      patch([{ op: "replace", path: `${basePath}/dialog`, value: lines }], { reason: "검수: 이미지 왼쪽 인용문(dialog) 수정" });
    }
  });
}

// 2026-09-29 사용자 지시 - essay(STEP3) 페이지 이미지 아래에 요약/동기부여
// 문구 박스를 추가했다(renderer.js .essay-note). 이것도 자유 편집(LLM)
// 스키마 밖의 자유 텍스트라 인스펙터에서 검수자가 직접 채운다 - 빈 값이면
// 렌더러가 박스 자체를 안 그린다.
function renderEssayNoteInspector(body, page, idx, basePath) {
  body.append(el("hr", { class: "section-divider" }));
  const ta = el("textarea", { rows: "3", placeholder: "이미지 아래에 보여줄 요약·동기부여 문구(비워두면 박스가 안 보입니다)" }, page.note || "");
  body.append(field("이미지 아래 문구(note)", ta));
  ta.addEventListener("blur", () => {
    if (ta.value !== (page.note || "")) {
      // note는 layout/step1.py 등에서 기본값을 안 넣는 새 필드라 처음엔 키가
      // 없다 - "add"는 없을 때 새로 만들고 있을 때는 교체해서 두 경우 다 된다.
      patch([{ op: "add", path: `${basePath}/note`, value: ta.value }], { reason: "검수: 이미지 아래 문구 수정" });
    }
  });
}

function field(labelText, inputEl, extra) {
  return el("div", { class: "field" }, [el("label", {}, labelText), inputEl, extra || null]);
}

function renderQuestionInspector(body, page, idx, basePath) {
  const q = page.q;
  const orderNo = parseInt(String(q.id).split(/[-#]/)[0], 10);
  const qPath = `${basePath}/q`;

  const textArea = el("textarea", { rows: "4" }, q.t || "");
  const diffBox = el("div", { class: "diff-box" }, "원문 불러오는 중…");
  body.append(field("문항 텍스트(q.t)", textArea, diffBox));
  textArea.addEventListener("blur", () => {
    if (textArea.value !== (q.t || "")) {
      patch([{ op: "replace", path: `${qPath}/t`, value: textArea.value }], { reason: "검수: 문항 텍스트 수정" });
    }
  });
  if (q.src_page != null) {
    // 방식 B(비전) 문항 - q.id가 order_no와 대응하지 않으므로 페이지 단위
    // 후보를 보여준다(2026-09-26, 야옹아 9쪽 오매칭 발견 후 수정).
    fetchSourceCandidates(q.src_page).then(res => {
      const candidates = res && res.candidates;
      if (!candidates || !candidates.length) { diffBox.textContent = `DB 원문을 찾을 수 없음(원본 ${q.src_page}쪽에 해당 행 없음).`; return; }
      diffBox.innerHTML = "";
      diffBox.append(el("span", { class: "diff-label" }, `DB 원문 후보 - 원본 ${q.src_page}쪽 전체 행(${candidates.length}개, q.id는 이 번호들과 1:1 대응이 아님)`));
      candidates.forEach(c => {
        if (!c.question_text && !c.excerpt_text) return;
        const row = el("div", { style: "margin-top:6px" });
        row.append(el("span", { class: "diff-label" }, `order_no ${c.order_no}(${c.order_label || ""})`));
        if (c.excerpt_text) row.append(document.createTextNode("제시문: " + c.excerpt_text));
        if (c.question_text) { if (c.excerpt_text) row.append(el("br")); row.append(document.createTextNode("질문: " + c.question_text)); }
        diffBox.append(row);
      });
    });
  } else {
    fetchSourceText(orderNo).then(src => {
      if (!src) { diffBox.textContent = "DB 원문을 찾을 수 없음(order_no 추정이 안 맞을 수 있음)."; return; }
      diffBox.innerHTML = "";
      diffBox.append(el("span", { class: "diff-label" }, "DB 원문(question_text)"));
      diffBox.append(document.createTextNode(src.question_text || "(비어있음)"));
      if (src.excerpt_text) {
        diffBox.append(el("span", { class: "diff-label", style: "margin-top:6px" }, "DB 원문(excerpt_text)"));
        diffBox.append(document.createTextNode(src.excerpt_text));
      }
    });
  }

  const formSelect = el("select", {}, [
    "single", "blanks", "table", "list", "compare", "pledge", "speech", "choice", "choiceList",
  ].map(f => el("option", { value: f, selected: f === currentFormValue(q) ? "selected" : false }, f)));
  body.append(field("위젯 종류(form)", formSelect,
    el("div", { class: "hint" }, "바꾸면 correction_log에 kind=form_change로 기록됩니다.")));

  const subfieldHost = el("div", {});
  body.append(subfieldHost);
  renderFormSubfields(subfieldHost, q, qPath, currentFormValue(q));

  formSelect.addEventListener("change", () => {
    const newForm = formSelect.value;
    subfieldHost.innerHTML = "";
    renderFormSubfields(subfieldHost, q, qPath, newForm, /* justSwitched */ true);
  });
}

function defaultsForForm(form, q) {
  switch (form) {
    case "single": return { kind: q.kind === "short" ? "short" : "long", starter: q.starter || "" };
    case "blanks": return { kind: "multi", blanks: q.blanks && q.blanks.length ? q.blanks : [{ label: "", prompt: q.t || "" }] };
    case "table": return { form: "table", rows: q.rows && q.rows.length ? q.rows : [{ label: "", hint: "", prompt: q.t || "" }] };
    case "list": return { form: "list", items: q.items && q.items.length ? q.items : [{ hint: "", prompt: q.t || "" }] };
    case "compare": return { form: "compare", cards: q.cards && q.cards.length ? q.cards : [{ title: "", prompt: q.t || "" }, { title: "", prompt: "" }] };
    case "pledge": return { form: "pledge", n: q.n || 3 };
    case "speech": return { form: "speech", starter: q.starter || "" };
    case "choice": return { form: "choice", cards: q.cards && q.cards.length === 2 ? q.cards : [{ title: "" }, { title: "" }] };
    case "choiceList": return { form: "choiceList", options: q.options && q.options.length ? q.options : ["", ""], single: q.single !== false };
    default: return {};
  }
}

function renderFormSubfields(host, q, qPath, form, justSwitched) {
  const defaults = defaultsForForm(form, q);
  const save = (overrides) => {
    const newQ = { ...defaults, ...overrides };
    const ops = diffQPatch(q, newQ, qPath);
    if (ops.length) patch(ops, { reason: `검수: 위젯 종류/필드 변경(${form})` });
  };

  if (form === "single") {
    const kindSelect = el("select", {}, ["long", "short"].map(k =>
      el("option", { value: k, selected: k === defaults.kind ? "selected" : false }, k)));
    const starterInput = el("input", { type: "text", value: defaults.starter, placeholder: "(짧은 답란 시작 문구, 선택)" });
    host.append(field("답란 길이(kind)", kindSelect));
    host.append(field("시작 문구(starter, 선택)", starterInput));
    kindSelect.onchange = () => save({ kind: kindSelect.value, starter: starterInput.value });
    starterInput.onblur = () => save({ kind: kindSelect.value, starter: starterInput.value });
    if (justSwitched) save({ kind: kindSelect.value, starter: starterInput.value });
    return;
  }

  if (form === "blanks") {
    let items = defaults.blanks.map(b => ({ label: b.label || "", prompt: b.prompt || "" }));
    const listHost = el("div", {});
    const rerender = () => {
      listHost.innerHTML = "";
      items.forEach((b, i) => {
        const labelInput = el("input", { type: "text", placeholder: "라벨", value: b.label });
        const promptInput = el("input", { type: "text", placeholder: "안내 문구(prompt, 선택)", value: b.prompt });
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { items.splice(i, 1); rerender(); save({ blanks: items }); } }, "삭제");
        labelInput.onblur = () => { items[i].label = labelInput.value; save({ blanks: items }); };
        promptInput.onblur = () => { items[i].prompt = promptInput.value; save({ blanks: items }); };
        listHost.append(el("div", { class: "subfield-row" }, [labelInput, promptInput, removeBtn]));
      });
    };
    rerender();
    host.append(field("빈칸 라벨(blanks)", listHost,
      el("button", { class: "btn btn--small subfield-add", onclick: () => { items.push({ label: "", prompt: q.t || "" }); rerender(); save({ blanks: items }); } }, "+ 빈칸 추가")));
    if (justSwitched) save({ blanks: items });
    return;
  }

  if (form === "table") {
    let rows = defaults.rows.map(r => ({ label: r.label || "", hint: r.hint || "", prompt: r.prompt || "" }));
    const listHost = el("div", {});
    const rerender = () => {
      listHost.innerHTML = "";
      rows.forEach((r, i) => {
        const labelInput = el("input", { type: "text", placeholder: "머리칸(label)", value: r.label });
        const hintInput = el("input", { type: "text", placeholder: "힌트(선택)", value: r.hint });
        const promptInput = el("input", { type: "text", placeholder: "안내 문구(prompt, 선택)", value: r.prompt });
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { rows.splice(i, 1); rerender(); save({ rows }); } }, "삭제");
        labelInput.onblur = () => { rows[i].label = labelInput.value; save({ rows }); };
        hintInput.onblur = () => { rows[i].hint = hintInput.value; save({ rows }); };
        promptInput.onblur = () => { rows[i].prompt = promptInput.value; save({ rows }); };
        listHost.append(el("div", { class: "subfield-row" }, [labelInput, hintInput, promptInput, removeBtn]));
      });
    };
    rerender();
    host.append(field("표 행(rows)", listHost,
      el("button", { class: "btn btn--small subfield-add", onclick: () => { rows.push({ label: "", hint: "", prompt: q.t || "" }); rerender(); save({ rows }); } }, "+ 행 추가")));
    if (justSwitched) save({ rows });
    return;
  }

  if (form === "list") {
    let items = defaults.items.map(it => ({ hint: it.hint || "", prompt: it.prompt || "" }));
    const listHost = el("div", {});
    const rerender = () => {
      listHost.innerHTML = "";
      items.forEach((it, i) => {
        const hintInput = el("input", { type: "text", placeholder: "힌트(선택, 쪽수 등)", value: it.hint });
        const promptInput = el("input", { type: "text", placeholder: "안내 문구(prompt, 선택)", value: it.prompt });
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { items.splice(i, 1); rerender(); save({ items }); } }, "삭제");
        hintInput.onblur = () => { items[i].hint = hintInput.value; save({ items }); };
        promptInput.onblur = () => { items[i].prompt = promptInput.value; save({ items }); };
        listHost.append(el("div", { class: "subfield-row" }, [hintInput, promptInput, removeBtn]));
      });
    };
    rerender();
    host.append(field(`목록 항목(items, ${items.length}개 - 3개 이하면 rowTall 자동 적용)`, listHost,
      el("button", { class: "btn btn--small subfield-add", onclick: () => { items.push({ hint: "", prompt: q.t || "" }); rerender(); save({ items, rowKind: items.length <= 3 ? "rowTall" : "row" }); } }, "+ 항목 추가")));
    if (justSwitched) save({ items, rowKind: items.length <= 3 ? "rowTall" : "row" });
    return;
  }

  if (form === "compare") {
    let cards = defaults.cards.map(c => ({ title: c.title || "", prompt: c.prompt || "", n: c.n || null }));
    const listHost = el("div", {});
    const rerender = () => {
      listHost.innerHTML = "";
      cards.forEach((c, i) => {
        const titleInput = el("input", { type: "text", placeholder: "카드 제목", value: c.title });
        const promptInput = el("input", { type: "text", placeholder: "안내 문구(prompt, 선택)", value: c.prompt });
        const nInput = el("input", { type: "number", placeholder: "번호 칸(선택)", value: c.n || "", min: "0", style: "width:80px" });
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { cards.splice(i, 1); rerender(); save({ cards: cards.map(cleanCard) }); } }, "삭제");
        titleInput.onblur = () => { cards[i].title = titleInput.value; save({ cards: cards.map(cleanCard) }); };
        promptInput.onblur = () => { cards[i].prompt = promptInput.value; save({ cards: cards.map(cleanCard) }); };
        nInput.onblur = () => {
          cards[i].n = nInput.value ? parseInt(nInput.value, 10) : null;
          // 사용자 지시 2번: 한쪽에 번호 칸이 있으면 반대쪽도 같은 n으로 맞춘다(비교형 대칭 규칙)
          if (cards[i].n) cards.forEach(c => { c.n = cards[i].n; });
          rerender(); save({ cards: cards.map(cleanCard) });
        };
        listHost.append(el("div", { class: "subfield-row" }, [titleInput, promptInput, nInput, removeBtn]));
      });
    };
    const cleanCard = c => c.n ? { title: c.title, prompt: c.prompt, n: c.n } : { title: c.title, prompt: c.prompt };
    rerender();
    host.append(field("비교 카드(cards)", listHost,
      el("button", { class: "btn btn--small subfield-add", onclick: () => { cards.push({ title: "", prompt: "", n: null }); rerender(); save({ cards: cards.map(cleanCard) }); } }, "+ 카드 추가")));
    if (justSwitched) save({ cards: cards.map(cleanCard) });
    return;
  }

  if (form === "pledge") {
    const nInput = el("input", { type: "number", value: defaults.n, min: "1" });
    host.append(field("서약 줄 수(n)", nInput));
    nInput.onblur = () => save({ n: parseInt(nInput.value, 10) || 3 });
    if (justSwitched) save({ n: defaults.n });
    return;
  }

  if (form === "speech") {
    const starterInput = el("input", { type: "text", value: defaults.starter, placeholder: "말풍선 시작 문구" });
    host.append(field("시작 문구(starter)", starterInput));
    starterInput.onblur = () => save({ starter: starterInput.value });
    if (justSwitched) save({ starter: defaults.starter });
    return;
  }

  if (form === "choice") {
    // compare 확장형 - 카드 정확히 2장(양자택일). 학생은 탭으로 고르고(#choice)
    // 아래 한 칸에 이유를 쓴다(#reason) - 사용자 지시(2026-09-23 "choice_ab 위젯 구현").
    const cards = [
      { title: defaults.cards[0]?.title || "" },
      { title: defaults.cards[1]?.title || "" },
    ];
    const aInput = el("textarea", { rows: "2" }, cards[0].title);
    const bInput = el("textarea", { rows: "2" }, cards[1].title);
    host.append(field("선택지 A", aInput));
    host.append(field("선택지 B", bInput,
      el("div", { class: "hint" }, "학생은 둘 중 하나를 탭으로 고르고 그 아래에 이유를 씁니다.")));
    const commit = () => save({ cards: [{ title: aInput.value }, { title: bInput.value }] });
    aInput.onblur = commit; bInput.onblur = commit;
    if (justSwitched) commit();
    return;
  }

  if (form === "choiceList") {
    // choice_multi(N지선다, 2~5개) - 세로 목록. 기본 single(단일 선택), 검수에서
    // 체크를 풀면 복수 선택으로 바뀐다(사용자 지시 2026-09-23).
    let options = defaults.options.slice();
    let single = defaults.single;
    const listHost = el("div", {});
    const commit = () => save({ options, single });
    const rerender = () => {
      listHost.innerHTML = "";
      options.forEach((opt, i) => {
        const optInput = el("input", { type: "text", placeholder: `선택지 ${i + 1}`, value: opt });
        const removeBtn = el("button", {
          class: "btn btn--small btn--danger", disabled: options.length <= 2 ? "disabled" : false,
          onclick: () => { options.splice(i, 1); rerender(); commit(); },
        }, "삭제");
        optInput.onblur = () => { options[i] = optInput.value; commit(); };
        listHost.append(el("div", { class: "subfield-row" }, [optInput, removeBtn]));
      });
    };
    rerender();
    host.append(field(`선택지(options, ${options.length}개 - 2~5개 권장)`, listHost,
      el("button", {
        class: "btn btn--small subfield-add", disabled: options.length >= 5 ? "disabled" : false,
        onclick: () => { options.push(""); rerender(); commit(); },
      }, "+ 선택지 추가")));
    const singleToggle = el("label", { class: "field-check" }, [
      el("input", { type: "checkbox", checked: single ? "checked" : false, onchange: e => { single = e.target.checked; commit(); } }),
      " 단일 선택(끄면 여러 개를 동시에 고를 수 있음)",
    ]);
    host.append(singleToggle);
    if (justSwitched) commit();
    return;
  }
}

async function fetchAvailableImages() {
  try { return (await api(`/api/editions/${editionId}/available-images`)).images; }
  catch (e) { return []; }
}

// 2026-09-28 [3] 페이지별 이미지 생성 - 일괄 생성이 아니라 검수하다가 그
// 페이지에서 바로 만든다. scene+avoid+분기 하우스 스타일로 2~3장 생성 후
// 미리보기, 고르면 슬롯에 반영(아니면 지시문 고쳐 다시 생성).
function candidateCard(cand, onChoose) {
  const costLabel = cand.cost_usd != null ? `$${cand.cost_usd.toFixed(3)}` : "-";
  const timeLabel = cand.elapsed_ms != null ? `${(cand.elapsed_ms / 1000).toFixed(1)}s` : "-";
  const card = el("div", { class: `img-cand${cand.chosen ? " img-cand--chosen" : ""}` }, [
    el("img", { src: cand.url, alt: "생성 이미지 후보" }),
    el("div", { class: "img-cand__meta" }, `${cand.width}×${cand.height} · ${costLabel} · ${timeLabel}`),
  ]);
  if (cand.chosen) {
    card.append(el("div", { class: "img-cand__badge" }, "사용 중"));
  } else {
    // 2026-09-29 사용자 피드백 - 처리 중에도 버튼이 그대로 눌려 있어서
    // 헷갈려 두 번 누르는 일이 있었다("후보 만들기" 버튼은 이미 이렇게
    // 돼 있었는데 여기는 빠져 있었음).
    const btn = el("button", { class: "btn btn--small btn--primary" }, "이 이미지 쓰기");
    btn.onclick = async () => {
      btn.disabled = true;
      btn.textContent = "적용 중…";
      try {
        await onChoose(cand.id);
      } finally {
        btn.disabled = false;
        btn.textContent = "이 이미지 쓰기";
      }
    };
    card.append(btn);
  }
  return card;
}

async function renderImageGen(host, pageIdx, slotPath, sceneInput, avoidInput) {
  host.append(el("h3", { style: "font-size:12px;margin:4px 0" }, "이미지 생성(후보 만들기)"));
  const dimsHost = el("div", { class: "hint" }, "슬롯 크기 측정 중…");
  host.append(dimsHost);

  let ratio = "1:1";
  try {
    const frame = $("#previewFrame").contentWindow;
    const dims = frame && frame.__viewer && frame.__viewer.getSlotDims(pageIdx);
    if (dims) {
      ratio = dims.ratio;
      dimsHost.textContent = `실측 ${Math.round(dims.widthMm)}×${Math.round(dims.heightMm)}mm → ${dims.ratio} 비율로 생성`;
    } else {
      dimsHost.textContent = "슬롯 크기를 측정할 수 없습니다(1:1로 생성).";
    }
  } catch (e) {
    dimsHost.textContent = "슬롯 크기를 측정할 수 없습니다(1:1로 생성).";
  }

  const countSel = el("select", {}, [new Option("2장", "2"), new Option("3장", "3", true, true)]);
  const genBtn = el("button", { class: "btn btn--small btn--primary" }, "후보 만들기");
  const usageLabel = el("span", { class: "hint", style: "margin-left:8px" }, "");
  host.append(el("div", { class: "subfield-row" }, [genBtn, countSel, usageLabel]));

  const gallery = el("div", { class: "img-cand-gallery" });
  host.append(gallery);

  const renderUsage = usage => {
    usageLabel.textContent = `누적 ${usage.total_count}장 · 추정 $${usage.total_cost_usd.toFixed(2)}`;
  };

  const chooseImage = async candidateId => {
    try {
      await api(`/api/editions/${editionId}/choose-image`, {
        method: "POST", body: JSON.stringify({ candidate_id: candidateId, editor: "reviewer" }),
      });
      toast("이미지를 슬롯에 반영했습니다.");
      await refreshAll({ keepPreview: false });
    } catch (e) {
      toast(`반영 실패: ${e.message}`);
    }
  };

  const renderGallery = candidates => {
    gallery.innerHTML = "";
    candidates.forEach(cand => gallery.append(candidateCard(cand, chooseImage)));
  };
  const appendGallery = candidates => {
    candidates.forEach(cand => gallery.append(candidateCard(cand, chooseImage)));
  };

  try {
    const data = await api(`/api/editions/${editionId}/pages/${pageIdx}/image-candidates`);
    renderGallery(data.candidates);
    renderUsage(data.usage);
  } catch (e) { /* 목록 조회 실패는 조용히 무시 - 후보 만들기는 계속 가능해야 함 */ }

  genBtn.onclick = async () => {
    genBtn.disabled = true;
    genBtn.textContent = "생성 중…";
    try {
      const data = await api(`/api/editions/${editionId}/pages/${pageIdx}/generate-images`, {
        method: "POST",
        body: JSON.stringify({
          page_idx: pageIdx, scene: sceneInput.value, avoid: avoidInput.value,
          ratio, count: parseInt(countSel.value, 10), editor: "reviewer",
        }),
      });
      appendGallery(data.candidates);
      renderUsage(data.usage);
      toast(`${data.candidates.length}장 생성했습니다.`);
    } catch (e) {
      toast(`생성 실패: ${e.message}`);
    } finally {
      genBtn.disabled = false;
      genBtn.textContent = "후보 만들기";
    }
  };
}

// 2026-09-26 [5순위] 검수자가 이미지 자리를 직접 추가·삭제·변경(=이동)할 수
// 있게 한다. 스키마는 페이지당 slot 하나뿐이라 "이동"은 "이 자리에 쓸 이미지를
// 바꾼다"로 구현한다(다른 페이지로 옮기는 건 검수자가 삭제 후 그 페이지에서
// 새로 추가하면 된다 - 페이지가 다르면 어차피 원본 삽화 후보도 달라진다).
function renderSlotInspector(body, page, idx, basePath) {
  const slot = page.slot;
  const slotPath = `${basePath}/slot`;
  body.append(el("hr", { class: "section-divider" }));

  if (!slot) {
    const addBtn = el("button", { class: "btn btn--small" }, "이미지 자리 추가");
    body.append(field("이미지 슬롯", addBtn,
      el("div", { class: "hint" }, "추가하면 자리가 부족해질 수 있습니다 - 추가 직후 미리보기에서 확인하세요.")));
    addBtn.onclick = async () => {
      const ok = await patch([{
        op: "add", path: slotPath,
        value: { scene: "", avoid: "질문이 요구하는 답을 암시하지 않는다." },
      }], { reason: "검수: 이미지 자리 추가" });
      if (ok) toast("이미지 자리를 추가했습니다. 자리가 좁으면 답란을 줄여야 할 수 있습니다.");
    };
    return;
  }

  const delBtn = el("button", { class: "btn btn--small" }, "이미지 자리 삭제");
  delBtn.onclick = () => patch([{ op: "remove", path: slotPath }], { reason: "검수: 이미지 자리 삭제" });
  body.append(field("이미지 슬롯", delBtn));

  if (slot.img) {
    body.append(field("현재 이미지", el("div", { class: "hint" }, `원본 이미지 사용 중: ${slot.img}`)));
  }

  const pickHost = el("div", { class: "hint" }, "원본 이미지 목록 불러오는 중…");
  body.append(field("원본 이미지로 바꾸기(미사용 이미지가 먼저 나옵니다)", pickHost));
  fetchAvailableImages().then(images => {
    pickHost.innerHTML = "";
    if (!images.length) { pickHost.textContent = "이 문서에 등록된 원본 이미지가 없습니다."; return; }
    // 2026-09-27 사용자 지시 [3] - 자동 배치 규칙이 다 못 쓰고 남긴 이미지를
    // 검수자가 여기서 직접 골라 붙일 수 있어야 한다. used=false(미사용)를
    // 먼저 보여줘 검수자가 "남는 이미지"를 바로 찾게 한다.
    const sorted = images.slice().sort((a, b) => (a.used === b.used) ? 0 : (a.used ? 1 : -1));
    sorted.forEach(img => {
      const label = `${img.used ? "" : "★ "}${img.image_type} p.${img.source_page ?? "?"}`;
      const btn = el("button", {
        class: "btn btn--small", style: `margin:2px${img.used ? ";opacity:.6" : ""}`,
        title: img.used ? "이미 다른 자리에서 쓰인 이미지입니다" : "아직 어디에도 배치되지 않은 이미지입니다",
      }, label);
      const btnLabel = btn.textContent;
      btn.onclick = async () => {
        btn.disabled = true;
        btn.textContent = "적용 중…";
        try {
          await patch([{
            op: "replace", path: slotPath,
            value: { img: img.file_path, src: `원본 ${img.source_page}쪽` },
          }], { reason: "검수: 이미지 자리에 원본 이미지 지정" });
        } finally {
          btn.disabled = false;
          btn.textContent = btnLabel;
        }
      };
      pickHost.append(btn);
    });
  });

  // 2026-09-29 사용자 지시 - AI 생성/원본 선택 외에 검수자가 가진 파일을
  // 직접 올릴 수 있어야 한다는 요청. FormData라 api()(Content-Type:
  // application/json 고정)를 못 쓰고 raw fetch로 올린 뒤, 받은 file_path로
  // "원본 이미지로 바꾸기"와 같은 모양의 PATCH를 그대로 태운다.
  const uploadInput = el("input", { type: "file", accept: "image/jpeg,image/png,image/webp" });
  const uploadStatus = el("span", { class: "hint" });
  body.append(field("직접 업로드", uploadInput, uploadStatus));
  uploadInput.onchange = async () => {
    const file = uploadInput.files[0];
    if (!file) return;
    uploadInput.disabled = true;
    uploadStatus.textContent = "업로드 중…";
    try {
      const fd = new FormData();
      fd.append("file", file);
      const res = await fetch(`/api/editions/${editionId}/pages/${idx}/upload-image`, { method: "POST", body: fd });
      if (!res.ok) {
        let detail = res.statusText;
        try { detail = (await res.json()).detail || detail; } catch (e) {}
        throw new Error(detail);
      }
      const data = await res.json();
      uploadStatus.textContent = "";
      await patch([{ op: "replace", path: slotPath, value: { img: data.file_path, src: "직접 업로드" } }],
        { reason: "검수: 이미지 자리에 업로드한 이미지 지정" });
    } catch (e) {
      uploadStatus.textContent = "";
      toast(`업로드 실패: ${e.message}`, { sticky: true });
    } finally {
      uploadInput.value = "";
      uploadInput.disabled = false;
    }
  };

  const sceneInput = el("textarea", { rows: "2" }, slot.scene || "");
  const avoidInput = el("textarea", { rows: "2" }, slot.avoid || "");
  body.append(field("또는 생성 지시문 - 장면(scene)", sceneInput));
  body.append(field("생성 지시문 - 금지 요소(avoid)", avoidInput));
  const saveGen = () => patch([{
    op: "replace", path: slotPath,
    value: { scene: sceneInput.value, avoid: avoidInput.value },
  }], { reason: "검수: 이미지 생성 지시문 수정" });
  sceneInput.onblur = () => { if (sceneInput.value !== (slot.scene || "")) saveGen(); };
  avoidInput.onblur = () => { if (avoidInput.value !== (slot.avoid || "")) saveGen(); };

  body.append(el("hr", { class: "section-divider" }));
  const genSection = el("div", { class: "image-gen" });
  body.append(genSection);
  renderImageGen(genSection, idx, slotPath, sceneInput, avoidInput);

  const q = page.q;
  if (q && q.form === "single" && q.kind === "long") {
    const shrinkBtn = el("button", { class: "btn btn--small" }, "답란을 짧게(short)로 바꿔 공간 확보");
    shrinkBtn.onclick = () => patch([{ op: "replace", path: `${basePath}/q/kind`, value: "short" }],
      { reason: "검수: 이미지 자리 확보를 위해 답란을 짧게 변경" });
    body.append(field("공간이 부족하면", shrinkBtn));
  }
}

/* ============ 답란 줄 수(문서 전체, tone.lines) ============ */
const LINE_KINDS = ["long", "short", "blank", "blankTall", "cell", "row", "rowTall", "cardInk", "memo", "vocab", "inline", "one"];

function renderToneLines() {
  const host = $("#toneLinesBody");
  host.innerHTML = "";
  const lines = state.layout.tone.lines || {};
  for (const kind of LINE_KINDS) {
    const cur = lines[kind];
    const mnInput = el("input", { type: "number", min: "1", value: cur ? cur[0] : "", placeholder: "최소", style: "width:60px" });
    const mxInput = el("input", { type: "number", min: "1", value: cur ? cur[1] : "", placeholder: "최대", style: "width:60px" });
    const save = () => {
      const mn = mnInput.value ? parseInt(mnInput.value, 10) : null;
      const mx = mxInput.value ? parseInt(mxInput.value, 10) : null;
      const had = Object.prototype.hasOwnProperty.call(lines, kind);
      const ops = [];
      if (mn != null && mx != null) {
        if (!("lines" in state.layout.tone)) ops.push({ op: "add", path: "/tone/lines", value: {} });
        ops.push({ op: had ? "replace" : "add", path: `/tone/lines/${kind}`, value: [mn, mx] });
      } else if (had) {
        ops.push({ op: "remove", path: `/tone/lines/${kind}` });
      } else {
        return;
      }
      patch(ops, { reason: `검수: ${kind} 답란 줄 수 조정` });
    };
    mnInput.onblur = save;
    mxInput.onblur = save;
    host.append(el("div", { class: "subfield-row" }, [
      el("span", { style: "width:64px;font-size:11.5px;color:var(--sub)" }, kind), mnInput, mxInput,
    ]));
  }
}

/* ============ 문서 간 이동(같은 레벨·분기 안 이전/다음) ============ */
async function setupDocNav() {
  try {
    const nav = await api(`/api/editions/${editionId}/neighbors`);
    const prevBtn = $("#btnPrevDoc"), nextBtn = $("#btnNextDoc");
    if (nav.prev) {
      prevBtn.disabled = false;
      prevBtn.title = `이전: ${nav.prev.doc_id}`;
      prevBtn.onclick = () => { location.href = `index.html?edition=${nav.prev.edition_id}`; };
    } else {
      prevBtn.disabled = true;
    }
    if (nav.next) {
      nextBtn.disabled = false;
      nextBtn.title = `다음: ${nav.next.doc_id}`;
      nextBtn.onclick = () => { location.href = `index.html?edition=${nav.next.edition_id}`; };
    } else {
      nextBtn.disabled = true;
    }
  } catch (e) {
    /* 네비게이션은 부가 기능 - 실패해도 검수 자체는 계속 가능해야 함 */
  }
}

/* ============ 부팅 ============ */
(async function boot() {
  if (!editionId) {
    document.body.innerHTML = '<p style="padding:24px">URL에 ?edition=&lt;id&gt; 가 필요합니다.</p>';
    return;
  }
  try {
    await refreshAll();
    await setupDocNav();
  } catch (e) {
    toast(`불러오기 실패: ${e.message}`);
  }
})();
