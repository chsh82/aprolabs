/* 5단계 검수·편집 페이지 - 사용자 지시(2026-09-23) "3단: 좌 페이지 목록 / 중앙 렌더러
 * 미리보기(reviewer 모드) / 우 인스펙터". 우선순위: 플래그 큐(1) > 텍스트+원문대조(2) >
 * included 토글(3) > form 변경(4) > 줄 수·순서(5, 이번엔 자리만) > 이미지 슬롯 지시문(6) >
 * 분기색·인쇄 미리보기·승인(7).
 *
 * 편집은 전부 PATCH(JSON Patch)로 보내고, 성공하면 즉시 전체를 다시 불러와 미리보기
 * iframe을 새로고침한다. rev로 낙관적 잠금을 건다(다른 사람이 먼저 고치면 409).
 */

const qs = new URLSearchParams(location.search);
const editionId = qs.get("edition");

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
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 2600);
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
async function patch(ops, { reason } = {}) {
  if (!ops.length) return;
  try {
    const data = await api(`/api/editions/${editionId}`, {
      method: "PATCH",
      body: JSON.stringify({ patch: ops, editor: "reviewer", reason, expected_rev: state.rev }),
    });
    state.layout = data.layout; state.rev = data.rev; state.status = data.status;
    await refreshAll();
    toast("저장했습니다.");
    return true;
  } catch (e) {
    if (e.status === 409) {
      $("#conflictBanner").hidden = false;
      await refreshAll();
      setTimeout(() => { $("#conflictBanner").hidden = true; }, 4000);
      return false;
    }
    toast(`저장 실패: ${e.message}`);
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
  cover: "COV", vocab: "VOC", oxp: "OXP", draw: "DRW", bgline: "BGL", bgtext: "BGT",
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
  { key: "mirror", label: "좌우 바꾸기", types: new Set(["qa", "qaband", "qaref", "solo"]) },
  { key: "title_top", label: "제목 중앙 상단", types: new Set(["qa", "qaband", "qaref", "solo"]) },
  { key: "three_tier", label: "3층 구조", types: new Set(["qa", "qaband", "qaref"]) },
  { key: "lines_more", label: "답란 늘리기" },
  { key: "lines_less", label: "답란 줄이기" },
  { key: "add_image_slot", label: "이미지 자리 추가" },
  { key: "table_header", label: "표 머리칸 넣기" },
  { key: "merge_pages", label: "페이지 합치기" },
  { key: "split_page", label: "페이지 나누기" },
];

function renderPresetPanel(body, page, idx) {
  body.append(el("hr", { class: "section-divider" }));
  body.append(el("h3", {}, "프리셋 버튼"));
  body.append(el("p", { class: "hint" }, "LLM 없이 바로 적용되는 고정 패치입니다. 눌러도 바로 저장되지 않고 먼저 미리보기로 보여줍니다."));
  const grid = el("div", { class: "preset-grid" });
  PRESET_DEFS.forEach(def => {
    if (def.types && !def.types.has(page.type)) return;
    const btn = el("button", { class: "btn btn--small" }, def.label);
    btn.onclick = () => openPresetPreview(idx, def.key, def.label);
    grid.append(btn);
  });
  body.append(grid);
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

async function openPresetPreview(pageIdx, presetKey, label) {
  try {
    const res = await api(`/api/editions/${editionId}/preset-preview`, {
      method: "POST",
      body: JSON.stringify({ preset: presetKey, page_idx: pageIdx }),
    });
    const key = `preset-preview:${editionId}`;
    sessionStorage.setItem(key, JSON.stringify(res.layout));
    state.presetPreview = { pageIdx, preset: presetKey, label, summary: res.summary, key, showingPreview: true };
    renderPresetPreviewBar();
    await loadFrameWithSrc(previewSrc(key), pageIdx);
  } catch (e) {
    toast(`적용 불가: ${e.message}`);
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
    const data = await api(`/api/editions/${editionId}/preset-apply`, {
      method: "POST",
      body: JSON.stringify({ preset: pp.preset, page_idx: pp.pageIdx, editor: "reviewer", expected_rev: state.rev }),
    });
    state.layout = data.layout; state.rev = data.rev; state.status = data.status;
    sessionStorage.removeItem(pp.key);
    state.presetPreview = null;
    renderPresetPreviewBar();
    await refreshAll();
    toast(`적용했습니다: ${pp.label}`);
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
    toast(`적용 실패: ${e.message}`);
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

  if (page.q) renderQuestionInspector(body, page, idx, basePath);
  if (SLOT_CAPABLE_TYPES.has(page.type)) renderSlotInspector(body, page, idx, basePath);
  renderPresetPanel(body, page, idx);

  body.append(el("hr", { class: "section-divider" }));
  body.append(el("div", { class: "todo-note" },
    "페이지 순서 이동은 페이지 목록의 ▲▼ 버튼으로 할 수 있습니다."));
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
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { items.splice(i, 1); rerender(); save({ blanks: items }); } }, "삭제");
        labelInput.onblur = () => { items[i].label = labelInput.value; save({ blanks: items }); };
        listHost.append(el("div", { class: "subfield-row" }, [labelInput, removeBtn]));
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
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { rows.splice(i, 1); rerender(); save({ rows }); } }, "삭제");
        labelInput.onblur = () => { rows[i].label = labelInput.value; save({ rows }); };
        hintInput.onblur = () => { rows[i].hint = hintInput.value; save({ rows }); };
        listHost.append(el("div", { class: "subfield-row" }, [labelInput, hintInput, removeBtn]));
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
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { items.splice(i, 1); rerender(); save({ items }); } }, "삭제");
        hintInput.onblur = () => { items[i].hint = hintInput.value; save({ items }); };
        listHost.append(el("div", { class: "subfield-row" }, [hintInput, removeBtn]));
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
        const nInput = el("input", { type: "number", placeholder: "번호 칸(선택)", value: c.n || "", min: "0", style: "width:80px" });
        const removeBtn = el("button", { class: "btn btn--small btn--danger", onclick: () => { cards.splice(i, 1); rerender(); save({ cards: cards.map(cleanCard) }); } }, "삭제");
        titleInput.onblur = () => { cards[i].title = titleInput.value; save({ cards: cards.map(cleanCard) }); };
        nInput.onblur = () => {
          cards[i].n = nInput.value ? parseInt(nInput.value, 10) : null;
          // 사용자 지시 2번: 한쪽에 번호 칸이 있으면 반대쪽도 같은 n으로 맞춘다(비교형 대칭 규칙)
          if (cards[i].n) cards.forEach(c => { c.n = cards[i].n; });
          rerender(); save({ cards: cards.map(cleanCard) });
        };
        listHost.append(el("div", { class: "subfield-row" }, [titleInput, nInput, removeBtn]));
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
      btn.onclick = () => patch([{
        op: "replace", path: slotPath,
        value: { img: img.file_path, src: `원본 ${img.source_page}쪽` },
      }], { reason: "검수: 이미지 자리에 원본 이미지 지정" });
      pickHost.append(btn);
    });
  });

  const sceneInput = el("textarea", { rows: "2" }, slot.scene || "");
  const avoidInput = el("textarea", { rows: "2" }, slot.avoid || "");
  body.append(field("또는 생성 지시문 - 장면(scene)", sceneInput));
  body.append(field("생성 지시문 - 금지 요소(avoid)", avoidInput,
    el("div", { class: "hint" }, "실제 이미지 생성 연동은 다음 단계에서 붙습니다.")));
  const saveGen = () => patch([{
    op: "replace", path: slotPath,
    value: { scene: sceneInput.value, avoid: avoidInput.value },
  }], { reason: "검수: 이미지 생성 지시문 수정" });
  sceneInput.onblur = () => { if (sceneInput.value !== (slot.scene || "")) saveGen(); };
  avoidInput.onblur = () => { if (avoidInput.value !== (slot.avoid || "")) saveGen(); };

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

/* ============ 부팅 ============ */
(async function boot() {
  if (!editionId) {
    document.body.innerHTML = '<p style="padding:24px">URL에 ?edition=&lt;id&gt; 가 필요합니다.</p>';
    return;
  }
  try {
    await refreshAll();
  } catch (e) {
    toast(`불러오기 실패: ${e.message}`);
  }
})();
