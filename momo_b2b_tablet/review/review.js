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
  state.selectedPageIdx = idx;
  renderPageList();
  renderInspector();
  goToPreviewPage(idx);
}

/* ============ 중앙 미리보기(iframe) ============ */
function previewSrc() {
  return `/renderer/viewer.html?doc=${encodeURIComponent(state.docId)}&mode=review&adapter=review&edition=${editionId}&_=${Date.now()}`;
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
  frame.src = previewSrc();
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
  if (page.slot) renderSlotInspector(body, page, idx, basePath);

  body.append(el("hr", { class: "section-divider" }));
  body.append(el("div", { class: "todo-note" },
    "답란 줄 수 조정·페이지 순서 이동은 다음 반복에서 연결 예정입니다(뼈대 우선순위 5번)."));
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

function renderSlotInspector(body, page, idx, basePath) {
  const slot = page.slot;
  const slotPath = `${basePath}/slot`;
  body.append(el("hr", { class: "section-divider" }));
  if (slot.img) {
    body.append(field("이미지 슬롯", el("div", { class: "hint" }, `원본 이미지 사용 중: ${slot.img}`)));
    return;
  }
  const sceneInput = el("textarea", { rows: "2" }, slot.scene || "");
  const avoidInput = el("textarea", { rows: "2" }, slot.avoid || "");
  body.append(field("이미지 생성 지시문 - 장면(scene)", sceneInput));
  body.append(field("이미지 생성 지시문 - 금지 요소(avoid)", avoidInput,
    el("div", { class: "hint" }, "실제 이미지 생성 연동은 다음 단계에서 붙습니다.")));
  sceneInput.onblur = () => { if (sceneInput.value !== (slot.scene || "")) patch([{ op: "replace", path: `${slotPath}/scene`, value: sceneInput.value }]); };
  avoidInput.onblur = () => { if (avoidInput.value !== (slot.avoid || "")) patch([{ op: "replace", path: `${slotPath}/avoid`, value: avoidInput.value }]); };
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
