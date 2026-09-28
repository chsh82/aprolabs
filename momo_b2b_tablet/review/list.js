/* 검수 목록 화면 (2026-09-28) - index.html이 edition 번호 없이 열리면 이 모듈이
 * /api/editions를 불러와 표로 보여준다. 클릭하면 해당 문서 검수 화면(index.html?edition=..)으로 이동.
 */
const $ = sel => document.querySelector(sel);

const state = {
  page: 1, per_page: 50, total: 0,
  level: "", quarter: "", status: "", search: "", sort: "default",
};

let searchTimer = 0;

function statusBadge(status) {
  const label = { draft: "draft", review: "review", approved: "approved", published: "published" }[status] || status;
  return `<span class="badge" data-status="${status}">${label}</span>`;
}

function fmtDate(iso) {
  if (!iso) return "-";
  try {
    const d = new Date(iso);
    return d.toLocaleString("ko-KR", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
  } catch (e) { return iso; }
}

async function load() {
  const params = new URLSearchParams({
    page: state.page, per_page: state.per_page, sort: state.sort,
  });
  if (state.level) params.set("level", state.level);
  if (state.quarter) params.set("quarter", state.quarter);
  if (state.status) params.set("status", state.status);
  if (state.search) params.set("search", state.search);

  const res = await fetch(`/api/editions?${params}`);
  const data = await res.json();
  state.total = data.total;
  renderRows(data.docs);
  renderPager();
}

function renderRows(docs) {
  const body = $("#listBody");
  if (!docs.length) {
    body.innerHTML = `<tr><td colspan="9" style="text-align:center;padding:32px;color:var(--sub)">문서가 없습니다.</td></tr>`;
    return;
  }
  body.innerHTML = docs.map(d => `
    <tr data-edition="${d.edition_id}">
      <td>${d.doc_id}</td>
      <td>${d.title || "-"}</td>
      <td>LV${d.level ?? "-"}</td>
      <td>${d.quarter ?? "-"}분기</td>
      <td>${d.week ?? "-"}주차</td>
      <td>${statusBadge(d.status)}</td>
      <td class="num">${d.total_pages}</td>
      <td class="num"><span class="flag-count" data-zero="${d.flag_count === 0}">${d.flag_count}</span></td>
      <td>${d.last_editor ? `${d.last_editor} · ${fmtDate(d.last_edited_at)}` : "-"}</td>
    </tr>
  `).join("");
  body.querySelectorAll("tr[data-edition]").forEach(tr => {
    tr.onclick = () => { location.href = `index.html?edition=${tr.dataset.edition}`; };
  });
}

function renderPager() {
  const pager = $("#listPager");
  const totalPages = Math.max(1, Math.ceil(state.total / state.per_page));
  if (totalPages <= 1) { pager.innerHTML = ""; return; }
  let html = "";
  for (let p = 1; p <= totalPages; p++) {
    html += `<button data-page="${p}" aria-current="${p === state.page}">${p}</button>`;
  }
  pager.innerHTML = html;
  pager.querySelectorAll("button").forEach(b => {
    b.onclick = () => { state.page = parseInt(b.dataset.page, 10); load(); };
  });
}

function setupFilters() {
  const levelSel = $("#listLevel");
  for (let lv = 1; lv <= 9; lv++) levelSel.append(new Option(`LV${lv}`, lv));
  const qtrSel = $("#listQuarter");
  for (let q = 1; q <= 4; q++) qtrSel.append(new Option(`${q}분기`, q));

  $("#listSearch").oninput = e => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { state.search = e.target.value; state.page = 1; load(); }, 250);
  };
  levelSel.onchange = e => { state.level = e.target.value; state.page = 1; load(); };
  qtrSel.onchange = e => { state.quarter = e.target.value; state.page = 1; load(); };
  $("#listStatus").onchange = e => { state.status = e.target.value; state.page = 1; load(); };
  $("#listSort").onchange = e => { state.sort = e.target.value; state.page = 1; load(); };
}

setupFilters();
load();
