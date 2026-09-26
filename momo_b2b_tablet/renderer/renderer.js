/* momo_b2b_tablet 렌더러 - prototype/template.html을 JSON 입력 + 어댑터 주입 구조로 모듈화한 것.
 * 원본 템플릿의 로직(조판 allocate/fit, 잉크 엔진, 인쇄, 답안 모아보기 등)은 그대로 이식했고
 * 바뀐 것은 세 가지뿐이다:
 *   1. __DATA__/__IMGS__로 HTML에 박아 넣던 값을 mountEdition()의 인자(edition, images)로 받는다.
 *   2. localStorage 직접 접근을 adapters.state.load()/save()로 바꾼다.
 *   3. window.claude.use("sample") 손글씨 인식을 adapters.recognize()로 바꾼다.
 * 세 어댑터 중 하나가 없어도(특히 recognize) 동작해야 한다 - adapters/local.js가 그 경우다.
 */

function esc(s) { return String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

function shellHtml({ logoUrl, brand, mode }) {
  return `
<header class="toolbar" role="toolbar" aria-label="학습지 도구">
  <div class="brand"><img src="${esc(logoUrl || "")}" alt="모모의 책장"><span>${esc(brand)}</span></div>
  <div class="grp" role="group" aria-label="쓰기 도구">
    <button class="tb" id="tPen" aria-pressed="true" title="펜"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20l4.5-1 10-10a2.1 2.1 0 0 0-3-3l-10 10L4 20z"/><path d="M13.5 7.5l3 3"/></svg><span class="lbl">펜</span></button>
    <button class="tb" id="tEraser" aria-pressed="false" title="지우개"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M7 20h11"/><path d="M5.5 14.5l8-8a2 2 0 0 1 2.8 0l2.2 2.2a2 2 0 0 1 0 2.8L12 18H8.5z"/><path d="M9.5 10.5l5 5"/></svg><span class="lbl">지우개</span></button>
    <button class="tb" id="tUndo" title="되돌리기" disabled><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9 14L4 9l5-5"/><path d="M4 9h10a6 6 0 0 1 0 12h-3"/></svg><span class="lbl">되돌리기</span></button>
  </div>
  <div class="grp">
    <button class="tb" id="tFinger" aria-pressed="false" title="손가락으로도 쓰기"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11V5a1.5 1.5 0 0 1 3 0v5"/><path d="M12 10V8.5a1.5 1.5 0 0 1 3 0V11"/><path d="M15 10.5a1.5 1.5 0 0 1 3 0V15a6 6 0 0 1-6 6h-1a5 5 0 0 1-4.2-2.3L4.5 15a1.5 1.5 0 0 1 2.5-1.7L9 16"/></svg><span class="lbl">손가락 쓰기</span></button>
    <button class="tb" id="tAnswers" title="쓴 답안 모아보기"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M8 4h8l3 3v13H5V4h3"/><path d="M8 11h8M8 15h6"/></svg><span class="lbl">답안 모아보기</span></button>
    ${mode === "review" ? `<button class="tb" id="tReview" aria-pressed="true" title="그림 자리에 생성 지시문 보기"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 16l5-5 4 4 3-3 6 6"/><circle cx="15.5" cy="9.5" r="1.5"/></svg><span class="lbl">그림 자리 설명</span></button>` : ""}
    <button class="tb" id="tPrint" title="A4 모아찍기로 인쇄"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M7 9V3h10v6"/><rect x="3" y="9" width="18" height="8" rx="2"/><path d="M7 14h10v7H7z"/></svg><span class="lbl">인쇄</span></button>
  </div>
  <div class="grp" role="group" aria-label="분기 색">
    <button class="tb sw" data-q="winter" title="겨울 고전"><i style="background:#1C2A39"></i><span class="lbl">겨울</span></button>
    <button class="tb sw" data-q="spring" title="봄 탐구"><i style="background:#3F7A46"></i><span class="lbl">봄</span></button>
    <button class="tb sw" data-q="summer" title="여름 문학"><i style="background:#0A6E92"></i><span class="lbl">여름</span></button>
    <button class="tb sw" data-q="autumn" title="가을 인문"><i style="background:#836B5D"></i><span class="lbl">가을</span></button>
  </div>
  <div class="grp" role="group" aria-label="페이지 이동">
    <button class="tb" id="tPrev" title="이전 페이지" aria-label="이전 페이지"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 5l-7 7 7 7"/></svg></button>
    <span class="pageno" id="pageNo" aria-live="polite">1 / 10</span>
    <button class="tb" id="tNext" title="다음 페이지" aria-label="다음 페이지"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5l7 7-7 7"/></svg></button>
  </div>
</header>

<main class="stage" id="stage"><div class="scaler" id="scaler"><div class="pages" id="pages"></div></div></main>

<section class="sheet" id="readSheet" aria-hidden="true" aria-labelledby="rsTitle">
  <h3 id="rsTitle">글자로 확인하기</h3>
  <p class="sq" id="rsQ"></p>
  <div class="pair">
    <div class="preview" id="rsPrev"></div>
    <div style="display:flex;flex-direction:column;gap:6px">
      <textarea id="rsText" aria-label="인식된 글자" placeholder="손글씨를 읽는 중이에요"></textarea>
      <div class="status" id="rsStatus"></div>
    </div>
  </div>
  <div class="row"><button id="rsClose">닫기</button><button class="primary" id="rsSave" disabled>이대로 저장</button></div>
</section>

<section class="sheet" id="ansSheet" aria-hidden="true" aria-labelledby="asTitle">
  <h3 id="asTitle">쓴 답안 모아보기</h3>
  <p class="sq">글자로 저장한 답안이 문항과 답란별로 모입니다. 빈칸이 나뉜 문항은 칸마다 따로 평가됩니다.</p>
  <div class="list" id="ansList"></div>
  <div class="row"><button id="asClose">닫기</button></div>
</section>

<div class="toast" id="toast" role="status"></div>
`;
}

/**
 * edition: { book, tone, quarter, pages } (samples/*.layout.json에서 schema/doc_id를 뺀 나머지)
 * images: { [key]: url } - layout JSON의 이미지 키 -> 실제 URL (data adapter가 이미 해석해서 넘김)
 * mode: "review" | "student" - review는 그림 자리 설명 토글·검수 전용 표시(.review 클래스)를 켠다
 * adapters.state: { load(): Promise<{ink,text,ox}>, save(state): void|Promise }
 * adapters.recognize: (({prompt, blob, partId, signal}) => Promise<{text, unclear}>) | undefined
 */
export async function mountEdition({ edition, images, mode = "review", brand, adapters = {} }) {
  const BOOK = edition.book, TONE = edition.tone, QUARTER = edition.quarter, PAGES = edition.pages;
  const IMG = images || {};

  document.title = `${BOOK.title} ${BOOK.week || ""} · 학습지`.trim();
  document.body.innerHTML = shellHtml({
    logoUrl: IMG.logo || IMG.logoIvory,
    brand: brand || `${BOOK.title} · ${TONE.grade || ""}`.trim(),
    mode,
  });
  document.body.dataset.mode = mode;

  /* ============ quarter palettes (design guide 1.2) + derived colours ============ */
  const PALETTES = {
    winter: { main: "#1C2A39", sub: "#F5F2E7", point: "#5A3E36" },
    spring: { main: "#3F7A46", sub: "#FFF8E7", point: "#E2703A" },
    summer: { main: "#0A6E92", sub: "#E8F8FF", point: "#B8830F" },
    autumn: { main: "#836B5D", sub: "#E8DCC3", point: "#A8761F" },
  };
  const PAL_NAME = { winter: "겨울 고전", spring: "봄 탐구", summer: "여름 문학", autumn: "가을 인문예술" };
  let CURQ = QUARTER;
  const hx = h => [1, 3, 5].map(i => parseInt(h.slice(i, i + 2), 16));
  const toHex = a => "#" + a.map(v => Math.round(Math.max(0, Math.min(255, v))).toString(16).padStart(2, "0")).join("");
  const mix = (a, b, t) => toHex(hx(a).map((v, i) => v + (hx(b)[i] - v) * t));
  const lum = h => { const [r, g, b] = hx(h).map(v => { v /= 255; return v <= .03928 ? v / 12.92 : Math.pow((v + .055) / 1.055, 2.4); }); return .2126 * r + .7152 * g + .0722 * b; };
  const contrast = (a, b) => { const x = lum(a), y = lum(b); return (Math.max(x, y) + .05) / (Math.min(x, y) + .05); };
  function applyPalette(q) {
    CURQ = q; const p = PALETTES[q], r = document.documentElement.style;
    let pt = p.point; for (let i = 0; i < 20 && contrast(pt, "#FFFDF7") < 4.5; i++) pt = mix(pt, "#000000", .08);
    r.setProperty("--main", p.main); r.setProperty("--sub", p.sub); r.setProperty("--point", p.point);
    r.setProperty("--main-deep", mix(p.main, "#000000", q === "winter" ? .12 : .25));
    r.setProperty("--main-soft", mix(p.main, "#FFFFFF", .8));
    r.setProperty("--point-text", pt);
    document.querySelectorAll(".spine .q, .cover .qname").forEach(el => el.textContent = PAL_NAME[q]);
    document.querySelectorAll("[data-q]").forEach(b => b.setAttribute("aria-pressed", b.dataset.q === q));
  }
  document.documentElement.style.setProperty("--line", TONE.line + "mm");
  document.body.classList.add("band-" + TONE.band);

  /* ============ render ============ */
  const QTEXT = {};
  const QREG = [];
  const PARENT = {};
  const CHOICE_IDS = new Set(); // "choice" 위젯의 #choice part id - 잉크 아님, 탭 선택
  function partsOf(q) { return (q.blanks || []).map(x => typeof x === "string" ? { label: x } : x); }
  function register(q, parts) { QREG.push({ id: q.id, no: q.no || q.id, text: q.t, parts }); parts.forEach(pt => { PARENT[pt.id] = q.t; }); }
  const LINES = Object.assign({ blankTall: [2, 4], long: [2, 4], short: [2, 3], blank: [1, 2], memo: [3, 3], vocab: [2, 3], one: [1, 1], row: [1, 2], cardInk: [2, 4], inline: [1, 1], cell: [2, 4], rowTall: [1, 3] }, TONE.lines || {});
  function inkBox(id, kind, starter = "", ph = "") {
    const [mn, mx] = LINES[kind];
    return `<div class="ink" data-id="${esc(id)}" data-min="${mn}" data-max="${mx}" style="height:calc(var(--line) * ${mn})">${starter ? `<span class="starter">${esc(starter)}</span>` : ""}${ph ? `<span class="ph">${esc(ph)}</span>` : ""}<canvas aria-label="필기 답란"></canvas><span class="badge"></span><button class="ink-read" type="button">글자로 확인</button></div>`;
  }
  function drawBox(id) {
    return `<div class="ink plain draw" data-id="${esc(id)}"><span class="ph">여기에 그려 보세요</span><canvas aria-label="그림 칸"></canvas><span class="badge"></span><button class="ink-read" type="button">글자로 확인</button></div>`;
  }
  const STYLE = (() => { const p = PALETTES[QUARTER]; return `${PAL_NAME[QUARTER]} 분기 하우스 스타일: ${p.main}·${p.sub}·${p.point} 3색 이내, 가는 잉크 선의 평면 일러스트, 배경은 종이색. 이미지 안에 글자 없음. 원작 삽화의 화풍과 인물 표현을 따라 하지 않는다. 흑백으로 복사해도 형태를 알아볼 수 있게.`; })();
  function slotHtml(s, q) {
    if (s.img) return `<figure class="slot img"><img src="${IMG[s.img]}" alt=""><figcaption class="sl-p">원본 교재 이미지를 그대로 씁니다${s.src ? ` (${esc(s.src)})` : ""}. 생성하지 않아요.</figcaption></figure>`;
    const prompt = `장면: ${s.scene}\n답 유출 금지: 질문 "${q.t}"의 답을 드러내지 않는다. ${s.avoid}\n${STYLE}`;
    return `<figure class="slot" data-prompt="${esc(prompt)}"><span class="sl-t">생성 이미지 자리</span><span class="sl-r"></span><span class="sl-p">${esc(prompt).replace(/\n/g, "<br>")}</span></figure>`;
  }
  function spine(pageNo, p) {
    const title = (p && p.spineTitle) || BOOK.spineTitle || BOOK.title;
    return `<aside class="spine" aria-hidden="true"><span class="lv">${((p && p.level) || BOOK.level).replace(" ", "")}</span><span class="v q">${PAL_NAME[CURQ]}</span><span class="v t">${esc(title)}</span><span class="dotline"></span><span class="no">${pageNo}</span></aside>`;
  }
  function head(p) {
    return `<div class="head"><span class="step">${p.step}</span><h2>${esc(p.title)}</h2><div class="guide"><div class="who"><span class="rt">${esc(p.guide.rt)}</span>${p.guide.nm ? `<span class="nm">${esc(p.guide.nm)}</span>` : ""}</div>${p.guide.img ? `<img src="${IMG[p.guide.img]}" alt="">` : ""}</div></div>`;
  }
  function widget(q) {
    const f = q.form || (q.blanks ? "blanks" : "single"); let parts = [], h = "";
    const P = (id, label, prompt) => { const pt = { id, label: label || "", prompt: prompt || "" }; parts.push(pt); return pt; };
    if (f === "blanks") {
      partsOf(q).forEach((b, k) => { const pt = P(`${q.id}#${k + 1}`, b.label, b.prompt); h += `<div class="blank"><span class="lab">${esc(pt.label)}</span>${inkBox(pt.id, "blankTall")}</div>`; });
      h = `<div class="col" data-alloc>${h}</div>`;
    } else if (f === "single" || f === "speech") {
      P(q.id);
      const starter = q.starter && q.starter !== q.t ? q.starter : "";
      h = `<div class="col" data-alloc>${f === "speech" ? `<div class="bubble">${esc(q.starter)}</div>${inkBox(q.id, q.kind || "cardInk")}` : inkBox(q.id, q.kind || "long", starter)}</div>`;
    } else if (f === "list") {
      h = `<div class="nlist" data-alloc>${q.items.map((it, k) => { const pt = P(`${q.id}#${k + 1}`, `${k + 1}번${it.hint ? " (" + it.hint + ")" : ""}`, it.prompt); return `<div class="nrow"><div class="nside"><span class="nn">${k + 1}</span>${it.hint ? `<span class="hint">${esc(it.hint)}</span>` : ""}</div><div class="nb">${inkBox(pt.id, q.rowKind || "row")}</div></div>`; }).join("")}</div>`;
    } else if (f === "compare") {
      const cards = q.cards.map((c, ci) => {
        if (c.n) { const rows = Array.from({ length: c.n }, (_, k) => { const pt = P(`${q.id}#${ci + 1}-${k + 1}`, `${c.title} ${k + 1}`, c.prompt); return `<div class="nrow"><span class="nn">${k + 1}</span><div class="nb">${inkBox(pt.id, "row")}</div></div>`; }).join("");
          return `<div class="card" data-alloc><h4>${esc(c.title)}</h4><div class="nlist in">${rows}</div></div>`; }
        const pt = P(`${q.id}#${ci + 1}`, c.title, c.prompt);
        return `<div class="card" data-alloc><h4>${esc(c.title)}</h4>${inkBox(pt.id, "cardInk")}</div>`;
      }).join("");
      h = `<div class="cmp" style="grid-template-columns:${q.img ? "0.75fr " : ""}${q.cards.map(() => "1fr").join(" ")}">${q.img ? `<figure class="cmp-fig"><img src="${IMG[q.img]}" alt=""></figure>` : ""}${cards}</div>`;
    } else if (f === "table") {
      h = `<div class="atable" data-alloc>${q.rows.map((r, k) => { const pt = P(`${q.id}#${k + 1}`, r.label, r.prompt); return `<div class="ar"><div class="ah">${esc(r.label)}${r.hint ? `<small>${esc(r.hint)}</small>` : ""}</div><div class="ac">${inkBox(pt.id, "cell").replace('class="ink"', 'class="ink noframe"')}</div></div>`; }).join("")}</div>`;
    } else if (f === "pledge") {
      const nm = P(`${q.id}#name`, "서약하는 사람");
      const rows = Array.from({ length: q.n || 3 }, (_, k) => { const pt = P(`${q.id}#${k + 1}`, `서약 ${k + 1}`); return `<div class="nrow"><span class="nn">${k + 1}</span><div class="nb">${inkBox(pt.id, "row")}</div></div>`; }).join("");
      h = `<div class="pledge" data-alloc><div class="pl-title">${inkBox(nm.id, "inline")}<span>의 서약서</span></div><div class="nlist in">${rows}</div></div>`;
    } else if (f === "choice") {
      // compare(f==="compare") 위젯을 확장한 형태: 카드 두 장을 탭으로 고르고(#choice),
      // 그 아래 한 칸에 이유를 쓴다(#reason) - "무엇을 골랐나"와 "왜 그렇게 생각하나"를
      // 따로 채점할 수 있게 part를 나눈다(사용자 지시).
      const choicePt = P(`${q.id}#choice`, "선택");
      const reasonPt = P(`${q.id}#reason`, "이유");
      CHOICE_IDS.add(choicePt.id);
      const cards = q.cards.map((c, ci) => `<button type="button" class="card choice-card" data-id="${esc(choicePt.id)}" data-text="${esc(c.title)}" aria-pressed="false"><span class="choice-badge">${String.fromCharCode(65 + ci)}</span><h4>${esc(c.title)}</h4></button>`).join("");
      h = `<div class="choice-wrap" data-alloc><div class="cmp choice-cards" data-id="${esc(choicePt.id)}" style="grid-template-columns:repeat(${q.cards.length},1fr)">${cards}</div><div class="blank"><span class="lab">이유</span>${inkBox(reasonPt.id, "cardInk")}</div></div>`;
    } else if (f === "choiceList") {
      // choice_multi(N지선다, 2~5개) - 세로 목록형 탭 선택, part 하나(#choice)만
      // 쓴다(사용자 지시 2026-09-23: "이유 칸 없이 생성" - 질문 문구로 신뢰할 수
      // 있는 규칙을 못 찾아서 검수에서 필요하면 추가하는 쪽으로 감). 기본은
      // single 선택(q.single !== false)이지만 검수에서 single을 꺼서 복수
      // 선택으로 바꿀 수 있게 위젯 자체는 둘 다 지원한다 - 그때 값은 배열이 된다.
      const choicePt = P(`${q.id}#choice`, "선택");
      CHOICE_IDS.add(choicePt.id);
      const single = q.single !== false;
      const rows = (q.options || []).map((opt, i) => `<button type="button" class="choice-row" data-id="${esc(choicePt.id)}" data-text="${esc(opt)}" aria-pressed="false"><span class="choice-badge">${i + 1}</span><span class="choice-row__text">${esc(opt)}</span></button>`).join("");
      h = `<div class="choice-list-wrap" data-alloc><div class="choice-list" data-id="${esc(choicePt.id)}" data-single="${single}">${rows}</div></div>`;
    }
    parts.forEach(pt => { QTEXT[pt.id] = pt.prompt || (pt.label ? `${q.t} (${pt.label})` : q.t); });
    register(q, parts);
    return h;
  }
  function qHead(q) { QTEXT[q.id] = q.t; return `<div class="qhead"><span class="stamp">${esc(q.id)}</span><div class="qtext">${esc(q.t)}</div></div>`; }
  function question(q) {
    if (q.form && q.form !== "blanks") { return `<div class="q qw">${qHead(q)}${widget(q)}</div>`; }
    QTEXT[q.id] = q.t;
    const mark = q.kind === "memo" ? `<span class="num-sq">${esc(q.no)}</span>` : `<span class="stamp">${esc(q.id)}</span>`;
    const headHtml = `<div class="qhead">${mark}<div class="qtext">${esc(q.t)}</div></div>`;
    if (q.kind === "multi") {
      const parts = partsOf(q).map((b, i) => ({ id: `${q.id}#${i + 1}`, label: b.label, prompt: b.prompt || "" }));
      parts.forEach(pt => { QTEXT[pt.id] = pt.prompt || `${q.t} (${pt.label})`; });
      register(q, parts);
      const body = parts.map(pt => `<div class="blank"><span class="lab">${esc(pt.label)}</span>${inkBox(pt.id, "blank")}</div>`).join("");
      return `<div class="q">${headHtml}${body}</div>`;
    }
    register(q, [{ id: q.id, label: "", prompt: "" }]);
    return `<div class="q">${headHtml}${inkBox(q.id, q.kind, q.starter || "")}</div>`;
  }
  function renderPage(p, i) {
    if (p.type === "blank") {
      // ⑥단계 인쇄 전용(edition/store.py의 print_view) - 쪽수가 홀수일 때 2-up 짝을
      // 맞추려고 서버가 임시로 덧붙이는 완전히 빈 면. 스파인·헤더 없이 진짜 백지.
      return "";
    }
    if (p.type === "cover") {
      return `<div class="cover"><div class="l"><img class="logo" src="${IMG.logoIvory}" alt="모모의 책장"><div class="meta"><span>${BOOK.level}</span><span class="qname">${PAL_NAME[CURQ]}</span><span>${BOOK.week}</span><i></i></div><h1>${esc(BOOK.title)}</h1>${BOOK.subtitle ? `<div class="sub">${esc(BOOK.subtitle)}</div>` : ""}<div class="au">${esc(BOOK.byline || BOOK.author)}</div><div class="dots"></div><blockquote>${esc(BOOK.quote)}</blockquote></div><div class="r"><img src="${IMG[BOOK.cover]}" alt="『${esc(BOOK.title)}』 표지"></div></div>`;
    }
    let inner = "";
    if (p.type === "vocab") {
      const v = p.vocab.map(x => { QTEXT["V-" + x.w] = `'${x.w}'(으)로 문장 만들기`; return `<div class="vcard" data-alloc><div class="vtop"><b>${esc(x.w)}</b>${x.p ? `<span class="pg">p.${x.p}</span>` : ""}${x.sup ? `<span class="sup">뜻 보충: 검수 필요</span>` : ""}</div><p class="df">${esc(x.d)}</p><span class="vlab">문장 만들기</span>${inkBox("V-" + x.w, "vocab")}</div>`; }).join("");
      inner = `<p class="inst">${esc(p.inst || "뜻을 읽고, 낱말을 넣어 나만의 문장을 만들어 봅시다.")}</p><div class="vgrid" style="grid-template-columns:repeat(${p.vocab.length > 4 ? 3 : 2},1fr)">${v}</div>`;
    } else if (p.type === "oxp") {
      const o = p.ox.map((x, k) => `<div class="oxitem"><span class="n">${k + 1}</span><span class="st">${esc(x.s)}<span>p.${x.p}</span></span><span class="ox" data-ox="${k + 1}"><button type="button" aria-pressed="false" aria-label="${k + 1}번 O">O</button><button type="button" aria-pressed="false" aria-label="${k + 1}번 X">X</button></span></div>`).join("");
      const oxList = `<p class="inst">책의 내용과 맞으면 O, 틀리면 X를 누르세요.</p><div class="oxlist">${o}</div>`;
      // 저학년 STEP1(낱말->생각상자->OX)처럼 이미지 슬롯이 없는 OX 페이지도 있다(SPEC §3.4) -
      // 그때는 한 칸짜리로, 슬롯이 있으면 원래대로 두 칸으로 나눈다.
      inner = p.slot
        ? `<div class="cols" style="grid-template-columns:1.45fr 1fr"><div class="col">${oxList}</div><div class="col" data-alloc>${slotHtml(p.slot, { t: "O·X 내용 확인 5문항" })}</div></div>`
        : `<div class="col">${oxList}</div>`;
    } else if (p.type === "excerpt") {
      // 제시문 전용 페이지(2026-09-26, 긴 제시문 분리 - SPEC §3.5.3) - 문항 없이
      // 제시문만 전체 폭으로 보여준다. 다음 쪽에 이어지면 안내 문구를 더한다.
      const ex = `<div class="excerpt fill">${p.excerpt.text.map(t => `<p>${esc(t)}</p>`).join("")}${p.excerpt.p ? `<span class="cite">p.${p.excerpt.p}</span>` : ""}</div>`;
      inner = `<div class="col">${ex}</div>${p.continues ? `<p class="excerpt-cont">다음 쪽에 이어집니다 ▶</p>` : ""}`;
    } else if (p.type === "qa") {
      // 2026-09-26 좌우 배치 - A안(이미지+문항+답란 왼쪽/제시문 오른쪽)을 적용해
      // 5종 검수했더니 오히려 기존(제시문 왼쪽/이미지+문항+답란 오른쪽)이 낫다는
      // 판단으로 되돌림(사용자 지시, 야옹아·젊은 예술가·긴긴밤·열하일기·두근두근
      // 한국사 전부에서 동일하게 요청) - qa는 원래 형태로 복귀.
      const contNote = p.excerpt.continued ? `<p class="excerpt-cont">◀ 앞쪽에서 이어짐</p>` : "";
      const ex = `${contNote}<div class="excerpt fill">${p.excerpt.text.map(t => `<p>${esc(t)}</p>`).join("")}<span class="cite">p.${p.excerpt.p}</span></div>`;
      inner = `<div class="cols" style="grid-template-columns:${p.ratio}"><div class="col">${ex}</div><div class="col" data-alloc>${p.slot ? slotHtml(p.slot, p.q) : ""}${question(p.q)}</div></div>`;
    } else if (p.type === "essay") {
      // 2026-09-26 STEP3 첫 페이지 규칙(사용자 지시, 5종 공통) - 글쓰기 전체
      // 질문(주제)은 단 구분 없이 페이지 중앙 상단에 큰 폰트로, 그 아래를
      // 2단(도입·인용 / 이미지)으로 구성한다. slotHtml 두 번째 인자는 생성
      // 지시문에 넣을 "질문 원문" 자리라 특정 책 제목을 박아 두면 안 되므로
      // topic으로 일반화한다(예전엔 다른 책 제목이 하드코딩돼 있던 버그).
      inner = `<h3 class="essay-topic">${esc(p.topic)}</h3><div class="cols" style="grid-template-columns:1fr 1.15fr"><div class="col">${p.lead ? `<p class="lead">${esc(p.lead)}</p>` : ""}<div class="dialog">${p.dialog.map(t => `<p>${esc(t)}</p>`).join("")}</div><p class="closing">${esc(p.closing)}</p></div><div class="col" data-alloc>${slotHtml(p.slot, { t: p.topic })}</div></div>`;
    } else if (p.type === "bgline") {
      const rows = p.rows.map(row => `<div class="tl-row">${row.map((n, j) => `${j ? '<span class="tl-arr" aria-hidden="true"></span>' : ""}<div class="tl-n${n.hl ? " hl" : ""}${n.end ? " end" : ""}"><span class="y">${esc(n.y || "")}</span><span class="e">${esc(n.e)}</span>${n.nt ? `<span class="nt">${esc(n.nt)}</span>` : ""}</div>`).join("")}</div>`).join("");
      inner = `<p class="inst">${esc(p.inst)}</p><div class="tl">${rows}</div><div class="bg-bottom"><figure class="bg-fig"><img src="${IMG[p.image.key]}" alt="${esc(p.image.caption)}"><figcaption>${esc(p.image.caption)}</figcaption></figure><div class="term"><h4>${esc(p.term.title)}</h4><p>${esc(p.term.text)}</p></div></div>`;
    } else if (p.type === "bgtext") {
      // 중등 배경지식이 연표가 아니라 산문형일 때(layout/background.py의 bgtext) - 좌 본문/우 이미지
      const paras = p.paragraphs.map(t => `<p>${esc(t)}</p>`).join("");
      const body = `<div class="bgtext-body">${paras}</div>`;
      inner = p.slot
        ? `<div class="cols" style="grid-template-columns:1.3fr 1fr"><div class="col">${body}</div><div class="col" data-alloc>${slotHtml(p.slot, { t: p.title })}</div></div>`
        : `<div class="col">${body}</div>`;
    } else if (p.type === "draw") {
      QTEXT[p.id] = p.inst;
      inner = `<div class="draw-top"><p class="inst">${esc(p.inst)}</p><span class="chip">${esc(p.chip || "그림 칸은 글자로 바꾸지 않아요")}</span></div>${drawBox(p.id)}`;
    } else if (p.type === "qaband") {
      const band = `${p.excerpt.continued ? `<p class="excerpt-cont">◀ 앞쪽에서 이어짐</p>` : ""}<div class="excerpt band">${p.excerpt.text.map(x => `<p>${esc(x)}</p>`).join("")}<span class="cite">p.${esc(p.excerpt.p)}</span></div>`;
      if (p.wide) {
        inner = `${band}${qHead(p.q)}${widget(p.q)}`;
      } else {
        const w = widget(p.q);
        inner = `${band}<div class="cols" style="grid-template-columns:${p.ratio || "1fr 1.15fr"}"><div class="col" data-alloc>${qHead(p.q)}${p.slot ? slotHtml(p.slot, p.q) : ""}</div><div class="col wcol">${w}</div></div>`;
      }
    } else if (p.type === "qaref") {
      const band = `${p.excerpt.continued ? `<p class="excerpt-cont">◀ 앞쪽에서 이어짐</p>` : ""}<div class="excerpt band">${p.excerpt.text.map(x => `<p>${esc(x)}</p>`).join("")}<span class="cite">p.${esc(p.excerpt.p)}</span></div>`;
      const rows = p.ref.rows.map(r => r.gap ? `<tr class="gap"><td colspan="2">⋯</td></tr>` : `<tr><th>${esc(r.n)}</th><td>${esc(r.v)}</td></tr>`).join("");
      const ref = `<div class="ref"><h4>${esc(p.ref.title)}</h4><div class="ref-body"><table class="rt"><tbody>${rows}</tbody></table>${p.ref.img ? `<img src="${IMG[p.ref.img]}" alt="">` : ""}</div>${p.ref.note ? `<p class="note">${esc(p.ref.note)}</p>` : ""}</div>`;
      inner = `${band}<div class="cols" style="grid-template-columns:${p.ratio || "1fr 1fr"}"><div class="col">${ref}</div><div class="col">${qHead(p.q)}<div class="wcol">${widget(p.q)}</div></div></div>`;
    } else if (p.type === "solo") {
      const w = widget(p.q);
      inner = `<div class="cols" style="grid-template-columns:${p.ratio || "1fr 1.25fr"}"><div class="col" data-alloc>${p.slot ? slotHtml(p.slot, p.q) : ""}</div><div class="col">${qHead(p.q)}<div class="wcol">${w}</div></div></div>`;
    } else if (p.type === "memos") {
      const rows = p.qs.map(q => { QTEXT[q.id] = q.t; register(q, [{ id: q.id, label: "", prompt: "" }]); return `<div class="mrow"><div class="mq"><span class="num-sq">${esc(q.no)}</span><div class="qtext">${esc(q.t)}</div></div>${inkBox(q.id, "memo")}</div>`; }).join("");
      inner = `<div class="mtop"><span class="mtopic">${esc(p.topic)}</span><span class="mlead">${esc(p.closing)}</span></div><div class="mrows">${rows}</div>`;
    }
    return `<div class="frame">${spine(i, p)}<div class="body">${head(p)}${inner}</div></div>`;
  }
  const pagesEl = document.getElementById("pages");
  pagesEl.innerHTML = PAGES.map((p, i) => `<section class="page ${p.type}" data-i="${i}" aria-label="${i === 0 ? "표지" : i + "쪽"}">${renderPage(p, i)}</section>`).join("");
  const pageEls = [...pagesEl.children];

  /* ============ state (adapter 주입) ============ */
  let S = { ink: {}, text: {}, ox: {}, choice: {} };
  if (adapters.state && adapters.state.load) {
    try { const loaded = await adapters.state.load(); if (loaded) S = Object.assign(S, loaded); } catch (e) {}
  }
  let saveT = 0;
  function persist() {
    if (!adapters.state || !adapters.state.save) return;
    clearTimeout(saveT);
    saveT = setTimeout(() => { try { adapters.state.save(S); } catch (e) {} }, 400);
  }

  /* ============ ink engine ============ */
  const MM = 96 / 25.4, LINE_PX = TONE.line * MM;
  const RULE = getComputedStyle(document.documentElement).getPropertyValue("--rule").trim() || "#C9C0AB";
  const tools = { mode: "pen", finger: false };
  const history = [];
  let lastPenAt = 0;
  const boxes = new Map();

  class InkBox {
    constructor(el) {
      this.el = el; this.id = el.dataset.id; this.cv = el.querySelector("canvas"); this.ctx = this.cv.getContext("2d");
      this.strokes = S.ink[this.id] || []; this.cur = null; this.k = 1;
      this.cv.addEventListener("pointerdown", e => this.down(e));
      this.cv.addEventListener("pointermove", e => this.move(e));
      ["pointerup", "pointercancel", "lostpointercapture"].forEach(t => this.cv.addEventListener(t, e => this.up(e)));
      el.querySelector(".ink-read").addEventListener("click", e => { e.stopPropagation(); openRead(this); });
      el.querySelector(".ink-read").addEventListener("pointerdown", e => e.stopPropagation());
      this.syncState();
    }
    size(k) {
      const w = this.cv.offsetWidth, h = this.cv.offsetHeight; if (!w || !h) return;
      this.k = k; this.cv.width = Math.round(w * k); this.cv.height = Math.round(h * k); this.draw();
    }
    pt(e) {
      const r = this.cv.getBoundingClientRect(); const f = this.cv.offsetWidth / r.width;
      const p = e.pointerType === "pen" ? Math.max(.12, e.pressure || .5) : .5;
      return [(e.clientX - r.left) * f, (e.clientY - r.top) * f, p];
    }
    accepts(e) { return e.pointerType !== "touch" || tools.finger; }
    down(e) {
      if (!this.accepts(e)) return;
      e.preventDefault(); e.stopPropagation();
      if (e.pointerType === "pen") lastPenAt = Date.now();
      this.cv.setPointerCapture(e.pointerId);
      if (tools.mode === "eraser") { this.erasing = { removed: [] }; this.eraseAt(this.pt(e)); return; }
      this.cur = { w: 2.1, pts: [this.pt(e)] }; this.strokes.push(this.cur); this.drawStroke(this.cur);
    }
    move(e) {
      if (!this.cur && !this.erasing) return;
      if (e.pointerType === "pen") lastPenAt = Date.now();
      const evs = e.getCoalescedEvents ? e.getCoalescedEvents() : [e];
      for (const ev of (evs.length ? evs : [e])) {
        const p = this.pt(ev);
        if (this.erasing) { this.eraseAt(p); continue; }
        const a = this.cur.pts[this.cur.pts.length - 1];
        if (Math.hypot(p[0] - a[0], p[1] - a[1]) < .6) continue;
        this.cur.pts.push(p); this.seg(this.cur, this.cur.pts.length - 1);
      }
    }
    up() {
      if (this.erasing) {
        if (this.erasing.removed.length) { history.push({ box: this, type: "erase", items: this.erasing.removed }); this.commit(); }
        this.erasing = null; return;
      }
      if (!this.cur) return;
      history.push({ box: this, type: "add", stroke: this.cur }); this.cur = null; this.commit();
    }
    eraseAt(p) {
      const R = 7;
      for (let i = this.strokes.length - 1; i >= 0; i--) {
        const s = this.strokes[i];
        if (s.pts.some(q => Math.hypot(q[0] - p[0], q[1] - p[1]) < R)) { this.erasing.removed.push({ s, i }); this.strokes.splice(i, 1); }
      }
      if (this.erasing.removed.length) this.draw();
    }
    commit() {
      S.ink[this.id] = this.strokes;
      const t = S.text[this.id]; if (t && t.saved) t.stale = true;
      this.syncState(); persist(); updateUndo();
    }
    syncState() {
      const has = this.strokes.length > 0, t = S.text[this.id];
      this.el.classList.toggle("has", has);
      this.el.classList.toggle("saved", !!(t && t.saved && !t.stale));
      this.el.classList.toggle("stale", !!(t && t.saved && t.stale && has));
      this.el.querySelector(".badge").textContent = (t && t.stale) ? "다시 확인" : "글자 저장됨";
    }
    draw() {
      const c = this.ctx, W = this.cv.width, H = this.cv.height;
      c.setTransform(1, 0, 0, 1, 0, 0); c.clearRect(0, 0, W, H);
      const lw = Math.max(1, Math.round(this.k * .95));
      c.fillStyle = RULE;
      const n = Math.max(1, Math.round(this.cv.offsetHeight / LINE_PX));
      if (!this.el.classList.contains("plain")) for (let i = 1; i < n; i++) { const y = Math.round(i * LINE_PX * this.k - lw / 2); c.fillRect(0, y, W, lw); }
      if (!this.el.classList.contains("noframe")) { c.fillRect(0, 0, W, lw); c.fillRect(0, H - lw, W, lw); c.fillRect(0, 0, lw, H); c.fillRect(W - lw, 0, lw, H); }
      for (const s of this.strokes) this.drawStroke(s);
    }
    style(c, s, p) { c.setTransform(this.k, 0, 0, this.k, 0, 0); c.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue("--pen").trim() || "#1B2740"; c.fillStyle = c.strokeStyle; c.lineCap = "round"; c.lineJoin = "round"; c.lineWidth = s.w * (.55 + .9 * p); }
    drawStroke(s) {
      if (s.pts.length === 1) { const [x, y, p] = s.pts[0]; const c = this.ctx; this.style(c, s, p); c.beginPath(); c.arc(x, y, c.lineWidth / 2, 0, Math.PI * 2); c.fill(); return; }
      for (let i = 1; i < s.pts.length; i++) this.seg(s, i);
    }
    seg(s, i) {
      const c = this.ctx, a = s.pts[i - 1], b = s.pts[i], z = s.pts[i - 2] || a;
      this.style(c, s, (a[2] + b[2]) / 2);
      const m1 = [(z[0] + a[0]) / 2, (z[1] + a[1]) / 2], m2 = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2];
      c.beginPath(); c.moveTo(i === 1 ? a[0] : m1[0], i === 1 ? a[1] : m1[1]); c.quadraticCurveTo(a[0], a[1], m2[0], m2[1]);
      if (i === s.pts.length - 1) c.lineTo(b[0], b[1]);
      c.stroke();
    }
    toBlob() {
      const w = this.cv.offsetWidth, h = this.cv.offsetHeight, k = 2.4;
      const cv = document.createElement("canvas"); cv.width = Math.round(w * k); cv.height = Math.round(h * k);
      const c = cv.getContext("2d"); c.fillStyle = "#fff"; c.fillRect(0, 0, cv.width, cv.height);
      c.setTransform(k, 0, 0, k, 0, 0); c.strokeStyle = "#111"; c.fillStyle = "#111"; c.lineCap = "round"; c.lineJoin = "round";
      for (const s of this.strokes) {
        c.beginPath(); c.lineWidth = s.w * 1.25;
        s.pts.forEach((p, j) => j ? c.lineTo(p[0], p[1]) : c.moveTo(p[0], p[1]));
        if (s.pts.length === 1) { c.arc(s.pts[0][0], s.pts[0][1], 1.2, 0, Math.PI * 2); c.fill(); } else c.stroke();
      }
      return new Promise(res => cv.toBlob(b => res({ blob: b, url: cv.toDataURL("image/png") }), "image/png"));
    }
  }
  document.querySelectorAll(".ink").forEach(el => boxes.set(el.dataset.id, new InkBox(el)));

  function updateUndo() { document.getElementById("tUndo").disabled = history.length === 0; }
  document.getElementById("tUndo").addEventListener("click", () => {
    const h = history.pop(); if (!h) return;
    const b = h.box;
    if (h.type === "add") { const i = b.strokes.lastIndexOf(h.stroke); if (i >= 0) b.strokes.splice(i, 1); }
    else { h.items.slice().reverse().forEach(({ s, i }) => b.strokes.splice(Math.min(i, b.strokes.length), 0, s)); }
    b.draw(); b.commit();
    const pi = +b.el.closest(".page").dataset.i; if (pi !== cur) go(pi);
  });
  function setMode(m) { tools.mode = m; document.getElementById("tPen").setAttribute("aria-pressed", m === "pen"); document.getElementById("tEraser").setAttribute("aria-pressed", m === "eraser"); }
  document.getElementById("tPen").addEventListener("click", () => setMode("pen"));
  document.getElementById("tEraser").addEventListener("click", () => setMode("eraser"));
  document.getElementById("tFinger").addEventListener("click", e => { tools.finger = !tools.finger; e.currentTarget.setAttribute("aria-pressed", tools.finger); toast(tools.finger ? "손가락으로도 쓸 수 있어요. 페이지는 위쪽 화살표로 넘기세요." : "펜으로만 써요. 손가락으로 밀면 페이지가 넘어가요."); });

  /* ============ OX ============ */
  document.querySelectorAll(".ox").forEach(g => {
    const k = g.dataset.ox, [bo, bx] = g.querySelectorAll("button");
    const paint = () => { bo.setAttribute("aria-pressed", S.ox[k] === "O"); bx.setAttribute("aria-pressed", S.ox[k] === "X"); };
    bo.addEventListener("click", () => { S.ox[k] = S.ox[k] === "O" ? undefined : "O"; paint(); persist(); });
    bx.addEventListener("click", () => { S.ox[k] = S.ox[k] === "X" ? undefined : "X"; paint(); persist(); });
    paint();
  });

  /* ============ 선택형(choice) 카드 탭 ============ */
  document.querySelectorAll(".choice-cards").forEach(group => {
    const id = group.dataset.id;
    const cards = [...group.querySelectorAll(".choice-card")];
    const paint = () => cards.forEach(b => b.setAttribute("aria-pressed", String(b.dataset.text === S.choice[id])));
    cards.forEach(b => b.addEventListener("click", () => {
      S.choice[id] = S.choice[id] === b.dataset.text ? undefined : b.dataset.text; // 다시 누르면 선택 해제
      paint(); persist();
    }));
    paint();
  });

  /* ============ 선택형(choiceList) 세로 목록 탭 ============ */
  document.querySelectorAll(".choice-list").forEach(group => {
    const id = group.dataset.id;
    const single = group.dataset.single === "true";
    const rows = [...group.querySelectorAll(".choice-row")];
    const paint = () => {
      const v = S.choice[id];
      rows.forEach(b => b.setAttribute("aria-pressed",
        String(single ? v === b.dataset.text : Array.isArray(v) && v.includes(b.dataset.text))));
    };
    rows.forEach(b => b.addEventListener("click", () => {
      if (single) {
        S.choice[id] = S.choice[id] === b.dataset.text ? undefined : b.dataset.text;
      } else {
        const selected = Array.isArray(S.choice[id]) ? S.choice[id].slice() : [];
        const i = selected.indexOf(b.dataset.text);
        if (i >= 0) selected.splice(i, 1); else selected.push(b.dataset.text);
        S.choice[id] = selected.length ? selected : undefined;
      }
      paint(); persist();
    }));
    paint();
  });

  /* ============ layout & paging ============ */
  let cur = 0, scale = 1;
  const stage = document.getElementById("stage"), scaler = document.getElementById("scaler");
  const SLOT_MIN = 30 * MM, GAP = 2.6 * MM;
  const RATIOS = [["1:1", 1], ["4:3", 4 / 3], ["3:4", 3 / 4], ["3:2", 3 / 2], ["2:1", 2]];
  function allocate() {
    document.querySelectorAll("[data-alloc]").forEach(col => {
      const inks = [...col.querySelectorAll(".ink[data-max]")].filter(el => el.parentElement.closest("[data-alloc]") === col), slot = [...col.children].find(c => c.classList.contains("slot"));
      if (slot) slot.hidden = true;
      const shrink = col.classList.contains("atable"); if (shrink) col.style.flex = "";
      inks.forEach(el => { el._n = +el.dataset.min; el.style.height = (el._n * LINE_PX) + "px"; });
      const used = () => { const kids = [...col.children].filter(k => !k.hidden); const last = kids[kids.length - 1]; if (!last) return 0; return last.offsetTop + last.offsetHeight - col.offsetTop; };
      let free = col.clientHeight - used();
      const even = col.classList.contains("nlist");
      while (even) {
        const c = inks.filter(el => el._n < +el.dataset.max);
        if (!c.length || free < LINE_PX * c.length) break;
        c.forEach(el => { el._n++; el.style.height = (el._n * LINE_PX) + "px"; }); free -= LINE_PX * c.length;
      }
      for (; !even;) {
        const c = inks.filter(el => el._n < +el.dataset.max).sort((a, b) => a._n / +a.dataset.max - b._n / +b.dataset.max)[0];
        if (!c || free < LINE_PX) break;
        c._n++; c.style.height = (c._n * LINE_PX) + "px"; free -= LINE_PX;
      }
      if (shrink) col.style.flex = "0 0 auto";
      if (slot && free - GAP >= SLOT_MIN) {
        slot.hidden = false;
        const r = slot.offsetWidth / slot.offsetHeight;
        const best = RATIOS.reduce((a, b) => Math.abs(Math.log(b[1] / r)) < Math.abs(Math.log(a[1] / r)) ? b : a);
        if (slot.querySelector(".sl-r")) slot.querySelector(".sl-r").textContent = `${Math.round(slot.offsetWidth / MM)} × ${Math.round(slot.offsetHeight / MM)}mm, ${best[0]} 비율로 생성`;
      }
    });
  }
  function fit() {
    allocate();
    const pw = pagesEl.offsetWidth, ph = pagesEl.offsetHeight, pad = 18;
    scale = Math.max(.2, Math.min((stage.clientWidth - pad * 2) / pw, (stage.clientHeight - pad * 2) / ph));
    scaler.style.transform = `scale(${scale})`; scaler.style.width = pw * scale + "px"; scaler.style.height = ph * scale + "px";
    const k = scale * (window.devicePixelRatio || 1);
    boxes.forEach(b => b.size(k));
  }
  function go(i) {
    cur = Math.max(0, Math.min(PAGES.length - 1, i));
    pageEls.forEach((el, j) => { el.classList.toggle("on", j === cur); el.setAttribute("aria-hidden", j !== cur); });
    document.getElementById("pageNo").textContent = `${cur + 1} / ${PAGES.length}`;
    document.getElementById("tPrev").disabled = cur === 0; document.getElementById("tNext").disabled = cur === PAGES.length - 1;
  }
  document.getElementById("tPrev").addEventListener("click", () => go(cur - 1));
  document.getElementById("tNext").addEventListener("click", () => go(cur + 1));
  addEventListener("keydown", e => { if (e.target.tagName === "TEXTAREA") return; if (e.key === "ArrowRight") go(cur + 1); if (e.key === "ArrowLeft") go(cur - 1); });
  addEventListener("resize", fit);
  if (window.visualViewport) visualViewport.addEventListener("resize", fit);

  let sw = null;
  stage.addEventListener("pointerdown", e => { if (e.pointerType !== "touch") return; if (tools.finger && e.target.tagName === "CANVAS") return; sw = { x: e.clientX, y: e.clientY, t: Date.now(), id: e.pointerId }; }, true);
  stage.addEventListener("pointerup", e => {
    if (!sw || e.pointerId !== sw.id) return;
    const dx = e.clientX - sw.x, dy = e.clientY - sw.y, dt = Date.now() - sw.t; sw = null;
    if (Date.now() - lastPenAt < 900) return;
    if (Math.abs(dx) > 70 && Math.abs(dx) > Math.abs(dy) * 1.6 && dt < 700) go(cur + (dx < 0 ? 1 : -1));
  }, true);

  function printPrep() { boxes.forEach(b => b.size(3.2)); }
  addEventListener("beforeprint", printPrep);
  addEventListener("afterprint", fit);
  document.getElementById("tPrint").addEventListener("click", () => { printPrep(); try { window.print(); } catch (e) { toast("이 화면에서는 인쇄를 열 수 없어요. 브라우저 메뉴의 인쇄를 이용하세요."); } });

  /* ============ toast ============ */
  let tt = 0;
  function toast(m) { const t = document.getElementById("toast"); t.textContent = m; t.classList.add("show"); clearTimeout(tt); tt = setTimeout(() => t.classList.remove("show"), 3200); }

  /* ============ handwriting -> text (adapters.recognize 주입) ============ */
  let recognizeAvailable = typeof adapters.recognize === "function";
  let readCtl = null, readBox = null;
  const rs = { sheet: document.getElementById("readSheet"), q: document.getElementById("rsQ"), prev: document.getElementById("rsPrev"), text: document.getElementById("rsText"), status: document.getElementById("rsStatus"), save: document.getElementById("rsSave") };
  function openSheet(el) { el.classList.add("open"); el.setAttribute("aria-hidden", "false"); }
  function closeSheet(el) { el.classList.remove("open"); el.setAttribute("aria-hidden", "true"); }
  function hideReading() { document.body.classList.remove("ready"); recognizeAvailable = false; }
  const COPY = {
    rate_limited: "요청이 많아 잠시 쉬어야 해요. 조금 뒤에 다시 눌러 주세요.",
    session_expired: "다시 로그인한 뒤 눌러 주세요.",
    image_rejected: "필기 이미지를 보낼 수 없었어요. 조금 더 써 본 뒤 다시 눌러 주세요.",
    refused: "이 필기는 글자로 바꿀 수 없었어요. 직접 입력해 주세요.",
    empty_completion: "글자를 찾지 못했어요. 직접 입력해 주세요.",
    invalid_json: "결과를 읽지 못했어요. 다시 눌러 주세요.",
    upstream_error: "연결이 끊겼어요. 다시 눌러 주세요.",
  };
  async function openRead(box) {
    if (!recognizeAvailable || !box.strokes.length) return;
    readBox = box; readCtl && readCtl.abort(); readCtl = new AbortController();
    rs.q.textContent = QTEXT[box.id] || ""; rs.text.value = ""; rs.text.placeholder = "손글씨를 읽는 중이에요"; rs.save.disabled = true; rs.status.textContent = "읽는 중…";
    const { blob, url } = await box.toBlob();
    rs.prev.innerHTML = `<img alt="보낸 필기" src="${url}">`;
    openSheet(rs.sheet);
    const prompt = [
      `이미지는 ${TONE.grade} 학생이 태블릿에 펜으로 쓴 한국어 손글씨 답안입니다.`,
      ...(PARENT[box.id] && PARENT[box.id] !== QTEXT[box.id]
        ? [`전체 질문: ${PARENT[box.id]}`, `이 답란이 답하는 부분: ${QTEXT[box.id]}`]
        : [`학생이 답한 질문: ${QTEXT[box.id] || "(없음)"}`]),
      "할 일: 이미지에 적힌 글자를 보이는 그대로 옮겨 적으세요.",
      "- 맞춤법과 띄어쓰기를 고치지 마세요. 내용을 보태거나 질문에 맞게 바꾸지 마세요.",
      "- 줄바꿈은 쓴 그대로 유지하세요.",
      "- 도저히 알아볼 수 없는 글자는 [?] 로 적으세요. 지운 흔적과 낙서는 무시하세요.",
      'JSON 하나로만 답하세요. 예: {"text": "옮겨 적은 내용", "unclear": 0}',
    ].join("\n");
    const ctl = readCtl;
    try {
      const r = await adapters.recognize({ prompt, blob, partId: box.id, signal: ctl.signal });
      if (ctl.signal.aborted) return;
      const txt = (r && typeof r.text === "string") ? r.text : "";
      rs.text.value = txt; rs.save.disabled = false;
      const n = Number(r && r.unclear) || 0;
      rs.status.textContent = n ? `알아보기 어려운 글자 ${n}개를 [?]로 표시했어요. 고친 뒤 저장하세요.` : "읽은 내용이 맞는지 확인하고, 틀린 곳은 고쳐서 저장하세요.";
      rs.text.focus({ preventScroll: true });
    } catch (e) {
      if (e && e.code === "cancelled") return;
      if (e && ["not_granted", "sampling_disabled", "not_declared", "capability_disabled", "capability_removed", "images_unavailable"].includes(e.code)) {
        hideReading(); rs.status.textContent = "이 화면에서는 글자 확인을 쓸 수 없어요. 아래에 직접 입력해 저장할 수 있어요."; rs.save.disabled = false; return;
      }
      rs.status.textContent = COPY[e && e.code] || COPY.upstream_error; rs.save.disabled = false;
    }
  }
  rs.text.addEventListener("input", () => { rs.save.disabled = false; });
  rs.save.addEventListener("click", () => {
    if (!readBox) return;
    S.text[readBox.id] = { text: rs.text.value.trim(), saved: true, stale: false, at: Date.now() };
    readBox.syncState(); persist(); closeSheet(rs.sheet); toast("글자로 저장했어요.");
  });
  document.getElementById("rsClose").addEventListener("click", () => { readCtl && readCtl.abort(); closeSheet(rs.sheet); });

  /* answers overview */
  function answerOf(id) {
    if (CHOICE_IDS.has(id)) {
      const v = S.choice[id];
      return { answer: v || "", confirmed: !!v, has_ink: false };
    }
    const t = S.text[id], b = boxes.get(id); return { answer: t && t.saved ? t.text : "", confirmed: !!(t && t.saved && !t.stale), has_ink: !!(b && b.strokes.length) };
  }
  function evalPayload() {
    const ox = []; PAGES.forEach(p => { if (p.type === "oxp") p.ox.forEach((x, k) => ox.push({ no: k + 1, statement: x.s, choice: S.ox[k + 1] || null })); });
    return {
      doc_id: BOOK.id, book: BOOK.title, level: BOOK.level, ox,
      questions: QREG.map(q => ({ question_id: q.id, number: q.no, question: q.text,
        parts: q.parts.map(pt => Object.assign({ part_id: pt.id }, pt.label ? { label: pt.label } : {}, pt.prompt ? { prompt: pt.prompt } : {}, answerOf(pt.id))) })),
    };
  }
  function stateLine(a) { return a.answer ? esc(a.answer) + (a.confirmed ? "" : "<br><small>(필기를 고친 뒤 다시 확인하지 않았어요)</small>") : (a.has_ink ? "필기만 있어요. 답란의 '글자로 확인'을 눌러 주세요." : "아직 쓰지 않았어요."); }
  document.getElementById("tAnswers").addEventListener("click", () => {
    const P = evalPayload(); let h = "";
    if (P.ox.length) h += `<div class="it"><div class="k">내용 확인 O·X</div><div class="v">${P.ox.map(o => `${o.no}. ${o.choice || "–"}`).join("   ")}</div></div>`;
    P.questions.forEach(q => {
      if (q.parts.length === 1 && !q.parts[0].label) { const a = q.parts[0]; h += `<div class="it"><div class="k">${esc(q.number)}. ${esc(q.question)}</div><div class="v ${a.answer ? "" : "empty"}">${stateLine(a)}</div></div>`; return; }
      h += `<div class="it"><div class="k">${esc(q.number)}. ${esc(q.question)}</div>${q.parts.map((a, i) => `<div class="pt"><div class="pl">(${i + 1}) ${esc(a.label)}${a.prompt ? ` <span>— ${esc(a.prompt)}</span>` : ""}</div><div class="v ${a.answer ? "" : "empty"}">${stateLine(a)}</div></div>`).join("")}</div>`;
    });
    h += `<details class="it json"><summary>평가용 데이터(JSON) 보기</summary><pre>${esc(JSON.stringify(P, null, 2))}</pre></details>`;
    document.getElementById("ansList").innerHTML = h;
    openSheet(document.getElementById("ansSheet"));
  });
  document.getElementById("asClose").addEventListener("click", () => closeSheet(document.getElementById("ansSheet")));

  if (mode === "review") {
    document.body.classList.add("review");
    const tReview = document.getElementById("tReview");
    if (tReview) tReview.addEventListener("click", e => { const on = !document.body.classList.contains("review"); document.body.classList.toggle("review", on); e.currentTarget.setAttribute("aria-pressed", on); });
  }
  document.querySelectorAll("[data-q]").forEach(b => b.addEventListener("click", () => applyPalette(b.dataset.q)));

  /* boot */
  applyPalette(QUARTER);
  go(0);
  requestAnimationFrame(() => { fit(); });
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(fit);
  setTimeout(() => toast("펜으로 답란에 써 보세요. 손가락으로 밀면 페이지가 넘어가요."), 600);
  if (recognizeAvailable) document.body.classList.add("ready");

  /* 외부(호스트 페이지·테스트)에 필요한 최소 API. printPrep/evalPayload/applyPalette는
   * 툴바 버튼이 내부적으로 쓰는 것과 같은 함수라 여기서도 그대로 노출한다 - 예: 웹뷰를 감싸는
   * 네이티브 앱이 자체 인쇄 버튼에서 printPrep()을 먼저 부르고 싶을 수 있다. */
  return { pageCount: PAGES.length, go, fit, printPrep, evalPayload, applyPalette };
}
