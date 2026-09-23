/* 2단계 완료 기준 확인: samples/*.layout.json 3종을 local 어댑터로 렌더한 결과가
 * prototype/out/*.html(원본 시안, prototype/build_local.py로 생성)과
 * 페이지 수 / 답란(.ink) 줄 수 / 이미지 슬롯 위치가 같은지 자동 비교한다.
 *
 * 실행: node tests/verify_stage2.js  (momo_b2b_tablet/을 http-server로 띄운 뒤 Playwright로 비교)
 */
const path = require("path");
const http = require("http");
const fs = require("fs");
const { chromium } = require("playwright");

const ROOT = path.resolve(__dirname, "..");
const PORT = 8971;

const MIME = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".json": "application/json",
  ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg" };

function startServer() {
  const server = http.createServer((req, res) => {
    const urlPath = decodeURIComponent(req.url.split("?")[0]);
    const filePath = path.join(ROOT, urlPath);
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end("not found: " + urlPath); return; }
      const ext = path.extname(filePath);
      res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
      res.end(data);
    });
  });
  return new Promise(resolve => server.listen(PORT, () => resolve(server)));
}

const DOCS = ["L5-Q3-W10", "L9-Q3-W07", "L2-Q2-W08"];
const VIEWPORT = { width: 1600, height: 1000 };

async function measure(page, url) {
  await page.goto(url, { waitUntil: "networkidle" });
  await page.waitForFunction(() => document.querySelector(".pages") && document.querySelector(".pages").children.length > 0);
  await page.evaluate(() => document.fonts && document.fonts.ready ? document.fonts.ready : null);
  await page.waitForTimeout(300);
  return page.evaluate(() => {
    const pages = [...document.querySelectorAll(".page")];
    const inks = [...document.querySelectorAll(".ink[data-min]")].map(el => {
      const pg = el.closest(".page");
      return { id: el.dataset.id, pageIndex: +pg.dataset.i, heightPx: el.style.height };
    });
    const slots = [...document.querySelectorAll(".slot")].map(el => {
      const pg = el.closest(".page");
      const r = el.getBoundingClientRect(), pr = pg.getBoundingClientRect();
      return {
        pageIndex: +pg.dataset.i, hidden: !!el.hidden,
        x: Math.round(r.left - pr.left), y: Math.round(r.top - pr.top),
        w: Math.round(r.width), h: Math.round(r.height),
      };
    });
    return { pageCount: pages.length, inks, slots };
  });
}

function closeEnough(a, b, tol) { return Math.abs(a - b) <= tol; }

async function compareDoc(browser, docId) {
  const results = [];
  const check = (ok, label) => { results.push({ ok, label }); console.log(`${ok ? "[PASS]" : "[FAIL]"} ${docId}: ${label}`); };

  const protoPage = await browser.newPage({ viewport: VIEWPORT });
  const proto = await measure(protoPage, `http://localhost:${PORT}/prototype/out/${docId}.html`);
  await protoPage.close();

  const rendererPage = await browser.newPage({ viewport: VIEWPORT });
  const mine = await measure(rendererPage, `http://localhost:${PORT}/renderer/viewer.html?doc=${docId}&mode=review&adapter=local`);
  await rendererPage.close();

  check(proto.pageCount === mine.pageCount, `페이지 수 일치 (원본 ${proto.pageCount} / 렌더러 ${mine.pageCount})`);

  const protoInkById = Object.fromEntries(proto.inks.map(i => [i.id, i]));
  const mineInkById = Object.fromEntries(mine.inks.map(i => [i.id, i]));
  check(proto.inks.length === mine.inks.length, `답란 개수 일치 (원본 ${proto.inks.length} / 렌더러 ${mine.inks.length})`);
  for (const id of Object.keys(protoInkById)) {
    const p = protoInkById[id], m = mineInkById[id];
    if (!m) { check(false, `답란[${id}]: 렌더러 결과에 없음`); continue; }
    check(p.heightPx === m.heightPx && p.pageIndex === m.pageIndex,
      `답란[${id}] 줄 수/페이지 일치 (원본 ${p.heightPx}@p${p.pageIndex} / 렌더러 ${m.heightPx}@p${m.pageIndex})`);
  }

  check(proto.slots.length === mine.slots.length, `이미지 슬롯 개수 일치 (원본 ${proto.slots.length} / 렌더러 ${mine.slots.length})`);
  proto.slots.forEach((p, i) => {
    const m = mine.slots[i];
    if (!m) { check(false, `슬롯[${i}]: 렌더러 결과에 없음`); return; }
    const posOk = p.hidden === m.hidden && (p.hidden || (
      closeEnough(p.x, m.x, 2) && closeEnough(p.y, m.y, 2) && closeEnough(p.w, m.w, 2) && closeEnough(p.h, m.h, 2)
    ));
    check(posOk, `슬롯[${i}]@p${p.pageIndex} 위치/표시 일치 (원본 ${JSON.stringify(p)} / 렌더러 ${JSON.stringify(m)})`);
  });

  return results;
}

(async () => {
  const server = await startServer();
  const browser = await chromium.launch();
  let all = [];
  try {
    for (const docId of DOCS) {
      const r = await compareDoc(browser, docId);
      all = all.concat(r);
    }
  } finally {
    await browser.close();
    server.close();
  }
  const fails = all.filter(r => !r.ok).length;
  console.log(`\n총 ${all.length}건 중 실패 ${fails}건`);
  process.exit(fails === 0 ? 0 : 1);
})();
