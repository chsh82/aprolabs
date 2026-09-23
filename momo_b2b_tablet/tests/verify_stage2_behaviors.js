/* 2단계 보완 검증 (사용자 지시, 3단계 착수 전):
 *   1. 인쇄: A4 2-up PDF 페이지 수 = 전체 페이지/2(올림), 필기가 인쇄에 포함되는지
 *   2. 분기 색 전환: 4분기 각각 CSS 변수(--main-deep, --main-soft, --point-text) 값이
 *      원본 시안과 일치하는지
 *   3. 답안 모아보기: 평가용 JSON의 questions/parts 구조(part_id, label, prompt)가 원본과 일치하는지
 *   4. 필기: 마우스 포인터로 획을 그렸을 때 저장(새로고침 후 복원)·되돌리기·지우개가 동작하는지
 *
 * 원본 시안(prototype/out/*.html)의 evalPayload/applyPalette/printPrep은 클래식 <script>의
 * 최상위 함수 선언이라 window.evalPayload 처럼 그대로 쓸 수 있다(직접 확인함). 렌더러 쪽은
 * mountEdition()이 돌려주는 window.__viewer.{evalPayload,applyPalette,printPrep}를 쓴다.
 *
 * 실행: node tests/verify_stage2_behaviors.js
 */
const path = require("path");
const http = require("http");
const fs = require("fs");
const os = require("os");
const { chromium } = require("playwright");

const ROOT = path.resolve(__dirname, "..");
const PORT = 8974;
const MIME = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".json": "application/json",
  ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg" };

function startServer() {
  const server = http.createServer((req, res) => {
    const urlPath = decodeURIComponent(req.url.split("?")[0]);
    const filePath = path.join(ROOT, urlPath);
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end("not found: " + urlPath); return; }
      res.writeHead(200, { "Content-Type": MIME[path.extname(filePath)] || "application/octet-stream" });
      res.end(data);
    });
  });
  return new Promise(resolve => server.listen(PORT, () => resolve(server)));
}

const DOC = "L9-Q3-W07";
const VIEWPORT = { width: 1600, height: 1000 };
const results = [];
function check(ok, label) { results.push({ ok, label }); console.log(`${ok ? "[PASS]" : "[FAIL]"} ${label}`); }

async function bootPage(browser, url) {
  const page = await browser.newPage({ viewport: VIEWPORT });
  await page.goto(url, { waitUntil: "networkidle" });
  await page.waitForFunction(() => document.querySelector(".pages") && document.querySelector(".pages").children.length > 0);
  await page.evaluate(() => document.fonts && document.fonts.ready ? document.fonts.ready : null);
  await page.waitForTimeout(300);
  return page;
}

function countPdfPages(buf) {
  const s = buf.toString("latin1");
  const m = s.match(/\/Type\s*\/Page(?![a-zA-Z])/g);
  return m ? m.length : 0;
}

/* ============ 1. 인쇄 ============ */
async function verifyPrint(browser) {
  const protoPage = await bootPage(browser, `http://localhost:${PORT}/prototype/out/${DOC}.html`);
  const pageCount = await protoPage.evaluate(() => document.querySelectorAll(".page").length);
  await protoPage.close();

  const page = await bootPage(browser, `http://localhost:${PORT}/renderer/viewer.html?doc=${DOC}&mode=review&adapter=local`);

  // 2쪽 뒤(잉크가 있는 qaref 페이지)로 이동해 필기 한 획을 그린다
  await page.evaluate(() => window.__viewer.go(2));
  await page.waitForTimeout(200);
  const cv = page.locator(".page.on .ink canvas").first();
  const box = await cv.boundingBox();
  await page.mouse.move(box.x + 10, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width * 0.6, box.y + box.height / 2, { steps: 8 });
  await page.mouse.up();
  await page.waitForTimeout(100);

  await page.evaluate(() => window.__viewer.printPrep());
  await page.waitForTimeout(100);

  await page.emulateMedia({ media: "print" });
  const toolbarHidden = await page.evaluate(() => getComputedStyle(document.querySelector(".toolbar")).display === "none");
  check(toolbarHidden, "인쇄 미디어에서 툴바 숨김");
  const readBtnHidden = await page.evaluate(() => {
    const b = document.querySelector(".page.on .ink-read");
    return b ? getComputedStyle(b).display === "none" : false;
  });
  check(readBtnHidden, "인쇄 미디어에서 '글자로 확인' 버튼 숨김(필기 캔버스 자체는 유지)");

  const hasInkPixels = await page.evaluate(() => {
    const cv = document.querySelector(".page.on .ink canvas");
    const ctx = cv.getContext("2d");
    const { data } = ctx.getImageData(0, 0, cv.width, cv.height);
    const RULE_ISH = 210; // 괘선(연한 회갈색)보다 확실히 어두운 픽셀만 "잉크"로 센다
    for (let i = 0; i < data.length; i += 4) {
      if (data[i] < RULE_ISH && data[i + 3] > 0) return true;
    }
    return false;
  });
  check(hasInkPixels, "인쇄 해상도로 재조정된 캔버스에 필기 픽셀이 남아 있음(필기가 인쇄에 포함됨)");

  const pdfPath = path.join(os.tmpdir(), `momo_b2b_stage2_${DOC}.pdf`);
  await page.pdf({ path: pdfPath, printBackground: true });
  const pdfPages = countPdfPages(fs.readFileSync(pdfPath));
  fs.unlinkSync(pdfPath);
  const expected = Math.ceil(pageCount / 2);
  check(pdfPages === expected, `A4 2-up PDF 페이지 수 = 전체 페이지/2 올림 (전체 ${pageCount} → 기대 ${expected} / 실제 ${pdfPages})`);

  await page.close();
}

/* ============ 2. 분기 색 전환 ============ */
async function verifyQuarterColors(browser) {
  const VARS = ["--main-deep", "--main-soft", "--point-text"];
  const QUARTERS = ["winter", "spring", "summer", "autumn"];

  async function readAll(page, applyFn) {
    const out = {};
    for (const q of QUARTERS) {
      await page.evaluate(([q, fnKey]) => (fnKey ? window.__viewer[fnKey](q) : window.applyPalette(q)), [q, applyFn]);
      out[q] = await page.evaluate(vars => {
        const s = getComputedStyle(document.documentElement);
        const o = {}; vars.forEach(v => o[v] = s.getPropertyValue(v).trim()); return o;
      }, VARS);
    }
    return out;
  }

  const protoPage = await bootPage(browser, `http://localhost:${PORT}/prototype/out/${DOC}.html`);
  const proto = await readAll(protoPage, null);
  await protoPage.close();

  const page = await bootPage(browser, `http://localhost:${PORT}/renderer/viewer.html?doc=${DOC}&mode=review&adapter=local`);
  const mine = await readAll(page, "applyPalette");
  await page.close();

  for (const q of QUARTERS) {
    for (const v of VARS) {
      check(proto[q][v] === mine[q][v], `분기 색[${q}] ${v} 일치 (원본 ${proto[q][v]} / 렌더러 ${mine[q][v]})`);
    }
  }
}

/* ============ 3. 답안 모아보기 JSON 구조 ============ */
async function verifyAnswerJsonStructure(browser) {
  const protoPage = await bootPage(browser, `http://localhost:${PORT}/prototype/out/${DOC}.html`);
  const proto = await protoPage.evaluate(() => window.evalPayload());
  await protoPage.close();

  const page = await bootPage(browser, `http://localhost:${PORT}/renderer/viewer.html?doc=${DOC}&mode=review&adapter=local`);
  const mine = await page.evaluate(() => window.__viewer.evalPayload());
  await page.close();

  check(proto.questions.length === mine.questions.length, `questions 개수 일치 (원본 ${proto.questions.length} / 렌더러 ${mine.questions.length})`);
  const byId = arr => Object.fromEntries(arr.map(q => [q.question_id, q]));
  const pq = byId(proto.questions), mq = byId(mine.questions);
  for (const qid of Object.keys(pq)) {
    const p = pq[qid], m = mq[qid];
    if (!m) { check(false, `questions[${qid}]: 렌더러 결과에 없음`); continue; }
    check(p.parts.length === m.parts.length, `questions[${qid}].parts 개수 일치 (원본 ${p.parts.length} / 렌더러 ${m.parts.length})`);
    p.parts.forEach((pp, i) => {
      const mp = m.parts[i] || {};
      check(pp.part_id === mp.part_id && (pp.label || "") === (mp.label || "") && (pp.prompt || "") === (mp.prompt || ""),
        `questions[${qid}].parts[${i}] part_id/label/prompt 일치 (원본 ${JSON.stringify({ part_id: pp.part_id, label: pp.label, prompt: pp.prompt })} / 렌더러 ${JSON.stringify({ part_id: mp.part_id, label: mp.label, prompt: mp.prompt })})`);
    });
  }
  check(proto.ox.length === mine.ox.length, `ox 개수 일치 (원본 ${proto.ox.length} / 렌더러 ${mine.ox.length})`);
}

/* ============ 4. 필기(마우스): 저장·되돌리기·지우개 ============ */
async function verifyHandwriting(browser) {
  const url = `http://localhost:${PORT}/renderer/viewer.html?doc=${DOC}&mode=review&adapter=local`;
  const page = await bootPage(browser, url);
  await page.evaluate(() => { try { localStorage.clear(); } catch (e) {} });
  await page.reload({ waitUntil: "networkidle" });
  await page.waitForFunction(() => document.querySelector(".pages") && document.querySelector(".pages").children.length > 0);
  await page.evaluate(() => window.__viewer.go(2));
  await page.waitForTimeout(200);

  const inkEl = page.locator(".page.on .ink").first();
  const cv = inkEl.locator("canvas");
  const box = await cv.boundingBox();
  const y = box.y + box.height / 2;

  async function drawStroke() {
    await page.mouse.move(box.x + 10, y);
    await page.mouse.down();
    await page.mouse.move(box.x + box.width * 0.3, y - 5, { steps: 4 });
    await page.mouse.move(box.x + box.width * 0.6, y + 5, { steps: 4 });
    await page.mouse.up();
    await page.waitForTimeout(100);
  }

  await drawStroke();
  check(await inkEl.evaluate(el => el.classList.contains("has")), "마우스로 그은 획이 답란에 반영됨(.has)");
  const undoDisabled1 = await page.evaluate(() => document.getElementById("tUndo").disabled);
  check(!undoDisabled1, "획을 그은 뒤 되돌리기 버튼이 활성화됨");

  await page.click("#tUndo");
  await page.waitForTimeout(100);
  check(await inkEl.evaluate(el => !el.classList.contains("has")), "되돌리기 클릭 후 답란이 다시 비어 있음(.has 해제)");

  // 저장: 다시 그리고, 디바운스 저장(400ms) 이후 새로고침해서 복원되는지 확인
  await drawStroke();
  await page.waitForTimeout(600);
  await page.reload({ waitUntil: "networkidle" });
  await page.waitForFunction(() => document.querySelector(".pages") && document.querySelector(".pages").children.length > 0);
  await page.evaluate(() => window.__viewer.go(2));
  await page.waitForTimeout(300);
  const inkEl2 = page.locator(".page.on .ink").first();
  check(await inkEl2.evaluate(el => el.classList.contains("has")), "새로고침 후에도 필기가 복원됨(local 어댑터 localStorage 저장 확인)");

  // 지우개: 방금 복원된 획을 지운다
  await page.click("#tEraser");
  const box2 = await inkEl2.locator("canvas").boundingBox();
  const y2 = box2.y + box2.height / 2;
  await page.mouse.move(box2.x + 10, y2);
  await page.mouse.down();
  await page.mouse.move(box2.x + box2.width * 0.6, y2, { steps: 10 });
  await page.mouse.up();
  await page.waitForTimeout(150);
  check(await inkEl2.evaluate(el => !el.classList.contains("has")), "지우개로 드래그한 뒤 필기가 지워짐(.has 해제)");
  await page.click("#tPen");

  await page.close();
}

(async () => {
  const server = await startServer();
  const browser = await chromium.launch();
  try {
    await verifyPrint(browser);
    await verifyQuarterColors(browser);
    await verifyAnswerJsonStructure(browser);
    await verifyHandwriting(browser);
  } finally {
    await browser.close();
    server.close();
  }
  const fails = results.filter(r => !r.ok).length;
  console.log(`\n총 ${results.length}건 중 실패 ${fails}건`);
  process.exit(fails === 0 ? 0 : 1);
})();
