/* 4단계 완료 기준 4번: GET /api/runtime/{edition_id}가 renderer의 api 어댑터로
 * 그대로 렌더되는지 "실제 서버"에 대고 확인한다(2단계에서 만든 api.js가 구조만
 * 맞춰 둔 스켈레톤이 아니라 진짜로 붙는지).
 *
 * 사전 조건: edition API 서버가 떠 있고(uvicorn edition.api:app), edition_id=1이
 * L9-Q3-W07로 만들어져 approve+publish까지 끝난 상태여야 한다(tests/README 절차 참고).
 *
 * 실행: node tests/verify_stage4_api_render.js [baseUrl] [editionId]
 */
const { chromium } = require("playwright");

const BASE = process.argv[2] || "http://127.0.0.1:8790";
const EDITION_ID = process.argv[3] || "1";

const results = [];
function check(ok, label) {
  results.push({ ok, label });
  console.log(`${ok ? "[PASS]" : "[FAIL]"} ${label}`);
}

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });

  const netErrors = [];
  page.on("requestfailed", r => netErrors.push(r.url()));
  const consoleErrors = [];
  page.on("console", m => { if (m.type() === "error") consoleErrors.push(m.text()); });

  const url = `${BASE}/renderer/viewer.html?doc=L9-Q3-W07&mode=student&adapter=api&edition=${EDITION_ID}`;
  await page.goto(url, { waitUntil: "networkidle" });
  await page.waitForFunction(() => document.querySelector(".pages") && document.querySelector(".pages").children.length > 0,
    { timeout: 10000 }).catch(() => {});

  const pageCount = await page.evaluate(() => document.querySelectorAll(".page").length).catch(() => 0);
  check(pageCount > 0, `api 어댑터로 실제 페이지가 렌더됨 (${pageCount}쪽)`);

  const bookTitle = await page.evaluate(() => document.title).catch(() => "");
  check(bookTitle.includes("열하일기"), `문서 제목이 실제 API 응답 기반으로 채워짐 (title="${bookTitle}")`);

  const coverImgOk = await page.evaluate(() => {
    const img = document.querySelector(".cover .r img");
    return !!(img && img.naturalWidth > 0);
  }).catch(() => false);
  check(coverImgOk, "표지 이미지가 /static/extracted/... URL에서 실제로 로드됨(momo_book_db 원본 이미지)");

  // student 모드에서는 "그림 자리 설명" 토글이 없어야 한다(SPEC §8 학생 런타임 표)
  const hasReviewToggle = await page.evaluate(() => !!document.getElementById("tReview")).catch(() => true);
  check(!hasReviewToggle, "student 모드에는 '그림 자리 설명' 토글이 없음(검수 전용 UI 제거 확인)");

  // 마우스로 답란에 한 획 그어서 PUT /answers/{part_id}가 실제로 저장되는지 확인
  await page.evaluate(() => window.__viewer.go(2));
  await page.waitForTimeout(200);
  const cv = page.locator(".page.on .ink canvas").first();
  const box = await cv.boundingBox();
  let putOk = false;
  if (box) {
    const [resp] = await Promise.all([
      page.waitForResponse(r => r.url().includes("/answers/") && r.request().method() === "PUT", { timeout: 5000 }).catch(() => null),
      (async () => {
        await page.mouse.move(box.x + 10, box.y + box.height / 2);
        await page.mouse.down();
        await page.mouse.move(box.x + box.width * 0.5, box.y + box.height / 2, { steps: 6 });
        await page.mouse.up();
        await page.waitForTimeout(600); // renderer.js의 저장 디바운스(400ms)
      })(),
    ]);
    putOk = !!resp && resp.ok();
  }
  check(putOk, "마우스로 그은 필기가 디바운스 뒤 PUT /api/runtime/{id}/answers/{part}로 실제 저장됨(200 응답)");

  check(netErrors.length === 0, `네트워크 요청 실패 없음 (실패 ${netErrors.length}건: ${netErrors.join(", ")})`);
  check(consoleErrors.length === 0, `콘솔 에러 없음 (${consoleErrors.length}건: ${consoleErrors.join(" | ")})`);

  await browser.close();
  const fails = results.filter(r => !r.ok).length;
  console.log(`\n총 ${results.length}건 중 실패 ${fails}건`);
  process.exit(fails === 0 ? 0 : 1);
})();
