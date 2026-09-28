/* 답란 "두 번 탭 확대"(2026-09-27) 검증 - InkBox의 .ink 엘리먼트를 그대로
 * backdrop으로 옮겨 transform:scale만 입히는 방식이라, 이동 후 너비가 무너지지
 * 않는지/더블탭 때 점(dot) 스트로크가 안 남는지/확대 상태에서 그린 필기가
 * 닫은 뒤에도 유지되는지를 실제 헤드리스 브라우저로 확인한다.
 *
 * 사전 조건: edition API 서버가 RUNTIME_AUTH_DISABLED=true로 떠 있어야 한다.
 * 실행: RUNTIME_AUTH_DISABLED=true uvicorn edition.api:app --port 8799 (별도 터미널)
 *       node tests/verify_ink_zoom.js [baseUrl]
 *
 * print 어댑터 + 임의 학생 id를 쓴다(state.save가 없어 서버에 아무것도 쓰지
 * 않음 - 실제 학생 필기 데이터를 건드리지 않고 순수 UI 동작만 확인).
 */
const { chromium } = require("playwright");

const BASE = process.argv[2] || "http://127.0.0.1:8799";
const results = [];
function check(ok, label) { results.push({ ok, label }); console.log(`${ok ? "[PASS]" : "[FAIL]"} ${label}`); }

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 900, height: 1200 } });
  const errors = [];
  page.on("pageerror", e => errors.push("pageerror: " + e.message));
  page.on("console", m => { if (m.type() === "error") errors.push("console: " + m.text()); });

  const student = "verify-ink-zoom-" + Date.now();
  await page.goto(`${BASE}/renderer/viewer.html?doc=L2-Q2-W08&mode=student&adapter=print&edition=102&ink=false&student=${student}`,
    { waitUntil: "networkidle" });
  await page.waitForFunction(() => window.__viewerReady === true, { timeout: 15000 });

  // part_id "3"(서술형, LINES.long)이 있는 페이지 - .page.on으로 반드시 좁혀야
  // 한다(안 보이는 페이지의 캔버스를 집으면 클릭 좌표가 실제로 보이는 다른
  // 페이지 위에 떨어진다 - 최초 구현 때 이 실수로 오탐이 났었음).
  await page.evaluate(() => window.__viewer.go(6));
  await page.waitForTimeout(200);

  const canvas = page.locator(".page.on .ink canvas").first();
  const box = await canvas.boundingBox();
  check(!!box && box.width > 0, `대상 캔버스를 찾음 (width=${box && box.width})`);
  const nativeWidthBefore = await canvas.evaluate(cv => cv.offsetWidth);
  const cx = box.x + box.width / 2, cy = box.y + box.height / 2;

  await page.mouse.click(cx, cy);
  await page.waitForTimeout(60);
  await page.mouse.click(cx, cy);
  await page.waitForTimeout(250);

  check((await page.locator(".ink-zoom-backdrop").count()) === 1, "두 번 탭 후 확대 오버레이가 정확히 1개 생김");
  const hasDotResidue = await page.evaluate(() => document.querySelector(".ink-zoom-backdrop .ink")?.classList.contains("has"));
  check(hasDotResidue === false, "더블탭 두 번의 tap이 점(dot) 스트로크를 남기지 않음");

  const zbox = await page.locator(".ink-zoom-backdrop .ink canvas").boundingBox();
  check(!!zbox && zbox.width > 300, `확대된 캔버스가 실제로 커짐(너비 붕괴 없음, width=${zbox && zbox.width})`);

  // 2026-09-27 실제로 잡았던 버그: 배율 계산에 getBoundingClientRect()(페이지
  // 전체 확대/축소까지 섞인 값)를 쓰면 확대 중 캔버스의 고유 너비(offsetWidth)
  // 자체가 바뀌어 버려서, pt()의 좌표 보정 기준이 확대 전/후로 달라지고 확대
  // 중 그린 필기가 축소 후 칸 밖으로 잘렸다 - 고유 너비가 확대 전후로 그대로인지
  // 반드시 확인한다(시각적으로도 재현 이미지로 확인했었음).
  const nativeWidthDuringZoom = await page.locator(".ink-zoom-backdrop .ink canvas").evaluate(cv => cv.offsetWidth);
  check(nativeWidthDuringZoom === nativeWidthBefore,
    `확대 중에도 캔버스 고유 너비(좌표 보정 기준)가 그대로임 (전:${nativeWidthBefore}, 중:${nativeWidthDuringZoom})`);

  await page.mouse.move(zbox.x + 20, zbox.y + 20);
  await page.mouse.down();
  await page.mouse.move(zbox.x + 100, zbox.y + 80, { steps: 10 });
  await page.mouse.up();
  await page.waitForTimeout(150);
  check(await page.evaluate(() => document.querySelector(".ink-zoom-backdrop .ink").classList.contains("has")),
    "확대 상태에서 실제로 필기가 됨");

  await page.click(".ink-zoom-close");
  await page.waitForTimeout(200);
  check((await page.locator(".ink-zoom-backdrop").count()) === 0, "닫기 후 오버레이가 제거됨");
  const restored = await page.evaluate(() => {
    const el = document.querySelector('.page.on .ink[data-id="3"]');
    return !!el && !el.classList.contains("zoomed") && el.style.transform === "" && el.style.width === "";
  });
  check(restored, "박스가 원래 위치/스타일로 복원됨(zoomed 클래스·transform·width 모두 해제)");
  check(await page.evaluate(() => document.querySelector('.page.on .ink[data-id="3"]').classList.contains("has")),
    "확대 중 그린 필기가 닫은 뒤에도 유지됨");

  check(errors.length === 0, `콘솔/페이지 에러 없음 (${errors.length}건: ${errors.slice(0, 3).join(" | ")})`);

  await browser.close();
  const fails = results.filter(r => !r.ok).length;
  console.log(`\n총 ${results.length}건 중 실패 ${fails}건`);
  process.exit(fails === 0 ? 0 : 1);
})();
