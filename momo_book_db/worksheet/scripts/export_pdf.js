// export_pdf.js — 생성된 학습지 index.html을 실제 PDF로 뽑는다(check.js가 내부적으로
// 인쇄 괘선 검사용으로 만드는 것과 같은 page.pdf() 호출이지만, 그쪽은 임시 폴더에 썼다가
// 지운다 - 이건 사용자가 내려받을 PDF를 실제로 남기는 용도).
// 실행: node export_pdf.js <index.html경로> <출력pdf경로>
'use strict';
const { chromium } = require('playwright');
const path = require('path');

async function main() {
  const htmlPath = process.argv[2];
  const outPath = process.argv[3];
  if (!htmlPath || !outPath) {
    console.error('사용법: node export_pdf.js <index.html경로> <출력pdf경로>');
    process.exit(1);
  }
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto('file:///' + path.resolve(htmlPath).replace(/\\/g, '/'));
  await page.evaluate(() => document.fonts.ready);
  await page.pdf({ path: outPath, printBackground: true, preferCSSPageSize: true });
  await browser.close();
  console.log(`작성 완료: ${outPath}`);
}

main().catch((err) => { console.error(err); process.exit(1); });
