// page_manifest.js — 2026-09-19 12차(페이지 편집기) 신규. 현재 조판 결과를
// "표시 쪽수와 무관한 page_id/block_id(item_id)/fragment_id(part)"로 된 페이지 구성표로
// 얼린다(momo_page_editor_plan.md §5). generate.js/paginate.js가 이미 만드는 페이지 배열을
// 그대로 재사용하고, 각 반면(half)/전체페이지(fullpage)가 어느 item의 어느 조각인지
// 이름표(tag)만 추가로 기록한다 - 조판 알고리즘 자체는 전혀 새로 만들지 않는다.
//
// 실행: node page_manifest.js freeze <data.json 경로> <출력 manifest.json 경로>
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { chromium } = require('playwright');
const blocks = require('./blocks');
const { paginateStep2, PROBE_URL } = require('./paginate');
const gen = require('./generate');

function sha256(s) {
  return crypto.createHash('sha256').update(s, 'utf-8').digest('hex');
}
function genId(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString('hex')}`;
}

// data.json(build_effective_data가 만든 것과 동일한 형태) -> 페이지 구성표.
// generate.js의 main()과 완전히 같은 순서로 같은 함수를 호출한다("기존 자동 조판을
// 그대로 재사용" - 새 배치 로직을 만들지 않음). 유일한 차이는 최종 HTML 문자열을
// 만들지 않고, 페이지별로 조각·이름표를 그대로 들고 있는다는 점이다.
async function freeze(data) {
  const tone = data.meta.tone_class;
  blocks.resetIssues();

  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(PROBE_URL);
  await page.evaluate(() => document.fonts.ready);

  const step2Units = await paginateStep2(page, tone, data.step2);

  let pageNum = 1;
  const pages = [];

  pages.push({ role: 'cover', role_index: 0, layout_type: 'cover', slots: [], html_raw: blocks.coverPage(data.meta) });

  const step1Html = await gen.buildStep1Pages(page, tone, data, pageNum);
  // 2026-09-19 12차 P1 범위: step1(어휘/OX) 페이지는 이번 범위에서 직접 편집 대상이
  // 아니다(문항#2절 B의 편집 항목은 step2 위주) - slots를 비워 "읽기 전용(불투명)"으로
  // 표시하되, 페이지 자체는 다른 step2 페이지와 완전히 동등하게 고정·보존된다.
  step1Html.forEach((html, i) => pages.push({ role: 'step1', role_index: i, layout_type: 'opaque', slots: [], html_raw: html }));
  pageNum += step1Html.length;

  const step2Html = gen.buildStep2Pages(data, step2Units, pageNum);
  step2Units.forEach((entry, i) => {
    const isFullpage = !!(entry && entry.fullpage);
    // slots[].html: 조각의 "감싸이기 전" 원본 문자열(leadHtml/answerHtml/merged 그대로) -
    // wrapHalves()/fullpage 래퍼가 이 문자열을 그대로 <div class="half">...</div> 또는
    // <div class="page-body">...</div> 안에 넣으므로 html_raw의 부분 문자열임이 보장된다.
    // page_candidate.js가 편집된 조각 하나만 다시 만들어 이 문자열을 정확히 치환할 수
    // 있게 하기 위한 것 - "다른 조각·래퍼(sheet-head/footer/data-screen-label)는 절대
    // 다시 계산하지 않는다"는 불변 조건의 핵심 장치다.
    const slots = isFullpage
      ? [{ itemId: entry.tag.itemId, part: entry.tag.part, html: entry.fullpage, center: false }]
      : entry.map((h) => ({ itemId: h.tag.itemId, part: h.tag.part, html: h.html, center: h.center }));
    pages.push({
      role: 'step2', role_index: i, layout_type: isFullpage ? 'fullpage' : 'halves',
      slots, html_raw: step2Html[i],
    });
  });
  pageNum += step2Units.length;

  if (data.step3) {
    const html = await gen.buildStep3Page(page, tone, data, pageNum++);
    pages.push({ role: 'step3', role_index: 0, layout_type: 'opaque', slots: [], html_raw: html });
  }

  await browser.close();

  const issues = blocks.getIssues();
  const manifestPages = pages.map((p) => ({
    page_id: genId('pg'),
    role: p.role,
    role_index: p.role_index,
    layout_type: p.layout_type,
    slots: p.slots,
    html_raw: p.html_raw,
    content_hash: sha256(p.html_raw),
  }));

  return {
    revision_id: genId('rev'),
    parent_revision: null,
    created_at: new Date().toISOString(),
    template_version: gen.TEMPLATE_VERSION,
    edit_doc_id: data.doc_id,
    quarter_class: data.meta.quarter_class,
    tone_class: tone,
    pages: manifestPages,
    freeze_issues: issues,
  };
}

// manifest(고정된 페이지 구성표) + overrides({page_id: 새 html_raw, ...} - 보통 이번에
// 수정한 페이지 하나만) -> 실제 서빙 가능한 index.html. 다른 페이지는 manifest의
// html_raw를 그대로 이어붙인다(재조판 없음) - generate.js main()과 동일한 문서 골격
// (doctype/head/body/div.worksheet)과 동일한 fixAssetPaths()를 그대로 재사용해서,
// "표준 파이프라인이 만드는 문서"와 "페이지 편집기가 조립하는 문서"가 구조적으로
// 같은 함수에서 나오게 한다(두 군데서 따로 관리하다 한쪽만 고쳐지는 위험 제거).
function assemble(manifest, overrides, bookTitle, quarterClass, outPath) {
  const sections = manifest.pages.map((p) => (overrides && overrides[p.page_id]) || p.html_raw);
  const html = `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${blocks.esc(bookTitle)} · 학습지</title>
<link rel="stylesheet" href="../../../../worksheet/build/styles.css">
</head>
<body>
<div class="worksheet ${quarterClass} ${manifest.tone_class}">
${sections.join('\n')}
</div>
</body>
</html>
`;
  return gen.fixAssetPaths(html, outPath);
}

async function main() {
  const cmd = process.argv[2];
  if (cmd === 'freeze') {
    const dataPath = process.argv[3];
    const outPath = process.argv[4];
    if (!dataPath || !outPath) {
      console.error('사용법: node page_manifest.js freeze <data.json 경로> <출력 manifest.json 경로>');
      process.exit(1);
    }
    const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
    const manifest = await freeze(data);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(manifest, null, 2), 'utf-8');
    console.log(`OK pages=${manifest.pages.length} revision_id=${manifest.revision_id}`);
    return;
  }
  if (cmd === 'assemble') {
    // 사용법: node page_manifest.js assemble <manifest.json> <overrides.json|-없음-> <bookTitle> <quarterClass> <outPath>
    const manifestPath = process.argv[3];
    const overridesPath = process.argv[4];
    const bookTitle = process.argv[5];
    const quarterClass = process.argv[6];
    const outPath = process.argv[7];
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
    const overrides = overridesPath && overridesPath !== '-'
      ? JSON.parse(fs.readFileSync(overridesPath, 'utf-8')) : {};
    const html = assemble(manifest, overrides, bookTitle, quarterClass, outPath);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, html, 'utf-8');
    console.log(`OK ${outPath}`);
    return;
  }
  console.error('사용법: node page_manifest.js freeze|assemble ...');
  process.exit(1);
}

module.exports = { freeze, assemble };

if (require.main === module) {
  main().catch((err) => { console.error(err); process.exit(1); });
}
