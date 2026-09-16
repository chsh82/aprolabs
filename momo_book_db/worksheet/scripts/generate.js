// generate.js — 학습지 생성기 CLI. extract_worksheet_json.py가 만든 data.json을 읽어
// build/styles.css 클래스 계약에 맞는 정적 HTML을 만든다. DB에는 직접 접근하지 않는다.
// 실행: node scripts/generate.js <doc_id> [data.json 경로] [출력 경로]
'use strict';
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const blocks = require('./blocks');
const { paginateStep2 } = require('./paginate');

const STEP1_VOCAB_INTRO = '제시된 단어의 뜻을 확인하고, 이 단어들을 이용해 각각 하나씩 ‘문장’을 만들어 보세요.';
const STEP1_OX_INTRO = '열심히 읽은 책 내용을 떠올리며 O·X 퀴즈를 풀어 보세요.';

// 화면 미리보기용 라벨 번호(인쇄에는 안 나감) - 표지가 슬롯 1을 쓰므로 내지 페이지 번호(=
// footer에 찍히는 번호)보다 하나 크게 매김(golden 샘플 v2.dc.html의 라벨 규칙과 동일).
function screenLabelNum(pageNum) {
  return String(pageNum + 1).padStart(2, '0');
}

function buildStep1Page(data, pageNum) {
  const half1 = `<p class="step-intro">${STEP1_VOCAB_INTRO}</p>` + blocks.vocabTable(data.step1.vocab);
  const half2 = `<p class="step-intro step-intro--tight">${STEP1_OX_INTRO}</p>` + blocks.oxList(data.step1.ox);
  return wrapHalves(`${screenLabelNum(pageNum)} 1단계`, 'STEP 01', '어휘력 향상 & 내용 확인 O·X 퀴즈', [half1, half2], pageNum, data.meta);
}

// halvesContent 항목은 일반 HTML 문자열(1/3단계, center 없음)이거나 paginate.js가 만든
// {html, center} 객체(2단계, 발췌문 단독 반면이면 center:true)일 수 있다.
function wrapHalves(screenLabel, step, title, halvesContent, pageNum, meta) {
  const halvesHtml = halvesContent.map((c, i) => {
    const { html, center } = typeof c === 'string' ? { html: c, center: false } : c;
    const cls = ['half', i > 0 ? 'half--gap' : null, center ? 'half--center' : null].filter(Boolean).join(' ');
    return `<div class="${cls}">${html}</div>`;
  });
  return blocks.sheetPage(screenLabel, step, title, halvesHtml, pageNum, meta);
}

function buildStep2Pages(data, pageHalves, startPageNum) {
  return pageHalves.map((halves, idx) =>
    wrapHalves(`${screenLabelNum(startPageNum + idx)} 2단계 ${idx + 1}`, 'STEP 02', '질문과 토론 — 함께 들여다보기',
      halves.filter(Boolean), startPageNum + idx, data.meta));
}

function buildStep3Page(data, pageNum) {
  const { essay, outline_questions: outline } = data.step3;
  const body = blocks.topicBand(essay.main_topic) +
    blocks.bgQuote(blocks.docImageSrc(essay.image_path), essay.writing_guide) +
    '<div class="step3-body">' +
    blocks.stepHead('Step 1.', '질문에 따라 생각을 모아 봅시다.') +
    outline.map((q, i) => blocks.memoQRow(i + 1, q.question_text, i > 0)).join('') +
    blocks.stepHead('Step 2.', '글의 첫 대목을 잡아 봅시다.', true) +
    (essay.closing_instruction
      ? `<p class="memo-example memo-example--flush">${blocks.esc(essay.closing_instruction)}</p>`
      : '') +
    blocks.answerLines() +
    '</div>';
  return `<section class="page" data-screen-label="${screenLabelNum(pageNum)} 3단계"><div class="sheet">` +
    blocks.sheetHead('STEP 03', '글쓰기 — 내 글로 엮기') + body +
    blocks.footer(pageNum, data.meta) + '</div></section>';
}

async function main() {
  const docId = process.argv[2];
  if (!docId) {
    console.error('사용법: node generate.js <doc_id> [data.json 경로] [출력 경로]');
    process.exit(1);
  }
  const baseDir = path.resolve(__dirname, '../../generated', docId);
  const dataPath = process.argv[3] || path.join(baseDir, 'data.json');
  const outPath = process.argv[4] || path.join(baseDir, 'index.html');

  const data = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));
  const tone = data.meta.tone_class;

  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(require('./paginate').PROBE_URL);
  await page.evaluate(() => document.fonts.ready);

  const step2Pages = await paginateStep2(page, tone, data.step2);
  await browser.close();

  let pageNum = 1;
  const sections = [blocks.coverPage(data.meta)];
  sections.push(buildStep1Page(data, pageNum++));
  sections.push(...buildStep2Pages(data, step2Pages, pageNum));
  pageNum += step2Pages.length;
  if (data.step3) sections.push(buildStep3Page(data, pageNum++));

  const html = `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${blocks.esc(data.meta.book.title)} · 학습지</title>
<link rel="stylesheet" href="../../worksheet/build/styles.css">
</head>
<body>
<div class="worksheet ${data.meta.quarter_class} ${tone}">
${sections.join('\n')}
</div>
</body>
</html>
`;

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, html, 'utf-8');
  console.log(`작성 완료: ${outPath} (${pageNum - 1}p 내지 + 표지 1p)`);
}

main().catch((err) => { console.error(err); process.exit(1); });
