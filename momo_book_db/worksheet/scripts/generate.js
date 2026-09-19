// generate.js — 학습지 생성기 CLI. extract_worksheet_json.py가 만든 data.json을 읽어
// build/styles.css 클래스 계약에 맞는 정적 HTML을 만든다. DB에는 직접 접근하지 않는다.
// 실행: node scripts/generate.js <doc_id> [data.json 경로] [출력 경로]
//
// 2026-09-18 1차 안정화: 출력 경로를 명시하지 않으면(기존 호출부 호환을 위해 명시하면
// 그 경로에 그대로 씀) generated/<doc_id>/builds/<build_id>/index.html에 쓰고
// manifest.json(입력 해시·템플릿 버전·issues)을 같이 남긴다. "정상본" 포인터
// (generated/<doc_id>/current.json) 교체는 이 스크립트가 하지 않는다 - QA(check.js)
// 통과 여부를 알아야 판단할 수 있는 일이라 호출부(app/routers/momo_book_worksheet.py)의
// 책임이다. 이 스크립트는 빌드 하나를 안전하게(기존 정상본을 건드리지 않고) 만드는
// 것까지만 한다.
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { chromium } = require('playwright');
const blocks = require('./blocks');
const { paginateStep2, paginateStep1, overflowFull, PROBE_URL } = require('./paginate');

// blocks.js/paginate.js/styles.css의 마크업·클래스 계약이 바뀔 때마다 올린다.
// manifest.json에 찍어서 "DB만 바뀐 재빌드"와 "템플릿도 바뀐 재빌드"를 구분할 수 있게 함.
// 2026-09-18.1 이후 실제로 마크업/배치 로직이 여러 번 바뀌었는데(1단계 다중 페이지,
// 참고이미지 크기, 2단계 고밀도 배치) 버전을 안 올려서 manifest.json만 보고는 구분이
// 안 됐다 - 3차 안정화 기준선부터 제대로 올린다.
const TEMPLATE_VERSION = '2026-09-19.1';

const STEP1_VOCAB_INTRO = '제시된 단어의 뜻을 확인하고, 이 단어들을 이용해 각각 하나씩 ‘문장’을 만들어 보세요.';
const STEP1_OX_INTRO = '열심히 읽은 책 내용을 떠올리며 O·X 퀴즈를 풀어 보세요.';

// 화면 미리보기용 라벨 번호(인쇄에는 안 나감) - 표지가 슬롯 1을 쓰므로 내지 페이지 번호(=
// footer에 찍히는 번호)보다 하나 크게 매김(golden 샘플 v2.dc.html의 라벨 규칙과 동일).
function screenLabelNum(pageNum) {
  return String(pageNum + 1).padStart(2, '0');
}

// halvesContent 항목은 일반 HTML 문자열(1/3단계, center 없음)이거나 paginate.js가 만든
// {html, center} 객체(2단계, 발췌문 단독 반면이면 center:true)일 수 있다. 반면이 1개만
// 와도(예: 어휘만 있고 OX는 없는 1단계) 그대로 동작한다.
function wrapHalves(screenLabel, step, title, halvesContent, pageNum, meta) {
  const halvesHtml = halvesContent.map((c, i) => {
    const { html, center } = typeof c === 'string' ? { html: c, center: false } : c;
    const cls = ['half', i > 0 ? 'half--gap' : null, center ? 'half--center' : null].filter(Boolean).join(' ');
    return `<div class="${cls}">${html}</div>`;
  });
  return blocks.sheetPage(screenLabel, step, title, halvesHtml, pageNum, meta);
}

// 2026-09-19 2차 안정화: 어휘/OX를 고정 반면(374px 절반씩) 대신 머리말·푸터를 뺀 실제
// .sheet 여유 공간 전체에 배분한다(paginate.js의 paginateStep1). 한 페이지에 다 안 들어가면
// 어휘 행 단위로(쪼개지 않고, 다음 페이지에 표 머리글 반복) 여러 페이지로 이어간다 -
// 승인 문서 7건 중 6건이 겪던 "어휘표가 반면과 14~56px 겹침" 문제의 근본 원인(고정 반면
// 예산)을 없앤다. 어휘/OX가 둘 다 없으면 빈 표·안내문을 만들지 않고 background_text로
// 채우거나(그것도 없으면 issues만 남기고 안내 문구 1줄) 기존과 동일하게 처리한다.
async function buildStep1Pages(page, tone, data, startPageNum) {
  const { vocab, ox, vocab_match, vocab_fillblank } = data.step1;
  const bg = data.meta.background_text;

  let bodies; // 페이지별 본문 HTML 배열(각 페이지는 .sheet 하나)
  if (vocab_match) {
    // 2026-09-19 9차 안정화: L1-Q4-W01 원본 PDF 실물 대조로 어휘 활동이 "단어+문장
    // 만들기" 표가 아니라 "낱말 뜻 선 연결하기"+"<보기> 빈칸 채우기"였음이 밝혀져
    // 추가한 전용 경로(RESTORATION_PATCHES.json 참고) - vocab_match가 있을 때만 타고,
    // 없는 기존 문서는 이 분기 자체를 안 지나가므로 기존 동작과 100% 동일하다.
    const vocabHtml = blocks.vocabMatchBlock(vocab_match.words, vocab_match.definitions) +
      (vocab_fillblank ? blocks.vocabFillBlankBlock(vocab_fillblank.bank, vocab_fillblank.sentences) : '');
    // 2026-09-19 10차 안정화: step-intro--tight(표 바로 아래용, 여백 거의 없음) 대신
    // section-gap을 써서 어휘 활동과 OX 사이에 구분 여백을 준다(사용자 지시).
    const oxHtml = (ox && ox.length)
      ? `<p class="step-intro step-intro--section-gap">${STEP1_OX_INTRO}</p>${blocks.oxList(ox)}` : '';
    if (oxHtml && !(await overflowFull(page, vocabHtml + oxHtml, tone))) {
      bodies = [vocabHtml + oxHtml];
    } else {
      bodies = oxHtml ? [vocabHtml, oxHtml] : [vocabHtml];
    }
    if (bg && bg.trim()) {
      const lastIdx = bodies.length - 1;
      const withBg = bodies[lastIdx] + blocks.backgroundText(bg);
      if (!(await overflowFull(page, withBg, tone))) bodies[lastIdx] = withBg;
      else bodies.push(blocks.backgroundText(bg));
    }
  } else if (vocab.length === 0 && ox.length === 0) {
    if (bg && bg.trim()) {
      bodies = [blocks.backgroundText(bg)];
    } else {
      blocks.pushIssue({ doc_id: data.doc_id, order_label: '1단계' },
        '어휘·OX·background_text가 모두 없음 - 1단계에 채울 내용이 없음(extract_worksheet_json.py issues 참고)');
      bodies = ['<p class="step-intro">이 문서에는 1단계에 채울 어휘·OX·배경지식 내용이 없습니다.</p>'];
    }
  } else {
    bodies = await paginateStep1(page, tone, data, STEP1_VOCAB_INTRO, STEP1_OX_INTRO);
    if (bg && bg.trim()) {
      // 어휘/OX 중 하나만 있고 background_text도 있는 경우 - 둘 다 버리지 않고 마지막
      // 페이지 끝에 이어 붙인다(그 페이지가 넘치면 새 페이지로 분리).
      const lastIdx = bodies.length - 1;
      const withBg = bodies[lastIdx] + blocks.backgroundText(bg);
      if (!(await overflowFull(page, withBg, tone))) {
        bodies[lastIdx] = withBg;
      } else {
        bodies.push(blocks.backgroundText(bg));
      }
    }
  }

  return bodies.map((body, i) => {
    const pageNum = startPageNum + i;
    const label = bodies.length > 1
      ? `${screenLabelNum(pageNum)} 1단계 ${i + 1}`
      : `${screenLabelNum(pageNum)} 1단계`;
    // .page-body(2026-09-19 8차 안정화 추가): body를 .sheet의 직계 자식으로 바로 넣으면
    // (예: 어휘·OX 없이 background_text만 있는 짧은 페이지) 그 콘텐츠가 flex:1이 아니라서
    // footer가 콘텐츠 바로 아래에 붙어버리고 페이지 하단까지 빈 공간이 남는다(실측:
    // L1-Q4-W01 - 배경지식만 있는 페이지에서 footer가 페이지 중간쯤에 떠 있었음). 래퍼에
    // flex:1을 줘서 짧은 콘텐츠도 항상 footer가 정해진 하단 위치를 지키게 한다.
    return `<section class="page" data-screen-label="${label}"><div class="sheet">` +
      blocks.sheetHead('STEP 01', '어휘력 향상 & 내용 확인 O·X 퀴즈') + `<div class="page-body">${body}</div>` +
      blocks.footer(pageNum, data.meta) + '</div></section>';
  });
}

// pageHalves의 각 원소는 [halfA, halfB] 배열(기존과 동일) 또는 { fullpage: html }
// (2026-09-19 3차 안정화 - 고밀도 Step2 문항이 반면 대신 페이지 하나를 통째로 쓰는 경우,
// paginate.js의 placeItem() 3단계 사다리 참고).
function buildStep2Pages(data, pageHalves, startPageNum) {
  return pageHalves.map((entry, idx) => {
    const pageNum = startPageNum + idx;
    const label = `${screenLabelNum(pageNum)} 2단계 ${idx + 1}`;
    if (entry && entry.fullpage) {
      return `<section class="page" data-screen-label="${label}"><div class="sheet">` +
        blocks.sheetHead('STEP 02', '질문과 토론 — 함께 들여다보기') + `<div class="page-body">${entry.fullpage}</div>` +
        blocks.footer(pageNum, data.meta) + '</div></section>';
    }
    return wrapHalves(label, 'STEP 02', '질문과 토론 — 함께 들여다보기', entry.filter(Boolean), pageNum, data.meta);
  });
}

async function buildStep3Page(page, tone, data, pageNum) {
  const { essay, outline_questions: outline } = data.step3;
  const body = blocks.topicBand(essay.main_topic) +
    blocks.bgQuote(blocks.docImageSrc(essay.image_path), essay.writing_guide) +
    '<div class="step3-body">' +
    blocks.stepHead('Step 1.', '질문에 따라 생각을 모아 봅시다.') +
    outline.map((q, i) => blocks.memoQRow(i + 1, q.question_text, i > 0, q.id)).join('') +
    blocks.stepHead('Step 2.', '글의 첫 대목을 잡아 봅시다.', true) +
    (essay.closing_instruction
      ? `<p class="memo-example memo-example--flush">${blocks.esc(essay.closing_instruction)}</p>`
      : '') +
    blocks.answerLines() +
    '</div>';

  if (await overflowFull(page, body, tone)) {
    blocks.pushIssue({ doc_id: data.doc_id, order_label: '3단계' }, '3단계 본문이 넘침 - 최종 출력 확정 차단 대상');
  }

  return `<section class="page" data-screen-label="${screenLabelNum(pageNum)} 3단계"><div class="sheet">` +
    blocks.sheetHead('STEP 03', '글쓰기 — 내 글로 엮기') + body +
    blocks.footer(pageNum, data.meta) + '</div></section>';
}

// 2026-09-19 12차(페이지 편집기) 안정화: 출력 파일의 실제 깊이에 맞춰 styles.css/
// extracted_images/ASSET_BASE 상대경로를 다시 쓰는 로직을 main()에서 뽑아냈다 - 표준
// 파이프라인(generated/<doc_id>/builds/<build_id>/)과 페이지 편집기의 부분 재조립
// (edit_projects/<pid>/versions/<vid>/ 및 page_proposals/<...>/preview/)이 서로 다른
// 코드로 "따로" 경로를 고치면 한쪽만 고쳐질 위험이 있어, 하나의 함수를 공유한다.
function fixAssetPaths(html, outFilePath) {
  const buildDir = path.dirname(outFilePath);
  const depth = path.relative(path.resolve(__dirname, '../..'), buildDir).split(path.sep).filter(Boolean).length;
  const upPrefix = '../'.repeat(depth);
  return html
    .replace('../../../../worksheet/build/styles.css', `${upPrefix}worksheet/build/styles.css`)
    .split('../../extracted_images/').join(`${upPrefix}extracted_images/`)
    .split('../../worksheet/build/assets/images/').join(`${upPrefix}worksheet/build/assets/images/`);
}

function inputHash(dataText) {
  return crypto.createHash('sha256').update(dataText, 'utf-8').digest('hex').slice(0, 16);
}

function makeBuildId() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

async function main() {
  const docId = process.argv[2];
  if (!docId) {
    console.error('사용법: node generate.js <doc_id> [data.json 경로] [출력 경로]');
    process.exit(1);
  }
  const docBaseDir = path.resolve(__dirname, '../../generated', docId);
  const dataPath = process.argv[3] || path.join(docBaseDir, 'data.json');
  const explicitOutPath = process.argv[4]; // 지정하면 예전처럼 그 경로에 그대로 씀(테스트/애드혹용)

  const dataText = fs.readFileSync(dataPath, 'utf-8');
  const data = JSON.parse(dataText);
  const tone = data.meta.tone_class;

  blocks.resetIssues();
  // extract_worksheet_json.py가 이미 남긴 입력 검증 issues도 최종 issues.json에 합쳐서
  // "입력 단계 문제"와 "조판 단계 문제"를 한 파일에서 같이 볼 수 있게 한다.
  const inputIssues = (data.issues || []).map((i) => ({ ...i, stage: 'input' }));

  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(PROBE_URL);
  await page.evaluate(() => document.fonts.ready);

  const step2Pages = await paginateStep2(page, tone, data.step2);

  let pageNum = 1;
  const sections = [blocks.coverPage(data.meta)];
  const step1Pages = await buildStep1Pages(page, tone, data, pageNum);
  sections.push(...step1Pages);
  pageNum += step1Pages.length;
  sections.push(...buildStep2Pages(data, step2Pages, pageNum));
  pageNum += step2Pages.length;
  if (data.step3) sections.push(await buildStep3Page(page, tone, data, pageNum++));

  await browser.close();

  const layoutIssues = blocks.getIssues().map((i) => ({ ...i, stage: 'layout' }));
  const issues = [...inputIssues, ...layoutIssues];

  const html = `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${blocks.esc(data.meta.book.title)} · 학습지</title>
<link rel="stylesheet" href="../../../../worksheet/build/styles.css">
</head>
<body>
<div class="worksheet ${data.meta.quarter_class} ${tone}">
${sections.join('\n')}
</div>
</body>
</html>
`;

  let outPath, buildDir, buildId;
  if (explicitOutPath) {
    outPath = explicitOutPath;
    buildDir = path.dirname(outPath);
    buildId = null;
  } else {
    buildId = makeBuildId();
    buildDir = path.join(docBaseDir, 'builds', buildId);
    outPath = path.join(buildDir, 'index.html');
  }
  fs.mkdirSync(buildDir, { recursive: true });

  // 상대경로 자산 링크(styles.css/extracted_images/ASSET_BASE)는 출력 파일의 실제 깊이에
  // 맞춰야 한다(fixAssetPaths 참고 - 2026-09-19 8차에서 발견된 "QA PASS인데 로고가 깨진"
  // 버그의 근본 수정이 이 안에 있다. 12차에서 표준 파이프라인/페이지 편집기가 공유하도록 뽑아냄).
  const htmlFixed = fixAssetPaths(html, outPath);
  fs.writeFileSync(outPath, htmlFixed, 'utf-8');

  if (buildId) {
    const manifest = {
      build_id: buildId,
      doc_id: docId,
      created_at: new Date().toISOString(),
      template_version: TEMPLATE_VERSION,
      input_hash: inputHash(dataText),
      schema_version: data.schema_version || null,
      status: 'built', // check.js가 이 값을 'checked_pass'/'checked_fail'로 갱신
      issue_count: issues.length,
    };
    fs.writeFileSync(path.join(buildDir, 'manifest.json'), JSON.stringify(manifest, null, 2), 'utf-8');
    fs.writeFileSync(path.join(buildDir, 'issues.json'), JSON.stringify(issues, null, 2), 'utf-8');
    fs.copyFileSync(dataPath, path.join(buildDir, 'data.json'));
  }

  console.log(`작성 완료: ${outPath} (${pageNum - 1}p 내지 + 표지 1p)${buildId ? `, build_id=${buildId}` : ''}`);
  console.log(`issues: ${issues.length}건${issues.length ? ' (blk-fallback/degraded 등 - 최종 출력 확정 전 확인 필요)' : ''}`);
  if (buildId) console.log(`BUILD_DIR=${buildDir}`);
}

// 2026-09-19 12차(페이지 편집기) 안정화: page_manifest.js/page_candidate.js가 이 파일을
// require()해서 buildStep1Pages/buildStep2Pages/buildStep3Page/wrapHalves를 재사용한다
// (전체 문서를 다시 배치하지 않고 "이미 정해진 페이지 유형(halves/fullpage)"에 조각 하나만
// 다시 넣기 위함). require될 때 main()이 process.argv를 오독하며 즉시 실행되면 안 되므로
// CLI로 직접 실행됐을 때만(node generate.js ...) main()을 돈다 - 하위 호출부(Python
// 라우터의 subprocess.run(["node","generate.js",...]))는 100% 그대로 동작한다.
if (require.main === module) {
  main().catch((err) => { console.error(err); process.exit(1); });
}

module.exports = {
  buildStep1Pages, buildStep2Pages, buildStep3Page, wrapHalves, screenLabelNum,
  fixAssetPaths, TEMPLATE_VERSION, STEP1_VOCAB_INTRO, STEP1_OX_INTRO,
};
