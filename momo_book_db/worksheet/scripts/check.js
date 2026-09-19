// 자동 검사 (Playwright) - 학습지 HTML 결과물을 가이드 4.3 체크리스트 기준으로 검사한다.
// 실행: npm run check (인자 없으면 예전처럼 build/index.html을 검사 - 하위 호환)
//       node scripts/check.js <html경로> [data.json경로] [보고서출력경로]
//
// 2026-09-18 1차 안정화: build/index.html 고정 대신 임의 HTML을 검사할 수 있게 하고,
// PASS/FAIL/NOT_EXECUTED/BLOCKED 네 가지 상태를 구분한다(예전엔 "검사를 못 돌렸다"와
// "돌려서 통과했다"가 구분 안 돼서 의존성이 없어도 그냥 PASS로 보였다). data.json을
// 같이 주면 입력->출력 콘텐츠 보존 대조(3D)도 돈다.
//
// 적용 항목:
//   1. 오버플로 - 페이지(.sheet/.cover) 전체뿐 아니라 반면(.half) 단위로도 본다.
//      .half는 .sheet(flex-direction:column) 안에서 flex:1로 배치되는데, 형제
//      .half가 있으면 내용이 넘쳐도 이웃 반면을 밀어내지 않고 그 위에 겹쳐 그려진다 -
//      .sheet 레벨 스크롤 높이만 보면 이 겹침을 못 잡는다(2026-09-18 파일럿에서 실제로
//      L5-Q4-W10 1단계 어휘표가 OX 문단과 14px 겹치는 걸 이 방식으로 처음 발견함).
//   2. 한 면에 point 색 사용이 4곳 이하인가
//   3. 답란 최소 높이 >= 괘선 2줄(15mm)
//   4. 그림이 원본 픽셀보다 확대되지 않았는가 + 이미지 로딩 자체가 실패하지 않았는가
//   5. 답란 배경 괘선이 실제 PDF 인쇄 경로에서 그려지는가(PyMuPDF로 래스터화해 확인).
//      이 검사를 못 돌리면(파이썬/PyMuPDF 없음) 전체를 PASS로 보고하지 않고 BLOCKED로 남긴다.
//   6. 콘텐츠 보존 대조(data.json 주어졌을 때만) - 문항/어휘/OX/글감 outline이 각각
//      대응하는 data-qa-id/data-vocab-id/data-ox-id/data-outline-id로 DOM에 있는지.
//   7. 폰트 로딩 상태, 페이지 크기(A4), PDF 생성 자체, PDF 페이지 수 일치.
const { chromium } = require('playwright');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');

const MM_TO_PX96 = 96 / 25.4; // CSS px(96dpi) <-> mm
const MIN_ANSWER_HEIGHT_MM = 15;
const PX_TO_PT = 0.75; // CSS px(96dpi) -> PDF pt(72dpi), preferCSSPageSize라 배율 없음
const RULE_STD_MIN = 1.0; // 이 값 미만이면 "행 간 밝기 차이가 없다" = 괘선이 안 그려진 것으로 판단(0~255 스케일)
const A4_WIDTH_MM = 210, A4_HEIGHT_MM = 297;
const A4_TOLERANCE_MM = 1;

const PY_PROBE_SCRIPT = `
import fitz, json, sys
pdf_path, boxes_path, out_path = sys.argv[1:4]
boxes = json.load(open(boxes_path, encoding='utf-8'))
doc = fitz.open(pdf_path)
ZOOM = 3
results = []
for b in boxes:
    page = doc[b['pageIndex']]
    rect = fitz.Rect(b['x0'], b['y0'], b['x1'], b['y1'])
    pix = page.get_pixmap(clip=rect, matrix=fitz.Matrix(ZOOM, ZOOM))
    n, w, h = pix.n, pix.width, pix.height
    samples = pix.samples
    row_means = []
    for row in range(h):
        base = row * w * n
        row_bytes = samples[base:base + w * n]
        row_means.append(sum(row_bytes) / len(row_bytes))
    mean = sum(row_means) / len(row_means) if row_means else 0.0
    var = sum((v - mean) ** 2 for v in row_means) / len(row_means) if row_means else 0.0
    std = var ** 0.5
    results.append({**b, 'rowStd': round(std, 3)})
json.dump(results, open(out_path, 'w', encoding='utf-8'))
PDF_PAGE_COUNT_MARKER = len(doc)
print(PDF_PAGE_COUNT_MARKER)
`;

function findPython() {
  for (const cmd of ['py', 'python3', 'python']) {
    try {
      execFileSync(cmd, cmd === 'py' ? ['-3', '--version'] : ['--version']);
      return cmd === 'py' ? ['py', '-3'] : [cmd];
    } catch (e) { /* try next */ }
  }
  return null;
}

// PDF 답란 괘선 검사. 성공하면 {status:'PASS'|'FAIL', results}, 파이썬/PyMuPDF가 없거나
// 실행에 실패하면 {status:'BLOCKED', reason}을 돌려준다 - 절대 조용히 생략하고 넘어가지
// 않는다(2026-09-18 이전 버전은 catch만 하고 printRuleResults=null로 둔 채 exitCode에
// 반영을 안 해서, 이 검사가 아예 안 돌아도 전체 PASS로 보일 수 있었다).
async function checkAnswerLinePrintRendering(page, pageReports) {
  const python = findPython();
  if (!python) {
    return { status: 'BLOCKED', reason: 'python 실행 파일을 찾을 수 없음(py/python3/python 전부 실패)' };
  }

  const boxesByPage = await page.evaluate(() => {
    const pages = Array.from(document.querySelectorAll('.page'));
    return pages.map((pg) => {
      const pageBox = pg.getBoundingClientRect();
      return Array.from(pg.querySelectorAll('.answer-lines')).map((el) => {
        const r = el.getBoundingClientRect();
        return {
          x: r.left - pageBox.left, y: r.top - pageBox.top,
          width: r.width, height: r.height,
          tableNested: !!el.closest('table'),
        };
      });
    });
  });

  const boxes = [];
  boxesByPage.forEach((list, pageIndex) => {
    list.forEach((b, i) => {
      boxes.push({
        pageIndex, index: i, tableNested: b.tableNested,
        x0: b.x * PX_TO_PT, y0: b.y * PX_TO_PT,
        x1: (b.x + b.width) * PX_TO_PT, y1: (b.y + b.height) * PX_TO_PT,
      });
    });
  });

  if (boxes.length === 0) {
    return { status: 'PASS', results: [], note: '검사할 답란(.answer-lines)이 없음' };
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'worksheet-check-'));
  const pdfPath = path.join(tmpDir, 'index.pdf');
  const boxesPath = path.join(tmpDir, 'boxes.json');
  const scriptPath = path.join(tmpDir, 'pdf_probe.py');
  const outPath = path.join(tmpDir, 'result.json');

  try {
    await page.pdf({ path: pdfPath, printBackground: true, preferCSSPageSize: true });
    fs.writeFileSync(boxesPath, JSON.stringify(boxes));
    fs.writeFileSync(scriptPath, PY_PROBE_SCRIPT);

    let pdfPageCount;
    try {
      const out = execFileSync(python[0], [...python.slice(1), scriptPath, pdfPath, boxesPath, outPath], { encoding: 'utf-8' });
      pdfPageCount = parseInt(out.trim().split('\n').pop(), 10);
    } catch (e) {
      return { status: 'BLOCKED', reason: `PDF 괘선 검사 실행 실패: ${e.message}` };
    }
    const results = JSON.parse(fs.readFileSync(outPath, 'utf-8'));

    const issues = [];
    for (const r of results) {
      const target = pageReports[r.pageIndex];
      if (!target) continue;
      if (r.rowStd < RULE_STD_MIN) {
        const msg = `답란 괘선 미검출(PDF 인쇄 경로, ${r.tableNested ? '표 셀 안' : '일반'} 답란 #${r.index}): ` +
          `행별 밝기 표준편차=${r.rowStd} (기준 ${RULE_STD_MIN} 미만 = 줄무늬 없이 밋밋함)`;
        target.issues.push(msg);
        issues.push({ page: r.pageIndex, message: msg });
      }
    }
    return { status: issues.length > 0 ? 'FAIL' : 'PASS', results, pdfPageCount, pdfPath: null };
  } catch (e) {
    return { status: 'BLOCKED', reason: `PDF 생성/검사 중 예외: ${e.message}` };
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

// 콘텐츠 보존 대조(3D) - data.json이 주어졌을 때만 돈다. 각 원본 행이 렌더된 DOM에
// data-*-id로 남아 있는지만 확인한다(텍스트 자체를 다시 비교하면 공백 정규화 등으로
// "그럴듯하게 통과"시키는 함정이 생기기 쉬워서, blocks.js가 이미 새긴 안정적 id로
// "그 행이 렌더링 결과에 존재하는가"를 1차로 본다 - 이 ID들은 esc()된 텍스트와 별개로
// blocks.js가 항상 실제 DB id를 그대로 찍으므로 내용 누락과 별개로 신뢰할 수 있다).
// model_answer는 설계상 학생용 출력에서 의도적으로 제외되는 필드라 대조 대상에서 뺀다.
async function checkContentParity(page, data) {
  const issues = [];
  const domIds = await page.evaluate(() => ({
    qa: Array.from(document.querySelectorAll('[data-qa-id]')).map(el => el.getAttribute('data-qa-id')),
    vocab: Array.from(document.querySelectorAll('[data-vocab-id]')).map(el => el.getAttribute('data-vocab-id')),
    ox: Array.from(document.querySelectorAll('[data-ox-id]')).map(el => el.getAttribute('data-ox-id')),
    outline: Array.from(document.querySelectorAll('[data-outline-id]')).map(el => el.getAttribute('data-outline-id')),
    imgSrcs: Array.from(document.querySelectorAll('img')).map(el => el.getAttribute('src')),
  }));

  const qaIds = new Set(domIds.qa);
  for (const item of data.step2 || []) {
    if (!qaIds.has(String(item.id))) {
      issues.push(`discussion_qa#${item.id}(order ${item.order_label})가 결과 DOM에 없음 - 문항 누락`);
    }
  }
  const vocabIds = new Set(domIds.vocab);
  for (const v of (data.step1 && data.step1.vocab) || []) {
    if (!vocabIds.has(String(v.id))) issues.push(`vocabulary#${v.id}(${v.word})가 결과 DOM에 없음`);
  }
  const oxIds = new Set(domIds.ox);
  for (const o of (data.step1 && data.step1.ox) || []) {
    if (!oxIds.has(String(o.id))) issues.push(`ox_quiz#${o.id}가 결과 DOM에 없음`);
  }
  const outlineIds = new Set(domIds.outline);
  if (data.step3 && data.step3.outline_questions) {
    for (const q of data.step3.outline_questions) {
      if (!outlineIds.has(String(q.id))) issues.push(`essay_outline_question#${q.id}가 결과 DOM에 없음`);
    }
  }

  // 이미지: data.json이 가리키는 경로 중 실제로 하나라도 <img src>에 등장하는지(경로
  // 보정 접두사가 build마다 달라질 수 있어 파일명 기준 부분일치로 본다).
  const srcJoined = domIds.imgSrcs.join('\n');
  const checkImg = (relPath, label) => {
    if (!relPath) return;
    const filename = String(relPath).split('/').pop();
    if (!srcJoined.includes(filename)) issues.push(`${label} 이미지가 결과 DOM에 없음: ${relPath}`);
  };
  checkImg(data.meta && data.meta.book && data.meta.book.cover_image, '표지');
  for (const item of data.step2 || []) {
    checkImg(item.excerpt_image_path, `discussion_qa#${item.id} 발췌`);
    checkImg(item.reference_image_path, `discussion_qa#${item.id} 참고자료`);
    // 2026-09-19 8차 안정화: 참고자료 2번째 그림(id=1607 복원처럼 참고자료 이미지가
    // 2개인 경우) - blocks.js의 referenceBlock() 확장과 짝을 이룸.
    checkImg(item.ui_config && item.ui_config.reference_image2_path, `discussion_qa#${item.id} 참고자료2`);
    // 2026-09-19 6차 안정화: text_short_multi의 blanks[] 행별 그림(id=1611 복원처럼
    // 대상별 그림이 있는 경우) - 있으면 다른 이미지 필드와 동일하게 존재 여부를 검사.
    for (const b of (item.ui_config && item.ui_config.blanks) || []) {
      if (b && typeof b === 'object' && b.image_path) {
        checkImg(b.image_path, `discussion_qa#${item.id} 빈칸(${b.label || b.image_caption || ''})`);
      }
    }
  }
  if (data.step3 && data.step3.essay) checkImg(data.step3.essay.image_path, '글쓰기 안내');

  return issues;
}

async function main() {
  const htmlArg = process.argv[2];
  const rest = process.argv.slice(3);
  // data.json 경로 자동 추정: html과 같은 폴더의 data.json (설명서 없이 두 번째
  // 인자를 보고서 경로로 오해하지 않도록, .json으로 끝나는 인자만 data.json으로 취급).
  let dataArg = null, reportArg = null;
  for (const a of rest) {
    if (a.endsWith('.json') && !dataArg && fs.existsSync(a)) dataArg = a;
    else reportArg = a;
  }

  const htmlPath = htmlArg
    ? path.resolve(htmlArg)
    : path.resolve(__dirname, '../build/index.html'); // 인자 없으면 예전 기본값 유지(하위 호환)
  if (!dataArg) {
    const guess = path.join(path.dirname(htmlPath), 'data.json');
    if (fs.existsSync(guess)) dataArg = guess;
  }
  const outDir = reportArg ? path.dirname(path.resolve(reportArg)) : path.resolve(__dirname, '../docs');
  const outPath = reportArg ? path.resolve(reportArg) : path.join(outDir, '_check_raw.json');

  // generate.js가 같은 빌드 폴더에 남긴 issues.json(입력 검증 + 조판 단계에서 degraded
  // fallback/unknown/넘침으로 낮춘 항목들) - "6. 불완전한 문항의 처리": approved 데이터라도
  // 이 목록에 뭔가 있으면 자동으로 최종 출력 승인하지 않는다(데이터 승인 상태와 조판 검사
  // 상태는 별개). 검토용 미리보기에서는 보여도 되지만 QA는 통과시키지 않는다.
  const issuesPath = path.join(path.dirname(htmlPath), 'issues.json');
  let genIssues = [];
  if (fs.existsSync(issuesPath)) {
    try { genIssues = JSON.parse(fs.readFileSync(issuesPath, 'utf-8')); } catch (e) { /* 무시하지 않고 아래서 별도 issue로 남김 */
      genIssues = [{ reason: `issues.json 파싱 실패: ${e.message}`, stage: 'meta' }];
    }
  }

  if (!fs.existsSync(htmlPath)) {
    const result = { status: 'FAIL', reason: `검사 대상 HTML이 없음: ${htmlPath}`, pages: [] };
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
    console.log(`FAIL: ${result.reason}`);
    process.exitCode = 1;
    return;
  }

  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = 'file:///' + htmlPath.replace(/\\/g, '/');
  await page.goto(url);
  const fontStatus = await page.evaluate(async () => {
    await document.fonts.ready;
    const families = ['Noto Sans KR', 'Noto Serif KR', 'Gowun Batang'];
    return families.map(f => ({ family: f, loaded: document.fonts.check(`16px "${f}"`) }));
  });

  const { report, pageCount } = await page.evaluate(({ MM_TO_PX96, MIN_ANSWER_HEIGHT_MM, A4_WIDTH_MM, A4_HEIGHT_MM, A4_TOLERANCE_MM }) => {
    const pages = Array.from(document.querySelectorAll('.page'));
    const rows = pages.map((pg, idx) => {
      const label = pg.getAttribute('data-screen-label') || `page-${idx + 1}`;
      const issues = [];
      const warnings = [];

      // 1a. 페이지 전체(.sheet/.cover) 오버플로
      const box = pg.querySelector('.sheet') || pg.querySelector('.cover');
      let overflow = false;
      if (box) {
        overflow = box.scrollHeight > box.clientHeight + 1 || box.scrollWidth > box.clientWidth + 1;
        if (overflow) issues.push(`오버플로(페이지 전체): scrollHeight=${box.scrollHeight} clientHeight=${box.clientHeight}`);
      }
      // 1b. 반면(.half) 단위 오버플로 - flex 형제라 안 밀리고 겹쳐 그려지므로 페이지
      // 레벨 검사만으로는 못 잡는다(주석 상단 설명 참고).
      const halves = Array.from(pg.querySelectorAll('.half'));
      halves.forEach((h, i) => {
        if (h.scrollHeight > h.clientHeight + 1) {
          issues.push(`오버플로(반면 #${i}, ${h.className}): scrollHeight=${h.scrollHeight} clientHeight=${h.clientHeight} - 형제 반면과 겹쳐 그려질 수 있음`);
        }
      });

      // 2. point 색 사용 위치 개수
      const pointSelectors = ['.excerpt__cite', '.q__example', '.reference__label', '.figure-block figcaption'];
      const pointCount = pointSelectors.reduce((sum, sel) => sum + pg.querySelectorAll(sel).length, 0);
      if (pointCount > 4) issues.push(`point 색 사용 ${pointCount}곳(4곳 초과)`);

      // 3. 서술형 필기 공간 최소 높이(15mm) - 괘선 답란(.answer-lines)뿐 아니라
      // 말풍선(.speech-bubble, 2026-09-19 10차 안정화 추가)도 "학생이 문장 하나를
      // 온전히 쓰는 공간"이라는 점은 같다 - 답란 모양이 괘선에서 말풍선으로 바뀌었다고
      // 최소 크기 검사를 우회하면 안 된다는 사용자 지시("긴 서술형의 기존 최소 필기
      // 공간 기준은 유지"). 인라인 빈칸(.inline-blank)은 단어 하나 채우는 용도라 이
      // 검사 대상이 아니다(다른 성격 - 아래 5-2 참고).
      const minAnswerPx = MIN_ANSWER_HEIGHT_MM * MM_TO_PX96;
      const answerAreas = Array.from(pg.querySelectorAll('.answer-lines, .speech-bubble'));
      const shortAnswers = answerAreas.filter(el => el.getBoundingClientRect().height < minAnswerPx - 1);
      if (shortAnswers.length > 0) {
        issues.push(`서술형 필기 공간 ${shortAnswers.length}개가 15mm 미만(예: ${Math.round(shortAnswers[0].getBoundingClientRect().height)}px, ${shortAnswers[0].className})`);
      }

      // 4. 그림 확대 여부(로고 제외) + 로딩 실패(로고 포함 - 전부)
      // 2026-09-19 8차 안정화에서 발견된 QA 사각지대: 이전엔 .sheet-head(머리말 로고)/
      // .cover__brand(표지 로고)를 "로딩 실패" 검사에서도 통째로 제외하고 있어서, 로고
      // 이미지 경로가 깨져도(generate.js의 ASSET_BASE 경로 버그 - 그쪽 수정 참고) QA가
      // 계속 PASS로 나왔다(실측: L1-Q4-W01-927494f5 등 - 로고가 깨진 PDF인데도 PASS).
      // 로고는 "저해상도(원본보다 확대됨)" 검사에서만 제외한다(고정 크기 브랜드 자산이라
      // 이 검사는 애초에 의미가 없음) - "로딩 자체가 됐는지"는 다른 이미지와 동일하게
      // 전부 검사해야 정직한 QA다.
      const allImgs = Array.from(pg.querySelectorAll('img'));
      const resCheckImgs = allImgs.filter(img => !img.closest('.sheet-head') && !img.closest('.cover__brand'));
      const upscaled = [];
      const failedImages = [];
      for (const img of allImgs) {
        if (img.complete && img.naturalWidth === 0) {
          // naturalWidth===0인데 complete===true면 로딩 자체가 실패한 것(2026-09-18
          // 이전 버전은 naturalWidth>0 조건 때문에 이 경우를 그냥 건너뛰어 통과시켰다).
          failedImages.push(img.getAttribute('src'));
        } else if (!img.complete) {
          failedImages.push(img.getAttribute('src') + ' (로딩 미완료)');
        }
      }
      for (const img of resCheckImgs) {
        if (!img.complete || img.naturalWidth === 0) continue; // 로딩 실패는 위에서 이미 잡음
        const rect = img.getBoundingClientRect();
        const widthMm = rect.width / MM_TO_PX96;
        const requiredNaturalWidth = widthMm * 5.9; // 가이드 4.4 공식
        if (img.naturalWidth < requiredNaturalWidth * 0.98) {
          upscaled.push({ src: img.getAttribute('src'), naturalWidth: img.naturalWidth, requiredNaturalWidth: Math.round(requiredNaturalWidth), renderedWidthMm: Math.round(widthMm) });
        }
      }
      if (failedImages.length > 0) issues.push(`이미지 로딩 실패 ${failedImages.length}건: ${failedImages.join(', ')}`);
      if (upscaled.length > 0) {
        warnings.push(`그림 저해상도(경고) ${upscaled.length}건: ` + upscaled.map(u => `${u.src}(원본${u.naturalWidth}px < 필요${u.requiredNaturalWidth}px)`).join(', '));
      }

      // A4 페이지 크기(표지 제외 - .cover는 .page 자체 크기는 동일하므로 같이 봐도 됨)
      const rect = pg.getBoundingClientRect();
      const wMm = rect.width / MM_TO_PX96, hMm = rect.height / MM_TO_PX96;
      if (Math.abs(wMm - A4_WIDTH_MM) > A4_TOLERANCE_MM || Math.abs(hMm - A4_HEIGHT_MM) > A4_TOLERANCE_MM) {
        issues.push(`페이지 크기가 A4(210x297mm)와 다름: ${wMm.toFixed(1)}x${hMm.toFixed(1)}mm`);
      }

      return { label, overflow, pointCount, shortAnswerCount: shortAnswers.length, upscaledImages: upscaled, issues, warnings };
    });
    return { report: rows, pageCount: pages.length };
  }, { MM_TO_PX96, MIN_ANSWER_HEIGHT_MM, A4_WIDTH_MM, A4_HEIGHT_MM, A4_TOLERANCE_MM });

  const printRuleCheck = await checkAnswerLinePrintRendering(page, report);
  if (printRuleCheck.pdfPageCount != null && printRuleCheck.pdfPageCount !== pageCount) {
    printRuleCheck.status = 'FAIL';
    printRuleCheck.reason = `PDF 페이지 수(${printRuleCheck.pdfPageCount})가 화면 .page 개수(${pageCount})와 다름`;
  }

  let contentParityIssues = null;
  if (dataArg) {
    const data = JSON.parse(fs.readFileSync(dataArg, 'utf-8'));
    contentParityIssues = await checkContentParity(page, data);
  }

  await browser.close();

  const fontIssues = fontStatus.filter(f => !f.loaded).map(f => `폰트 미로딩: ${f.family}`);

  if (pageCount === 0) {
    const result = { status: 'FAIL', reason: '검사 대상 페이지(.page)가 0개', pages: [], printRuleCheck, fontStatus };
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
    console.log('FAIL: 검사 대상 페이지가 0개');
    process.exitCode = 1;
    return;
  }

  const blockingCount = report.filter(r => r.issues.length > 0).length +
    (printRuleCheck.status === 'FAIL' ? 1 : 0) +
    (contentParityIssues && contentParityIssues.length > 0 ? 1 : 0) +
    (fontIssues.length > 0 ? 1 : 0) +
    genIssues.length;
  const warnCount = report.filter(r => r.warnings.length > 0).length;

  let status;
  if (blockingCount > 0) status = 'FAIL';
  else if (printRuleCheck.status === 'BLOCKED') status = 'BLOCKED';
  else status = 'PASS';

  console.log(`=== 검사 완료: ${report.length}페이지, status=${status} (차단 이슈 ${blockingCount}건 / 경고 ${warnCount}건) ===`);
  for (const r of report) {
    const mark = r.issues.length > 0 ? '❌' : (r.warnings.length > 0 ? '⚠️ ' : '✅');
    console.log(`${mark} ${r.label}`);
    r.issues.forEach(i => console.log(`     - [FAIL] ${i}`));
    r.warnings.forEach(w => console.log(`     - [WARN] ${w}`));
  }
  if (genIssues.length) {
    console.log(`❌ 생성 단계 issues.json ${genIssues.length}건(데이터 불완전/degraded fallback 등 - 승인 대상 아님):`);
    genIssues.forEach(i => console.log(`     - [${i.stage || '?'}] ${i.table || i.order_label || ''} ${i.field || ''}: ${i.reason}`));
  }
  if (printRuleCheck.status === 'BLOCKED') console.log(`⛔ 답란 괘선 PDF 검사: BLOCKED - ${printRuleCheck.reason}`);
  if (fontIssues.length) fontIssues.forEach(f => console.log(`❌ ${f}`));
  if (contentParityIssues && contentParityIssues.length) {
    console.log(`❌ 콘텐츠 보존 대조 실패 ${contentParityIssues.length}건:`);
    contentParityIssues.forEach(i => console.log(`     - ${i}`));
  } else if (contentParityIssues) {
    console.log('✅ 콘텐츠 보존 대조 통과');
  } else {
    console.log('ℹ️  콘텐츠 보존 대조: data.json을 못 찾아 NOT_EXECUTED');
  }

  const contentParity = contentParityIssues === null
    ? { status: 'NOT_EXECUTED' }
    : { status: contentParityIssues.length === 0 ? 'PASS' : 'FAIL', issues: contentParityIssues };

  const result = {
    status, blockingCount, warnCount,
    pages: report, printRuleCheck, fontStatus, contentParity, generationIssues: genIssues,
  };

  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));

  if (status !== 'PASS') process.exitCode = 1;
}

main();
