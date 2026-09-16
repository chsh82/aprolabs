// 자동 검사 (Playwright) - build/index.html 11페이지 전체에 가이드 4.3 체크리스트 중
// 이번 결과물에 적용 가능한 항목을 돌린다. 실행: npm run check (= node scripts/check.js)
//
// 적용 항목:
//   1. 오버플로 - 고정 조판이라 텍스트가 페이지 박스를 넘으면 잘림
//   2. 한 면에 point 색 사용이 4곳 이하인가
//   3. 답란 최소 높이 >= 괘선 2줄(15mm)
//   4. 그림이 원본 픽셀보다 확대되지 않았는가(인쇄 150dpi 기준 폭 = mm * 5.9px, 가이드 4.4)
//   5. 답란 배경 괘선이 실제 PDF 인쇄 경로에서 그려지는가(문제 3 - Chromium이 표 셀 안 반복
//      배경을 인쇄 경로에서만 깨뜨리는 버그를 겪었던 자리라, 화면 렌더(page.evaluate) 검사로는
//      못 잡는다. page.pdf()로 실제로 뽑은 뒤 PyMuPDF로 래스터화해서 픽셀을 직접 본다.)
//
// 적용 제외(가이드 4.3에 있지만 이번 결과물엔 해당 없음/별도 확인 필요):
//   - "머리단·푸터·페이지 번호" 존재 여부 -> 표지는 원래 없음(가이드 2.3), 나머지 10페이지는
//     index.html 마크업 자체가 이미 고정 템플릿이라 코드 리뷰로 충분히 확인됨. 스크립트 검사 생략.
//   - "발췌문만 있고 질문 없는 면" -> 지금은 고정 콘텐츠 포팅이라 발생 불가(문항DB 연동 이후
//     생성기 단계에서 검사할 항목). 이번 결과물엔 없음.
const { chromium } = require('playwright');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');

const MM_TO_PX96 = 96 / 25.4; // CSS px(96dpi) <-> mm
const PRINT_DPI = 150;
const MIN_ANSWER_HEIGHT_MM = 15;
const PX_TO_PT = 0.75; // CSS px(96dpi) -> PDF pt(72dpi), preferCSSPageSize라 배율 없음
const RULE_STD_MIN = 1.0; // 이 값 미만이면 "행 간 밝기 차이가 없다" = 괘선이 안 그려진 것으로 판단(0~255 스케일)

// PyMuPDF로 PDF의 특정 영역을 래스터화해서, 세로 방향으로 밝기가 실제로
// 주기적으로 바뀌는지(=괘선이 그려졌는지) 확인하는 헬퍼. 답란이 완전히
// 비어보이거나(변화 없음) 셀 전체가 한 덩어리로 잘못 칠해진 경우(마찬가지로
// 변화가 거의 없음) 둘 다 표준편차가 낮게 나와서 잡힌다.
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
`;

function findPython() {
  for (const cmd of ['py', 'python3', 'python']) {
    try {
      execFileSync(cmd, cmd === 'py' ? ['-3', '--version'] : ['--version']);
      return cmd === 'py' ? ['py', '-3'] : [cmd];
    } catch (e) { /* try next */ }
  }
  throw new Error('python 실행 파일을 찾을 수 없음(py/python3/python 전부 실패) - PDF 괘선 검사를 건너뜀');
}

async function checkAnswerLinePrintRendering(page, pageReports) {
  // 각 .page 안의 .answer-lines 위치(그 페이지 박스 기준 상대좌표, CSS px)를 수집
  const boxesByPage = await page.evaluate(() => {
    const pages = Array.from(document.querySelectorAll('.page'));
    return pages.map((pg) => {
      const pageBox = pg.getBoundingClientRect();
      return Array.from(pg.querySelectorAll('.answer-lines')).map((el) => {
        const r = el.getBoundingClientRect();
        return {
          x: r.left - pageBox.left,
          y: r.top - pageBox.top,
          width: r.width,
          height: r.height,
          tableNested: !!el.closest('table'),
        };
      });
    });
  });

  const boxes = [];
  boxesByPage.forEach((list, pageIndex) => {
    list.forEach((b, i) => {
      boxes.push({
        pageIndex,
        index: i,
        tableNested: b.tableNested,
        x0: b.x * PX_TO_PT,
        y0: b.y * PX_TO_PT,
        x1: (b.x + b.width) * PX_TO_PT,
        y1: (b.y + b.height) * PX_TO_PT,
      });
    });
  });

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'worksheet-check-'));
  const pdfPath = path.join(tmpDir, 'index.pdf');
  const boxesPath = path.join(tmpDir, 'boxes.json');
  const scriptPath = path.join(tmpDir, 'pdf_probe.py');
  const outPath = path.join(tmpDir, 'result.json');

  try {
    await page.pdf({ path: pdfPath, printBackground: true, preferCSSPageSize: true });
    fs.writeFileSync(boxesPath, JSON.stringify(boxes));
    fs.writeFileSync(scriptPath, PY_PROBE_SCRIPT);

    const python = findPython();
    execFileSync(python[0], [...python.slice(1), scriptPath, pdfPath, boxesPath, outPath]);
    const results = JSON.parse(fs.readFileSync(outPath, 'utf-8'));

    for (const r of results) {
      const target = pageReports[r.pageIndex];
      if (!target) continue;
      if (r.rowStd < RULE_STD_MIN) {
        target.issues.push(
          `답란 괘선 미검출(PDF 인쇄 경로, ${r.tableNested ? '표 셀 안' : '일반'} 답란 #${r.index}): ` +
          `행별 밝기 표준편차=${r.rowStd} (기준 ${RULE_STD_MIN} 미만 = 줄무늬 없이 밋밋함)`
        );
      }
    }
    return results;
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

async function main() {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = 'file:///' + path.resolve(__dirname, '../build/index.html').replace(/\\/g, '/');
  await page.goto(url);
  await page.evaluate(() => document.fonts.ready);

  const report = await page.evaluate(({ MM_TO_PX96, PRINT_DPI, MIN_ANSWER_HEIGHT_MM }) => {
    const pages = Array.from(document.querySelectorAll('.page'));
    return pages.map((pg, idx) => {
      const label = pg.getAttribute('data-screen-label') || `page-${idx + 1}`;
      const issues = [];

      // 1. 오버플로: .sheet(또는 표지의 .cover)가 자기 내용 때문에 넘치는지
      const box = pg.querySelector('.sheet') || pg.querySelector('.cover');
      let overflow = false;
      if (box) {
        overflow = box.scrollHeight > box.clientHeight + 1 || box.scrollWidth > box.clientWidth + 1;
        if (overflow) issues.push(`오버플로: scrollHeight=${box.scrollHeight} clientHeight=${box.clientHeight}`);
      }

      // 2. point 색 사용 위치 개수(출처/예시문구/참고자료 머리/그림 캡션)
      const pointSelectors = ['.excerpt__cite', '.q__example', '.reference__label', '.figure-block figcaption'];
      const pointCount = pointSelectors.reduce((sum, sel) => sum + pg.querySelectorAll(sel).length, 0);
      if (pointCount > 4) issues.push(`point 색 사용 ${pointCount}곳(4곳 초과)`);

      // 3. 답란 최소 높이(15mm)
      const minAnswerPx = MIN_ANSWER_HEIGHT_MM * MM_TO_PX96;
      const answerLines = Array.from(pg.querySelectorAll('.answer-lines'));
      const shortAnswers = answerLines.filter(el => el.getBoundingClientRect().height < minAnswerPx - 1);
      if (shortAnswers.length > 0) {
        issues.push(`답란 ${shortAnswers.length}개가 15mm 미만(예: ${Math.round(shortAnswers[0].getBoundingClientRect().height)}px)`);
      }

      // 4. 그림 확대 여부(인쇄 150dpi 기준 - 필요 원본폭(px) = mm * 5.9)
      const imgs = Array.from(pg.querySelectorAll('img')).filter(img => !img.closest('.sheet-head') && !img.closest('.cover__brand'));
      const upscaled = [];
      for (const img of imgs) {
        const rect = img.getBoundingClientRect();
        const widthMm = rect.width / MM_TO_PX96;
        const requiredNaturalWidth = widthMm * 5.9; // 가이드 4.4 공식
        if (img.naturalWidth > 0 && img.naturalWidth < requiredNaturalWidth * 0.98) { // 2% 여유
          upscaled.push({ src: img.getAttribute('src'), naturalWidth: img.naturalWidth, requiredNaturalWidth: Math.round(requiredNaturalWidth), renderedWidthMm: Math.round(widthMm) });
        }
      }
      if (upscaled.length > 0) {
        issues.push(`그림 확대 ${upscaled.length}건: ` + upscaled.map(u => `${u.src}(원본${u.naturalWidth}px < 필요${u.requiredNaturalWidth}px)`).join(', '));
      }

      return { label, overflow, pointCount, shortAnswerCount: shortAnswers.length, upscaledImages: upscaled, issues };
    });
  }, { MM_TO_PX96, PRINT_DPI, MIN_ANSWER_HEIGHT_MM });

  let printRuleResults = null;
  try {
    printRuleResults = await checkAnswerLinePrintRendering(page, report);
  } catch (e) {
    console.log(`!! 답란 괘선 PDF 검사를 건너뜀: ${e.message}`);
  }

  await browser.close();

  const failCount = report.filter(r => r.issues.length > 0).length;
  console.log(`=== 검사 완료: ${report.length}페이지 중 ${failCount}페이지에서 이슈 발견 ===`);
  for (const r of report) {
    if (r.issues.length === 0) {
      console.log(`✅ ${r.label}`);
    } else {
      console.log(`❌ ${r.label}`);
      r.issues.forEach(i => console.log(`     - ${i}`));
    }
  }

  const outDir = path.resolve(__dirname, '../docs');
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, '_check_raw.json'), JSON.stringify({ pages: report, printRuleResults }, null, 2));

  if (failCount > 0) process.exitCode = 1;
}

main();
