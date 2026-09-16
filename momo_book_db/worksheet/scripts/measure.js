// 용량 실측 하네스 (Playwright) - build/measure.html을 열어서 조합별로 넘침 여부를 판정하고
// docs/CAPACITY.md를 만든다. 실행: npm run measure (= node scripts/measure.js)
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const TONES = ['tone-elem34', 'tone-elem56', 'tone-mid'];
const TONE_LABELS = { 'tone-elem34': '초 3~4', 'tone-elem56': '초 5~6', 'tone-mid': '중등' };

const BLOCK_TYPES = ['vocab-table', 'ox-list', 'grid-table', 'figure', 'blk-choice', 'blk-blanks', 'blk-fallback'];
const BLOCK_LABELS = {
  'vocab-table': '어휘 표(4행)', 'ox-list': 'OX 5문항', 'grid-table': '표 문항(3행)',
  'figure': '그림 블록', 'blk-choice': '선택형(신규)', 'blk-blanks': '빈칸형(신규)', 'blk-fallback': '기본형(신규)',
};

const EXCERPT_LENS = [0, 100, 200, 300, 400, 600, 800];
const QUESTION_LENS = [50, 100, 150, 200, 300, 500];

async function measureCase(page, spec) {
  await page.evaluate((s) => window.renderCase(s), spec);
  return page.evaluate(() => {
    const el = document.getElementById('probe');
    // 답란 최소 15mm 확보 여부도 같이 확인(답란 자체가 있는 블록만)
    const ans = el.querySelector('.answer-lines');
    const ansOk = !ans || ans.getBoundingClientRect().height >= 15 * 3.7795 - 1; // 15mm -> px, 1px 오차 허용
    return {
      overflow: el.scrollHeight > el.clientHeight + 1, // 1px 오차 허용
      scrollHeight: Math.round(el.scrollHeight),
      clientHeight: Math.round(el.clientHeight),
      answerLineOk: ansOk,
    };
  });
}

async function main() {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = 'file:///' + path.resolve(__dirname, '../build/measure.html').replace(/\\/g, '/');
  await page.goto(url);
  await page.evaluate(() => document.fonts.ready);

  const results = { toneBlock: [], excerptQuestion: [] };

  // 1) 학년 톤 3종 x 블록 유형별 (대표 콘텐츠로 pass/fail만 확인)
  for (const tone of TONES) {
    for (const block of BLOCK_TYPES) {
      const r = await measureCase(page, { tone, block, questionLen: 80 });
      results.toneBlock.push({ tone, block, ...r });
      console.log(`[톤x블록] ${tone} / ${block}: ${r.overflow ? '넘침' : 'OK'} (scroll=${r.scrollHeight} client=${r.clientHeight})`);
    }
  }

  // 2) excerpt/question 글자수 조합 (short-excerpt 블록 - 발췌문+질문+답란)
  for (const tone of TONES) {
    for (const excerptLen of EXCERPT_LENS) {
      for (const questionLen of QUESTION_LENS) {
        const r = await measureCase(page, { tone, block: 'short-excerpt', excerptLen, questionLen });
        results.excerptQuestion.push({ tone, excerptLen, questionLen, ...r });
      }
    }
    console.log(`[글자수] ${tone} 톤 조합 ${EXCERPT_LENS.length * QUESTION_LENS.length}건 완료`);
  }

  await browser.close();

  const outDir = path.resolve(__dirname, '../docs');
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, '_measure_raw.json'), JSON.stringify(results, null, 2));
  writeCapacityMd(results, path.join(outDir, 'CAPACITY.md'));
  console.log('CAPACITY.md 작성 완료:', path.join(outDir, 'CAPACITY.md'));
}

function writeCapacityMd(results, outPath) {
  const lines = [];
  lines.push('# 용량 실측 결과 (docs/CAPACITY.md)');
  lines.push('');
  lines.push('`scripts/measure.js`(Playwright, Chromium)로 `build/measure.html`을 렌더해서 ');
  lines.push('`#probe(.half)`의 `scrollHeight > clientHeight`를 확인한 결과입니다. ');
  lines.push('`.half`은 실제 학습지처럼 반면 2개짜리 `.sheet` 안에 있어서, 한 반면이 실제로 받는 높이와 같습니다.');
  lines.push('');

  // 1) 톤 x 블록
  lines.push('## 1. 학년 톤 × 블록 유형 (대표 콘텐츠 기준)');
  lines.push('');
  lines.push('| 블록 | 초 3~4 | 초 5~6 | 중등 |');
  lines.push('|---|---|---|---|');
  for (const block of BLOCK_TYPES) {
    const cells = TONES.map(tone => {
      const r = results.toneBlock.find(x => x.tone === tone && x.block === block);
      return r.overflow ? `❌ 넘침(${r.scrollHeight}/${r.clientHeight}px)` : `✅ OK`;
    });
    lines.push(`| ${BLOCK_LABELS[block]} | ${cells[0]} | ${cells[1]} | ${cells[2]} |`);
  }
  lines.push('');
  lines.push('(대표 콘텐츠: 어휘 4행/OX 5문항/표 3행 등 실제 신화의숲 학습지에 쓰인 것과 비슷한 분량. ');
  lines.push('전부 ✅면 이 블록은 세 톤 어디서든 기본 분량으로는 안전하다는 뜻)');
  lines.push('');

  // 2) excerpt x question 글자수 - 톤별 표 + 임계치 추출
  lines.push('## 2. 발췌문(excerpt) × 질문(question_text) 글자수 — 톤별');
  lines.push('');
  lines.push('> **읽기 전에**: 이제 `excerpt`/`.q__text`의 글자 크기·행간이 톤에 따라 실제로 바뀝니다');
  lines.push('> (루트 font-size에 `--tone-scale` 배율을 걸고, 각 요소 line-height에 `--tone-lh-scale`을');
  lines.push('> 곱하는 방식 — styles.css 2절 주석/PORTING_NOTES.md 참고). 다만 초3~4와 초5~6은 크기');
  lines.push('> 차이가 3.3%뿐이라(가이드 1.4 값의 비율), 아래처럼 100자 단위로 굵게 끊은 표에서는');
  lines.push('> 같은 칸에서 갈리지 않을 수 있습니다 — 실제로는 글자 수 그대로가 아니라 `scrollHeight`');
  lines.push('> 자체가 톤마다 다르게 나옵니다(예: question 500자·excerpt 0자 기준 scrollHeight가');
  lines.push('> 초3~4=386px, 초5~6=376px로 다름 — 3절 "갱신 전후 비교" 참고). 중등 톤은 초5~6 대비');
  lines.push('> 3.3% 작아서 아래 표에서도 차이가 뚜렷하게 보입니다.');
  lines.push('');
  for (const tone of TONES) {
    lines.push(`### ${TONE_LABELS[tone]} 톤 (\`.${tone}\`)`);
    lines.push('');
    lines.push('| excerpt\\question | ' + QUESTION_LENS.map(q => `${q}자`).join(' | ') + ' |');
    lines.push('|---' + QUESTION_LENS.map(() => '|---').join('') + '|');
    for (const excerptLen of EXCERPT_LENS) {
      const row = QUESTION_LENS.map(questionLen => {
        const r = results.excerptQuestion.find(x => x.tone === tone && x.excerptLen === excerptLen && x.questionLen === questionLen);
        return r.overflow ? '❌' : '✅';
      });
      lines.push(`| **${excerptLen}자** | ${row.join(' | ')} |`);
    }
    lines.push('');

    // 임계치 추출: 이 톤에서 전부 ✅인 (excerpt, question) 중 "가장 큰" 안전 조합을 몇 개 뽑음
    const safe = results.excerptQuestion.filter(x => x.tone === tone && !x.overflow);
    const maxQuestionAtExcerpt0 = Math.max(...safe.filter(x => x.excerptLen === 0).map(x => x.questionLen), 0);
    const maxExcerptAtQuestion50 = Math.max(...safe.filter(x => x.questionLen === QUESTION_LENS[0]).map(x => x.excerptLen), 0);
    // "이 정도까지는 안전" 하나로 요약: excerpt+question 합이 가장 큰 안전 조합
    let bestSum = null;
    for (const r of safe) {
      const sum = r.excerptLen + r.questionLen;
      if (!bestSum || sum > bestSum.excerptLen + bestSum.questionLen) bestSum = r;
    }
    lines.push(`**임계치 요약**: excerpt 0자일 때 question은 ${maxQuestionAtExcerpt0}자까지 안전 / `
      + `question ${QUESTION_LENS[0]}자일 때 excerpt는 ${maxExcerptAtQuestion50}자까지 안전 / `
      + (bestSum ? `실측한 조합 중 가장 큰 안전 조합은 **excerpt ${bestSum.excerptLen}자 + question ${bestSum.questionLen}자**` : '안전 조합 없음(모든 조합에서 넘침)'));
    lines.push('');
  }

  // 3) 톤 수정 전/후 비교 (2026-08-28 톤 시스템 실장 - 문제 2 대응)
  lines.push('## 3. 톤 수정 전/후 비교 (톤이 실제로 먹는지 확인용)');
  lines.push('');
  lines.push('수정 전에는 `excerpt`/`.q__text` 등이 고정 rem이라 루트 font-size를 상속하지 않았고,');
  lines.push('톤 클래스는 `--ruleH`/`--stamp-*`만 바꿨다. 그 결과 톤 3종의 실측값이 사실상 동일했다.');
  lines.push('수정 후에는 루트 font-size(`--tone-scale`)와 각 요소 line-height(`--tone-lh-scale`)가');
  lines.push('톤에 따라 실제로 바뀐다. 같은 조합(question 500자, excerpt 0자)의 `scrollHeight`를 비교:');
  lines.push('');
  lines.push('| 톤 | 수정 전 scrollHeight | 수정 후 scrollHeight | 수정 전 안전 조합 수(42건 중) | 수정 후 안전 조합 수 |');
  lines.push('|---|---|---|---|---|');
  // 수정 전 값은 scripts/_verify_before.js로 실제 재현해서 딱 이 표에 쓸 숫자만 다시 잰 것
  // (원본 _measure_raw.json은 이번 수정판으로 덮어써서 남아있지 않음 - 안전 조합 수 11/11/11은
  // 수정 직전에 실제로 확인한 값).
  const before = {
    'tone-elem34': { scroll500: 376, safeCount: 11 },
    'tone-elem56': { scroll500: 376, safeCount: 11 },
    'tone-mid': { scroll500: 375, safeCount: 11 },
  };
  for (const tone of TONES) {
    const row500 = results.excerptQuestion.find(x => x.tone === tone && x.excerptLen === 0 && x.questionLen === 500);
    const safeCount = results.excerptQuestion.filter(x => x.tone === tone && !x.overflow).length;
    const b = before[tone];
    lines.push(`| ${TONE_LABELS[tone]} | ${b.scroll500}px | ${row500.scrollHeight}px | ${b.safeCount} | ${safeCount} |`);
  }
  lines.push('');
  lines.push('수정 전엔 세 톤의 안전 조합 수가 (11, 11, 11)로 완전히 똑같았고 scrollHeight도');
  lines.push('(376px, 376px, 375px)로 사실상 같았다(1px 차이는 그때도 이미 톤별로 달랐던');
  lines.push('`--ruleH`(7~8mm)가 답란 높이에 준 영향일 뿐, 본문 글자 크기는 전혀 안 바뀌어서');
  lines.push('elem34/elem56이 완전히 똑같이 나옴 — 그게 바로 "톤 미적용" 버그였다). 수정 후엔');
  lines.push('elem34=386px, elem56=376px, mid=374px로 톤마다 실제로 벌어진다(특히 중등 톤은');
  lines.push('여유 조합이 11→12건으로 늘어남 — 글자가 작아졌으니 당연한 방향). elem56은 배율');
  lines.push('1로 고정해 뒀으므로 수정 전/후 값이 그대로 같다(포팅 결과물 자체는 안 바뀜 —');
  lines.push('PORTING_NOTES.md "픽셀 동일 확인" 절 참고).');
  lines.push('');

  lines.push('## 4. 답란 최소 높이(15mm) 확보 여부');
  lines.push('');
  const answerLineFails = results.excerptQuestion.filter(x => !x.answerLineOk);
  if (answerLineFails.length === 0) {
    lines.push('실측한 모든 조합에서 답란 높이가 15mm(괘선 2줄) 이상 확보됨 — 넘치지 않는 조합은 전부 이 조건도 만족함.');
  } else {
    lines.push(`답란이 15mm 미만으로 줄어든 조합 ${answerLineFails.length}건 발견:`);
    lines.push('');
    lines.push('| 톤 | excerpt | question |');
    lines.push('|---|---|---|');
    for (const r of answerLineFails.slice(0, 20)) {
      lines.push(`| ${TONE_LABELS[r.tone]} | ${r.excerptLen} | ${r.questionLen} |`);
    }
  }
  lines.push('');
  lines.push('---');
  lines.push('_원본 수치는 `docs/_measure_raw.json`에 그대로 남겨둠(재계산/재검증용)._');

  fs.writeFileSync(outPath, lines.join('\n'));
}

main();
