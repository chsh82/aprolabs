// paginate.js — DESIGN_GUIDE.md §4.2 알고리즘: step2 문항들을 반면(half) 단위로
// 슬롯에 채우고, 발췌문+질문+답을 한 반면에 합칠 수 있는지는 build/probe.html을
// Playwright로 실측해서 판정한다(정적 글자수 추정이 아니라 실제 렌더 결과 기준).
'use strict';
const path = require('path');
const blocks = require('./blocks');

const PROBE_URL = 'file:///' + path.resolve(__dirname, '../build/probe.html').replace(/\\/g, '/');

async function overflow(page, html, tone) {
  await page.evaluate(({ html, tone }) => window.renderHtml(html, tone), { html, tone });
  return page.evaluate(() => {
    const el = document.getElementById('probe');
    return el.scrollHeight > el.clientHeight + 1; // 1px 오차 허용(measure.js와 동일 기준)
  });
}

// half 하나를 {html, center} 형태로 감싼다. center:true면 .half--center를 붙여서
// (styles.css 참고) flex:1이 아닌 콘텐츠(발췌문 등)가 반면 혼자 차지할 때 아래가
// 휑하게 비지 않고 세로 가운데로 정렬되게 한다.
function half(html, center) {
  return { html, center: !!center };
}

// item 하나 -> half 배열(1개 또는 2개: [excerpt+답 합친 반면] 또는 [발췌문 반면, 답 반면]
// 또는 [그림 반면, 답 반면]).
async function buildHalvesForItem(page, tone, item) {
  const hasFigure = !!item.excerpt_image_path;
  const answerHtml = blocks.answerSlotHtml(item);

  if (hasFigure) {
    // 가이드 규칙 3: 그림이 있으면 그림=상반부, 질문+답=하반부. 실측 없이 고정 2슬롯.
    // excerptBlock()은 독해유형 라벨을 이미 포함하므로, 발췌문이 없을 때만 라벨을 따로 붙인다
    // (가이드 규칙 4: 라벨은 항상 블록 머리에 붙음 - 두 번 넣지 않도록 주의).
    const figureHalf = (item.excerpt_text
      ? blocks.excerptBlock(item, { tight: true })
      : blocks.readingTypeLabel(item)) +
      blocks.figureBlock(blocks.docImageSrc(item.excerpt_image_path), null);
    return [half(figureHalf), half(answerHtml)];
  }

  if (item.excerpt_text) {
    const merged = blocks.excerptBlock(item) + answerHtml;
    const overflowed = await overflow(page, merged, tone);
    if (!overflowed) return [half(merged)];
    return [half(blocks.excerptBlock(item), true), half(answerHtml)];
  }

  return [half(answerHtml)];
}

const DISCUSSION_MEMO_HALF = half(
  '<p class="rt-label"><span class="rt-label__text">토론 메모</span><span class="rt-label__rule"></span></p>' +
  '<p class="step-intro">친구들의 의견 가운데 기억하고 싶은 말을 적어 두세요.</p>' +
  blocks.answerLines());

// step2 전체 -> 페이지 배열([{halves: [htmlA, htmlB?]}, ...])로 반환. 2개씩 페이지에 채우고
// 마지막 페이지에 반면이 하나만 남으면 토론 메모로 채움(가이드 3절 배치 규칙 5).
async function paginateStep2(page, tone, step2Items) {
  const allHalves = [];
  for (const item of step2Items) {
    const halves = await buildHalvesForItem(page, tone, item);
    allHalves.push(...halves);
  }
  if (allHalves.length % 2 === 1) allHalves.push(DISCUSSION_MEMO_HALF);

  const pages = [];
  for (let i = 0; i < allHalves.length; i += 2) {
    pages.push([allHalves[i], allHalves[i + 1]]);
  }
  return pages;
}

module.exports = { paginateStep2, overflow, PROBE_URL };
