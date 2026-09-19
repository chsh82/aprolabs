// paginate.js — DESIGN_GUIDE.md §4.2 알고리즘: step2 문항들을 반면(half) 단위로
// 슬롯에 채우고, 발췌문+질문+답을 한 반면에 합칠 수 있는지는 build/probe.html을
// Playwright로 실측해서 판정한다(정적 글자수 추정이 아니라 실제 렌더 결과 기준).
'use strict';
const path = require('path');
const blocks = require('./blocks');

const PROBE_URL = 'file:///' + path.resolve(__dirname, '../build/probe.html').replace(/\\/g, '/');

// 동적 삽입 직후 바로 재면 이미지가 아직 안 떠서(레이아웃에 실제로 잡아먹을 높이가 0)
// 오버플로를 놓칠 수 있다(2026-09-18 발견 - "동적 콘텐츠 삽입 후 필요한 폰트·이미지의
// 로딩을 기다린다"). img 태그들의 decode()가 끝날 때까지 기다린 뒤 잰다. 로딩 실패
// 이미지는 decode()가 reject하므로 개별로 잡아서 무시(그 이미지 자체는 check.js가
// naturalWidth===0으로 따로 잡음 - 여기서는 오버플로 측정만 막지 않으면 됨).
async function waitForImages(page) {
  await page.evaluate(async () => {
    const imgs = Array.from(document.querySelectorAll('#probe img, #probe-full img'));
    await Promise.all(imgs.map((img) => (img.complete ? Promise.resolve() : img.decode().catch(() => {}))));
  });
}

async function overflow(page, html, tone) {
  await page.evaluate(({ html, tone }) => window.renderHtml(html, tone), { html, tone });
  await waitForImages(page);
  return page.evaluate(() => {
    const el = document.getElementById('probe');
    return el.scrollHeight > el.clientHeight + 1; // 1px 오차 허용(measure.js와 동일 기준)
  });
}

// 1/3단계처럼 반면 둘로 안 나뉘고 .sheet 전체를 한 콘텐츠가 차지하는 경우용(probe.html의
// #probe-full + renderFullSheet 사용). .sheet 자체의 scrollHeight/clientHeight를 잰다 -
// display:flex인 wrapper 자신의 높이가 아니라 실제 페이지 박스 기준으로 넘치는지 봐야
// 정확하다(가이드 "1단계와 3단계도 넘침 검사를 적용한다").
async function overflowFull(page, html, tone) {
  await page.evaluate(({ html, tone }) => window.renderFullSheet(html, tone), { html, tone });
  await waitForImages(page);
  return page.evaluate(() => {
    const el = document.getElementById('probe-full').closest('.sheet');
    return el.scrollHeight > el.clientHeight + 1;
  });
}

// half 하나를 {html, center} 형태로 감싼다. center:true면 .half--center를 붙여서
// (styles.css 참고) flex:1이 아닌 콘텐츠(발췌문 등)가 반면 혼자 차지할 때 아래가
// 휑하게 비지 않고 세로 가운데로 정렬되게 한다.
// tag(선택, {itemId, part}): 2026-09-19 12차(페이지 편집기) 안정화 - 이 half/fullpage가
// 어느 item의 어느 조각(lead/answer/merged/memo)인지 이름표를 붙인다. generate.js의
// wrapHalves()는 여전히 html/center만 읽으므로(구조분해 나머지는 무시) 기존 출력은
// 100% 동일하다 - page_manifest.js가 "이 페이지의 이 반면은 item 1608의 answer다"를
// 알아내 나중에 그 조각만 다시 렌더링할 수 있게 하는 순수 부가 정보다.
function half(html, center, tag) {
  return { html, center: !!center, tag: tag || null };
}

// item 하나를 이루는 의미 단위(bundle) 둘을 만든다 - "발췌문·출처"/"그림·캡션" 중 있는
//쪽 하나(leadBundle)와 "질문·선택지/빈칸/표·답란"(answerBundle). 각 조각은 항상 통째로
// 붙어 다닌다(내부를 더 쪼개지 않음) - "발췌문·출처, 그림·캡션, 질문·선택지·답란의 연결을
// 유지"(3차 안정화 요청).
function buildBundles(item) {
  const answerHtml = blocks.answerSlotHtml(item);
  if (item.excerpt_image_path) {
    // ui_config.excerpt_image_caption(2026-09-19 6차 안정화 추가): 그림 아래 캡션을
    // 본문과 구분해 보존해야 하는 경우용(예: id=1607 '사랑을 뜻하는 한자 사랑 애') -
    // 없으면 기존과 완전히 동일(caption=null, figcaption 자체가 안 붙음).
    const caption = (item.ui_config && item.ui_config.excerpt_image_caption) || null;
    const leadHtml = (item.excerpt_text
      ? blocks.excerptBlock(item, { tight: true })
      : blocks.readingTypeLabel(item)) +
      blocks.figureBlock(blocks.docImageSrc(item.excerpt_image_path), caption);
    return { leadHtml, leadKind: 'figure', answerHtml };
  }
  if (item.excerpt_text) {
    return { leadHtml: blocks.excerptBlock(item), leadKind: 'excerpt', answerHtml };
  }
  return { leadHtml: null, leadKind: null, answerHtml };
}

// item 하나 -> 배치 단위(unit) 하나. 3단계 사다리(2026-09-19 3차 안정화):
//   1) 반면 배치 - 기존처럼 합치거나(발췌문+답) 반면 두 개로 나눠서 실측.
//   2) 그래도 넘치면 "한 페이지 전체"에 (lead+답) 통째로 실측 - 반면 절반이 아니라
//      머리말·푸터만 뺀 전체 공간이라 훨씬 여유롭다(1단계와 같은 원리).
//   3) 그래도 넘치면 lead/답을 각각 별도 "전체 페이지"로 분리(의미 단위로 다음 페이지
//      이동) - 이때도 lead 따로, 답 따로는 항상 온전한 채로 옮긴다.
//   4) lead 또는 답 단독으로도 전체 페이지 하나에 안 들어가면(자동 문장 분할은 범위
//      밖) 거기서 멈추고 구체적 이유를 issues에 남겨 차단한다.
async function placeItem(page, tone, item) {
  const { leadHtml, leadKind, answerHtml } = buildBundles(item);

  const id = item.id;
  if (!leadHtml) {
    // 발췌문도 그림도 없음 - 답 단위 하나뿐.
    if (!(await overflow(page, answerHtml, tone))) return { type: 'halves', halves: [half(answerHtml, false, { itemId: id, part: 'answer' })] };
    if (!(await overflowFull(page, answerHtml, tone))) return { type: 'fullpage', html: answerHtml, tag: { itemId: id, part: 'answer' } };
    blocks.pushIssue(item, '답 단위가 전체 페이지 하나보다 커서 안전하게 배치할 수 없음(문장 자동 분할 미구현) - 최종 출력 확정 차단 대상');
    return { type: 'fullpage', html: answerHtml, blocked: true, tag: { itemId: id, part: 'answer' } };
  }

  // 1) 반면 배치: lead가 그림이면 원래부터 반면 두 개 고정(가이드 규칙 3), 발췌문이면
  // 합쳐서 한 반면에 들어가는지 먼저 본다.
  if (leadKind === 'excerpt') {
    const merged = leadHtml + answerHtml;
    if (!(await overflow(page, merged, tone))) return { type: 'halves', halves: [half(merged, false, { itemId: id, part: 'merged' })] };
  }
  const leadFitsHalf = !(await overflow(page, leadHtml, tone));
  const answerFitsHalf = !(await overflow(page, answerHtml, tone));
  if (leadFitsHalf && answerFitsHalf) {
    return {
      type: 'halves',
      halves: [
        half(leadHtml, leadKind === 'excerpt', { itemId: id, part: 'lead' }),
        half(answerHtml, false, { itemId: id, part: 'answer' }),
      ],
    };
  }

  // 2) 반면으로 안 되면 전체 페이지 하나에 lead+답을 통째로.
  const combinedFull = leadHtml + answerHtml;
  if (!(await overflowFull(page, combinedFull, tone))) {
    return { type: 'fullpage', html: combinedFull, tag: { itemId: id, part: 'merged' } };
  }

  // 3) 그것도 안 되면 lead/답을 각각 별도 전체 페이지로(의미 단위 이동). 각 조각이 전체
  // 페이지 하나에도 안 들어가면 4)로 - 구체적 오류를 남기고 차단(범용 문장 분할 없음).
  const leadFitsFull = !(await overflowFull(page, leadHtml, tone));
  const answerFitsFull = !(await overflowFull(page, answerHtml, tone));
  if (!leadFitsFull) {
    blocks.pushIssue(item, `${leadKind === 'figure' ? '그림·캡션' : '발췌문·출처'} 단위가 전체 페이지보다 커서 안전하게 배치할 수 없음(문장 자동 분할 미구현) - 최종 출력 확정 차단 대상`);
  }
  if (!answerFitsFull) {
    blocks.pushIssue(item, '질문·선택지·답란 단위가 전체 페이지보다 커서 안전하게 배치할 수 없음(문장 자동 분할 미구현) - 최종 출력 확정 차단 대상');
  }
  return {
    type: 'fullpages',
    pages: [leadHtml, answerHtml],
    tags: [{ itemId: id, part: 'lead' }, { itemId: id, part: 'answer' }],
    blocked: !leadFitsFull || !answerFitsFull,
  };
}

const DISCUSSION_MEMO_HALF = half(
  '<p class="rt-label"><span class="rt-label__text">토론 메모</span><span class="rt-label__rule"></span></p>' +
  '<p class="step-intro">친구들의 의견 가운데 기억하고 싶은 말을 적어 두세요.</p>' +
  blocks.answerLines(), false, { itemId: null, part: 'memo' });

// step2 전체 -> 페이지 배열. 각 원소는 다음 중 하나:
//   ['half', 'half']                 (기존과 동일 - generate.js가 반면 두 개로 렌더)
//   { fullpage: html }               (해당 문항이 페이지 하나를 통째로 씀)
// 반면 단위(half)는 기존처럼 2개씩 짝지어 페이지를 채우고, fullpage 단위는 그 자리에서
// 바로 자기 페이지를 하나 만든다(짝이 안 맞아 남은 반면이 있으면 그 직전에 토론 메모로
// 마무리) - 순서를 그대로 유지한다.
async function paginateStep2(page, tone, step2Items) {
  const pages = [];
  let pendingHalf = null;
  const flushPendingWithMemo = () => {
    if (pendingHalf) { pages.push([pendingHalf, DISCUSSION_MEMO_HALF]); pendingHalf = null; }
  };

  for (const item of step2Items) {
    const unit = await placeItem(page, tone, item);
    if (unit.type === 'halves') {
      for (const h of unit.halves) {
        if (pendingHalf) { pages.push([pendingHalf, h]); pendingHalf = null; }
        else pendingHalf = h;
      }
    } else if (unit.type === 'fullpage') {
      flushPendingWithMemo();
      pages.push({ fullpage: unit.html, tag: unit.tag });
    } else if (unit.type === 'fullpages') {
      flushPendingWithMemo();
      unit.pages.forEach((html, i) => pages.push({ fullpage: html, tag: unit.tags[i] }));
    }
  }
  flushPendingWithMemo();
  return pages;
}

// ---- 1단계(어휘/OX) 페이지네이션 (2026-09-19 2차 안정화) ---------------------
// 기존엔 어휘표/OX목록을 무조건 반면 두 개(각 374px 고정)에 절반씩 넣어서, 실제 DB의
// 어휘 4~5개+정의 길이가 그 예산을 조금씩 넘기는 게 반복적으로 발생했다(승인 문서 7건
// 중 6건). 고정 반면 대신 머리말·푸터를 뺀 실제 .sheet 여유 공간 전체를 실측
// (overflowFull)해서 배분하고, 그래도 안 들어가면 어휘 행 단위로(쪼개지 않고, 표
// 머리글을 반복해서) 다음 페이지로 넘긴다.

// buildHtmlForN(n)이 만드는 후보를 실제로 렌더해서 넘치지 않는 최대 n을 이분탐색으로
// 찾는다. n=0(빈 상태, 예: 표 머리글만)은 항상 들어간다고 가정하지 않고 직접 확인한다.
async function fitCount(page, buildHtmlForN, total, tone) {
  if (total === 0) return 0;
  if (!(await overflowFull(page, buildHtmlForN(total), tone))) return total;
  let lo = 0, hi = total;
  // lo=0이 진짜 들어가는지부터 확인(머리글+안내문만으로도 안 들어가는 극단적 경우 대비).
  if (await overflowFull(page, buildHtmlForN(0), tone)) return -1; // 표/목록 골격조차 안 들어감
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (await overflowFull(page, buildHtmlForN(mid), tone)) hi = mid - 1;
    else lo = mid;
  }
  return lo;
}

function vocabSectionHtml(intro, rowsHtml, repeatHeaderOnly) {
  const label = repeatHeaderOnly ? '' : `<p class="step-intro">${intro}</p>`;
  return label + blocks.vocabTableFrom(rowsHtml);
}
function oxSectionHtml(intro, itemsHtml, repeatHeaderOnly) {
  const label = repeatHeaderOnly ? '' : `<p class="step-intro step-intro--tight">${intro}</p>`;
  return label + blocks.oxListFrom(itemsHtml);
}

// vocab/ox 배열(행 html 문자열)을 "지금 페이지에 남은 공간"에 최대한 채우고, 남은 건
// 다음 페이지로 넘기는 재귀적 흐름. section이 비어 있으면(rows.length===0) 건너뛴다.
// 반환: { placed: 이번 페이지에 들어간 rowsHtml, rest: 다음 페이지로 넘길 rowsHtml,
//         html: 이번 페이지에 실제로 넣을 조각(머리글 포함), blocked: 행 하나도 못 들어감 }
async function flowSection(page, tone, kind, intro, rowsHtml, isContinuation, doc_id, order_label) {
  if (rowsHtml.length === 0) return { placed: [], rest: [], html: '', blocked: false };
  const build = kind === 'vocab'
    ? (n) => vocabSectionHtml(intro, rowsHtml.slice(0, n), isContinuation)
    : (n) => oxSectionHtml(intro, rowsHtml.slice(0, n), isContinuation);
  const n = await fitCount(page, build, rowsHtml.length, tone);
  if (n <= 0) {
    // 행 하나도 못 들어감 - 진행을 위해 1개는 강제로 배치하되 issues에 명확히 남긴다
    // (가이드: "안전하게 배치할 수 없는 경우 문항 ID와 이유를 표시하고 차단").
    blocks.pushIssue({ doc_id, order_label: order_label || '1단계' },
      `${kind === 'vocab' ? '어휘' : 'OX'} 행이 한 페이지 여유 공간보다 커서 안전하게 배치할 수 없음(행 1개 강제 배치, 최종 출력 확정 차단 대상)`);
    return { placed: rowsHtml.slice(0, 1), rest: rowsHtml.slice(1), html: build(1), blocked: true };
  }
  return { placed: rowsHtml.slice(0, n), rest: rowsHtml.slice(n), html: build(n), blocked: false };
}

// data.step1(vocab/ox) -> 페이지 배열(각 원소는 그 페이지의 .sheet 안에 그대로 넣을 HTML
// 문자열, sheet-head/footer는 generate.js가 붙임). 순서 보존, 행 안 쪼갬, 연속 페이지엔
// 표 머리글만 반복(안내문은 첫 페이지에만).
async function paginateStep1(page, tone, data, vocabIntro, oxIntro) {
  const doc_id = data.doc_id;
  const vocabRows = data.step1.vocab.map(blocks.vocabRowHtml);
  const oxItems = data.step1.ox.map(blocks.oxItemHtml);

  if (vocabRows.length === 0 && oxItems.length === 0) return null; // 호출부(배경지식/빈 안내)가 처리

  // 1차: 전체가 한 페이지에 통째로 들어가는지 먼저 확인(대부분의 실제 데이터는 고정
  // 반면 예산만 없애면 여기서 끝난다 - 실측: 이전엔 반면당 374px 강제였던 게 원인).
  const fullHtml = vocabSectionHtml(vocabIntro, vocabRows, false) + oxSectionHtml(oxIntro, oxItems, false);
  if (!(await overflowFull(page, fullHtml, tone))) {
    return [fullHtml];
  }

  // 2차: 안 들어가면 어휘부터 행 단위로 페이지에 채우고, 다 배치한 뒤 OX를 이어서 채운다.
  const pages = [];
  let remainingVocab = vocabRows;
  let vocabIsContinuation = false;

  while (remainingVocab.length > 0) {
    const res = await flowSection(page, tone, 'vocab', vocabIntro, remainingVocab, vocabIsContinuation, doc_id, '1단계');
    pages.push(res.html);
    remainingVocab = res.rest;
    vocabIsContinuation = true;
  }

  // 어휘를 다 배치한 뒤, OX를 새 페이지(또는 어휘가 아예 없었으면 첫 페이지)에 이어서 채운다.
  // "마지막 어휘 페이지의 남는 공간에 OX를 이어붙이는" 최적화는 하지 않는다 - 어휘 마지막
  // 페이지에 이미 실측으로 꽉 채운 상태라 여유가 있다고 가정하면 오히려 오탐 위험이 있고,
  // 요청 범위("한 페이지에 안 들어가면 다음 페이지로 이어줘")는 "새 페이지에서 이어감"으로
  // 충분히 만족한다.
  let remainingOx = oxItems;
  let oxIsContinuation = false;
  while (remainingOx.length > 0) {
    const res = await flowSection(page, tone, 'ox', oxIntro, remainingOx, oxIsContinuation, doc_id, '1단계');
    pages.push(res.html);
    remainingOx = res.rest;
    oxIsContinuation = true;
  }

  if (pages.length === 0) {
    // vocab/ox 둘 다 있었는데 어느 쪽도 안 배치된 극단적 케이스(이론상 도달 안 함 -
    // 방어적으로 안내 문구만 있는 빈 페이지를 만들어 완전히 빈 출력은 피한다).
    blocks.pushIssue({ doc_id, order_label: '1단계' }, '1단계 콘텐츠를 어떤 페이지에도 배치하지 못함 - 최종 출력 확정 차단 대상');
    pages.push('<p class="step-intro">1단계 내용을 배치하지 못했습니다.</p>');
  }
  return pages;
}

module.exports = {
  paginateStep2, paginateStep1, overflow, overflowFull, PROBE_URL,
  // 2026-09-19 12차(페이지 편집기): 단일 item의 lead/answer 조각을 그때그때 다시
  // 렌더링해야 해서(전체 문서를 다시 배치하지 않고 이 페이지 이 조각만) 노출한다.
  buildBundles, half,
};
