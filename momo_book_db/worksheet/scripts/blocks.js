// blocks.js — DB에서 뽑은 문항 데이터를 build/styles.css의 블록 클래스 계약(=
// build/blocks-demo.html에 정리된 마크업)에 맞춰 HTML 문자열로 렌더링한다.
// DB 접근 없음 — extract_worksheet_json.py가 만든 data.json의 값만 입력으로 받는다.
'use strict';

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// 출력 파일이 항상 momo_book_db/generated/<doc_id>/index.html(momo_book_db 기준 2단계
// 아래)에 놓인다는 전제로 상대경로를 고정한다 - generate.js의 출력 경로 규칙과 짝을 이룸.
const ASSET_BASE = '../../worksheet/build/assets/images/';
const DOC_IMAGE_BASE = '../../extracted_images/';

function docImageSrc(relPath) {
  return relPath ? DOC_IMAGE_BASE + relPath : null;
}

// ---- 공통 조각 ------------------------------------------------------------

function stampSpan(orderLabel) {
  const sub = /-/.test(orderLabel) ? ' stamp--sub' : '';
  return `<span class="stamp${sub}">${esc(orderLabel)}</span>`;
}

function qHeader(item, opts) {
  opts = opts || {};
  const tightClass = opts.tight ? ' q--tight' : (opts.tight2 ? ' q--tight2' : '');
  return `<div class="q${tightClass}">${stampSpan(item.order_label)}<p class="q__text">${esc(item.question_text)}</p></div>`;
}

function readingTypeLabel(item) {
  if (!item.reading_type_label) return '';
  return `<p class="rt-label"><span class="rt-label__text">${esc(item.reading_type_label)}</span><span class="rt-label__rule"></span></p>`;
}

function excerptBlock(item, opts) {
  opts = opts || {};
  const tight = opts.tight ? ' excerpt--tight' : '';
  const cite = item.excerpt_page ? `<cite class="excerpt__cite">p.${esc(item.excerpt_page)}</cite>` : '';
  return `${readingTypeLabel(item)}<blockquote class="excerpt${tight}">${esc(item.excerpt_text)}${cite}</blockquote>`;
}

function figureBlock(src, caption) {
  return `<figure class="figure-block"><img src="${esc(src)}" alt="${esc(caption || '')}">` +
    (caption ? `<figcaption>${esc(caption)}</figcaption>` : '') + `</figure>`;
}

function answerLines(extraClass) {
  const cls = extraClass ? ` ${extraClass}` : '';
  return `<div class="answer-lines${cls}" role="textbox"></div>`;
}

function referenceBlock(item) {
  if (!item.reference_text) return '';
  return `<div class="reference"><p class="reference__label">참고 자료</p>` +
    `<p class="reference__body">${esc(item.reference_text)}</p></div>`;
}

// ---- ui_type -> 답란 블록 --------------------------------------------------
// short: text_short/text_long — .q 헤더는 half.js가 붙이고, 여기서는 몸통만 만든다.
function bodyShort(item) {
  const starter = item.ui_config && item.ui_config.starter
    ? `<p class="q__example">${esc(item.ui_config.starter)}</p>` : '';
  return starter + answerLines();
}

function bodyChoice(item) {
  const options = (item.ui_config && item.ui_config.options) || [];
  if (options.length < 2) return null; // 데이터 불완전 - 호출부에서 fallback으로 전환
  const opts = options.map(o =>
    `<label class="blk-choice__option"><span class="blk-choice__box"></span>${esc(o)}</label>`).join('');
  return `<div class="blk-choice"><div class="blk-choice__options">${opts}</div>` +
    `<p class="blk-choice__reason-label">그렇게 생각한 이유를 적어 보세요.</p>${answerLines()}</div>`;
}

function bodyBlanks(item) {
  const blanks = (item.ui_config && item.ui_config.blanks) || [];
  if (blanks.length < 1) return null;
  const rows = blanks.map(label =>
    `<div class="blk-blanks__row"><p class="blk-blanks__label">${esc(label)}</p>${answerLines()}</div>`).join('');
  return `<div class="blk-blanks">${rows}</div>`;
}

function bodyTableCompare(item) {
  const cfg = item.ui_config || {};
  const columns = cfg.columns || [];
  const rowCount = cfg.rows || 1;
  if (columns.length < 1) return null;
  const thead = `<tr>${columns.map(c => `<th>${esc(c)}</th>`).join('')}</tr>`;
  const rowHtml = `<tr>${columns.map(() =>
    `<td class="grid-table__answer">${answerLines('answer-lines--tall')}</td>`).join('')}</tr>`;
  const tbody = new Array(rowCount).fill(rowHtml).join('');
  return `<table class="grid-table"><thead>${thead}</thead><tbody>${tbody}</tbody></table>`;
}

function bodyFallback(item) {
  return `<div class="blk-fallback"><p class="blk-fallback__text">${esc(item.question_text)}</p>${answerLines()}</div>`;
}

// item -> "질문+답" 슬롯 전체 HTML(참고자료 포함). blk-choice/blk-blanks/blk-fallback은
// 자체적으로 .q를 감싸지 않으므로 여기서 필요한 경우 qHeader를 앞에 붙인다.
// ui_config가 비어 있어 제대로 된 블록을 못 만드는 경우(예: choice_multi인데 options가
// 없음 - 실제 DB에 이런 승인된 행이 있었음, 2026-09-17 파일럿에서 발견) qHeader + 빈
// 답란만 있는 최소 형태로 조용히 낮춘다(crash도 안 하고, 있지도 않은 선택지를 지어내지도
// 않음). 콘솔에 경고를 남겨서 검수화면에서 ui_config를 채워 넣어야 한다는 걸 알 수 있게 함.
function degradedShort(item, reason) {
  console.warn(`[blocks] ${item.doc_id}#${item.order_label}: ${reason} - 기본 답란으로 대체`);
  return qHeader(item, { tight2: !!item.reference_text }) + referenceBlock(item) + answerLines();
}

function answerSlotHtml(item) {
  const tight2 = !!item.reference_text;
  // 가이드 규칙 4: 독해유형 라벨은 항상 블록 머리에 붙는다. 발췌문이 있으면 excerptBlock()이
  // 이미 라벨을 포함하므로(호출부에서 excerptBlock을 따로 붙임), 여기서는 발췌문이 없을
  // 때만 라벨을 직접 단다 - 안 그러면 발췌문 없는 문항(예: 3번처럼 ui_config가 비어
  // degradedShort로 빠지는 경우)은 라벨이 통째로 사라짐(2026-09-17 파일럿에서 발견).
  const leadLabel = item.excerpt_text ? '' : readingTypeLabel(item);

  let body;
  switch (item.ui_type) {
    case 'choice_ab':
    case 'choice_multi': {
      const b = bodyChoice(item);
      body = b === null ? degradedShort(item, `${item.ui_type} options 없음`)
        : qHeader(item, { tight2 }) + referenceBlock(item) + b;
      break;
    }
    case 'text_short_multi': {
      const b = bodyBlanks(item);
      body = b === null ? degradedShort(item, 'text_short_multi blanks 없음')
        : qHeader(item, { tight2 }) + referenceBlock(item) + b;
      break;
    }
    case 'table_compare': {
      const b = bodyTableCompare(item);
      body = b === null ? degradedShort(item, 'table_compare columns 없음')
        : qHeader(item, { tight2 }) + referenceBlock(item) + b;
      break;
    }
    case 'text_short':
    case 'text_long':
      body = qHeader(item, { tight2 }) + referenceBlock(item) + bodyShort(item);
      break;
    case 'unknown':
    default:
      body = bodyFallback(item);
      break;
  }
  return leadLabel + body;
}

// ---- 1단계 ------------------------------------------------------------
function vocabTable(vocabRows) {
  const rows = vocabRows.map(v => `<tr>
    <td class="vocab-word-cell"><p class="vocab-word">${esc(v.word)}${v.book_page ? `<span class="vocab-word__page">${esc(v.book_page)}쪽</span>` : ''}</p>
    <p class="vocab-def">${esc(v.definition)}</p></td>
    <td class="vocab-answer-cell">${answerLines('answer-lines--table')}</td>
  </tr>`).join('');
  return `<table class="vocab-table"><thead><tr><th>단어</th><th>각 단어를 이용해 문장 만들기</th></tr></thead>` +
    `<tbody>${rows}</tbody></table>`;
}

function oxList(oxRows) {
  const items = oxRows.map(o => `<li class="ox-item">
    <p class="ox-item__text">${esc(o.question)}${o.evidence_page ? `<span class="ox-item__page">${esc(o.evidence_page)}쪽</span>` : ''}</p>
    <div class="ox-item__marks"><span class="ox-item__mark">O</span><span class="ox-item__mark">X</span></div>
  </li>`).join('');
  return `<ol class="ox-list">${items}</ol>`;
}

// ---- 3단계 ------------------------------------------------------------
function topicBand(topic) {
  return `<div class="topic-band"><p class="topic-band__label">오늘의 글감</p>` +
    `<p class="topic-band__title">${esc(topic)}</p></div>`;
}

function bgQuote(imgSrc, text) {
  if (!imgSrc) return '';
  return `<div class="bg-quote"><img class="bg-quote__image" src="${esc(imgSrc)}" alt="">` +
    `<div class="bg-quote__tint"></div><blockquote class="bg-quote__text">${esc(text || '')}</blockquote></div>`;
}

function memoQRow(index, text, spaced) {
  return `<p class="memo-q${spaced ? ' memo-q--spaced' : ''}"><b>${index}</b> ${esc(text)}</p>${answerLines()}`;
}

function stepHead(title, small, spaced) {
  return `<h3 class="step-head${spaced ? ' step-head--spaced' : ''}">${esc(title)}` +
    (small ? `<small>${esc(small)}</small>` : '') + `</h3>`;
}

// ---- 표지/공통 골격 ------------------------------------------------------
function coverPage(meta) {
  const coverImg = docImageSrc(meta.book.cover_image);
  return `<section class="page" data-screen-label="01 표지"><div class="cover">
    <div class="cover__stars" aria-hidden="true"></div>
    <div class="cover__brand">
      <img class="cover__logo" src="${ASSET_BASE}logo-momo-ivory.png" alt="모모의 책장">
      <div class="cover__level-row"><p>${esc(meta.level_label)} &nbsp;·&nbsp; ${esc(meta.quarter_label)}</p></div>
    </div>
    <div class="cover__title-block">
      <h1 class="cover__title">${esc(meta.book.title)}</h1>
      <p class="cover__byline">${esc(meta.book.author || '')} 지음</p>
      <div class="cover__divider"><span></span><span></span><span></span></div>
    </div>
    ${coverImg ? `<img class="cover__image" src="${esc(coverImg)}" alt="${esc(meta.book.title)} 표지">` : ''}
    ${meta.cover_message ? `<blockquote class="cover__quote">“${esc(meta.cover_message)}”</blockquote>` : ''}
  </div></section>`;
}

function sheetHead(step, title) {
  return `<div class="sheet-head"><img class="sheet-head__logo" src="${ASSET_BASE}logo-momo.png" alt="모모의 책장">
    <div class="sheet-head__meta"><p class="sheet-head__step">${esc(step)}</p><h2 class="sheet-head__title">${esc(title)}</h2></div>
  </div><div class="sheet-hr"></div>`;
}

function footer(pageNum, meta) {
  return `<div class="footer"><span class="footer__brand">모모의 책장 · 독서논술</span>` +
    `<span class="footer__page">${pageNum}</span>` +
    `<span class="footer__book">${esc(meta.book.title)} · ${esc(meta.level_label)}</span></div>`;
}

function sheetPage(screenLabel, step, title, halvesHtml, pageNum, meta) {
  return `<section class="page" data-screen-label="${esc(screenLabel)}"><div class="sheet">` +
    sheetHead(step, title) + halvesHtml.join('') + footer(pageNum, meta) + `</div></section>`;
}

module.exports = {
  esc, stampSpan, qHeader, readingTypeLabel, excerptBlock, figureBlock, answerLines,
  referenceBlock, answerSlotHtml, vocabTable, oxList, topicBand, bgQuote, memoQRow,
  stepHead, coverPage, sheetHead, footer, sheetPage, docImageSrc,
};
