// blocks.js — DB에서 뽑은 문항 데이터를 build/styles.css의 블록 클래스 계약(=
// build/blocks-demo.html에 정리된 마크업)에 맞춰 HTML 문자열로 렌더링한다.
// DB 접근 없음 — extract_worksheet_json.py가 만든 data.json의 값만 입력으로 받는다.
'use strict';

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// ---- 구조화된 이슈 수집 ----------------------------------------------------
// degradedShort 등 "데이터가 불완전해서 원래 블록을 못 만들고 기본 답란으로 낮췄다"는
// 상황을 console.warn뿐 아니라 프로그램적으로도 모을 수 있게 한다. generate.js가 문서
// 하나를 빌드하기 전에 resetIssues()를 부르고, 끝나면 getIssues()로 모아서
// generated/<doc_id>/issues.json 등에 남긴다(검토용 미리보기와 최종 출력 승인을 구분하는
// 근거 - "6. 불완전한 문항의 처리" 참고).
let ISSUES = [];
function resetIssues() { ISSUES = []; }
function getIssues() { return ISSUES.slice(); }
function pushIssue(item, reason) {
  const entry = { doc_id: item && item.doc_id, order_label: item && item.order_label, ui_type: item && item.ui_type, reason };
  ISSUES.push(entry);
  console.warn(`[blocks] ${entry.doc_id}#${entry.order_label}: ${reason}`);
  return entry;
}

// 출력 파일이 항상 momo_book_db/generated/<doc_id>/index.html(momo_book_db 기준 2단계
// 아래)에 놓인다는 전제로 상대경로를 고정한다 - generate.js의 출력 경로 규칙과 짝을 이룸.
const ASSET_BASE = '../../worksheet/build/assets/images/';
const DOC_IMAGE_BASE = '../../extracted_images/';

// extract_worksheet_json.py의 IMAGE_PATH_RE와 같은 규칙(허용 디렉터리 밖 접근 차단) -
// data.json이 파이썬 검증을 거쳤다는 전제를 신뢰하지 않고 여기서도 한 번 더 막는다.
const IMAGE_PATH_RE = /^[A-Za-z0-9_-]+\/[A-Za-z0-9_.-]+\.(?:jpe?g|png|webp|gif)$/i;
function docImageSrc(relPath) {
  if (!relPath) return null;
  const normalized = String(relPath).replace(/\\/g, '/');
  if (normalized.includes('..') || normalized.startsWith('/') || /^[A-Za-z]:/.test(normalized) || !IMAGE_PATH_RE.test(normalized)) {
    console.warn(`[blocks] 허용되지 않은 이미지 경로 - 무시함: ${relPath}`);
    return null;
  }
  return DOC_IMAGE_BASE + normalized;
}

// ---- 공통 조각 ------------------------------------------------------------

function stampSpan(orderLabel) {
  const sub = /-/.test(orderLabel) ? ' stamp--sub' : '';
  return `<span class="stamp${sub}">${esc(orderLabel)}</span>`;
}

// data-qa-id: discussion_qa.id 그대로 - 3D(콘텐츠 대조)에서 렌더된 DOM을 원본 행에
// 대응시키는 안정적 식별자. 화면에는 안 보이고 CSS에도 안 걸린다(data-* 속성).
function qHeader(item, opts) {
  opts = opts || {};
  const tightClass = opts.tight ? ' q--tight' : (opts.tight2 ? ' q--tight2' : '');
  return `<div class="q${tightClass}" data-qa-id="${esc(item.id)}" data-order-label="${esc(item.order_label)}">` +
    `${stampSpan(item.order_label)}<p class="q__text">${esc(item.question_text)}</p></div>`;
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

// heightMm: 최소 편집 기능의 "답란 높이 조절"(design override)용. 답란 최소 15mm
// 정책은 project_store.py(편집 저장 단계)에서 이미 강제하지만, 여기서도 한 번 더
// 방어한다(파이썬을 거치지 않고 다른 경로로 값이 들어올 가능성 대비 - 다른 검증들과
// 같은 이중 방어 원칙). 값이 없으면 기존과 완전히 동일(스타일 속성 자체가 안 붙음).
function answerLines(extraClass, heightMm) {
  const cls = extraClass ? ` ${extraClass}` : '';
  const style = heightMm ? ` style="min-height:${Math.max(15, Number(heightMm))}mm"` : '';
  return `<div class="answer-lines${cls}" role="textbox"${style}></div>`;
}

// ui_config.reference_image2_path/caption(2026-09-19 8차 안정화 추가): 참고자료
// 이미지가 2개인 경우(예: id=1607 - 한자책 사진 + 愛 그림) 그림 전용 "lead 반면"을
// 새로 만들지 않고(원본보다 훨씬 크게 확대되는 문제, RESTORATION_PATCHES.json 참고)
// 기존 참고자료 박스 안에 작은 썸네일을 넣는다 - 질문·설명·빈칸·그림이 한 반면(또는
// 한 페이지)에서 함께 읽히게 하기 위함. 없으면 기존과 완전히 동일.
// 2026-09-19 10차 안정화: 이미지 2개를 세로로 쌓지 않고 종횡비를 유지한 채 가로로
// 나란히 배치하고(.reference__images, flex row), 그림 하나하나를 또 감싸던
// .figure-block 자체 테두리를 참고자료 안에서는 없앤다(바깥 .reference 테두리와
// 중첩되어 "그림 주변 빈 상자"처럼 보이던 문제 - 사용자 지적으로 발견).
function referenceBlock(item) {
  const cfg = item.ui_config || {};
  if (!item.reference_text && !item.reference_image_path && !cfg.reference_image2_path) return '';
  const img = item.reference_image_path
    ? figureBlock(docImageSrc(item.reference_image_path), null) : '';
  const img2 = cfg.reference_image2_path
    ? figureBlock(docImageSrc(cfg.reference_image2_path), cfg.reference_image2_caption || null) : '';
  const body = item.reference_text ? `<p class="reference__body">${esc(item.reference_text)}</p>` : '';
  const images = (img || img2) ? `<div class="reference__images">${img}${img2}</div>` : '';
  return `<div class="reference"><p class="reference__label">참고 자료</p>${body}${images}</div>`;
}

// 문장 안에 빈칸이 인라인으로 뚫려 있는 원본 그대로를 재현할 때 쓴다(2026-09-19 10차
// 안정화 - id=1607 실제 페이지가 "우리말은 ___과 달라서..."처럼 문장 속에 빈칸이 있는
// 형태인데, 기존엔 문장을 행별로 쪼개 각 행 아래 전체너비 여러 줄 답란(.answer-lines)을
// 붙이는 식이었다 - "밑줄 문자 반복 대신 폭을 조절할 수 있는 인라인 빈칸으로"라는
// 사용자 지시로 교체). widthMm을 안 주면 기본 폭(CSS의 min-width)을 쓴다.
function inlineBlankSpan(widthMm) {
  const style = widthMm ? ` style="width:${Number(widthMm)}mm"` : '';
  return `<span class="inline-blank"${style}></span>`;
}

// ui_config.inline_blanks: [{parts: [{text}|{blank,width_mm}, ...]}, ...] - 문단 배열,
// 문단마다 텍스트 조각과 인라인 빈칸 조각을 순서대로 섞어 하나의 흐르는 문장으로 만든다.
// text_short_multi인데 이 필드가 있으면 answerSlotHtml()에서 bodyBlanks() 대신 이걸
// 쓴다(기존 blanks[] 문자열 방식은 그대로 남겨두고, 인라인이 필요한 문항만 옵트인).
function bodyInlineBlanks(item) {
  const paras = (item.ui_config && item.ui_config.inline_blanks) || [];
  if (paras.length < 1) return null;
  const html = paras.map((p) => {
    const partsHtml = (p.parts || []).map((part) =>
      part.blank ? inlineBlankSpan(part.width_mm) : esc(part.text)).join('');
    return `<p class="inline-blanks__para">${partsHtml}</p>`;
  }).join('');
  return `<div class="inline-blanks" data-qa-id="${esc(item.id)}">${html}</div>`;
}

// ---- ui_type -> 답란 블록 --------------------------------------------------
// short: text_short/text_long — .q 헤더는 half.js가 붙이고, 여기서는 몸통만 만든다.
// ui_config.starter_suffix(2026-09-19 8차 안정화 추가): starter가 답란 "앞"에 붙는
// 문장 시작 힌트라면, starter_suffix는 답란 "뒤"에 붙는 문장 마무리 고정 문구(예:
// id=1612 "찌아찌아족이 ... 이유는," [학생이 쓰는 답란] "(이)기 때문이에요." - 원본
// 페이지에 앞뒤로 인쇄된 고정 문장을 학생 답란과 함께 보존). 둘 다 없으면 기존과 동일.
function bodyShort(item) {
  const cfg = item.ui_config || {};
  const starter = cfg.starter ? `<p class="q__example">${esc(cfg.starter)}</p>` : '';
  const suffix = cfg.starter_suffix ? `<p class="q__example q__example--suffix">${esc(cfg.starter_suffix)}</p>` : '';
  return starter + answerLines(null, item.answer_height_mm) + suffix;
}

function bodyChoice(item) {
  const options = (item.ui_config && item.ui_config.options) || [];
  if (options.length < 2) return null; // 데이터 불완전 - 호출부에서 fallback으로 전환
  const opts = options.map(o =>
    `<label class="blk-choice__option"><span class="blk-choice__box"></span>${esc(o)}</label>`).join('');
  return `<div class="blk-choice" data-qa-id="${esc(item.id)}"><div class="blk-choice__options">${opts}</div>` +
    `<p class="blk-choice__reason-label">그렇게 생각한 이유를 적어 보세요.</p>${answerLines(null, item.answer_height_mm)}</div>`;
}

// blanks[] 항목은 다음 중 하나일 수 있다:
//   - 문자열(기존 - 라벨만)
//   - {label?, image_path?, image_caption?, suffix?} 객체(2026-09-19 6차 안정화 추가 -
//     id=1611 "대상별 그림+개별 답란" 복원용). image_path가 있으면 그 행에 그림(+캡션)을
//     답란 위에 붙인다 - 각 대상이 자기 그림·답란을 온전히 갖고, 선택지(choice)나 공유
//     답란으로 합쳐지지 않는다. label이 없으면(그림 캡션이 이미 행의 정체를 나타낼 때)
//     라벨 문단은 생략해 캡션과 중복 안 함. suffix가 있으면 답란 "아래"에 고정 문구를
//     붙인다(예: id=1609 "...라는 뜻이에요." - 긴 답란 뒤에 원본 문장이 이어지는 경우).
//   - {chargrid: N, prefix?, mid_suffix?, blank_width_mm?, suffix?} 객체(2026-09-19
//     8차 안정화 추가, 10차에서 mid_suffix/blank_width_mm 추가) - id=1609 "훈민정음"처럼
//     답이 한 글자씩 빈 칸 N개에 들어가는 경우. prefix/글자칸/mid_suffix/인라인빈칸/
//     suffix를 전부 한 줄로 붙여 렌더링한다(글자 칸 자체가 답이라 answer-lines 없음).
//     "이 글자의 이름은 [칸칸칸칸]이에요."나 "[칸칸칸칸]은 [___]라는 뜻이에요."처럼
//     문장이 멀리 떨어지지 않고 붙어 읽히게 하려는 용도(사용자 지시, RESTORATION_PATCHES
//     참고) - blank_width_mm이 있으면 그 뒤에 inlineBlankSpan()도 같은 줄에 붙인다.
//   - {bubble: true, image_path?, image_caption?} 객체(2026-09-19 10차 안정화 추가) -
//     괘선 답란 대신 빈 말풍선(.speech-bubble)을 그린다(id=1608 장쇠/간난이/꽃네처럼
//     원본이 실제 말풍선인 경우). 그림이 있으면 그림+말풍선을 좌우로 배치한다.
function bodyBlanks(item) {
  const blanks = (item.ui_config && item.ui_config.blanks) || [];
  if (blanks.length < 1) return null;
  const rows = blanks.map((entry) => {
    if (entry && typeof entry === 'object') {
      if (entry.chargrid) {
        const boxes = new Array(entry.chargrid).fill('<span class="char-box"></span>').join('');
        const prefix = entry.prefix ? `<span class="blk-blanks__prefix">${esc(entry.prefix)}</span>` : '';
        const midSuffix = entry.mid_suffix ? `<span class="blk-blanks__suffix">${esc(entry.mid_suffix)}</span>` : '';
        const blank = entry.blank_width_mm ? inlineBlankSpan(entry.blank_width_mm) : '';
        const suffix = entry.suffix ? `<span class="blk-blanks__suffix">${esc(entry.suffix)}</span>` : '';
        return `<div class="blk-blanks__row blk-blanks__row--chargrid">${prefix}<span class="char-grid">${boxes}</span>${midSuffix}${blank}${suffix}</div>`;
      }
      if (entry.bubble) {
        const heightMm = Math.max(15, Number(item.answer_height_mm) || 24);
        const fig = entry.image_path
          ? `<figure class="speech-row__figure"><img src="${esc(docImageSrc(entry.image_path))}" alt="">` +
            (entry.image_caption ? `<figcaption>${esc(entry.image_caption)}</figcaption>` : '') + `</figure>`
          : '';
        return `<div class="blk-blanks__row speech-row">${fig}` +
          `<div class="speech-bubble" role="textbox" style="min-height:${heightMm}mm"></div></div>`;
      }
      const img = entry.image_path
        ? figureBlock(docImageSrc(entry.image_path), entry.image_caption || null) : '';
      const label = entry.label ? `<p class="blk-blanks__label">${esc(entry.label)}</p>` : '';
      const suffix = entry.suffix ? `<p class="blk-blanks__suffix">${esc(entry.suffix)}</p>` : '';
      return `<div class="blk-blanks__row">${img}${label}${answerLines(null, item.answer_height_mm)}${suffix}</div>`;
    }
    return `<div class="blk-blanks__row"><p class="blk-blanks__label">${esc(entry)}</p>${answerLines(null, item.answer_height_mm)}</div>`;
  }).join('');
  // ui_config.note: 답란 영역 밑에 보존할 짧은 참고 문구(예: 원본 페이지의 영상 링크) -
  // 없으면 기존과 완전히 동일(문단 자체가 안 붙음). note_href(2026-09-19 8차 안정화
  // 추가)가 있으면 실제 클릭 가능한 링크로 만든다 - PDF에 원본과 같은 실제 링크 대상을
  // 그대로 보존하기 위함(id=1611: 원본 PDF의 임베드 하이퍼링크 URI를 그대로 씀 - 표시
  // 문자열은 대문자 O처럼 보이지만 실제 URI는 숫자 0으로 끝난다, RESTORATION_PATCHES.json
  // 참고). http(s)만 허용(그 외 스킴은 일반 텍스트로 처리 - 위험한 스킴 삽입 방지).
  const cfg = item.ui_config || {};
  const noteHref = cfg.note_href && /^https?:\/\//.test(cfg.note_href) ? cfg.note_href : null;
  const note = cfg.note
    ? `<p class="blk-blanks__note">${noteHref ? `<a href="${esc(noteHref)}">${esc(cfg.note)}</a>` : esc(cfg.note)}</p>` : '';
  return `<div class="blk-blanks" data-qa-id="${esc(item.id)}">${rows}${note}</div>`;
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
  return `<table class="grid-table" data-qa-id="${esc(item.id)}"><thead>${thead}</thead><tbody>${tbody}</tbody></table>`;
}

function bodyFallback(item) {
  // unknown/미지원 ui_type도 참고자료(텍스트/이미지)는 다른 유형과 동일하게 보여준다 -
  // 2026-09-18 발견: choice/blanks/table_compare/short 경로만 referenceBlock을 붙이고
  // 있어서, ui_type='unknown'인 실제 승인 대상 행(예: L5-Q4-W02#435, reference_image만
  // 있음)의 참고 이미지가 여기서만 계속 누락되고 있었다.
  return `<div class="blk-fallback" data-qa-id="${esc(item.id)}" data-order-label="${esc(item.order_label)}">` +
    `<p class="blk-fallback__text">${esc(item.question_text)}</p>${referenceBlock(item)}${answerLines(null, item.answer_height_mm)}</div>`;
}

// item -> "질문+답" 슬롯 전체 HTML(참고자료 포함). blk-choice/blk-blanks/blk-fallback은
// 자체적으로 .q를 감싸지 않으므로 여기서 필요한 경우 qHeader를 앞에 붙인다.
// ui_config가 비어 있어 제대로 된 블록을 못 만드는 경우(예: choice_multi인데 options가
// 없음 - 실제 DB에 이런 승인된 행이 있었음, 2026-09-17 파일럿에서 발견) qHeader + 빈
// 답란만 있는 최소 형태로 조용히 낮춘다(crash도 안 하고, 있지도 않은 선택지를 지어내지도
// 않음). 콘솔에 경고를 남겨서 검수화면에서 ui_config를 채워 넣어야 한다는 걸 알 수 있게 함.
function degradedShort(item, reason) {
  pushIssue(item, `${reason} - 기본 답란으로 대체(검토용 미리보기에서만 허용, 최종 출력 승인 대상 아님)`);
  return qHeader(item, { tight2: !!item.reference_text }) + referenceBlock(item) + answerLines(null, item.answer_height_mm);
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
      // 2026-09-19 10차 안정화: ui_config.inline_blanks가 있으면(문장 속 인라인 빈칸
      // 형태) bodyBlanks() 대신 bodyInlineBlanks()를 쓴다 - 문서 id가 아니라 데이터
      // 형태로만 분기하므로, 이 필드를 안 쓰는 기존 text_short_multi 문항은 전과 100%
      // 동일하게 동작한다.
      const useInline = !!(item.ui_config && item.ui_config.inline_blanks);
      const b = useInline ? bodyInlineBlanks(item) : bodyBlanks(item);
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
      pushIssue(item, `ui_type='unknown' - 원문은 보존하되 유형 확인 대상`);
      body = bodyFallback(item);
      break;
    default:
      pushIssue(item, `ui_type=${JSON.stringify(item.ui_type)} - blocks.js가 모르는 값(UI_TYPE_ALLOWED 밖), unknown과 동일하게 처리`);
      body = bodyFallback(item);
      break;
  }
  return leadLabel + body;
}

// ---- 1단계 ------------------------------------------------------------
// 2026-09-19: paginate.js가 행 단위로 "여기까지 넣으면 넘치는지" 실측해야 해서
// (어휘 행을 쪼개지 않고, 다음 페이지엔 표 머리글을 반복) 헤더/행을 따로 뽑아낸다.
// vocabTable()은 기존 호출부(있다면) 호환용으로 헤더+전체행을 그대로 이어붙여 반환.
function vocabTableHeader() {
  return `<thead><tr><th>단어</th><th>각 단어를 이용해 문장 만들기</th></tr></thead>`;
}
function vocabRowHtml(v) {
  return `<tr data-vocab-id="${esc(v.id)}">
    <td class="vocab-word-cell"><p class="vocab-word">${esc(v.word)}${v.book_page ? `<span class="vocab-word__page">${esc(v.book_page)}쪽</span>` : ''}</p>
    <p class="vocab-def">${esc(v.definition)}</p></td>
    <td class="vocab-answer-cell">${answerLines('answer-lines--table')}</td>
  </tr>`;
}
// rows: vocabRowHtml() 출력 문자열 배열(이미 만들어진 것 중 일부/전체를 그대로 이어붙임) -
// 페이지네이션이 "여기까지"로 자른 부분집합을 다시 표로 감싸는 용도.
function vocabTableFrom(rowsHtml) {
  return `<table class="vocab-table">${vocabTableHeader()}<tbody>${rowsHtml.join('')}</tbody></table>`;
}
function vocabTable(vocabRows) {
  return vocabTableFrom(vocabRows.map(vocabRowHtml));
}

function oxItemHtml(o) {
  return `<li class="ox-item" data-ox-id="${esc(o.id)}">
    <p class="ox-item__text">${esc(o.question)}${o.evidence_page ? `<span class="ox-item__page">${esc(o.evidence_page)}쪽</span>` : ''}</p>
    <div class="ox-item__marks"><span class="ox-item__mark">O</span><span class="ox-item__mark">X</span></div>
  </li>`;
}
function oxListFrom(itemsHtml) {
  return `<ol class="ox-list">${itemsHtml.join('')}</ol>`;
}
function oxList(oxRows) {
  return oxListFrom(oxRows.map(oxItemHtml));
}

// 2026-09-19 9차 안정화: L1-Q4-W01 원본 PDF 2쪽 실물 대조 결과, 1단계 어휘 활동이
// vocabTable()(기존 "단어+문장 만들기" 표)이 아니라 "낱말 뜻 선 연결하기"였음이 밝혀져
// 추가한 전용 블록(RESTORATION_PATCHES.json 참고 - 8차의 "원본과 일치" 판단은 오판이었고
// 이번에 정정함). words는 {id, word}(원본 vocabulary.id 그대로 - data-vocab-id로
// 콘텐츠 대조 유지), definitions는 원본 페이지 인쇄 순서 그대로(단어와 같은 줄 순서가
// 아님 - 실제로 줄을 그어 연결하는 문제라 정답 짝을 학생용 출력에 드러내면 안 됨. 정답
// 순서는 RESTORATION_PATCHES.json에 근거만 기록하고 화면에는 안 드러냄).
function vocabMatchBlock(words, definitions) {
  const wordsHtml = words.map(w =>
    `<li class="vocab-match__item" data-vocab-id="${esc(w.id)}">${esc(w.word)}</li>`).join('');
  const defsHtml = definitions.map(d =>
    `<li class="vocab-match__item">${esc(d)}</li>`).join('');
  return `<div class="vocab-match"><p class="step-intro">낱말의 뜻을 찾아 선으로 이어보세요.</p>` +
    `<div class="vocab-match__cols">` +
    `<ul class="vocab-match__col">${wordsHtml}</ul>` +
    `<ul class="vocab-match__col vocab-match__col--defs">${defsHtml}</ul>` +
    `</div></div>`;
}

// bank: 보기 낱말 배열(원문 그대로, 순서 유지). sentences: {before, after} 배열 - 빈칸
// 자리를 비워 두고 앞/뒤 문장만 원문 그대로 옮긴다(원본 PDF 추출 텍스트에는 빈칸에 정답이
// 이미 채워져 있고 뜻풀이(* ...) 줄도 있었지만, 실제 인쇄된 학생용 화면에는 둘 다 없고
// 빈 괄호만 있었음 - 육안 대조로 확인, RESTORATION_PATCHES.json 참고).
function vocabFillBlankBlock(bank, sentences) {
  const bankHtml = bank.map(esc).join('&nbsp;&nbsp;&nbsp;');
  const itemsHtml = sentences.map((s, i) =>
    `<li class="vocab-fillblank__item">(${i + 1}) ${esc(s.before)}<span class="inline-blank"></span>${esc(s.after)}</li>`
  ).join('');
  return `<div class="vocab-fillblank">` +
    `<p class="step-intro">각 문장에 들어갈 알맞은 낱말을 &lt;보기&gt;에서 골라 쓰세요.</p>` +
    `<div class="vocab-fillblank__bank"><b>&lt;보기&gt;</b> ${bankHtml}</div>` +
    `<ol class="vocab-fillblank__list">${itemsHtml}</ol></div>`;
}

// documents.background_text 전용 블록(2026-09-18 추가) - DESIGN_GUIDE.md에 별도 시안이
// 없어 새 시각 언어를 만들지 않고, 이미 있는 .reference(라벨+본문) 구성을 그대로 재사용한다.
function backgroundText(text) {
  if (!text) return '';
  return `<div class="reference reference--background"><p class="reference__label">배경지식</p>` +
    `<p class="reference__body">${esc(text)}</p></div>`;
}

// ---- 3단계 ------------------------------------------------------------
function topicBand(topic) {
  return `<div class="topic-band"><p class="topic-band__label">오늘의 글감</p>` +
    `<p class="topic-band__title">${esc(topic)}</p></div>`;
}

// 이미지와 글쓰기 안내문(writing_guide)은 독립 요소다 - 이미지가 없다고 안내문까지
// 통째로 사라지면 안 된다(2026-09-18 발견: image_path가 비어 있으면 writing_guide가
// 있어도 빈 문자열을 반환하던 버그). 텍스트 없이 이미지만 있는 경우도 그대로 유지.
function bgQuote(imgSrc, text) {
  if (!imgSrc && !text) return '';
  const img = imgSrc ? `<img class="bg-quote__image" src="${esc(imgSrc)}" alt="">` : '';
  return `<div class="bg-quote">${img}<div class="bg-quote__tint"></div>` +
    `<blockquote class="bg-quote__text">${esc(text || '')}</blockquote></div>`;
}

function memoQRow(index, text, spaced, id) {
  return `<p class="memo-q${spaced ? ' memo-q--spaced' : ''}" data-outline-id="${esc(id)}"><b>${index}</b> ${esc(text)}</p>${answerLines()}`;
}

function stepHead(title, small, spaced) {
  return `<h3 class="step-head${spaced ? ' step-head--spaced' : ''}">${esc(title)}` +
    (small ? `<small>${esc(small)}</small>` : '') + `</h3>`;
}

// DB의 cover_message가 이미 인용부호를 포함한 채 저장된 경우(추출 원문 그대로 - 예:
// L1-Q4-W01, L5-Q4-W06/07, L3-Q4-W02/03/04/06)와 안 포함된 경우(L5-Q4-W01/09/10/11 등)가
// 섞여 있다 - 이 함수가 항상 겹따옴표를 덧붙이면 이미 포함된 쪽은 인용부호가 중복
// 표시된다(2026-09-19 8차 안정화에서 L1-Q4-W01 실제 PDF로 발견 - 다른 문서에도 같은
// 결함이 있었을 것). 문자열 시작/끝이 이미 따옴표(직선·구부러진 큰따옴표·작은따옴표
// 전부)면 그대로 두고, 아니면 브랜드 스타일 겹따옴표를 붙인다.
const QUOTE_CHARS_OPEN = ['"', '“', "'", '‘'];
const QUOTE_CHARS_CLOSE = ['"', '”', "'", '’'];
function quoteCoverMessage(msg) {
  const trimmed = String(msg).trim();
  const alreadyQuoted = QUOTE_CHARS_OPEN.includes(trimmed[0]) && QUOTE_CHARS_CLOSE.includes(trimmed[trimmed.length - 1]);
  return alreadyQuoted ? esc(trimmed) : `“${esc(trimmed)}”`;
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
    ${meta.cover_message ? `<blockquote class="cover__quote">${quoteCoverMessage(meta.cover_message)}</blockquote>` : ''}
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
  referenceBlock, answerSlotHtml, vocabTable, vocabRowHtml, vocabTableFrom, vocabTableHeader,
  oxList, oxItemHtml, oxListFrom, vocabMatchBlock, vocabFillBlankBlock,
  backgroundText, topicBand, bgQuote, memoQRow,
  stepHead, coverPage, sheetHead, footer, sheetPage, docImageSrc,
  resetIssues, getIssues, pushIssue,
};
