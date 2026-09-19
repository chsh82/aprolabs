// page_candidate.js — 2026-09-19 12차(페이지 편집기) 신규. "검증 가능한 편집 명령 계약"
// (momo_page_editor_plan.md §6/§7) — 페이지 하나, 그 안의 조각(item_id+part) 하나만
// 새로 렌더링하고 실측 오버플로 검사를 거쳐 후보를 만든다. 다른 조각·다른 페이지는
// 절대 건드리지 않는다(전체 재조판 호출 없음) - 이 파일이 하는 일은 딱 두 가지:
//   1) blocks.js/paginate.js의 기존 순수 렌더 함수로 조각 하나를 다시 만든다.
//   2) 그 조각을 실제로 반면(half) 또는 전체페이지(fullpage) 예산에 실측해 넣어보고,
//      넘치면 구체적 이유와 함께 차단한다(자동 축소·다음 페이지 이동 없음).
// 후속 LLM 대화 편집(P3)도 이 계약(입력 JSON -> 출력 JSON)을 그대로 재사용하면 된다 -
// 모델이 임의 코드를 실행하는 대신 set_text/replace_image/set_answer_area 세 연산과
// item_effective(병합된 문항 데이터)만 만들면 되고, 실제 조판·검증은 항상 이 스크립트가 한다.
//
// 실행: node page_candidate.js <요청 JSON 파일 경로>
// 요청 JSON: {
//   operation: "set_text" | "replace_image" | "set_answer_area",
//   tone: "tone-elem34" 등,
//   page: { layout_type: "halves"|"fullpage", html_raw: "<section...>", slots: [...] },
//   target: { itemId: 1607, part: "answer" },
//   item_effective: { ...base item + 이번 수정이 반영된 전체 필드... }
// }
// 응답 JSON(stdout): { status: "ok", html_raw, changed_slot_html }
//               또는 { status: "blocked", reason, part, budget }
//               또는 { status: "error", reason }
'use strict';
const fs = require('fs');
const { chromium } = require('playwright');
const blocks = require('./blocks');
const paginate = require('./paginate');

function partHtml(part, bundles) {
  if (part === 'lead') return bundles.leadHtml;
  if (part === 'answer') return bundles.answerHtml;
  if (part === 'merged') return (bundles.leadHtml || '') + bundles.answerHtml;
  return null;
}

function overflowReason(operation, budget, item) {
  const budgetLabel = budget === 'halves' ? '반면(페이지 절반)' : '페이지 전체';
  const hint = operation === 'set_answer_area'
    ? '답란 높이를 줄여 보세요.'
    : operation === 'replace_image'
      ? '더 작은 이미지를 사용하거나 답란 높이를 줄여 보세요.'
      : '문장 길이를 줄이거나 답란 높이를 줄여 보세요.';
  const curHeight = item && item.answer_height_mm ? ` (현재 답란 높이 ${item.answer_height_mm}mm)` : '';
  return `수정한 내용이 ${budgetLabel} 안에 들어가지 않습니다${curHeight}. ${hint} 다음 페이지로 넘기거나 글자를 자동으로 줄이지 않습니다 - 이 페이지 안에서 조절해주세요.`;
}

async function renderCandidate(spec) {
  const { operation, tone, page: pageEntry, target, item_effective: item } = spec;

  const oldSlot = pageEntry.slots.find((s) => String(s.itemId) === String(target.itemId) && s.part === target.part);
  if (!oldSlot) {
    return { status: 'error', reason: `대상 조각을 찾을 수 없음(itemId=${target.itemId}, part=${target.part}) - 페이지 구성표와 대상이 불일치` };
  }

  const bundles = paginate.buildBundles(item);
  const newSlotHtml = partHtml(target.part, bundles);
  if (newSlotHtml === null) {
    return { status: 'error', reason: `알 수 없는 조각 종류: ${target.part}` };
  }

  const browser = await chromium.launch();
  const probePage = await browser.newPage();
  await probePage.goto(paginate.PROBE_URL);
  await probePage.evaluate(() => document.fonts.ready);

  const budget = pageEntry.layout_type; // 'halves' | 'fullpage'
  const overflowed = budget === 'halves'
    ? await paginate.overflow(probePage, newSlotHtml, tone)
    : await paginate.overflowFull(probePage, newSlotHtml, tone);

  await browser.close();

  if (overflowed) {
    return { status: 'blocked', reason: overflowReason(operation, budget, item), part: target.part, budget };
  }

  const occurrences = pageEntry.html_raw.split(oldSlot.html).length - 1;
  if (occurrences !== 1) {
    // 원본 조각 문자열이 페이지 안에서 정확히 한 번 나와야 안전하게 치환할 수 있다
    // (0번이면 구성표가 이미 어긋난 것, 2번 이상이면 다른 조각과 우연히 겹치는 극단적
    // 사례 - 둘 다 "몰래 다른 곳까지 바뀌는" 위험이 있으므로 조용히 진행하지 않고 차단한다).
    return { status: 'error', reason: `원본 조각을 페이지 안에서 정확히 한 번 찾지 못함(${occurrences}회 발견) - 안전하게 치환할 수 없어 차단` };
  }
  const newHtmlRaw = pageEntry.html_raw.split(oldSlot.html).join(newSlotHtml);

  return { status: 'ok', html_raw: newHtmlRaw, changed_slot_html: newSlotHtml, old_slot_html: oldSlot.html };
}

async function main() {
  const reqPath = process.argv[2];
  if (!reqPath) {
    console.error('사용법: node page_candidate.js <요청 JSON 파일 경로>');
    process.exit(1);
  }
  const spec = JSON.parse(fs.readFileSync(reqPath, 'utf-8'));
  const result = await renderCandidate(spec);
  process.stdout.write(JSON.stringify(result));
  if (result.status !== 'ok') process.exitCode = 0; // blocked/error도 정상 종료 - 호출부가 status 필드로 판단
}

module.exports = { renderCandidate };

if (require.main === module) {
  main().catch((err) => {
    process.stdout.write(JSON.stringify({ status: 'error', reason: String(err && err.stack || err) }));
    process.exitCode = 1;
  });
}
