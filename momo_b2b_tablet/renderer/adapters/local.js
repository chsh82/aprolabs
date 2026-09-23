/* local 어댑터 - 시안 검증(2단계 완료 기준)과 향후 정적 번들(오프라인) 납품용.
 * 데이터는 samples/*.layout.json을 fetch로 읽고, 답안은 브라우저 localStorage에 저장한다.
 * 손글씨 인식(recognize)은 일부러 넣지 않는다 - 실제 인식 백엔드가 없는 상태에서 흉내만 내면
 * "인식됨"을 거짓으로 보고하게 되므로, renderer.js가 recognize 부재를 원래 템플릿의
 * "window.claude.use 불가" 경로(글자로 확인 버튼 비활성)로 그대로 처리하게 둔다.
 */
export function createLocalAdapters({ docId, dataBaseUrl = "../samples", assetsBaseUrl = "../assets", storagePrefix = "momo-b2b-local" }) {
  async function loadEdition() {
    const res = await fetch(`${dataBaseUrl}/${docId}.layout.json`);
    if (!res.ok) throw new Error(`레이아웃을 불러오지 못함: ${docId} (${res.status})`);
    const full = await res.json();
    const manifestRes = await fetch(`${assetsBaseUrl}/manifest.json`);
    const manifest = manifestRes.ok ? await manifestRes.json() : {};
    const images = {};
    for (const [key, filename] of Object.entries(manifest)) images[key] = `${assetsBaseUrl}/${filename}`;
    // 표지 이미지는 book.cover에 자산 키가 직접 들어있다(예: "cover_yh") - manifest만
    // flat하게 노출하면 renderer.js가 IMG[BOOK.cover]로 알아서 찾는다. 원본 교재 이미지
    // (samjeondo 등)·캐릭터도 페이지 데이터 안에서 이미 같은 방식(자산 키)으로 참조된다.
    return {
      edition: { book: full.book, tone: full.tone, quarter: full.quarter, pages: full.pages },
      images,
    };
  }

  const storageKey = `${storagePrefix}:${docId}`;
  const state = {
    async load() {
      try { const raw = localStorage.getItem(storageKey); return raw ? JSON.parse(raw) : null; }
      catch (e) { return null; }
    },
    async save(s) {
      try { localStorage.setItem(storageKey, JSON.stringify(s)); } catch (e) {}
    },
  };

  return { loadEdition, state };
}
