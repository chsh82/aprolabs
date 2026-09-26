/* review 어댑터 - 5단계 검수·편집 페이지의 중앙 미리보기 전용.
 * local/api 어댑터와 다른 점: GET /api/runtime이 아니라 GET /api/editions/{id}를 쓴다
 * (runtime은 included=false 페이지를 걸러내지만, 검수자는 뺀 문항도 봐야 다시
 * 포함시킬지 판단할 수 있다 - edition/store.py의 review_images()와 짝).
 * 필기는 실제 학생 답안이 아니므로 저장하지 않는다(새로 열 때마다 빈 상태로 시작).
 */
export function createReviewAdapters({ editionId, baseUrl = "" }) {
  async function loadEdition() {
    // 2026-09-27 검수 프리셋 미리보기(SPEC_프롬프트_편집_기능.md 1단계) - URL에
    // previewKey가 있으면 sessionStorage에 review.js가 미리 넣어 둔 "저장 전"
    // layout을 그대로 쓴다(서버에 아직 반영 안 된 프리셋 토글 결과). 프리셋은
    // 새 이미지를 끌어오지 않으므로(기존 slot.img만 쓰거나 빈 자리표시자) 이미지
    // 목록은 그대로 저장된 edition 기준으로 받아도 된다.
    const previewKey = new URLSearchParams(location.search).get("previewKey");
    if (previewKey) {
      const raw = sessionStorage.getItem(previewKey);
      if (raw) {
        const imgRes = await fetch(`${baseUrl}/api/editions/${editionId}/images`);
        const images = imgRes.ok ? (await imgRes.json()).images : {};
        return { edition: JSON.parse(raw), images };
      }
    }
    const res = await fetch(`${baseUrl}/api/editions/${editionId}`);
    if (!res.ok) throw new Error(`edition 로드 실패: ${editionId} (${res.status})`);
    const data = await res.json();
    const imgRes = await fetch(`${baseUrl}/api/editions/${editionId}/images`);
    const images = imgRes.ok ? (await imgRes.json()).images : {};
    return { edition: data.layout, images };
  }

  const state = {
    async load() { return null; },
    async save() {},
  };

  return { loadEdition, state };
}
