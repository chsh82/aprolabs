/* review 어댑터 - 5단계 검수·편집 페이지의 중앙 미리보기 전용.
 * local/api 어댑터와 다른 점: GET /api/runtime이 아니라 GET /api/editions/{id}를 쓴다
 * (runtime은 included=false 페이지를 걸러내지만, 검수자는 뺀 문항도 봐야 다시
 * 포함시킬지 판단할 수 있다 - edition/store.py의 review_images()와 짝).
 * 필기는 실제 학생 답안이 아니므로 저장하지 않는다(새로 열 때마다 빈 상태로 시작).
 */
export function createReviewAdapters({ editionId, baseUrl = "" }) {
  async function loadEdition() {
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
