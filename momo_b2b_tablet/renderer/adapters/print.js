/* print 어댑터 - ⑥단계 서버 인쇄 PDF(edition/print_pdf.py) 전용. headless Chromium이
 * 이 어댑터로 마운트한 페이지를 그대로 캡처해서 PDF로 만든다.
 * review 어댑터와 비슷하지만 둘이 다르다:
 *   - GET /api/editions/{id}/print-view를 쓴다 - included=false 페이지가 빠지고,
 *     쪽수가 홀수면 서버가 빈 면을 하나 덧붙인 상태로 온다(edition/store.py의
 *     print_view). review는 검수자가 빼는/넣는 과정을 봐야 해서 안 거르지만,
 *     인쇄는 학생이 실제로 받는 쪽수 그대로 나와야 한다(사용자 지시 2026-09-23).
 *   - ink=true면 특정 학생(student_id)이 저장해 둔 필기·글자·O·X·선택을
 *     읽어서 처음부터 채워 넣는다("학생 필기 포함" 인쇄).
 */
export function createPrintAdapters({ editionId, baseUrl = "", ink = false, studentId = "" }) {
  async function loadEdition() {
    const res = await fetch(`${baseUrl}/api/editions/${editionId}/print-view`);
    if (!res.ok) throw new Error(`인쇄용 데이터 로드 실패: ${editionId} (${res.status})`);
    const data = await res.json();
    return { edition: data.layout, images: data.images || {} };
  }

  const state = {
    async load() {
      if (!ink || !studentId) return null;
      const res = await fetch(`${baseUrl}/api/editions/${editionId}/answers?student_id=${encodeURIComponent(studentId)}`);
      if (!res.ok) return null;
      return res.json(); // {ink,text,ox,choice}
    },
    async save() {}, // 인쇄본은 편집하지 않는다
  };

  return { loadEdition, state };
}
