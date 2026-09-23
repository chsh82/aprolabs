/* api 어댑터 - aprolabs 운영 백엔드용 (SPEC_aprolabs_porting_spec.md §6, §8).
 * 파트너 태블릿 웹뷰는 /runtime/{edition_id}?launch=토큰 으로 들어오고, aprolabs가 그 토큰을
 * httpOnly 세션 쿠키로 교환한다(문서 §호스팅/인증 방향) - 그래서 여기서는 토큰을 한 번만 쓰고
 * 이후 모든 호출은 credentials:"include"의 세션 쿠키에 맡긴다. API 키는 파트너 서버↔aprolabs
 * 간에만 오가며 이 어댑터(태블릿 앱 안)에는 절대 넣지 않는다.
 *
 * 세션이 아직 없어서(5단계 이후 과제) student_id/edition_id를 쿠키 대신 폼 필드·쿼리로
 * 임시로 실어 보낸다 - edition/api.py 쪽에도 같은 메모가 있다. 세션이 생기면 이 파일만
 * 고치면 된다(§6 표의 엔드포인트 나머지 - 세션 교환, export 등 - 도 마찬가지).
 */
export function createApiAdapters({ editionId, baseUrl, launchToken, recognizeModel }) {
  let exchanged = !launchToken;

  async function ensureSession() {
    if (exchanged) return;
    const res = await fetch(`${baseUrl}/api/partner/sessions/exchange`, {
      method: "POST", credentials: "include", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ edition_id: editionId, launch: launchToken }),
    });
    if (!res.ok) throw new Error(`세션 교환 실패 (${res.status})`);
    exchanged = true;
  }

  async function loadEdition() {
    await ensureSession();
    const res = await fetch(`${baseUrl}/api/runtime/${editionId}`, { credentials: "include" });
    if (!res.ok) throw new Error(`런타임 로드 실패: ${editionId} (${res.status})`);
    const data = await res.json(); // { layout: {book,tone,quarter,pages}, images: {key:url} }
    return { edition: data.layout, images: data.images || {} };
  }

  /* ============ 오프라인 대기열(사용자 지시 2026-09-23, 7단계) ============
   * 저장 PUT이 실패하면(연결 끊김 등) localStorage에 쌓아 뒀다가, 연결이 돌아오면
   * (online 이벤트) 또는 다음 저장 시도 때 자동으로 다시 보낸다. 같은 part_id는
   * 최신 값으로 덮어써서 쌓는다(중복 전송 방지 - 필기는 계속 바뀌므로 옛 버전을
   * 나중에 보내는 건 의미가 없다). */
  const QUEUE_KEY = `momo-b2b-offline-queue:${editionId}`;
  function loadQueue() {
    try { return JSON.parse(localStorage.getItem(QUEUE_KEY) || "[]"); } catch (e) { return []; }
  }
  function saveQueue(q) {
    try { localStorage.setItem(QUEUE_KEY, JSON.stringify(q)); } catch (e) {}
  }
  function queueSet(partId, value) {
    const q = loadQueue();
    const i = q.findIndex(item => item.partId === partId);
    if (i >= 0) q[i] = { partId, value }; else q.push({ partId, value });
    saveQueue(q);
  }
  async function putAnswer(partId, value) {
    const res = await fetch(`${baseUrl}/api/runtime/${editionId}/answers/${encodeURIComponent(partId)}`, {
      method: "PUT", credentials: "include", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(value),
    });
    if (!res.ok) throw new Error(`save failed (${res.status})`);
  }
  let flushing = false;
  async function flushQueue() {
    if (flushing) return;
    flushing = true;
    try {
      const queue = loadQueue();
      if (!queue.length) return;
      const stillFailed = [];
      for (const item of queue) {
        try { await putAnswer(item.partId, item.value); }
        catch (e) { stillFailed.push(item); }
      }
      saveQueue(stillFailed);
    } finally {
      flushing = false;
    }
  }
  if (typeof window !== "undefined") {
    window.addEventListener("online", flushQueue);
    flushQueue(); // 페이지를 열었을 때 이미 연결이 복구돼 있을 수도 있으니 한 번 시도
  }

  const state = {
    async load() {
      // TODO(백엔드): 저장된 답안을 한 번에 돌려주는 GET이 아직 SPEC에 없다(part 단위 PUT과
      // 평가용 GET export만 있음) - 붙는 대로 여기서 불러오게 연결한다. 그 전까지는 빈 상태로 시작.
      return null;
    },
    async save(s) {
      await ensureSession();
      const entries = [
        ...Object.entries(s.ink).map(([partId, strokes]) => [partId, { ink: strokes }]),
        ...Object.entries(s.text).map(([partId, record]) => [partId, { text: record }]),
        ...Object.entries(s.ox).map(([key, choice]) => [`ox#${key}`, { ox: choice }]),
        // "choice" 위젯(양자택일 탭 선택, choice_ab)의 #choice part - S.choice에 담긴다.
        // ox와 달리 이미 진짜 part_id(`{q.id}#choice`)라 그대로 쓴다.
        ...Object.entries(s.choice || {}).map(([partId, value]) => [partId, { choice: value }]),
      ];
      // 단순화: 바뀐 부분만 보내지 않고 매번 전체를 다시 보낸다 - 실제 백엔드가 붙으면 변경분만
      // 추리는 diff를 넣는다(§6 PUT은 원래 part 단위 호출을 상정).
      await Promise.all(entries.map(async ([partId, value]) => {
        try { await putAnswer(partId, value); }
        catch (e) { queueSet(partId, value); }
      }));
    },
  };

  async function recognize({ prompt, blob, partId, signal }) {
    await ensureSession();
    const form = new FormData();
    form.append("image", blob, "ink.png");
    form.append("prompt", prompt);
    form.append("part_id", partId);
    form.append("edition_id", String(editionId));
    if (recognizeModel) form.append("model", recognizeModel); // 사용자 지시: 모델 비교용 override
    const res = await fetch(`${baseUrl}/api/runtime/recognize`, {
      method: "POST", credentials: "include", body: form, signal,
    });
    if (!res.ok) {
      let code = res.status === 429 ? "rate_limited" : res.status === 401 ? "session_expired" : "upstream_error";
      try { const body = await res.json(); if (body && body.detail) code = body.detail; } catch (e) {}
      const err = new Error("recognize failed");
      err.code = code;
      throw err;
    }
    return res.json(); // { text, unclear }
  }

  return { loadEdition, state, recognize };
}
