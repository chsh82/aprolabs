# 문해력 게임 — 개발 이관 명령서

작성 기준: 프로토타입 12종 완성, 문항은 전부 하드코딩 상태.
이 문서는 **무엇을 서버에 붙일지**만 다룬다. 게임 규칙과 난이도는 이미 수업 기준으로 조정된 값이므로 임의로 바꾸지 않는다.

---

## 0. 배치 경로

```
<문해력_경로>/
├── idiom/
│   ├── schema.sql          ← db/idiom_schema.sql
│   ├── idiom.db            ← 생성물 (git 제외)
│   └── seed/               ← 표제어 CSV, 사전 덤프
└── games/                  ← games/ 전체
    ├── idiom/
    └── voyage/
```

FastAPI 앱은 `aprolabs/app/` 아래에 `idiom` 모듈을 추가한다. 기존 router/model/service 계층 관례를 그대로 따른다.

```bash
sqlite3 <문해력_경로>/idiom/idiom.db < <문해력_경로>/idiom/schema.sql
sqlite3 <문해력_경로>/idiom/idiom.db ".tables"
```

---

## 1. 작업 순서

| 순서 | 작업 | 산출물 |
|---|---|---|
| 1 | DB 생성 + 표제어 적재 | `idiom.db` |
| 2 | 문항 공급 API | `GET /quiz` |
| 3 | 각 게임의 `ITEMS` 를 API 호출로 교체 | 게임 12종 |
| 4 | 여정 연결 (지도 ↔ 구간) | `map.html` 중심 |
| 5 | 학습 기록 적재 | `attempt` 테이블 |

1~3까지가 1차 목표다. 4번은 3번이 끝나야 의미가 있다.

---

## 2. 문항 공급 API

모든 게임은 `ITEMS` 배열 하나만 바라본다. 그 자리를 API 응답으로 갈아끼우면 게임 로직은 손댈 필요가 없다.

```
GET /quiz?format=<형식>&count=<개수>&level=<학년>&topic=<주제>&exclude=<id,id>
```

응답 봉투:

```json
{
  "format": "mc",
  "items": [ { "id": 1204, "...형식별 필드": "..." } ]
}
```

형식별 필드는 `docs/quiz-api.md` 에 정리했다. 게임마다 요구하는 형식이 다르므로 **어댑터는 서버가 아니라 게임 쪽에 두지 않는다.** 서버가 형식에 맞춰 내려준다.

### 게임별 요구 형식

| 게임 | format | 비고 |
|---|---|---|
| voyage/suez | `gate3` | 3지선다, 뜻 → 낱말 |
| voyage/elephant | `gate3` | 속담·성어 |
| voyage/pacific | `mc4` | 교과학습어휘 |
| voyage/train | `mc4` | 문맥 속 낱말 |
| voyage/hongkong | `assemble` | 4글자 조합 |
| voyage/furnace | `gate3` | 복습 |
| voyage/map | 혼합 | `mc` `ox` `assemble` 을 한 배열에 |
| idiom/archery | `situation4` | 상황문 + 4지선다 |
| idiom/snake | `hanja` | 한자 구성 + 훈음 |
| idiom/runner | `hanja` | 한자 구성 |
| idiom/nautilus | `hanja` | 한자 구성 |
| idiom/judge | `usage` | 오용 판정 + 오류 유형 |

### 오답 보기 생성 규칙

무작위로 뽑지 말 것. 우선순위는 이렇다.

1. `relation` 테이블의 `confusable` (혼동쌍)
2. 같은 `topic_link` 주제의 다른 표제어
3. 같은 `level_min` 대의 표제어

혼동쌍이 오답 보기로 들어가야 게임이 성립한다. 무작위 보기는 학생이 읽지 않고도 맞힌다.

---

## 3. 게임 쪽 교체 지점

각 파일 `<script>` 최상단에 이런 블록이 있다.

```js
/* ════════════════════════════════════════════
   문항 — 연동 시 GET /quiz?type=... 로 대체
   ════════════════════════════════════════════ */
const ITEMS = [ ... ];
```

이 배열을 fetch 결과로 바꾸고, 시작 함수(`startGame`)를 async로 만들어 로딩 후 진입하면 된다. **그 아래 로직은 건드리지 않는다.**

```js
let ITEMS = [];
async function loadItems(){
  const r = await fetch(`/quiz?format=gate3&count=6&level=${LEVEL}`);
  ITEMS = (await r.json()).items;
}
```

일부 게임은 별도 상수도 함께 쓴다.

- `snake` / `runner` / `nautilus` — `HUN` (한자 훈음 사전). `hanja` 테이블에서 내려준다.
- `hongkong` — `DISTRACT` (방해 글자 풀). 다른 성어의 구성 글자에서 뽑는다.
- `judge` — 오용 문장과 `type`(오류 유형), `fix`(교정문). 이건 생성이 아니라 **검수된 데이터**여야 한다.

---

## 4. 여정 연결

지금은 7개 파일이 각각 따로 돈다. 학생이 여정을 이어서 하려면 상태 전달이 필요하다.

**구조**: `map.html` 이 부모, 각 구간은 iframe 또는 별도 화면. 부모가 상태를 쥔다.

**부모 → 구간**: 쿼리스트링으로 넘긴다.
```
suez.html?run=<runId>&level=5&stage=1
```

**구간 → 부모**: `postMessage` 로 결과를 올린다.
```js
parent.postMessage({
  type: 'stage_done',
  stage: 'suez',
  correct: 5,
  wrong: 1,
  missedIds: [1204, 1310],
  elapsed: 84.2,
  cleared: true
}, '*');
```

부모가 할 일:
- `correct`/`wrong` 을 80일 예산에 반영 (오답 1개당 1일)
- `missedIds` 를 누적. 마지막 화로 구간에 넘긴다
- `missedIds` 가 비어 있으면 화로 구간을 `?mode=bonus` 로 띄운다 (전속 항해)
- `elapsed` 를 합산해 결말에 실제 소요 시간 표시

**결말 처리**: 첫 회차만 날짜변경선 반전을 보여준다. 2회차부터는 80일 안에 들어와야 승리. 회차는 학생 계정에 저장한다.

---

## 5. 학습 기록

스키마에 없는 테이블 하나를 추가한다. 지금 넣지 않으면 그 전 기록이 전부 날아간다.

```sql
CREATE TABLE attempt (
  attempt_id  INTEGER PRIMARY KEY,
  student_id  TEXT NOT NULL,
  idiom_id    INTEGER REFERENCES idiom(idiom_id),
  game        TEXT NOT NULL,        -- 'archery' | 'suez' | ...
  format      TEXT NOT NULL,
  is_correct  INTEGER NOT NULL,
  ms          INTEGER,              -- 응답까지 걸린 시간
  answered_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX idx_attempt_student ON attempt(student_id, idiom_id);
```

`ms` 를 반드시 기록한다. 맞혔지만 오래 걸린 문항이 곧 그 학생의 약한 지점이고, 이게 있어야 나중에 재출제 간격(3일·1주·3주)과 만점자용 복습 문항 선정이 가능해진다.

---

## 6. 손대지 말 것

- **게임 규칙과 수치** — 낙하 속도, 화력 감소, 기울기 계수 등은 시뮬레이션과 시연을 거쳐 조정한 값이다. 바꿔야 할 근거가 생기면 파일 상단 상수만 고친다.
- **조작 계보** — 현재 7종(조준 / 이동해서 잡기 / 3레인 / 갑판 균형 / 연속 조향 / 병행 작업 / 끌어다 놓기)이다. 새 게임을 추가하더라도 이 안에서 재사용한다. 조작이 늘면 학생이 규칙 익히기에만 시간을 쓴다.
- **브라우저 저장소** — 프로토타입은 최고 기록을 메모리에만 둔다. 서버 연동 시 학생 계정에 저장한다. localStorage 로 대체하지 말 것.

---

## 7. 검수 체크리스트

DB
- [ ] `v_incomplete` 뷰에 걸리는 표제어가 없다
- [ ] `v_origin_mismatch` 가 비어 있다 (출전 없이 유래만 있는 항목 없음)
- [ ] 모든 표제어에 `inclusion_evidence` 가 최소 1건

API
- [ ] 같은 `level` 요청에 같은 문항이 연속으로 나오지 않는다
- [ ] 오답 보기에 정답과 무관한 낱말이 섞이지 않는다
- [ ] `exclude` 파라미터가 동작한다

게임
- [ ] 12종 모두 문항이 API에서 내려온다
- [ ] 모바일(가로 화면)에서 조작이 된다
- [ ] 결과 화면의 "다시 볼 말" 목록이 실제 오답과 일치한다

연결
- [ ] 구간을 마치면 지도의 남은 날짜가 줄어든다
- [ ] 만점이면 화로 구간이 보너스 모드로 뜬다
- [ ] 첫 회차에서 날짜변경선 반전이 나온다

---

## 8. 아직 결정되지 않은 것

- 표제어 700개의 출발 목록. 교과서 수록분이 1순위인데 검정 교과서라 출판사별로 다르다. 보유 교재에서 추출할지, 시중 학습서 목차를 교집합 처리할지 정해야 한다.
- 사전 데이터 확보 방식. 표준국어대사전·한국어기초사전·우리말샘 모두 오픈 API가 있고 공공데이터포털에도 등록돼 있다. 700개를 개별 호출하는 것보다 전체 덤프를 받아 로컬 매칭하는 쪽이 낫다.
- `judge` 게임의 오용 문장. LLM 생성은 가능하나 판정이 미묘해 검수 없이 내보내면 학생이 억울해한다. 현재 20문항은 수작업 검수분이다.
