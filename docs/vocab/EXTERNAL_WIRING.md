# app/vocab/ 밖에서 건드려야 하는 것

`app/vocab/` 안의 코드는 다른 app 모듈을 import하지 않는다(원칙 1). 하지만
FastAPI 앱에 라우터를 등록하고 정적 파일을 여는 것은 `app/main.py`가 할
일이라 `app/vocab/` 바깥에서 최소한의 배선이 필요하다. momoai.kr로 이관할
때 `app/vocab/` 디렉터리를 그대로 들어내면서 이 목록에 있는 줄만 지우면
된다 - 반대로 여기 없는 줄은 vocab과 무관하니 신경 쓸 필요 없다.

**2026-09-03 반영 완료** - 아래 1·2번은 이제 `app/main.py`에 실제로 들어가
있다. momoai.kr 이관 시 `main.py`에서 이 줄들만 지우면 된다.

---

## 1. 라우터 등록 (완료)

```python
from app.vocab.routers import quiz_api as vocab_quiz_api
...
app.include_router(vocab_quiz_api.router)
```

`app.routers` 아래가 아니라 `app.vocab.routers` 그대로 등록했다 - 원칙 1
("app/vocab/ 밖 모듈을 import하지 않는다")은 app/vocab/ 안쪽 코드에만
적용되고, main.py가 app/vocab을 참조하는 이 방향은 문제없다.

## 2. 정적 파일 마운트 (완료)

```python
app.mount("/vocab/games", StaticFiles(directory="app/vocab/static/games"), name="vocab_games")
```

`app/static`, `/uploads`, `/momo-images`와 같은 자리(`app/main.py` 상단
`app.mount(...)` 블록)에 추가했다.

## 3. 인증 화이트리스트 - 사용자 결정(2026-09-02): 열지 않음

```python
public_paths = {"/login", "/logout"}
```

지금은 그대로 둔다. 관리자 계정으로 로그인해서 테스트하는 것으로 충분하고,
학생 접근은 momoai.kr 이관 후에 처리한다. `attempt.student_id`에는
당분간 `'test_01'` 같은 임시 값이 들어간다.

**이관 시 재검토할 것**: momoai.kr에서는 학생이 aprolabs 로그인 없이
게임에 접근해야 하므로, 그쪽 인증 체계에 맞는 별도 처리가 필요하다
(aprolabs의 `public_paths`를 그대로 옮기는 게 아니라 momoai.kr 자체
인증과 통합해야 함 - `app/routers/literacy_api.py`가 남겨둔 것과 같은
갈림길).

## 3-1. 사이드바 링크 (완료, 2026-09-03)

```html
<!-- app/templates/base.html, "프로젝트" 섹션 -->
<a href="/vocab/games/index.html" class="...">🎮 어휘 게임 테스트</a>
```

정적 파일(`/vocab/games/index.html`)은 마운트만 해두면 URL로는 열리지만,
사이트 내 어디서도 링크가 없으면 사람이 못 찾는다("사이트 내에 게시판이
없는데?" - 사용자가 직접 겪은 문제). `app/templates/base.html`은
aprolabs 전체가 공유하는 템플릿이라 `app/vocab/` 밖이다 - 원칙 1과
무관(다른 방향, main.py와 같은 경우).

**이관 시**: 이 `<a>` 태그 한 줄만 지우면 된다. momoai.kr에는 이
페이지가 아예 필요 없을 수도 있다(관리자 전용 테스트 게시판이므로).

## 4. DB 파일 자체는 배선이 아니다

`data/vocab/idiom.db`는 `app/vocab/db.py`가 자체적으로 경로를 계산해서
연다(`REPO_ROOT / "data" / "vocab" / "idiom.db"`). `main.py`나 다른
어떤 파일도 이 경로를 알 필요가 없다 - 이관 시 DB 파일과 `app/vocab/`을
통째로 옮기면 그대로 동작한다.

---

## 현재 상태 (2026-09-03)

- [x] `data/vocab/idiom.db` 스키마 8테이블 + `attempt`(migrations/001) 생성 완료
- [x] literacy DB 사자성어 225건 시드 완료(`scripts/vocab/seed_from_literacy.py`)
- [x] `GET /vocab/quiz` 라우터 작성 및 `main.py` 등록 완료(gate3/assemble/hanja 실제 동작,
      mc4/situation4/usage는 빈 items+note)
- [x] 정적 마운트 완료 - `/vocab/games/voyage/suez.html`부터 실제 API 연동 시작
- [ ] 인증 화이트리스트 - 3번 항목대로 현재는 변경 없음(의도된 상태) - 그래서
      `/vocab/games/*`, `/vocab/quiz` 둘 다 아직 aprolabs 로그인이 있어야 열린다
