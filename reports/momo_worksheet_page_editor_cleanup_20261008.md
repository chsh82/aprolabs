# momo worksheet page editor 잔여 변경 정리 — 2026-10-08

## 범위

schema/literacy 정리 후 남아 있던 momo worksheet page editor 관련 변경을 별도 트랙으로 분리해 검토했다.

대상 변경은 `momo_book_db/PROGRESS.md`의 "13차: P1 범위 명확화 + 6개 항목 보완·검증" 기록과 일치한다.

## 커밋 후보

- `app/routers/momo_worksheet_page_editor.py`
- `app/templates/momo_worksheet_editor/pages.html`
- `momo_book_db/PROGRESS.md`
- `momo_book_db/worksheet/editor/page_store.py`
- `momo_book_db/worksheet/scripts/blocks.js`
- `momo_book_db/worksheet/scripts/paginate.js`
- `momo_book_db/worksheet/scripts/pdf_pixel_diff.py`

## 변경 요약

### 1. 그림 크기·캡션 조절

- `set_image_layout` proposal operation 추가.
- 지원 필드:
  - `reference_image_path`
  - `reference_image2_path`
  - `excerpt_image_path`
- 캡션은 빈 문자열이면 제거.
- 높이는 `10~200mm` 범위에서만 허용.
- 원본 이미지 파일은 변경하지 않고 `ui_config` 표시 설정만 조정한다.
- crop/자르기는 이번 범위에서 명시적으로 제외.

### 2. 미지원 페이지 사유 표시

- 표지, 1단계, 3단계 페이지는 직접 편집 불가 사유를 화면에 표시한다.
- 기존의 단순 `읽기 전용` 문구보다 사용자가 왜 편집할 수 없는지 명확히 알 수 있게 했다.

### 3. 페이지 복원 버그 수정

- `restore_revision()`이 복원 대상 시점의 overrides 대신 최신 overrides를 섞어 쓰던 문제를 고쳤다.
- 복원 대상 리비전의 `applied_version_id` 또는 `frozen_from_version` 기준으로 content/design overrides를 재사용한다.
- 목적: 과거 페이지 HTML과 최신 effective data가 섞여 QA가 거짓 FAIL하는 문제 방지.

### 4. PDF 픽셀 비교 도구 추가

- `momo_book_db/worksheet/scripts/pdf_pixel_diff.py` 추가.
- PyMuPDF로 PDF를 동일 DPI에서 래스터화해 페이지별 픽셀 차이를 JSON으로 기록한다.

## 검증

실행 완료:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile app/routers/momo_worksheet_page_editor.py momo_book_db/worksheet/editor/page_store.py momo_book_db/worksheet/scripts/pdf_pixel_diff.py
node --check momo_book_db/worksheet/scripts/blocks.js
node --check momo_book_db/worksheet/scripts/paginate.js
```

결과:

- Python syntax: pass
- JavaScript syntax: pass

추가로 표준 L5-Q4-W11 재생성/QA smoke를 시도했으나 현재 로컬 Playwright browser executable이 없어 차단됐다.

차단 메시지 요약:

```text
browserType.launch: Executable doesn't exist at C:\Users\aproa\AppData\Local\hermes\tools\chromium_headless_shell-1234\chrome-headless-shell-win64\chrome-headless-shell.exe
Looks like Playwright was just installed or updated. Please run: npx playwright install
```

이 문제는 이전에 확인된 Hermes/Playwright 계열 브라우저 도구 설치·권한 문제와 같은 환경 계열 blocker로 보인다. 따라서 이번 커밋 검증은 syntax와 정적 검토 기준으로 제한한다.

## 제외/보류

- `l2_check.json`: worksheet sample/debug JSON으로 보존하되 이번 커밋 제외.
- DB write 없음.
- 서버 배포 없음.
- 서비스 재시작 없음.
- 운영 DB/기존 프로젝트 데이터 직접 변경 없음.
- `.env`/secret 값 읽기 또는 출력 없음.

## 판정

변경 묶음은 P1 페이지별 편집기 개선/버그 수정으로 의미가 일관되며, 어휘 DB/schema-literacy 변경과 분리해 커밋하는 것이 안전하다.
