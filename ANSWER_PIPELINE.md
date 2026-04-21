# answer_pipeline.py — 수능 국어 정답/해설 PDF 파싱 파이프라인

## 개요

수능 국어 영역 정답 및 해설 PDF를 파싱하여 **정답표 · 지문해설 · 문항별해설**을 DB에 저장한다.  
`pipeline_jobs` 테이블에 이미 등록된 정답해설 PDF를 대상으로 실행한다.

---

## 사용법

```bash
python3 answer_pipeline.py <PDF_PATH> \
    --year 2026 \
    --exam-type 수능|9월모의평가|6월모의평가|학력평가 \
    --subject 국어|국어(언매)|국어(화작) \
    [--save]      # DB에 저장
    [--dry-run]   # 출력만 (저장 안 함)
    [--json]      # JSON 출력
```

### 전체 배치 예시 (GCP)

```bash
source venv/bin/activate

python3 answer_pipeline.py uploads/suneung/<JOB_ID>.pdf \
    --year 2026 --exam-type 수능 --subject 국어(언매) --save
```

---

## DB 스키마

### `passage_explanations` — 지문 해설

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `id` | TEXT PK | UUID |
| `paper_code` | TEXT | `"{year}-{exam_type}-{subject}"` |
| `question_range` | TEXT | `"1~3"` |
| `range_start` / `range_end` | INTEGER | 범위 숫자 |
| `domain` | TEXT | 독서 \| 문학 \| 화법 \| 언어 \| 매체 |
| `sub_domain` | TEXT | 독서 이론 \| 주제 통합 \| 현대소설 등 |
| `title` | TEXT | 제목 (따옴표 안 텍스트) |
| `passage_summary` | TEXT | 지문해설 : 전문 |
| `topic` | TEXT | [주제] 뒤 텍스트 |

### `question_explanations` — 문항별 해설

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `id` | TEXT PK | UUID |
| `paper_code` | TEXT | `"{year}-{exam_type}-{subject}"` |
| `question_number` | INTEGER | 문항 번호 |
| `question_type` | TEXT | 세부 내용 파악 / 내용 추론하기 등 |
| `correct_answer` | INTEGER | 정답 번호 (1~5) |
| `score` | INTEGER | 배점 (정답표에서 추출, 없으면 0) |
| `explanation` | TEXT | 정답해설 전문 |
| `wrong_answers` | TEXT | JSON: `{"1": "...", "2": "..."}` |
| `question_id` | TEXT | `questions` 테이블 FK (매칭 후 설정) |

---

## 지원 PDF 형식

| 형식 | 예시 파일 | 정답표 | 지문해설 | 문항해설 |
|---|---|---|---|---|
| **A** (2026~, ■ 마커) | 2026 수능 언매 | ✅ | ✅ | ✅ |
| **B** (2025~, 줄바꿈) | 2025 10월 학력평가 | ✅ | — | ✅ |
| **C** (1페이지, 정답표만) | 2026 6월 모의평가 | ✅ | — | — |

### 형식 A — 정답표 예시 (2026 수능)
```
■ [공통: 독서·문학]
01. ③
02. ⑤
...
■ [선택: 언어와 매체]
35. ③
...
```

### 형식 A — 지문 헤더 예시
```
[1~3] 독서
[1~3] 독서 이론, '독해 능력에 대한 관점'
지문해설 : ...
[주제] 독해 능력을 '해독'과 '언어 이해'로 단순화하여 설명한 단순 관점
```

### 형식 A — 문항 헤더 예시
```
1. 세부 내용 파악
정답해설 : ...
정답 ③
[오답피하기] ① ... ② ...
```

### 형식 B — 정답표 예시 (2025 학력평가)
```
1
②
2
③
...
```

---

## 기존 DB 연동 (`questions` 테이블 FK)

`question_explanations.question_id` 컬럼은 `questions` 테이블의 `id`와 연결된다.  
현재 자동 매칭 쿼리:

```sql
SELECT id, question_number FROM questions
WHERE paper_code LIKE '{year}%'
```

정확한 매칭을 위해 `questions.paper_code` 형식이 일치해야 한다.  
수동 매칭이 필요한 경우:

```python
import sqlite3, json
conn = sqlite3.connect('aprolabs.db')
cur = conn.cursor()
cur.execute("""
    UPDATE question_explanations
    SET question_id = (
        SELECT id FROM questions
        WHERE question_number = question_explanations.question_number
          AND paper_code = '2026-수능-국어(언매)'
    )
    WHERE paper_code = '2026-수능-국어(언매)'
""")
conn.commit()
```

---

## 실행 이력 (2026-04-21)

| paper_code | 정답 | 지문 | 문항 |
|---|---|---|---|
| 2026-수능-국어(언매) | 56 | 9 | 48 |
| 2026-수능-국어(화작) | 45 | 8 | 45 |
| 2026-9월모의평가-국어(언매) | 45 | 4 | 45 |
| 2026-9월모의평가-국어(화작) | 45 | 3 | 45 |
| 2026-6월모의평가-국어 | 45 | 0 | 0 |
| 2025-수능-국어 | 55 | 0 | 56 |
| 2025-10월학력평가-국어(언매) | 45 | 0 | 45 |
| 2025-10월학력평가-국어(화작) | 45 | 0 | 45 |
| 2025-7월학력평가-국어(언매) | 45 | 0 | 45 |
| 2025-7월학력평가-국어(화작) | 45 | 0 | 45 |

**DB 합계**: `passage_explanations` 24건, `question_explanations` 509건

---

## 알려진 한계

- 2025 형식 PDF는 지문 헤더가 `[N ~ M] <출전>` 형식이라 지문해설 파싱 불가
- 1페이지 정답표 전용 PDF는 문항해설 없음
- 배점은 형식 A만 파싱 가능 (형식 B/C는 0)
- `오답피하기` → `오답풀이` 이름 차이는 자동 처리됨
