# 논술교재 DB화 파이프라인 (nonsul_kb)

모모의 책장 논술교재(학생용 + 교사용 PDF 한 쌍)를 구조화된 SQLite DB로 자동 적재한다.
문항(발문·정답·독해유형·신뢰도)과 비문항 영역(표지·어휘·O/X·키워드정의·배경지식·글쓰기·작가소개)을
모두 담는다.

> aprolabs 저장소의 **독립 하위 폴더**로 동작한다. 기존 수능 파이프라인과 코드/DB를 공유하지 않는다.

---

## 파이프라인 개요

```
학생용 PDF ─ parse_skeleton ─►  문항 골격(발문·유형·페이지·하위번호)
                                     │  (seq, sub_seq) 키로 조인
교사용 PDF ─ extract_teacher ─►  정답 원문(답:/앵커) ─ gen_overlay ─► model_answer(JSON)+신뢰도
           └ extract_blocks  ─►  비문항 영역 + 빈칸/정의 정답 + O/X(비전)
                                     ▼
                                  momo_kb.db  (materials / items / item_tags / ref_notes / content_blocks)
```

두 가지 포맷 프로파일을 자동 처리:
- **α**: `2. [사실적/추론적 독해]` — 발문 줄에 유형 인라인 (금오신화·세계사)
- **β**: `사실적 / 적용적 독해`(별도 헤더 줄) + `1. …` (미래·키다리)

## 파일

| 파일 | 역할 |
|---|---|
| `parser.py` | 학생용 → 문항 골격 자동 추출 (2 프로파일) |
| `extract_teacher.py` | 교사용 → 문항별 정답 원문 (답: 마커 / 학생발문 앵커) |
| `extract_blocks.py` | 비문항 영역 → content_blocks (+교사용 정답 앵커) |
| `gen_overlay.py` | 정답 구조화(LLM) + 신뢰도 판정 + O/X·표 비전 판독 |
| `build_all.py` | 위를 묶어 momo_kb.db 생성 (엔트리포인트) |
| `verify_overlay.py` | `--llm` 정답 무결성 검증 (지어냄·scaffold·에코 탐지) |
| `schema.sql` | SQLite 스키마 (a+b 하이브리드) |
| `materials.json` | 교재 목록 설정 (경로·메타데이터) |

## 설치

```bash
pip install -r requirements.txt
# poppler-utils(pdftotext, pdftoppm) 별도 설치 — requirements.txt 주석 참고
```

## 사용법

1. `data/` 폴더에 교재 PDF를 넣는다 (학생용·교사용 쌍).
2. `materials.json.example` 를 `materials.json` 으로 복사 후 교재 목록 작성.
   - `student`/`teacher` 는 `data/` 기준 상대경로(또는 절대경로).
3. 실행:

```bash
# (A) 폴백 — LLM 없이 정답 원문 그대로 적재 (배관 확인용, 키 불필요)
python build_all.py

# (B) 실전 — MOMOAI로 정답 JSON 구조화 + O/X·표 비전 판독
export ANTHROPIC_API_KEY=sk-ant-...
python build_all.py --llm
```

### 모델 선택 (환경변수)

| 변수 | 기본값 | 용도 |
|---|---|---|
| `MOMOAI_MODEL` | `claude-haiku-4-5-20251001` | 텍스트 정답 구조화(저렴) |
| `MOMOAI_VISION_MODEL` | `claude-sonnet-5` | O/X·흩어진 표 비전 판독(정확) |

```bash
# 전부 sonnet 으로:
export MOMOAI_MODEL=claude-sonnet-5
export MOMOAI_VISION_MODEL=claude-sonnet-5
python build_all.py --llm
```

텍스트 구조화는 haiku로 충분하고, 비전만 sonnet을 쓰는 혼합이 품질/비용 균형에 유리하다.

## 정답 무결성 검증 (`--llm` 후 권장)

LLM이 정답을 지어내거나 왜곡하지 않았는지 자동 대조한다. 학부모 리포트로 나가기 전 게이트로 사용.

```bash
python verify_overlay.py               # min-overlap 기본 0.5
python verify_overlay.py --min-overlap 0.6
```

검사: JSON 유효성 / 빈 정답 / scaffold 라벨 채움 / 발문 에코 / **원문 일치율(지어냄 탐지)**.
- **텍스트 출처** 정답은 교사용 원문과 글자 n-gram 대조 → 일치율이 낮으면 `지어냄의심`으로 플래그.
- **비전 출처**(O/X·흩어진 표) 정답은 대조할 원문 텍스트가 없어 `수동확인` 큐로 분리.
- 하드 오류(지어냄/빈정답/JSON깨짐)가 있으면 **종료코드 1** → CI 게이트로 활용 가능.

## 신뢰도 (거짓 고신뢰 방지)

각 문항 정답에 `answer_confidence`(high/low)와 `answer_flags`가 기록된다.
- **high**: `답:`/앵커로 정확 추출된 서술형
- **low**: 표형·무마커·짧음, 또는 거짓 고신뢰 신호(유형헤더시작/발문에코/다음지문유입) 감지
  → `--llm` 모드에서 교사용 페이지를 렌더해 **비전으로 재판독**

`SELECT * FROM items WHERE answer_confidence='low'` 로 검토 큐를 뽑을 수 있다.

## 새 교재 추가 시

`materials.json` 에 항목 한 개 추가가 전부다. 교재 포맷이 다르면 대개 정규식 한 줄(새 독해유형·마커) 추가로 흡수된다.
검증된 편차: 단계명 상이, work 유무, `1)`/`(1)` 하위표기, `O.X`/`O/X`, `적용적` 유형, 작가소개 블록.

## 검증 현황 (5교재)

문항 41 / 정답 40(98%) / 고신뢰 32(78%) / 블록 20(8종).
블록유형: cover, vocab_quiz, ox_quiz, keyword_def, background, illustration_pick, author_bio, writing.
