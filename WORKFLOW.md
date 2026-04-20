# 수능 파이프라인 수정 작업 표준 절차

## 수정 작업 8단계

1. **카테고리 확정** — 수정할 경고 카테고리 1개 결정 (bleed-in, 이미지탈락 등)
2. **원인 분석** — 코드 위치 파악 (조사만, 수정 금지)
3. **접근법 제시** — 수정 방법 2~3개 + 영향범위·위험도 비교 → 사용자 승인
4. **로컬 수정** — 승인된 방법으로 코드 수정
5. **diff 검토** — 로컬 diff 사용자에게 제시 → 재승인 → git push
6. **샘플 재처리** — 대상 파일 1개만 재파이프라인:
   ```
   1) GCP pull (코드 반영 확인)
   2) DB 백업: cp aprolabs.db aprolabs_backup_$(date +%Y%m%d).db
   3) UI → /suneung/jobs → 해당 job "Reset" 버튼 → 상태 ready로 변경
   4) "Start" (또는 bulk-start) 버튼 → 전체 파이프라인 재실행
      (layout_analyzer → vision_analyzer → segmenter → tagger → verify_agent)
   ※ verify_agent만 재실행하려면: UI → 검수 화면 → "Rerun Agent" 버튼
      (segmenter 결과는 유지하고 verify_agent만 새로 실행)
   ```
7. **compare_baseline.py 실행** — 반드시 기준 baseline을 명시적으로 지정:
   ```bash
   python3 compare_baseline.py golden_tests/baseline_20260420.json
   ```
   - 대상 파일: 해당 카테고리 경고 감소 확인
   - 비대상 8개 파일: "변화없음" 확인 ← 매우 중요
8. **나머지 재처리** — 이상 없으면 나머지 대상 파일도 순차 재처리 (6~7단계 반복)

---

## 중간 규칙

- 한 번에 한 카테고리만 수정 (병렬 금지)
- 샘플 1개 확인 → 나머지 3개 (한꺼번에 4개 금지)
- 각 단계 완료 후 사용자 승인 없이 다음 단계 진행 금지
- **재처리 전 반드시 DB 백업** (DB는 이전 버전을 보관하지 않음)

---

## Regression 발생 시 롤백

코드 롤백:
```bash
git revert [커밋해시]
git push
# GCP: git pull → 서버 재시작
```

DB 롤백 (이미 재처리된 파일):
```bash
# 재처리 전에 백업했다면:
cp aprolabs_backup_YYYYMMDD.db aprolabs.db

# 백업이 없다면: 해당 job을 다시 reset → start로 재처리
# (롤백된 코드 기준으로 재처리됨)
```

주의: DB는 `reset` 시 `segments`와 `raw_result`가 `NULL`로 덮어씌워지며
이전 버전이 자동 보관되지 않는다. 재처리 전 백업이 유일한 복원 수단.

비대상 파일에 변화 생기면 즉시 중단 후 위 절차 실행.
baseline 파일은 수정하지 않음 (기준점 유지).

---

## 금지 사항

- GCP에서 sed/vi/nano로 코드 직접 수정
- baseline 파일 수동 편집
- 여러 카테고리 동시 수정
- compare_baseline.py 건너뛰기
- 재처리 전 DB 백업 생략

---

## Baseline 갱신 시점

한 카테고리의 수정이 완전히 완료되고 안정화된 후에만 새 baseline 저장.

절차:
1. 해당 카테고리의 모든 대상 파일 재처리 완료
2. compare_baseline.py로 예상된 개선만 확인 (명시적 baseline 지정)
3. 비대상 8개 파일 변화 없음 재확인
4. `python3 save_baseline.py` 실행
5. 출력 파일명을 `baseline_YYYYMMDD_카테고리명.json` 으로 수동 rename
6. 이후 compare_baseline.py는 이 새 파일을 명시적으로 지정해서 사용

파일명 예시:
```
golden_tests/
  baseline_20260420.json              ← 수정 전 기준점 (현재)
  baseline_20260422_bleedin.json      ← bleed-in 수정 완료 후
  baseline_20260425_image.json        ← 이미지 탈락 수정 완료 후
```

**수정 작업 중에는 항상 baseline을 명시적으로 지정.**
자동 선택(파일명 생략)은 편의 기능이지만 수정 작업 중에는 위험:
최신 baseline이 이미 갱신된 상태라면 이전 기준점과 비교가 불가능해짐.

```bash
# 올바른 사용 (수정 작업 중)
python3 compare_baseline.py golden_tests/baseline_20260420.json

# 위험한 사용 (수정 작업 중 금지)
python3 compare_baseline.py   # 자동 선택 — 어떤 baseline인지 불명확
```

---

## 현재 Baseline (2026-04-20 기준)

| 항목 | 수치 |
|------|------|
| 총 경고 | 150건 |
| bleed-in 선택지 | 19건 (4개 파일) |
| bleed-in 지문 | 36건 (4개 파일) |
| img_그림불일치 | 22건 |
| bracket텍스트불일치 | 20건 |
| 밑줄_PDF→JSON못찾음 | 34건 |
| 텍스트불일치 | 28건 |
| 문항못찾음 | 17건 |
| 지문못찾음 | 15건 |
| 이미지개수불일치 | 3건 |
| 미분류 | 11건 |

영향 파일: 2025 10월 언매/화작, 2025 7월 언매/화작
클린 파일: 나머지 8개
