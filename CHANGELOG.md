
## 2026-04-20 - bleed-in 카테고리 수정

### 수정 내용
- layout_analyzer.py: PAGE_HEADER_RE 필터 추가
- 페이지 헤더 ("국어영역", "고3") 제거

### 결과
- bleed-in 55건 → 0건 (100% 해결)
- 부작용: 지문못찾음 +6, 미분류 +3
- 순이익: -50건

### 영향 파일
- 2025 10월 언매/화작: 완벽 처리
- 2025 7월 언매/화작: bleed-in 제거 + 부작용 발생

### Baseline
- 이전: baseline_20260420.json
- 이후: baseline_20260420_bleedin.json
