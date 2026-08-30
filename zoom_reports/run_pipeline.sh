#!/usr/bin/env bash
# Zoom 요약 수집 -> 매핑 -> 회차 그룹핑 -> 초안 생성 -> 고유명사 교정.
# zoom-pipeline.timer(systemd)가 이 스크립트를 주기적으로 실행한다.
#
# 각 단계는 전부 재실행 안전(idempotent)하다 - 한 단계가 실패해도 다음
# 단계는 계속 시도한다(한 번의 실패가 전체 배치를 막지 않게). 마지막에
# 하나라도 실패한 단계가 있으면 exit 1로 끝내서 systemd/journal에
# 실패가 남게 한다.
#
# venv는 aprolabs와 공유(../venv) - 별도 설치 필요 없음.
set -uo pipefail
cd "$(dirname "$0")"

PYTHON=../venv/bin/python
STATUS=0

run_step() {
  echo "--- $(date -Iseconds) $1 시작 ---"
  "$PYTHON" "$1"
  code=$?
  if [ "$code" -ne 0 ]; then
    echo "!!! $1 실패 (exit $code)"
    STATUS=1
  fi
}

echo "=== $(date -Iseconds) 파이프라인 시작 ==="
run_step collector.py
run_step map_sessions.py
run_step migrate_class_meeting.py
run_step generate_reports.py
run_step correct_reports.py
echo "=== $(date -Iseconds) 파이프라인 종료 (상태 $STATUS) ==="

exit $STATUS
