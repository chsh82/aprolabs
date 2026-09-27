#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""L6 577건 대기열에서 AI 정의에 실제 오류가 확인된 항목의 영구 등록부.
이후 단계의 선정 스크립트(phase35 이상)와 자동 초안 생성·적재 후보 선택
로직은 반드시 이 파일을 참조해 등록된 term_id를 제외해야 한다 - phase31/32의
EXCLUDED_IDS 하드코딩 패턴과 달리, 이 등록부는 스크립트마다 복사하지 않고
공유 파일(CSV) 하나로 관리해 향후 단계가 놓치지 않게 한다.

DB 값 수정은 이 파일의 책임이 아니다 - 별도의 근거와 변경 전후 dry-run이
마련되기 전까지 literacy.db의 실제 definition 값은 건드리지 않는다
(resolution_status=UNRESOLVED_AWAITING_DRY_RUN인 동안은 파일 기반 제외만
적용).

사용 (다음 단계 선정 스크립트에서):
    from known_definition_errors import load_excluded_ids
    EXCLUDED_IDS |= load_excluded_ids()
"""
from __future__ import annotations

import csv
from pathlib import Path

REGISTRY_PATH = Path(__file__).resolve().parent.parent.parent / \
    "data/import/schema_reading_known_ai_definition_errors_20260928.csv"


def load_registry(path: Path = REGISTRY_PATH) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_excluded_ids(path: Path = REGISTRY_PATH) -> set[str]:
    """자동 초안 생성 또는 적재 후보 선택에서 제외해야 하는 term_id 집합."""
    rows = load_registry(path)
    return {
        r["term_id"] for r in rows
        if r["exclude_from_auto_draft"] == "True" or r["exclude_from_load_candidate"] == "True"
    }


def is_known_error(term_id: str, path: Path = REGISTRY_PATH) -> bool:
    return term_id in load_excluded_ids(path)


if __name__ == "__main__":
    ids = load_excluded_ids()
    print(f"등록된 오류 term_id {len(ids)}건: {sorted(ids)}")
