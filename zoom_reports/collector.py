"""
Zoom AI Companion 회의 요약 수집기 (배치 폴링).

CLAUDE.md 4절: 웹훅이 아니라 야간 배치 폴링이 주 경로다. 최근 3일을
조회해서 요약 생성 지연분을 놓치지 않는다. 중복은 zoom_summary_raw의
meeting_uuid PK로 걸러진다(같은 회의를 다시 긁어도 안전).

절대 원칙: **raw를 가공하지 않는다.** 여기서는 zoom_summary_raw에 detail
응답을 그대로 저장하는 것까지만 한다. 파싱/매핑은 별도 배치(다음 단계)의
일이다.

**중요 - 클라이언트 필터**: `/v2/meetings/meeting_summaries`의 `from`/`to`는
이 계정에서 서버 측 필터링을 전혀 하지 않는 것으로 실측 확인됐다(2026-08-31,
CLAUDE.md 참고 - from=2020-01-01&to=2020-01-31로 보내도 전체 이력이 그대로
반환됨). 그래서 `--days`/`--from`/`--to`는 목록을 받은 뒤 이 스크립트가
`meeting_start_time`(UTC)을 KST로 바꿔 직접 걸러낸다(map_sessions.to_kst
재사용) - 서버에 맡기지 않는다.

로그: 기본 실행은 한 줄 요약만 찍는다(목록/신규/실패 건수, 소요 시간).
--verbose를 줘야 요청 URL과 응답 상세가 찍힌다. Authorization 헤더/토큰은
verbose에서도 절대 출력하지 않는다(URL과 응답 바디만 찍음 - 토큰은 헤더로만
전송되고 로그 코드 어디에도 참조하지 않는다).

실행:
    python collector.py                        # 최근 3일, 요약 한 줄만
    python collector.py --verbose              # 요청/응답 상세까지
    python collector.py --days 7               # 백필 - 지난 7일
    python collector.py --from 2026-08-01 --to 2026-08-31   # 특정 구간 백필
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from map_sessions import to_kst  # noqa: F401 - win32 stdout UTF-8 래핑도 여기서 같이 적용됨(이중 래핑 방지 가드 있음)
from verify_zoom_auth import (
    ZoomAuthError,
    _http_request_with_retry,
    get_access_token,
    load_env,
)
from zoom_pipeline_core import init_db

LIST_URL = "https://api.zoom.us/v2/meetings/meeting_summaries"
DETAIL_URL_TMPL = "https://api.zoom.us/v2/meetings/{uuid}/meeting_summary"
DEFAULT_DAYS_BACK = 3  # CLAUDE.md 4절 - 요약 생성 지연분 대응


def double_encode_uuid(meeting_uuid: str) -> str:
    """meetingUUID에 '/' 또는 '+'가 있으면 더블 인코딩해야 한다(안 하면 3001 에러).

    항상 두 번 인코딩해도 안전하다 - '/' '+'가 없는 uuid는 한 번 인코딩해도
    변하는 문자가 없어서 두 번째 인코딩도 그대로 통과한다.
    """
    once = urllib.parse.quote(meeting_uuid, safe="")
    return urllib.parse.quote(once, safe="")


def list_meeting_summaries(token: str, date_from: date, date_to: date, verbose: bool = False) -> list[dict]:
    """전체 요약 목록(가벼운 메타데이터, next_page_token 페이지네이션).

    date_from/date_to는 요청 파라미터로 보내긴 하지만(서버가 무시해도 값
    자체는 유효한 형식이어야 하므로) 실제 필터링은 하지 않는다 - 호출부가
    반환된 목록을 KST 기준으로 직접 걸러내야 한다.
    """
    summaries: list[dict] = []
    next_page_token = ""
    page_no = 0
    while True:
        params = {
            "from": date_from.isoformat(),
            "to": date_to.isoformat(),
            "page_size": "100",
        }
        if next_page_token:
            params["next_page_token"] = next_page_token
        url = f"{LIST_URL}?{urllib.parse.urlencode(params)}"
        page_no += 1
        if verbose:
            print(f"  [요청 URL] page {page_no}: {url}")
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        data = _http_request_with_retry(req)
        if verbose:
            print(f"  [응답] from={data.get('from')!r} to={data.get('to')!r} "
                  f"summaries={len(data.get('summaries', []))} next_page_token={data.get('next_page_token')!r}")
        summaries.extend(data.get("summaries", []))
        next_page_token = data.get("next_page_token", "")
        if not next_page_token:
            break
    return summaries


def filter_by_kst_date(summaries: list[dict], date_from: date, date_to: date, verbose: bool = False) -> list[dict]:
    """meeting_start_time(UTC)을 KST 날짜로 바꿔 [date_from, date_to] 안에 드는 것만 남긴다.

    서버가 from/to를 걸러주지 않으므로 여기서 실제 필터링을 한다.
    meeting_start_time이 없는 항목은 걸러낼 기준이 없으므로 통과시킨다
    (조용히 버리지 않는다 - map_sessions.py에서 다시 판단하게 남겨둠).
    """
    kept = []
    for item in summaries:
        started_at = item.get("meeting_start_time")
        if not started_at:
            kept.append(item)
            continue
        kst_date = to_kst(started_at).date()
        if date_from <= kst_date <= date_to:
            kept.append(item)
        elif verbose:
            print(f"  [필터 제외] {item.get('meeting_uuid')} - KST 날짜 {kst_date}가 "
                  f"[{date_from}, {date_to}] 밖")
    return kept


def fetch_meeting_summary_detail(token: str, meeting_uuid: str, verbose: bool = False) -> dict:
    """요약 본문(overview/details/next_steps 등 포함)을 가져온다."""
    encoded = double_encode_uuid(meeting_uuid)
    url = DETAIL_URL_TMPL.format(uuid=encoded)
    if verbose:
        print(f"  [요청 URL] {url}")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    data = _http_request_with_retry(req)
    if verbose:
        print(f"  [응답] meeting_uuid={data.get('meeting_uuid')!r} "
              f"host={data.get('meeting_host_email')!r} "
              f"summary_title={data.get('summary_title')!r}")
    return data


def run_collector(date_from: date, date_to: date, verbose: bool = False) -> tuple[int, dict]:
    """수집 실행. (실패 건수, 통계 dict)를 반환한다."""
    conn = init_db()
    env = load_env(Path(__file__).resolve().parent / ".env")
    token = get_access_token(env["ZOOM_ACCOUNT_ID"], env["ZOOM_CLIENT_ID"], env["ZOOM_CLIENT_SECRET"])

    if verbose:
        print(f"목록 조회 중... (요청 범위 {date_from} ~ {date_to} - 서버는 무시하고 전체를 주므로 아래서 직접 필터링)")
    raw_summaries = list_meeting_summaries(token, date_from, date_to, verbose=verbose)
    summaries = filter_by_kst_date(raw_summaries, date_from, date_to, verbose=verbose)
    if verbose:
        print(f"목록 {len(raw_summaries)}건 조회 -> KST 날짜 필터 후 {len(summaries)}건")

    already, fetched, failed = 0, 0, 0
    failures: list[str] = []

    for item in summaries:
        meeting_uuid = item.get("meeting_uuid")
        host_email = item.get("meeting_host_email")
        if not meeting_uuid:
            failed += 1
            failures.append(f"meeting_uuid 없는 항목 (host={host_email}): {item}")
            continue

        existing = conn.execute(
            "SELECT 1 FROM zoom_summary_raw WHERE meeting_uuid = ?", (meeting_uuid,)
        ).fetchone()
        if existing:
            already += 1
            continue

        try:
            detail = fetch_meeting_summary_detail(token, meeting_uuid, verbose=verbose)
        except ZoomAuthError as e:
            failed += 1
            failures.append(f"detail 조회 실패 (uuid={meeting_uuid}, host={host_email}): {e}")
            continue

        conn.execute(
            "INSERT INTO zoom_summary_raw (meeting_uuid, payload_json) VALUES (?, ?)",
            (meeting_uuid, json.dumps(detail, ensure_ascii=False)),
        )
        conn.commit()  # 한 건씩 즉시 커밋 - 중간에 실패해도 이미 받은 raw는 지키기 위함(CLAUDE.md "원문 즉시 적재")
        fetched += 1

    if verbose and failures:
        print("\n실패:")
        for f in failures:
            print(f"  {f}")

    stats = {
        "listed": len(summaries),
        "listed_raw": len(raw_summaries),
        "already": already,
        "fetched": fetched,
        "failed": failed,
    }

    if verbose:
        total = conn.execute("SELECT COUNT(*) FROM zoom_summary_raw").fetchone()[0]
        print(f"\n신규 수집 {fetched}건, 이미 있어서 건너뜀 {already}건, 실패 {failed}건")
        print(f"zoom_summary_raw 전체: {total}건")

    conn.close()
    return failed, stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Zoom 회의 요약 배치 수집기")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS_BACK,
                       help=f"오늘부터 며칠 전까지 조회할지 (기본 {DEFAULT_DAYS_BACK}일)")
    parser.add_argument("--from", dest="date_from", type=str, default=None,
                       help="시작일(YYYY-MM-DD) - 지정하면 --days 대신 이 구간으로 백필")
    parser.add_argument("--to", dest="date_to", type=str, default=None,
                       help="종료일(YYYY-MM-DD), --from과 함께 사용")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="요청 URL과 응답 상세를 출력한다(토큰/Authorization 헤더는 출력하지 않음)")
    args = parser.parse_args()

    if args.date_from:
        date_from = date.fromisoformat(args.date_from)
        date_to = date.fromisoformat(args.date_to) if args.date_to else date.today()
    else:
        date_to = date.today()
        date_from = date_to - timedelta(days=args.days)

    t0 = time.monotonic()
    try:
        failed, stats = run_collector(date_from, date_to, verbose=args.verbose)
    except ZoomAuthError as e:
        print(f"치명적 오류: {e}", file=sys.stderr)
        return 1
    elapsed = time.monotonic() - t0

    print(f"목록 {stats['listed']}건, 신규 {stats['fetched']}건, 실패 {stats['failed']}건, 소요 {elapsed:.1f}초")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
