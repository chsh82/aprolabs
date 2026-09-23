"""⑥ 인쇄 PDF - headless Chromium(Playwright)으로 A4 2-up PDF를 만든다.
사용자 지시(2026-09-23):
  - 100% 크기(축소 금지): page.pdf(prefer_css_page_size=True) - renderer.css의
    @page{size:210mm 297mm} 크기를 그대로 따르고, scale은 기본값 1(=100%)로 둔다.
  - 필기 포함/미포함, included=false 제외, 홀수 쪽수 빈 면 보정은 이 파일이 아니라
    store.print_view()/renderer/adapters/print.js 쪽에서 처리한다(여기는 그 결과를
    그대로 화면 캡처만 한다).

브라우저 인스턴스는 프로세스당 하나만 띄워서 재사용한다(요청마다 새로 띄우면 느림).
"""
from __future__ import annotations

from playwright.async_api import async_playwright

_playwright = None
_browser = None


async def _get_browser():
    global _playwright, _browser
    if _browser is None:
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch()
    return _browser


async def render_pdf(url: str) -> bytes:
    browser = await _get_browser()
    page = await browser.new_page()
    try:
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_function("window.__viewerReady === true", timeout=15000)
        # printPrep()이 캔버스를 인쇄 해상도(3.2배)로 다시 그린다(기존 인쇄 버튼과 동일 경로).
        await page.evaluate("window.__viewer && window.__viewer.printPrep()")
        await page.emulate_media(media="print")
        return await page.pdf(print_background=True, prefer_css_page_size=True)
    finally:
        await page.close()


async def shutdown() -> None:
    global _playwright, _browser
    if _browser is not None:
        await _browser.close()
        _browser = None
    if _playwright is not None:
        await _playwright.stop()
        _playwright = None
