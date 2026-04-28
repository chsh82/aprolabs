"""
PDF 폰트 인코딩 진단 — 「」 등 코너 괄호가 어떤 Unicode로 추출되는지 확인
사용법: venv/Scripts/python diagnose_font.py <pdf_path>
"""
import sys
import fitz

def diagnose(pdf_path):
    doc = fitz.open(pdf_path)
    print(f"페이지 수: {doc.page_count}")

    # 관심 키워드 주변 텍스트에서 코너 괄호 탐색
    target_keywords = ["노자주", "노자", "주석", "주를"]
    corner_chars = set()

    for pn, page in enumerate(doc):
        rawdict = page.get_text("rawdict")
        for block in rawdict.get("blocks", []):
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text = span.get("text", "")
                    for ch in text:
                        cp = ord(ch)
                        # U+3000~U+33FF: CJK 특수문자 범위 (「」등 포함)
                        # U+FF00~U+FFEF: 전각·반각 특수문자
                        # U+E000~U+F8FF: Private Use Area
                        if (0x3000 <= cp <= 0x33FF or
                                0xFF00 <= cp <= 0xFFEF or
                                0xE000 <= cp <= 0xF8FF):
                            corner_chars.add((ch, hex(cp)))

                    # 키워드 주변 문자 상세 출력
                    for kw in target_keywords:
                        idx = text.find(kw)
                        if idx >= 0:
                            window = text[max(0, idx-5):idx+len(kw)+5]
                            print(f"\nP{pn+1} 키워드 '{kw}' 주변:")
                            print(f"  텍스트: {repr(window)}")
                            print(f"  코드포인트: {[hex(ord(c)) for c in window]}")

    print("\n\n발견된 CJK/특수 문자:")
    for ch, cp in sorted(corner_chars, key=lambda x: x[1]):
        name = ""
        try:
            import unicodedata
            name = unicodedata.name(ch, "unknown")
        except Exception:
            pass
        print(f"  {repr(ch)}  {cp}  {name}")

    doc.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python diagnose_font.py <pdf_path>")
    else:
        diagnose(sys.argv[1])
