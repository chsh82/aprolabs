"""파트너 API 키 발급 CLI. 평문 키는 이때 한 번만 출력되고 DB엔 해시만
남으므로, 화면 밖(비밀번호 관리자 등)에 안전하게 옮겨 적어 둘 것 - 잃어버리면
다시 만들어야 한다(같은 이름으로 여러 파트너를 만들 수 있으니 예전 것은
disable로 막고 새로 발급).

실행(momo_b2b_tablet/에서):
    python -m edition.partner_admin create "학원이름"
    python -m edition.partner_admin disable <partner_id>
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from . import auth, db


def main() -> int:
    parser = argparse.ArgumentParser(description="파트너 API 키 발급/비활성화")
    sub = parser.add_subparsers(dest="cmd", required=True)

    create_p = sub.add_parser("create", help="새 파트너 등록 + API 키 발급")
    create_p.add_argument("name", help="파트너(학원) 이름")

    disable_p = sub.add_parser("disable", help="파트너 비활성화(그 파트너의 API 키를 못 쓰게 함)")
    disable_p.add_argument("partner_id", type=int)

    args = parser.parse_args()
    db.init_db()

    if args.cmd == "create":
        partner_id, api_key = auth.create_partner(args.name)
        print(f"파트너 등록됨: id={partner_id} name={args.name!r}")
        print(f"API 키(지금만 보임 - 안전한 곳에 옮겨 적으세요): {api_key}")
        return 0

    if args.cmd == "disable":
        conn = db.get_connection()
        try:
            conn.execute("UPDATE partner SET disabled_at = ? WHERE id = ?",
                         (datetime.now(timezone.utc).isoformat(), args.partner_id))
            conn.commit()
        finally:
            conn.close()
        print(f"파트너 {args.partner_id} 비활성화됨")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
