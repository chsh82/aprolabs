"""data/vocab/idiom.db를 schema.sql부터 published 150건 상태까지 한 번에
재구성한다. 로컬에서 실제로 밟았던 단계들(seed_from_literacy.py,
split_hanja.py, fix_duplicates.py, split_exam_date_prefix.py,
normalize_hanja_nfkc.py, populate_hanja_strokes.py, calc_level.py,
promote_published.py + 마이그레이션 2개)을 의존관계가 맞는 순서로
고정해서 순서대로 실행한다. 각 스크립트의 로직을 복제하지 않고 그대로
서브프로세스로 호출한다 - 이 파일은 오케스트레이션만 한다.

순서와 그 이유:
    0. schema.sql             - 8개 기본 테이블 생성
    1. migrations/001_add_attempt.py         - attempt 테이블(다른 단계와 무관, 아무 때나 가능)
    2. migrations/002_add_hanja_stroke_count.py - hanja.stroke_count 컬럼(8번이 채우기 전에 있어야 함)
    3. seed_from_literacy.py  - idiom/inclusion_evidence 채움 (이후 모든 단계의 전제)
    4. fix_duplicates.py      - 헤드워드 파싱 오염 4건 정리 (225 -> 221)
    5. split_exam_date_prefix.py - meaning 앞 출제정보 접두어 분리 (9건)
    6. normalize_hanja_nfkc.py   - idiom.hanja NFKC 정규화 (7번의 "기준"이 되므로 먼저)
    7. split_hanja.py         - hanja/idiom_hanja 채움 (6번이 끝난 idiom.hanja를 기준으로 함)
    8. populate_hanja_strokes.py - hanja.stroke_count 채움 (7번이 만든 hanja 행이 있어야 함)
    9. calc_level.py          - level_min 등 계산 (7·8번의 결과가 재료)
   10. promote_published.py   - published 승격 (4·6·9번이 끝난 meaning/level_min/idiom_hanja가 기준)

--dry-run: 실제 data/vocab/idiom.db를 전혀 건드리지 않는다. schema.sql로
임시 파일에 빈 DB를 새로 만들고, VOCAB_DB_PATH 환경변수로 각 하위
스크립트가 그 임시 파일을 보게 한 다음, 위 순서를 전부 "실제 실행"으로
돌려서 결과를 보여준다(하위 스크립트 자체의 --dry-run은 쓰지 않는다 -
그러면 이 스크립트가 순서 자체를 검증할 수 없다). 끝나면 임시 파일을
지운다. literacy DB(data/literacy.db)는 오버라이드 대상이 아니라 항상
실제 파일을 읽는다 - 이 스크립트는 vocab DB 재구성만 검증한다.

--dry-run 없이 실행하면 실제 data/vocab/idiom.db에 그대로 적용된다.
이미 파일이 있으면 0번(스키마 생성)은 건너뛴다(멱등).

실행:
    python scripts/vocab/rebuild_db.py --dry-run   # 임시 사본에서 전체 파이프라인 검증
    python scripts/vocab/rebuild_db.py              # 실제 data/vocab/idiom.db 재구성
"""
from __future__ import annotations

import argparse
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "data" / "vocab" / "schema.sql"

TARGET = {"idiom": 221, "published": 150, "hanja": 404, "idiom_hanja": 616}

# (표시 이름, REPO_ROOT 기준 스크립트 경로)
STEPS = [
    ("migration_001_attempt", "app/vocab/migrations/001_add_attempt.py"),
    ("migration_002_stroke_count", "app/vocab/migrations/002_add_hanja_stroke_count.py"),
    ("seed_from_literacy", "scripts/vocab/seed_from_literacy.py"),
    ("fix_duplicates", "scripts/vocab/fix_duplicates.py"),
    ("split_exam_date_prefix", "scripts/vocab/split_exam_date_prefix.py"),
    ("normalize_hanja_nfkc", "scripts/vocab/normalize_hanja_nfkc.py"),
    ("split_hanja", "scripts/vocab/split_hanja.py"),
    ("populate_hanja_strokes", "scripts/vocab/populate_hanja_strokes.py"),
    ("calc_level", "scripts/vocab/calc_level.py"),
    ("promote_published", "scripts/vocab/promote_published.py"),
]


def create_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


def snapshot(db_path: Path) -> dict:
    conn = sqlite3.connect(db_path)
    try:
        def scalar(sql: str) -> int:
            return conn.execute(sql).fetchone()[0]

        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "idiom" not in tables:
            return {"idiom": 0, "published": 0, "hanja": 0, "idiom_hanja": 0}
        return {
            "idiom": scalar("SELECT COUNT(*) FROM idiom"),
            "published": scalar("SELECT COUNT(*) FROM idiom WHERE status='published'"),
            "hanja": scalar("SELECT COUNT(*) FROM hanja") if "hanja" in tables else 0,
            "idiom_hanja": scalar("SELECT COUNT(*) FROM idiom_hanja") if "idiom_hanja" in tables else 0,
        }
    finally:
        conn.close()


def run_step(name: str, script_rel_path: str, db_path: Path, env: dict) -> int:
    print(f"\n--- {name} ---")
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / script_rel_path)],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        print(f"[경고] {name} 종료 코드 {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(result.stderr.rstrip(), file=sys.stderr)
    snap = snapshot(db_path)
    print(f"  -> 현재 상태: idiom={snap['idiom']} published={snap['published']} "
          f"hanja={snap['hanja']} idiom_hanja={snap['idiom_hanja']}")
    return result.returncode


def print_final_comparison(db_path: Path) -> bool:
    snap = snapshot(db_path)
    print("\n=== 최종 대조 ===")
    all_match = True
    for key, target in TARGET.items():
        actual = snap[key]
        ok = actual == target
        all_match &= ok
        print(f"  {key}: 목표 {target} / 실제 {actual} -> {'OK' if ok else 'MISMATCH'}")
    print(f"\n{'전부 일치' if all_match else '불일치 있음 - 위 MISMATCH 항목 확인 필요'}")
    return all_match


def main() -> int:
    parser = argparse.ArgumentParser(description="idiom.db 전체 재구성")
    parser.add_argument("--dry-run", action="store_true",
                         help="임시 사본에서 전체 파이프라인을 실행해 검증만 하고, 실제 idiom.db는 건드리지 않음")
    args = parser.parse_args()

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    if args.dry_run:
        tmp_dir = Path(tempfile.mkdtemp(prefix="vocab_rebuild_dry_"))
        db_path = tmp_dir / "idiom.db"
        env["VOCAB_DB_PATH"] = str(db_path)
        # fix_duplicates.py가 실제 docs/vocab/MISSING.md에 중복 기록하지 않게 임시 경로로 돌린다
        env["VOCAB_MISSING_MD_PATH"] = str(tmp_dir / "MISSING.md")
        print(f"--dry-run: 임시 DB {db_path} 에서 검증 (실제 data/vocab/idiom.db는 건드리지 않음)")
        create_schema(db_path)
    else:
        db_path = REPO_ROOT / "data" / "vocab" / "idiom.db"
        if db_path.exists():
            print(f"{db_path} 이미 존재 - 스키마 생성 단계는 건너뜀(멱등)")
        else:
            print(f"{db_path} 없음 - schema.sql로 생성")
            create_schema(db_path)

    snap0 = snapshot(db_path)
    print(f"시작 상태: idiom={snap0['idiom']} published={snap0['published']} "
          f"hanja={snap0['hanja']} idiom_hanja={snap0['idiom_hanja']}")

    failed = []
    for name, script_rel_path in STEPS:
        rc = run_step(name, script_rel_path, db_path, env)
        if rc != 0:
            failed.append(name)

    all_match = print_final_comparison(db_path)

    if args.dry_run:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print(f"\n임시 DB 정리 완료 ({tmp_dir})")

    if failed:
        print(f"\n실패한 단계: {failed}", file=sys.stderr)
        return 1
    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())
