"""
DB에 이미 등록된 합본 파일을 분할하여 재등록.

사용법:
    python split_existing_combined.py --dry-run   # 분할 대상 확인만
    python split_existing_combined.py --apply     # 실제 분할 + DB 갱신
"""
import sys
import os
import uuid
import argparse

sys.path.insert(0, os.path.dirname(__file__))
os.environ.setdefault("DATABASE_URL", "sqlite:///./aprolabs.db")

from app.database import SessionLocal, init_db
from app.models.passage import PipelineJob
from sqlalchemy import func


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="분할 대상 출력만 (DB 변경 없음)")
    parser.add_argument("--apply",   action="store_true", help="실제 분할 + DB 갱신")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.print_help()
        sys.exit(1)

    init_db()
    from app.services.split_combined_pdf import is_combined_exam, split_combined_exam

    db = SessionLocal()
    candidates = db.query(PipelineJob).filter(
        PipelineJob.sub_type == "통합",
        PipelineJob.subject == "국어",
    ).all()

    print(f"통합 국어 파일 {len(candidates)}개 검사 중...\n")

    to_split = []
    for job in candidates:
        if not job.file_path or not os.path.exists(job.file_path):
            print(f"  [SKIP] #{job.job_number} {job.filename} — 파일 없음")
            continue
        if is_combined_exam(job.file_path):
            to_split.append(job)
            print(f"  [합본] #{job.job_number} {job.filename}")
        else:
            print(f"  [단일] #{job.job_number} {job.filename}")

    print(f"\n분할 대상: {len(to_split)}개")

    if args.dry_run or not to_split:
        db.close()
        return

    # --apply
    upload_dir = "uploads/suneung"
    ok_count = 0
    for job in to_split:
        print(f"\n분할 중: {job.filename}")
        splits = split_combined_exam(job.file_path, upload_dir)
        if not splits:
            print(f"  [FAIL] 분할 실패 — 원본 유지")
            continue

        max_num = db.query(func.max(PipelineJob.job_number)).scalar() or 0
        for sp in splits:
            sp_id = str(uuid.uuid4())
            max_num += 1
            new_job = PipelineJob(
                id=sp_id,
                job_number=max_num,
                filename=sp["filename"],
                file_path=sp["path"],
                source=job.source,
                source_year=job.source_year,
                exam_type=job.exam_type,
                subject=job.subject,
                sub_type=sp["sub_type"],
                grade=job.grade,
                answer_file_path=job.answer_file_path,
                status="ready",
            )
            db.add(new_job)
            print(f"  → 등록: {sp['filename']} ({sp['sub_type']})")

        db.delete(job)
        db.commit()
        ok_count += 1
        print(f"  원본 삭제 완료")

    db.close()
    print(f"\n완료: {ok_count}개 합본 분할됨")


if __name__ == "__main__":
    main()
