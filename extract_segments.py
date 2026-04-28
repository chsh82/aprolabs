from app.database import SessionLocal
from app.models.passage import PipelineJob
from app.models.question import Question  # SQLAlchemy 관계 해결용
import json

db = SessionLocal()
job = db.get(PipelineJob, '26af43a0-c6c0-402b-a3cb-fd5032fb7097')
with open('test_segments.json', 'w', encoding='utf-8') as f:
    json.dump(job.segments, f, ensure_ascii=False, indent=2)
db.close()
print('저장 완료: test_segments.json')
