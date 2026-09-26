import csv
import json
from pathlib import Path

csv_path = Path('data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.csv')
jsonl_path = Path('data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.jsonl')

with open(csv_path, encoding='utf-8-sig', newline='') as f:
    rows = list(csv.DictReader(f))
verdicts = [r['verdict'] for r in rows]
print('csv total rows:', len(rows))
print('csv PASS count:', verdicts.count('PASS'), 'HOLD count:', verdicts.count('HOLD'))
for r in rows:
    if r['item_id'] in ('MF_A_SC_SRL4L5PILOT_20260925_L4_008', 'MF_C_SC_SRL4L5PILOT_20260925_L4_008'):
        print(r['item_id'], r['verdict'])

lines = jsonl_path.read_text(encoding='utf-8').splitlines()
objs = [json.loads(l) for l in lines if l.strip()]
print('jsonl total:', len(objs), 'PASS:', sum(1 for o in objs if o['verdict'] == 'PASS'),
      'HOLD:', sum(1 for o in objs if o['verdict'] == 'HOLD'))
for o in objs:
    if o['item_id'] in ('MF_A_SC_SRL4L5PILOT_20260925_L4_008', 'MF_C_SC_SRL4L5PILOT_20260925_L4_008'):
        print(o['item_id'], o['verdict'])
