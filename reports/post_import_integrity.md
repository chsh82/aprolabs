# Post-import Integrity Report — v1.7.5 staging

- checked_at: 2026-10-08T10:43:46.351487
- database: `C:\Users\aproa\aprolabs\data\vocab\staging\vocabulary_quiz_v175_staging.db`
- actual v1.7.5 import executed: no — package files unavailable

## SQLite integrity

- integrity_check: `ok`
- foreign_key_check violations: `0`

## Table counts

```json
{
  "vocabulary_contents": 5723,
  "vocabulary_items": 5723,
  "vocabulary_content_levels": 5723,
  "vocabulary_multiformat_items": 1289,
  "vocabulary_multiformat_sessions": 0,
  "vocabulary_multiformat_responses": 0,
  "vocabulary_quiz_sessions": 0,
  "vocabulary_quiz_attempts": 0
}
```

## Source version counts

```json
{
  "vocabulary_contents": [
    [
      "2.1.29",
      5723
    ]
  ],
  "vocabulary_items": [
    [
      "2.1.29",
      5723
    ]
  ],
  "vocabulary_multiformat_items": [
    [
      "2.1.29",
      1289
    ]
  ]
}
```

## Result

Integrity of isolated staging DB is OK before import. v1.7.5 post-import content integrity cannot be completed until the package files are present.
