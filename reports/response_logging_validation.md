# Response Logging Validation — v1.7.5 runtime integration

- checked_at: 2026-10-08T10:43:46.351487
- database: `C:\Users\aproa\aprolabs\data\vocab\staging\vocabulary_quiz_v175_staging.db`

## Schema evidence

```json
{
  "tables": {
    "vocabulary_multiformat_sessions": [
      "id",
      "user_id",
      "source_version",
      "item_types_json",
      "question_count",
      "correct_count",
      "status",
      "started_at",
      "completed_at",
      "metadata_json"
    ],
    "vocabulary_multiformat_responses": [
      "id",
      "session_id",
      "item_id",
      "order_index",
      "item_type",
      "submitted_payload_json",
      "is_correct",
      "correct_count",
      "total_count",
      "answered_at",
      "attempt_count",
      "hint_used"
    ],
    "vocabulary_quiz_sessions": [
      "id",
      "user_id",
      "source_version",
      "question_count",
      "correct_count",
      "status",
      "started_at",
      "completed_at"
    ],
    "vocabulary_quiz_attempts": [
      "id",
      "session_id",
      "item_id",
      "order_index",
      "selected_option",
      "correct_option",
      "is_correct",
      "answered_at"
    ]
  },
  "has_user_session": true,
  "has_item_id": true,
  "has_sense_id_direct": false,
  "has_level_direct": false,
  "has_correct": true,
  "has_selected_answer_payload": true,
  "has_response_time": false,
  "has_attempt": true,
  "has_content_release": true
}
```

## Required field result

- user/session: PARTIAL — session tables have `user_id` and response rows have `session_id`.
- item_id: AVAILABLE.
- sense_id: PARTIAL — item table has sense/source content info, response row does not snapshot it directly.
- level: PARTIAL — available through content level join, response row does not snapshot it directly.
- correct: AVAILABLE as `is_correct`.
- selected answer: AVAILABLE as `submitted_payload_json` for multiformat; legacy has `selected_option`.
- response time: NOT_AVAILABLE — no `response_time`, `elapsed_ms`, or equivalent field found.
- attempt: AVAILABLE/PARTIAL as `attempt_count` in multiformat responses; legacy single quiz does not have same field.
- content release: AVAILABLE/PARTIAL through session `source_version`; response row does not snapshot release directly.

Result: `RESPONSE_LOG_BLOCKED_SCHEMA_GAP` for the exact Project 01 requirements.
