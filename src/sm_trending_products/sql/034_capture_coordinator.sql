CREATE TABLE IF NOT EXISTS capture_planner_state (
    synthetic boolean PRIMARY KEY,
    next_due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_started_at timestamptz,
    last_completed_at timestamptz,
    last_error text,
    last_scheduled_count integer NOT NULL DEFAULT 0 CHECK (last_scheduled_count >= 0)
);
