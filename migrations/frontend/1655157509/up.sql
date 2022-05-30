DELETE FROM access_tokens WHERE internal and note = 'batch-spec-execution';

DROP VIEW IF EXISTS batch_spec_workspace_execution_queue;
CREATE VIEW batch_spec_workspace_execution_queue AS
WITH user_queues AS (
    SELECT
        exec.user_id,
        MAX(exec.started_at) AS latest_dequeue
    FROM batch_spec_workspace_execution_jobs AS exec
    GROUP BY exec.user_id
),
-- We are creating this materialized CTE because PostgreSQL doesn't allow `FOR UPDATE` with window functions.
-- Materializing it makes sure that the view query is not inlined into the FOR UPDATE select the Dequeue method
-- performs.
materialized_queue_candidates AS MATERIALIZED (
    SELECT
        exec.id,
        exec.batch_spec_workspace_id,
        exec.state,
        exec.failure_message,
        exec.started_at,
        exec.finished_at,
        exec.process_after,
        exec.num_resets,
        exec.num_failures,
        exec.execution_logs,
        exec.worker_hostname,
        exec.last_heartbeat_at,
        exec.created_at,
        exec.updated_at,
        exec.cancel,
        exec.queued_at,
        exec.user_id,
        RANK() OVER (
            PARTITION BY queue.user_id
            -- Make sure the jobs are still fulfilled in timely order, and that the ordering is stable.
            ORDER BY exec.created_at ASC, exec.id ASC
        ) AS place_in_user_queue
    FROM batch_spec_workspace_execution_jobs exec
    JOIN user_queues queue ON queue.user_id = exec.user_id
    WHERE
    	-- Only queued records should get a rank.
        exec.state = 'queued'
    ORDER BY
        -- Round-robin let users dequeue jobs.
        place_in_user_queue,
        -- And ensure the user who dequeued the longest ago is next.
        queue.latest_dequeue ASC NULLS FIRST
)
SELECT
    ROW_NUMBER() OVER () AS place_in_global_queue, materialized_queue_candidates.*
FROM materialized_queue_candidates;


ALTER TABLE batch_spec_workspace_execution_jobs DROP COLUMN IF EXISTS access_token_id;
