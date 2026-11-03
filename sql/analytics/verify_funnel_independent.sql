-- Task: P3-A-A5-01 - Batch Conversion Funnel
-- Purpose: Independent verification of View -> Cart rate
-- Verification date: 2019-10-01
-- Source: /data/curated/events
--
-- Note:
-- The Spark application must register the Parquet input
-- as a temporary view named "events" before executing this SQL.

WITH valid_events AS (
    SELECT
        event_time,
        event_type,
        event_type_rank,
        event_sort_key,
        user_session
    FROM events
    WHERE user_session IS NOT NULL
      AND TRIM(user_session) <> ''
      AND event_time IS NOT NULL
      AND event_type_rank IS NOT NULL
      AND event_sort_key IS NOT NULL
      AND event_type IN ('view', 'cart', 'purchase')
),

ordered_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_session
            ORDER BY event_time, event_type_rank, event_sort_key
        ) AS event_position
    FROM valid_events
),

first_views AS (
    SELECT
        user_session,
        MIN(CASE
            WHEN event_type = 'view' THEN event_position
        END) AS first_view_position,
        MIN(CASE
            WHEN event_type = 'view' THEN event_time
        END) AS first_view_time
    FROM ordered_events
    GROUP BY user_session
),

session_funnel AS (
    SELECT
        v.user_session,
        v.first_view_time,
        MIN(CASE
            WHEN e.event_type = 'cart'
             AND e.event_position > v.first_view_position
            THEN e.event_position
        END) AS first_cart_position
    FROM first_views v
    JOIN ordered_events e
      ON v.user_session = e.user_session
    GROUP BY v.user_session, v.first_view_time
)

SELECT
    COUNT(*) AS view_sessions,
    SUM(CASE
        WHEN first_cart_position IS NOT NULL THEN 1
        ELSE 0
    END) AS cart_sessions,
    ROUND(
        SUM(CASE
            WHEN first_cart_position IS NOT NULL THEN 1.0
            ELSE 0.0
        END) / COUNT(*) * 100,
        2
    ) AS view_to_cart_rate
FROM session_funnel
WHERE TO_DATE(first_view_time) = DATE '2019-10-01';
