-- VELOCITY (D-06, D-07): more than max_tx non-late events of one card in (t - window, t].
-- Parameters: new_event_ids (rows inserted by this batch), max_tx, window_seconds.
-- Triggers are the new events, plus existing events of the same card up to one window after
-- a new event (an out-of-order arrival can complete a burst that ends at an older event).
-- Late events (is_late) are excluded on both sides (D-04).
-- Output columns (all rule files): card_token, trigger_event_id, event_time, txn_status,
-- event_ids (text[], oldest first), details (jsonb).
WITH new_events AS (
    SELECT e.card_token, e.event_time
      FROM sg.enriched_transactions e
     WHERE e.event_id = ANY (%(new_event_ids)s::uuid[])
       AND NOT e.is_late
),
candidates AS (
    SELECT DISTINCT c.event_id, c.card_token, c.event_time, c.status
      FROM new_events n
      JOIN sg.enriched_transactions c
        ON c.card_token = n.card_token
       AND NOT c.is_late
       AND c.event_time >= n.event_time
       AND c.event_time < n.event_time + make_interval(secs => %(window_seconds)s)
)
SELECT c.card_token,
       c.event_id::text  AS trigger_event_id,
       c.event_time,
       c.status          AS txn_status,
       w.event_ids,
       jsonb_build_object('tx_count', w.tx_count,
                          'max_tx', %(max_tx)s,
                          'window_seconds', %(window_seconds)s) AS details
  FROM candidates c
 CROSS JOIN LATERAL (
        SELECT array_agg(x.event_id::text ORDER BY x.event_time, x.event_id) AS event_ids,
               count(*)                                                      AS tx_count
          FROM sg.enriched_transactions x
         WHERE x.card_token = c.card_token
           AND NOT x.is_late
           AND x.event_time > c.event_time - make_interval(secs => %(window_seconds)s)
           AND x.event_time <= c.event_time
       ) w
 WHERE w.tx_count > %(max_tx)s
 ORDER BY c.event_time, c.event_id;
