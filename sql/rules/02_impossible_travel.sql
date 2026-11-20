-- IMPOSSIBLE_TRAVEL (D-06, D-07): the card's previous non-late event was in a different
-- country and within (t - window_minutes, t). Exactly window_minutes apart does not alert.
-- Comparing with the *previous* event (a change of country), not any earlier event, gives one
-- alert per border crossing instead of one for every later event in the window.
-- Parameters: new_event_ids, window_minutes.
-- Triggers are the new events, plus existing events of the same card up to one window after a
-- new event (out-of-order arrivals). Late events are excluded (D-04).
-- event_ids = [previous event, trigger].
WITH new_events AS (
    SELECT e.card_token, e.event_time
      FROM sg.enriched_transactions e
     WHERE e.event_id = ANY (%(new_event_ids)s::uuid[])
       AND NOT e.is_late
),
candidates AS (
    SELECT DISTINCT c.event_id, c.card_token, c.event_time, c.country, c.status
      FROM new_events n
      JOIN sg.enriched_transactions c
        ON c.card_token = n.card_token
       AND NOT c.is_late
       AND c.event_time >= n.event_time
       AND c.event_time < n.event_time + make_interval(mins => %(window_minutes)s)
)
SELECT c.card_token,
       c.event_id::text AS trigger_event_id,
       c.event_time,
       c.status         AS txn_status,
       ARRAY[p.event_id::text, c.event_id::text] AS event_ids,
       jsonb_build_object(
           'from_country', p.country,
           'to_country', c.country,
           'minutes_apart', round(extract(epoch FROM c.event_time - p.event_time) / 60.0, 2),
           'window_minutes', %(window_minutes)s) AS details
  FROM candidates c
 CROSS JOIN LATERAL (
        SELECT x.event_id, x.country, x.event_time
          FROM sg.enriched_transactions x
         WHERE x.card_token = c.card_token
           AND NOT x.is_late
           AND x.event_time < c.event_time
         ORDER BY x.event_time DESC, x.event_id
         LIMIT 1
       ) p
 WHERE p.country <> c.country
   AND p.event_time > c.event_time - make_interval(mins => %(window_minutes)s)
 ORDER BY c.event_time, c.event_id;
