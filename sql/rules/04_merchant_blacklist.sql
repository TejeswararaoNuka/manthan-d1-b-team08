-- MERCHANT_BLACKLIST (D-07, D-09): payment to a merchant in sg.merchant_blacklist.
-- Parameters: new_event_ids. Per-event rule: late events are evaluated too (D-04).
SELECT e.card_token,
       e.event_id::text      AS trigger_event_id,
       e.event_time,
       e.status              AS txn_status,
       ARRAY[e.event_id::text] AS event_ids,
       jsonb_build_object('merchant_id', e.merchant_id, 'reason', m.reason) AS details
  FROM sg.enriched_transactions e
  JOIN sg.merchant_blacklist m ON m.merchant_id = e.merchant_id
 WHERE e.event_id = ANY (%(new_event_ids)s::uuid[])
 ORDER BY e.event_time, e.event_id;
