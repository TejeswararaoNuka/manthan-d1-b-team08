-- AMOUNT_SPIKE (D-03, D-07): amount > multiplier x the card's 30-day average.
-- Parameters: new_event_ids, multiplier.
-- Per-event rule: late events are evaluated too (D-04). Skipped when the card has no
-- baseline row or tx_count_30d = 0 (nothing to compare against, D-03). The baseline in
-- sg.card_baseline already covers lookback_days; it is refreshed outside the stream.
SELECT e.card_token,
       e.event_id::text      AS trigger_event_id,
       e.event_time,
       e.status              AS txn_status,
       ARRAY[e.event_id::text] AS event_ids,
       jsonb_build_object(
           'amount', e.amount::text,
           'currency', e.currency,
           'avg_amount_30d', b.avg_amount_30d::text,
           'tx_count_30d', b.tx_count_30d,
           'multiplier', %(multiplier)s::numeric::text,
           'ratio', round(e.amount / b.avg_amount_30d, 2)::text) AS details
  FROM sg.enriched_transactions e
  JOIN sg.card_baseline b ON b.card_token = e.card_token
 WHERE e.event_id = ANY (%(new_event_ids)s::uuid[])
   AND b.tx_count_30d > 0
   AND b.avg_amount_30d > 0
   AND e.amount > %(multiplier)s::numeric * b.avg_amount_30d
 ORDER BY e.event_time, e.event_id;
