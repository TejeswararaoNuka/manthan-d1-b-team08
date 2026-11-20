-- 20 synthetic blacklisted merchants for the MERCHANT_BLACKLIST rule.
-- Re-runnable: existing rows (and any reason edited at runtime) are kept.
-- The producer can draw from these IDs to trigger the rule on purpose.

INSERT INTO sg.merchant_blacklist (merchant_id, reason) VALUES
    ('mer_bad00001', 'Chargeback ratio above network limit'),
    ('mer_bad00002', 'Card-testing pattern reported'),
    ('mer_bad00003', 'Confirmed account-takeover cash-out'),
    ('mer_bad00004', 'Shell merchant, no physical presence'),
    ('mer_bad00005', 'Network fraud advisory'),
    ('mer_bad00006', 'Chargeback ratio above network limit'),
    ('mer_bad00007', 'Card-testing pattern reported'),
    ('mer_bad00008', 'Confirmed account-takeover cash-out'),
    ('mer_bad00009', 'Shell merchant, no physical presence'),
    ('mer_bad00010', 'Network fraud advisory'),
    ('mer_bad00011', 'Chargeback ratio above network limit'),
    ('mer_bad00012', 'Card-testing pattern reported'),
    ('mer_bad00013', 'Confirmed account-takeover cash-out'),
    ('mer_bad00014', 'Shell merchant, no physical presence'),
    ('mer_bad00015', 'Network fraud advisory'),
    ('mer_bad00016', 'Chargeback ratio above network limit'),
    ('mer_bad00017', 'Card-testing pattern reported'),
    ('mer_bad00018', 'Confirmed account-takeover cash-out'),
    ('mer_bad00019', 'Shell merchant, no physical presence'),
    ('mer_bad00020', 'Network fraud advisory')
ON CONFLICT (merchant_id) DO NOTHING;
