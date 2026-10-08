-- Retention-natjobbet: lås-række id 3 serialiserer det på tværs af pods
-- (samme mønster som aktivering id 1 og expiry id 2), og indekset bærer
-- regel B's sletteforespørgsel (CLOSED-sessioner ældre end perioden).

INSERT INTO queue_activation_lock (id) VALUES (3);

CREATE INDEX ix_session_state_closed ON queue_session (state, closed_at);
