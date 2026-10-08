-- Luknings-årsag: hvorfor en session nåede CLOSED (COMPLETED/ABANDONED/
-- EXPIRED/ARCHIVED/ADMIN). Ortogonal til tilstanden — grundlaget for
-- gennemførselsrate, frafald vs. ventetid og spildt kapacitet.
-- Sat på transitionen (analytisk kilde) og spejlet på sessionen (opslags-
-- venlighed); begge skrives i samme transaktion.

ALTER TABLE queue_session
    ADD COLUMN close_reason VARCHAR(20) NULL;

ALTER TABLE queue_session_state_change
    ADD COLUMN reason VARCHAR(20) NULL;
