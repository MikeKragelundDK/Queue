-- Arkivering (soft delete) af køer + transitionslog for sessionstilstande.
-- archived_at: null = aktiv; sat = arkiveret (og retention-ur for senere sletning).
-- queue_session_state_change: én række pr. indtrådt tilstand — grundlaget for
-- statistik (ventetid, tryk over tid) og audit. created_at = tidspunktet.

ALTER TABLE queue
    ADD COLUMN archived_at DATETIME(6) NULL;

CREATE TABLE queue_session_state_change
(
    id         BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    session_id BIGINT      NOT NULL,
    state      VARCHAR(20) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    INDEX ix_state_change_session (session_id),
    INDEX ix_state_change_state_time (state, created_at),
    CONSTRAINT fk_state_change_session FOREIGN KEY (session_id) REFERENCES queue_session (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;
