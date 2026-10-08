-- Basisskema: det hierarkiske kø-træ (composite) og brugernes kø-sessioner.
-- Kolonnetyper matcher Hibernate-mappingen (validate kører i alle miljøer):
-- Instant → DATETIME(6), UUID → CHAR(36), enums → VARCHAR(20), boolean → BIT(1).

CREATE TABLE queue
(
    id                 BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    uuid               CHAR(36)     NOT NULL,
    name               VARCHAR(255) NOT NULL,
    level              VARCHAR(20)  NOT NULL,
    external_reference VARCHAR(255) NULL,
    max_capacity       INT          NULL,
    parent_id          BIGINT       NULL,
    dirty              BIT(1)       NOT NULL DEFAULT b'0',
    created_at         DATETIME(6)  NOT NULL,
    updated_at         DATETIME(6)  NOT NULL,
    CONSTRAINT ix_queue_uuid UNIQUE (uuid),
    INDEX ix_queue_parent (parent_id),
    CONSTRAINT fk_queue_parent FOREIGN KEY (parent_id) REFERENCES queue (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE queue_session
(
    id              BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    uuid            CHAR(36)    NOT NULL,
    queue_id        BIGINT      NOT NULL,
    state           VARCHAR(20) NOT NULL,
    sequence_number BIGINT      NOT NULL,
    created_at      DATETIME(6) NOT NULL,
    updated_at      DATETIME(6) NOT NULL,
    last_ping_at    DATETIME(6) NULL,
    opened_at       DATETIME(6) NULL,
    expires_at      DATETIME(6) NULL,
    closed_at       DATETIME(6) NULL,
    CONSTRAINT ix_session_uuid UNIQUE (uuid),
    -- FIFO-/tilstandsopslag: dækker også FK'en til queue (queue_id er venstrekolonne).
    INDEX ix_session_queue_state_seq (queue_id, state, sequence_number),
    CONSTRAINT fk_session_queue FOREIGN KEY (queue_id) REFERENCES queue (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;
