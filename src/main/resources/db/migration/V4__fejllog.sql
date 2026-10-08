-- Persistent fejllog bag admin-GUI'ets Logs-fane: WARN/ERROR fra alle pods.
-- Skrives batch-vist af ErrorLogFlusher (staging i in-memory ErrorLogBuffer);
-- created_at er logtidspunktet, ikke flush-tidspunktet. Rækker ældre end
-- retention-perioden (queue.error-log.retention-days) slettes af samme job,
-- så tabellen aldrig vokser uendeligt.

CREATE TABLE error_log
(
    id         BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    level      VARCHAR(20)   NOT NULL,
    logger     VARCHAR(255)  NOT NULL,
    message    VARCHAR(4000) NOT NULL,
    pod        VARCHAR(255)  NOT NULL,
    created_at DATETIME(6)   NOT NULL,
    updated_at DATETIME(6)   NOT NULL,
    INDEX ix_error_log_time (created_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;
