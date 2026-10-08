ALTER TABLE queue_session
    ADD COLUMN event_context varchar(255) NOT NULL DEFAULT "",
    DROP INDEX ux_session_active_visitor,
    ADD UNIQUE INDEX ux_session_active_visitor
        (queue_id, event_context, active_visitor_key);