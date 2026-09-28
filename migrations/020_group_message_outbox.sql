CREATE TABLE group_message_outbox (
    message_id BIGINT PRIMARY KEY REFERENCES group_messages(id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES chat_groups(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
