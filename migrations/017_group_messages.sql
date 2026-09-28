CREATE TABLE group_messages (
    id BIGSERIAL PRIMARY KEY,
    group_id BIGINT NOT NULL REFERENCES chat_groups(id) ON DELETE CASCADE,
    sender_id BIGINT NOT NULL REFERENCES users(id),
    client_message_id UUID NOT NULL DEFAULT gen_random_uuid(),
    body TEXT NOT NULL CHECK (octet_length(body) <= 4000 AND length(btrim(body)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
