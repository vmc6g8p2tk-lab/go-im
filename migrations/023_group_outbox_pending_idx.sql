CREATE INDEX group_message_outbox_pending_idx ON group_message_outbox (created_at, message_id)
WHERE published_at IS NULL;
