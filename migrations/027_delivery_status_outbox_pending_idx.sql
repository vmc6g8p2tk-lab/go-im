CREATE INDEX delivery_status_outbox_pending_idx ON delivery_status_outbox (created_at, message_id)
WHERE published_at IS NULL;
