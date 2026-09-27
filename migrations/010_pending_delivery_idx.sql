CREATE INDEX messages_pending_delivery_idx ON messages (recipient_id, next_delivery_at, id) WHERE delivery_status = 'created';
