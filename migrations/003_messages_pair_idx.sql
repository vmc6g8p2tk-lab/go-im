CREATE INDEX messages_pair_id_idx ON messages (sender_id, recipient_id, id DESC);
