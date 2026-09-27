CREATE UNIQUE INDEX messages_sender_client_id_uq ON messages (sender_id, client_message_id);
