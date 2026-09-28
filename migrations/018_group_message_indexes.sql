CREATE UNIQUE INDEX group_messages_sender_client_id_idx ON group_messages (sender_id, client_message_id);
