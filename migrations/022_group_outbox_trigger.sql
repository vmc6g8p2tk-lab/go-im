CREATE TRIGGER group_message_outbox_after_insert AFTER INSERT ON group_messages
FOR EACH ROW EXECUTE FUNCTION enqueue_group_message_outbox();
