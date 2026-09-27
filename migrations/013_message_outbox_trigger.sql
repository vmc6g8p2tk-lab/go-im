CREATE TRIGGER message_outbox_after_insert AFTER INSERT ON messages
FOR EACH ROW EXECUTE FUNCTION enqueue_message_outbox();
