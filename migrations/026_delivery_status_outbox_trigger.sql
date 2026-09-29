CREATE TRIGGER delivery_status_outbox_after_update AFTER UPDATE OF delivery_status ON messages
FOR EACH ROW EXECUTE FUNCTION enqueue_delivery_status_outbox();
