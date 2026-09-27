CREATE FUNCTION enqueue_message_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO message_outbox (message_id, recipient_id) VALUES (NEW.id, NEW.recipient_id);
    RETURN NEW;
END;
$$;
