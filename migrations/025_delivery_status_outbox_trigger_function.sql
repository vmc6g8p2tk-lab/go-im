CREATE FUNCTION enqueue_delivery_status_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.delivery_status = 'created' AND NEW.delivery_status = 'delivered' THEN
        INSERT INTO delivery_status_outbox (message_id, sender_id) VALUES (NEW.id, NEW.sender_id);
    END IF;
    RETURN NEW;
END;
$$;
