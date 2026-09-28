CREATE FUNCTION enqueue_group_message_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO group_message_outbox (message_id, group_id) VALUES (NEW.id, NEW.group_id);
    RETURN NEW;
END;
$$;
