ALTER TABLE messages ADD COLUMN client_message_id UUID NOT NULL DEFAULT gen_random_uuid();
