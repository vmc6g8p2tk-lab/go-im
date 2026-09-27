ALTER TABLE messages ADD COLUMN next_delivery_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '5 seconds');
