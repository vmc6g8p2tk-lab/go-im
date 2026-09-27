ALTER TABLE messages ADD COLUMN delivery_status TEXT NOT NULL DEFAULT 'created' CHECK (delivery_status IN ('created', 'delivered', 'read'));
