BEGIN;

-- Guarantee idempotency
TRUNCATE TABLE monument.unit;

-- Insert logic
INSERT INTO monument.unit (unit_id, unit_name, unit_description)
SELECT