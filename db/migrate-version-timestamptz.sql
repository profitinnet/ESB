-- Existing volumes do not re-run init.sql. Convert the version column once.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'delivery_state'
      AND column_name = 'version'
      AND data_type = 'text'
  ) THEN
    ALTER TABLE delivery_state
      ALTER COLUMN version TYPE timestamptz
      USING version::timestamptz;
  END IF;
END $$;
