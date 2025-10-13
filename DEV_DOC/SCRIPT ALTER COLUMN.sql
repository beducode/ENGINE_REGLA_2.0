DO $$
DECLARE
    r RECORD;
    v_has_data BOOLEAN;
BEGIN
    FOR r IN
        SELECT c.table_schema, c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t 
          ON c.table_name = t.table_name
         AND c.table_schema = t.table_schema
        WHERE c.data_type = 'real'
          AND c.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'  -- ✅ hanya table asli, bukan view
    LOOP
        -- Cek apakah tabel memiliki data
        -- EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I)', r.table_schema, r.table_name)
        -- INTO v_has_data;

        -- Jika tabel kosong → lakukan ALTER
        -- IF NOT v_has_data THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ALTER COLUMN %I TYPE double precision;',
                r.table_schema, r.table_name, r.column_name
            );
            RAISE NOTICE 'ALTERED: %.% (column %) → double precision',
                r.table_schema, r.table_name, r.column_name;
        -- ELSE
        --     RAISE NOTICE 'SKIPPED: %.% (column %) [Table has data]',
        --         r.table_schema, r.table_name, r.column_name;
        -- END IF;
    END LOOP;
END $$;
