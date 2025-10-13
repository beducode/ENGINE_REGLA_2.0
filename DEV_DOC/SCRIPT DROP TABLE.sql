DO $$
DECLARE
    r RECORD;
    v_has_data BOOLEAN;
BEGIN
    FOR r IN
			SELECT DISTINCT c.table_name
			FROM information_schema.columns c
			JOIN information_schema.tables t 
			ON c.table_name = t.table_name
			AND c.table_schema = t.table_schema
			WHERE c.data_type = 'real'
			AND c.table_schema = 'public'
			AND t.table_type = 'BASE TABLE'  -- ✅ hanya table asli, bukan view
			AND c.table_name LIKE '%_s_1%'
    LOOP
			EXECUTE format(
                'DROP TABLE %I ;',r.table_name
            );
            RAISE NOTICE '---> %', r.table_name;
    END LOOP;
END $$;
