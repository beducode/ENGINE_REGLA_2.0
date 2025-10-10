CREATE OR REPLACE FUNCTION F_OPTIMIZE_DATABASE_WITH_LOG()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_db_size TEXT;
    v_status TEXT := 'SUCCESS';
    v_message TEXT := '';
BEGIN
    RAISE NOTICE '--- Database Optimization Started at % ---', v_start_time;

    BEGIN
        -- 1. Reload konfigurasi
        PERFORM pg_reload_conf();
        RAISE NOTICE 'Config reloaded.';

        -- 2. Reset statistik
        PERFORM pg_stat_reset();
        RAISE NOTICE 'Statistics reset.';

        -- 3. Optimasi per tabel
        FOR r IN 
            SELECT schemaname, relname
            FROM pg_stat_user_tables
            ORDER BY schemaname, relname
        LOOP
            RAISE NOTICE 'Optimizing %.% ...', r.schemaname, r.relname;
            EXECUTE format('VACUUM (ANALYZE, VERBOSE) %I.%I;', r.schemaname, r.relname);
            EXECUTE format('REINDEX TABLE %I.%I;', r.schemaname, r.relname);
        END LOOP;

        -- 4. Optimasi database global
        EXECUTE format('REINDEX DATABASE %I;', current_database());
        EXECUTE 'ANALYZE VERBOSE;';
        v_db_size := pg_size_pretty(pg_database_size(current_database()));

        RAISE NOTICE 'Database size after optimization: %', v_db_size;

    EXCEPTION WHEN OTHERS THEN
        v_status := 'FAILED';
        v_message := SQLERRM;
        RAISE NOTICE 'Optimization failed: %', SQLERRM;
    END;

    -- 5. Catat waktu selesai dan simpan log
    v_end_time := clock_timestamp();

    INSERT INTO system_log_maintenance (
        db_name, start_time, end_time, duration_seconds, db_size, status, message
    )
    VALUES (
        current_database(),
        v_start_time,
        v_end_time,
        ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 2),
        v_db_size,
        v_status,
        v_message
    );

    RAISE NOTICE '--- Optimization completed at % (Duration: % seconds, Status: %) ---', 
        v_end_time, EXTRACT(EPOCH FROM (v_end_time - v_start_time)), v_status;

END;
$$;
