CREATE OR REPLACE PROCEDURE IFRS_OPTIMIZE_DATABASE()
LANGUAGE plpgsql
AS $$
DECLARE 
    r RECORD;
    fk RECORD;
    sql_cmd TEXT;
BEGIN
    RAISE NOTICE '=== Mulai Optimasi Tabel ===';

    -- 1. VACUUM / REINDEX untuk tabel dengan aktivitas tinggi
    FOR r IN
        SELECT 
            schemaname,
            relname AS table_name,
            n_dead_tup,
            seq_scan,
            idx_scan
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 10000 OR seq_scan > idx_scan * 2 AND schemaname = 'public'
    LOOP
        -- VACUUM jika dead tuples tinggi
        IF r.n_dead_tup > 10000 THEN
            sql_cmd := 'VACUUM (ANALYZE) ' || r.schemaname || '.' || r.table_name || ';';
            RAISE NOTICE 'Running: %', sql_cmd;
            EXECUTE sql_cmd;
        END IF;

        -- REINDEX jika seq scan jauh lebih tinggi
        IF r.seq_scan > r.idx_scan * 2 THEN
            sql_cmd := 'REINDEX TABLE ' || r.schemaname || '.' || r.table_name || ';';
            RAISE NOTICE 'Running: %', sql_cmd;
            EXECUTE sql_cmd;
        END IF;
    END LOOP;

    -- 2. Buat index untuk Foreign Key yang belum punya index
    FOR fk IN
        WITH fk_columns AS (
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                a.attname AS column_name,
                c.oid AS table_oid
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN unnest(con.conkey) AS cols(attnum) ON true
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = cols.attnum
            WHERE con.contype = 'f' AND n.nspname = 'public'
        ),
        indexed_columns AS (
            SELECT
                i.indrelid AS table_oid,
                a.attname AS column_name
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE i.indisvalid = true
        )
        SELECT 
            f.schema_name,
            f.table_name,
            f.column_name
        FROM fk_columns f
        LEFT JOIN indexed_columns ic
          ON f.table_oid = ic.table_oid
         AND f.column_name = ic.column_name
        WHERE ic.column_name IS NULL
    LOOP
        sql_cmd := 'CREATE INDEX idx_' || fk.table_name || '_' || fk.column_name ||
                   ' ON ' || fk.schema_name || '.' || fk.table_name || '(' || fk.column_name || ');';
        RAISE NOTICE 'Running: %', sql_cmd;
        EXECUTE sql_cmd;
    END LOOP;

    RAISE NOTICE '=== Optimasi Selesai ===';
END;
$$;
