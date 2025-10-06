DROP FUNCTION IF EXISTS GENERATE_ORDERED_DDL;

CREATE OR REPLACE FUNCTION GENERATE_ORDERED_DDL(schema_name text DEFAULT 'public')
RETURNS SETOF text
LANGUAGE plpgsql
AS $$
DECLARE
    r record;
    ddl text;
BEGIN
    ----------------------------------------------------------------
    -- 1. Sequence
    ----------------------------------------------------------------
    FOR r IN
        SELECT c.oid::regclass::text AS seq
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname = schema_name
        ORDER BY c.relname
    LOOP
        ddl := 'CREATE SEQUENCE ' || r.seq || ';';
        RETURN NEXT ddl;
    END LOOP;

    ----------------------------------------------------------------
    -- 2. Table + Constraints
    ----------------------------------------------------------------
    FOR r IN
        SELECT c.oid::regclass::text AS tbl
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname = schema_name
        ORDER BY c.relname
    LOOP
        ddl := 'CREATE TABLE ' || r.tbl || E'\n(' || E'\n' ||
               (
                 SELECT string_agg(
                          '    ' || a.attname || ' ' ||
                          pg_catalog.format_type(a.atttypid, a.atttypmod) ||
                          CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END,
                          ',' || E'\n'
                        )
                 FROM pg_attribute a
                 WHERE a.attrelid = r.tbl::regclass
                   AND a.attnum > 0
                   AND NOT a.attisdropped
               )
               || E'\n);';
        RETURN NEXT ddl;

        -- Constraints
        FOR ddl IN
            SELECT 'ALTER TABLE ' || r.tbl || ' ADD CONSTRAINT ' || con.conname || ' ' ||
                   pg_get_constraintdef(con.oid, true) || ';'
            FROM pg_constraint con
            WHERE con.conrelid = r.tbl::regclass
        LOOP
            RETURN NEXT ddl;
        END LOOP;
    END LOOP;

    ----------------------------------------------------------------
    -- 3. Index (non PK/unique)
    ----------------------------------------------------------------
    FOR r IN
        SELECT c.oid::regclass::text AS idx
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relkind = 'i'
          AND n.nspname = schema_name
          AND NOT i.indisprimary
          AND NOT i.indisunique
        ORDER BY c.relname
    LOOP
        ddl := pg_get_indexdef(r.idx::regclass) || ';';
        RETURN NEXT ddl;
    END LOOP;

    ----------------------------------------------------------------
    -- 4. View
    ----------------------------------------------------------------
    FOR r IN
        WITH RECURSIVE dep_view AS (
            SELECT c.oid, c.oid::regclass::text AS vw, 0 AS lvl
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'v'
              AND n.nspname = schema_name
            UNION
            SELECT c2.oid, c2.oid::regclass::text, dv.lvl+1
            FROM dep_view dv
            JOIN pg_depend d ON d.refobjid = dv.oid
            JOIN pg_rewrite rw ON rw.oid = d.objid
            JOIN pg_class c2 ON rw.ev_class = c2.oid
        )
        SELECT DISTINCT vw
        FROM dep_view
    LOOP
        ddl := 'CREATE OR REPLACE VIEW ' || r.vw || ' AS ' ||
               pg_get_viewdef(r.vw::regclass, true) || ';';
        RETURN NEXT ddl;
    END LOOP;

    ----------------------------------------------------------------
    -- 5. Function
    ----------------------------------------------------------------
    FOR r IN
        SELECT p.oid::regprocedure::text AS func
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = schema_name
          AND p.prokind = 'f'
        ORDER BY p.proname
    LOOP
        ddl := pg_get_functiondef(r.func::regprocedure);
        RETURN NEXT ddl;
    END LOOP;

    ----------------------------------------------------------------
    -- 6. Stored Procedure
    ----------------------------------------------------------------
    FOR r IN
        SELECT p.oid::regprocedure::text AS proc
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = schema_name
          AND p.prokind = 'p'
        ORDER BY p.proname
    LOOP
        ddl := pg_get_functiondef(r.proc::regprocedure);
        RETURN NEXT ddl;
    END LOOP;

    RETURN;
END;
$$;
