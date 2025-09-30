-- Buat tabel penampung DDL
DROP TABLE IF EXISTS DDL_GENERATOR_OUTPUT;
CREATE TABLE DDL_GENERATOR_OUTPUT (
    seq serial primary key,
    ddl text,
    category text
);

-- Function untuk generate seluruh DDL schema public
CREATE OR REPLACE FUNCTION GENERATE_PUBLIC_SCHEMA_DDL()
RETURNS void AS
$$
DECLARE
    rec record;
BEGIN
    -- Bersihkan hasil lama
    DELETE FROM DDL_GENERATOR_OUTPUT;

    -----------------------------------------------------------------
    -- 1. SEQUENCE
    -----------------------------------------------------------------
    FOR rec IN
        SELECT 'CREATE SEQUENCE ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || ';' AS ddl
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname = 'public'
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'SEQUENCE');
    END LOOP;

    -----------------------------------------------------------------
    -- 2. TABLE
    -----------------------------------------------------------------
    FOR rec IN
        SELECT 'CREATE TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(c.relname) || E' (\n' ||
               string_agg('    ' || quote_ident(a.attname) || ' ' ||
                          pg_catalog.format_type(a.atttypid, a.atttypmod) ||
                          CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END, E',\n')
               || E'\n);\n' AS ddl
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE c.relkind = 'r'
          AND n.nspname = 'public'
          AND a.attnum > 0
          AND NOT a.attisdropped
        GROUP BY n.nspname, c.relname
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'TABLE');
    END LOOP;

    -----------------------------------------------------------------
    -- 3. VIEW
    -----------------------------------------------------------------
    FOR rec IN
        SELECT pg_get_viewdef(c.oid, true) AS ddl,
               n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('v','m')  -- view biasa & materialized view
          AND n.nspname = 'public'
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category)
        VALUES ('CREATE VIEW ' || quote_ident(rec.nspname) || '.' || quote_ident(rec.relname) || ' AS ' || rec.ddl || ';', 'VIEW');
    END LOOP;

    -----------------------------------------------------------------
    -- 4. FUNCTION
    -----------------------------------------------------------------
    FOR rec IN
        SELECT pg_get_functiondef(p.oid) AS ddl
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prokind = 'f'   -- function
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'FUNCTION');
    END LOOP;

    -----------------------------------------------------------------
    -- 5. PROCEDURE
    -----------------------------------------------------------------
    FOR rec IN
        SELECT pg_get_functiondef(p.oid) AS ddl
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.prokind = 'p'   -- procedure
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'PROCEDURE');
    END LOOP;

    -----------------------------------------------------------------
    -- 6. CONSTRAINT
    -----------------------------------------------------------------
    FOR rec IN
        SELECT 'ALTER TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(rel.relname) ||
               ' ADD ' || pg_get_constraintdef(con.oid) || ';' AS ddl
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = rel.relnamespace
        WHERE n.nspname = 'public'
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'CONSTRAINT');
    END LOOP;

    -----------------------------------------------------------------
    -- 7. INDEX
    -----------------------------------------------------------------
    FOR rec IN
        SELECT indexdef || ';' AS ddl
        FROM pg_indexes
        WHERE schemaname = 'public'
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'INDEX');
    END LOOP;

    -----------------------------------------------------------------
    -- 8. TRIGGER
    -----------------------------------------------------------------
    FOR rec IN
        SELECT pg_get_triggerdef(t.oid) || ';' AS ddl
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND NOT t.tgisinternal
    LOOP
        INSERT INTO DDL_GENERATOR_OUTPUT(ddl, category) VALUES (rec.ddl, 'TRIGGER');
    END LOOP;

END;
$$ LANGUAGE plpgsql;
