CREATE OR REPLACE FUNCTION DUPLICATE_TABLE(
    source_table text,
    target_table text,
    copy_data boolean DEFAULT true
) RETURNS void AS
$$
DECLARE
    sql_create text;
    sql_insert text;
    seq record;
    trg record;
BEGIN
    -- Buat struktur tabel lengkap (index, constraint, default, storage)
    sql_create := format(
        'CREATE TABLE %I (LIKE %I INCLUDING ALL);',
        target_table, source_table
    );
    EXECUTE sql_create;

    -- Copy data jika diinginkan
    IF copy_data THEN
        sql_insert := format(
            'INSERT INTO %I SELECT * FROM %I;',
            target_table, source_table
        );
        EXECUTE sql_insert;
    END IF;

    -- Duplikasi sequence (untuk serial/identity columns)
    FOR seq IN
        SELECT c.oid::regclass::text AS seqname,
               n.nspname AS schemaname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = (source_table::regclass)::oid
                            AND a.attnum > 0
                            AND NOT a.attisdropped
		WHERE c.relkind = 'S' AND pg_get_serial_sequence(source_table, a.attname) = c.oid::regclass::text
    LOOP
        EXECUTE format(
            'CREATE SEQUENCE %I.%I OWNED BY %I;',
            seq.schemaname,
            target_table || '_' || split_part(seq.seqname, '_', 2),
            target_table
        );
    END LOOP;

    -- Duplikasi trigger
    FOR trg IN
        SELECT tgname, pg_get_triggerdef(t.oid) AS definition
        FROM pg_trigger t
        WHERE tgrelid = source_table::regclass
          AND NOT t.tgisinternal
    LOOP
        EXECUTE replace(trg.definition, source_table, target_table);
    END LOOP;

    RAISE NOTICE 'Tabel % sudah diduplikasi ke % (copy_data=%), termasuk index, constraint, sequence, trigger.',
        source_table, target_table, copy_data;
END;
$$ LANGUAGE plpgsql;



/*
-- Contoh penggunaan:

-- Duplikasi tabel lengkap dengan struktur, data, index, constraint, sequence, trigger
SELECT duplicate_table('old_table', 'new_table');

-- Jika hanya struktur + index + sequence + trigger tanpa data
SELECT duplicate_table('old_table', 'new_table2', false);

*/
