BEGIN
    FOR rec IN (
        SELECT 
            table_name,
            column_name,
            'ALTER TABLE ' || table_name ||
            ' RENAME COLUMN ' || column_name ||
            ' TO ' || REPLACE(column_name, 'ECL', 'EIL') AS alter_sql
        FROM user_tab_columns
        WHERE UPPER(column_name) LIKE 'ECL%'
    )
    LOOP
        BEGIN
            DBMS_OUTPUT.PUT_LINE('Executing: ' || rec.alter_sql);
            EXECUTE IMMEDIATE rec.alter_sql;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error on ' || rec.table_name || '.' || rec.column_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/
