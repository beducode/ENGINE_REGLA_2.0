DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    -- 1️⃣ Buat role jika belum ada
    BEGIN
        EXECUTE IMMEDIATE 'CREATE ROLE ROLE_REGLA_ACCESS';
        DBMS_OUTPUT.PUT_LINE('Role ROLE_REGLA_ACCESS berhasil dibuat.');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -1921 THEN -- ORA-01921: role name already exists
                DBMS_OUTPUT.PUT_LINE('Role ROLE_REGLA_ACCESS sudah ada, lanjut...');
            ELSE
                DBMS_OUTPUT.PUT_LINE('Error membuat role -> ' || SQLERRM);
            END IF;
    END;

    -- 2️⃣ Loop semua schema target
    FOR src IN (
        SELECT 'NTT_AUDIT' AS schema_name FROM dual UNION ALL
        SELECT 'NTT_CLASSIFICATION_MEASUREMENT' FROM dual UNION ALL
        SELECT 'NTT_CUSTOM_REPORT' FROM dual UNION ALL
        SELECT 'NTT_DATA_MANAGENENT' FROM dual UNION ALL
        SELECT 'NTT_EMAIL_NOTIFICATION' FROM dual UNION ALL
        SELECT 'NTT_FILE_MANAGER' FROM dual UNION ALL
        SELECT 'NTT_HANGFIRE' FROM dual UNION ALL
        SELECT 'NTT_IMPAIRMENT' FROM dual UNION ALL
        SELECT 'NTT_JOURNAL' FROM dual UNION ALL
        SELECT 'NTT_PARAMETER' FROM dual UNION ALL
        SELECT 'NTT_PLATFORM_SETTING' FROM dual UNION ALL
        SELECT 'NTT_RISK_MODELLING' FROM dual UNION ALL
        SELECT 'NTT_USER' FROM dual UNION ALL
        SELECT 'NTT_WORKFLOW' FROM dual
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE('Processing schema: ' || src.schema_name);

        -- 3️⃣ Grant DML (SELECT, INSERT, UPDATE, DELETE) dari setiap table ke ROLE
        FOR t IN (
            SELECT table_name
            FROM all_tables
            WHERE owner = src.schema_name
        )
        LOOP
            BEGIN
                v_sql := 'GRANT SELECT, INSERT, UPDATE, DELETE ON "'||
                         src.schema_name||'"."'||t.table_name||
                         '" TO ROLE_REGLA_ACCESS';
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Grant DML error on '||
                        src.schema_name||'.'||t.table_name||' -> '||SQLERRM);
            END;
        END LOOP;

        -- 4️⃣ Buat synonym di REGLAAPPS agar bisa akses langsung
        FOR t IN (
            SELECT table_name
            FROM all_tables
            WHERE owner = src.schema_name
        )
        LOOP
            BEGIN
                v_sql := 'CREATE OR REPLACE SYNONYM REGLAAPPS."'||
                         t.table_name||'" FOR "'||
                         src.schema_name||'"."'||t.table_name||'"';
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Synonym error on '||
                        src.schema_name||'.'||t.table_name||' -> '||SQLERRM);
            END;
        END LOOP;

        -- 5️⃣ Grant hak DDL (CREATE, ALTER, DROP) per schema ke ROLE
        BEGIN
            v_sql := 'GRANT CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE TO ROLE_REGLA_ACCESS';
            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Grant DDL error for schema '||src.schema_name||' -> '||SQLERRM);
        END;
    END LOOP;

    -- 6️⃣ Assign role ke REGLAAPPS
    BEGIN
        EXECUTE IMMEDIATE 'GRANT ROLE_REGLA_ACCESS TO REGLAAPPS';
        DBMS_OUTPUT.PUT_LINE('Role ROLE_REGLA_ACCESS diberikan ke REGLAAPPS.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error saat grant role ke REGLAAPPS -> ' || SQLERRM);
    END;

    DBMS_OUTPUT.PUT_LINE('=== Semua grant & synonym selesai dengan role ROLE_REGLA_ACCESS ===');
END;
/
