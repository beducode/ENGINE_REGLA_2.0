DECLARE
    v_sql VARCHAR2(4000);
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== MULAI ROLLBACK GRANT DAN SYNONYM ===');

    -- 1️⃣ Cabut role dari REGLAAPPS
    BEGIN
        EXECUTE IMMEDIATE 'REVOKE ROLE_REGLA_ACCESS FROM REGLAAPPS';
        DBMS_OUTPUT.PUT_LINE('Role ROLE_REGLA_ACCESS dicabut dari REGLAAPPS.');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error revoke role dari REGLAAPPS -> ' || SQLERRM);
    END;

    -- 2️⃣ Loop semua schema untuk cabut hak DML & DDL dari ROLE_REGLA_ACCESS
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
        DBMS_OUTPUT.PUT_LINE('Rollback schema: ' || src.schema_name);

        -- 3️⃣ Revoke semua DML (SELECT, INSERT, UPDATE, DELETE) dari tiap tabel
        FOR t IN (
            SELECT table_name
            FROM all_tables
            WHERE owner = src.schema_name
        )
        LOOP
            BEGIN
                v_sql := 'REVOKE SELECT, INSERT, UPDATE, DELETE ON "'||
                         src.schema_name||'"."'||t.table_name||
                         '" FROM ROLE_REGLA_ACCESS';
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Revoke DML error on '||
                        src.schema_name||'.'||t.table_name||' -> '||SQLERRM);
            END;
        END LOOP;

        -- 4️⃣ Drop semua synonym di REGLAAPPS yang terkait schema ini
        FOR s IN (
            SELECT synonym_name
            FROM all_synonyms
            WHERE owner = 'REGLAAPPS'
              AND table_owner = src.schema_name
        )
        LOOP
            BEGIN
                v_sql := 'DROP SYNONYM REGLAAPPS."'||s.synonym_name||'"';
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Drop synonym error: '||s.synonym_name||' -> '||SQLERRM);
            END;
        END LOOP;

        -- 5️⃣ Revoke hak DDL dari ROLE_REGLA_ACCESS (jika pernah diberikan)
        BEGIN
            v_sql := 'REVOKE CREATE TABLE, ALTER ANY TABLE, DROP ANY TABLE FROM ROLE_REGLA_ACCESS';
            EXECUTE IMMEDIATE v_sql;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Revoke DDL error for '||src.schema_name||' -> '||SQLERRM);
        END;
    END LOOP;

    -- 6️⃣ Drop role setelah semua hak dicabut
    BEGIN
        EXECUTE IMMEDIATE 'DROP ROLE ROLE_REGLA_ACCESS';
        DBMS_OUTPUT.PUT_LINE('Role ROLE_REGLA_ACCESS telah dihapus.');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -1919 THEN -- ORA-01919: role does not exist
                DBMS_OUTPUT.PUT_LINE('Role ROLE_REGLA_ACCESS tidak ditemukan, skip.');
            ELSE
                DBMS_OUTPUT.PUT_LINE('Error drop role -> ' || SQLERRM);
            END IF;
    END;

    DBMS_OUTPUT.PUT_LINE('=== ROLLBACK SELESAI ===');
END;
/
