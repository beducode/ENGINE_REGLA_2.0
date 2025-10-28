---- DROP PROCEDURE SP_IFRS_ARCHIVING_PROCESS;

CREATE OR REPLACE PROCEDURE SP_IFRS_ARCHIVING_PROCESS(
    IN P_RUNID VARCHAR(20) DEFAULT 'A_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL
)
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- ARCHIVING PARAM   
    V_RETENTION_ID VARCHAR;
    V_SERVERHOST VARCHAR;
    V_SERVERLINK VARCHAR;
    V_DBNAME VARCHAR;
    V_DBUSER VARCHAR;
    V_DBUSERDEST VARCHAR;
    V_DBPASS VARCHAR;
    V_DBPORT VARCHAR;
    V_STR_QUERY TEXT;
    COLUMN_RECORD RECORD;

    ----- PARAMTER GET PARAM
    V_CONNECTION_ID BIGINT;
    V_TABLESOURCE VARCHAR;
    V_TABLEDEST VARCHAR;
    V_SEQUENCE_PARAM BIGINT;
    V_TABLE_CONDITION TEXT;
    V_TABLE_CONDITION_RESULT TEXT;
    V_CONNECTION_KEY TEXT;
    
    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;

    --- VARIABLE
    V_CURRDATE DATE;
    V_SP_NAME VARCHAR(100);
    V_QUERYS TEXT;
    V_CHECKEXISTS INT;
    V_GETRECORD INT;
    V_GETPARAM INT;
    V_COLUMNDEST VARCHAR(100);
    V_OPERATION VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
BEGIN 
    -------- ====== VARIABLE ======
	GET DIAGNOSTICS STACK = PG_CONTEXT;
	FCESIG := substring(STACK from 'function (.*?) line');
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'A_00000_0000';
    END IF;

    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        EXECUTE 'SELECT * FROM dblink(''link_ifrs9'', ''SELECT CURRDATE FROM IFRS_PRC_DATE'') AS TABLE_PRC_DATE(CURRDATE DATE)' INTO V_CURRDATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    V_STR_QUERY := '';
    V_STR_QUERY := 'SELECT * FROM dblink(''link_db_ifrs9_access'', ''SELECT COUNT(*) FROM VW_DATA_RETENTION_POLICY'') AS ROW(RESULT BIGINT)';
    EXECUTE (V_STR_QUERY) INTO V_GETPARAM;
    
    IF V_GETPARAM > 0 THEN 
        ------ ====== BODY ======
        ----- START GET PARAM DATA FROM PARAMETER TABLE -----
        FOR V_RETENTION_ID, V_CONNECTION_ID, V_TABLESOURCE, V_TABLEDEST, V_SEQUENCE_PARAM, V_TABLE_CONDITION_RESULT, V_TABLE_CONDITION, V_CONNECTION_KEY IN
        EXECUTE 'SELECT * FROM dblink(''link_db_ifrs9_access'', ''SELECT retention_id, connection_id, table_source, table_destination, sequence, table_condition_result, table_condition, connection_key FROM VW_DATA_RETENTION_POLICY'') 
        AS IFRS_RETENTION_MASTER_GET (RETENTION_ID VARCHAR(50) ,CONNECTION_ID BIGINT, TABLE_SOURCE VARCHAR(100), TABLE_DESTINATION VARCHAR(100), SEQUENCE SMALLINT, TABLE_CONDITION_RESULT TEXT, TABLE_CONDITION TEXT, CONNECTION_KEY TEXT);'
        ----- END GET PARAM DATA FROM PARAMETER TABLE -----
        LOOP
            ----- START GET CONNECTION DETAIL -----
            EXECUTE 'SELECT pgp_sym_decrypt(dbhost, ''' || V_CONNECTION_KEY || '''), pgp_sym_decrypt(dbname, ''' || V_CONNECTION_KEY || '''), pgp_sym_decrypt(dbuser, ''' || V_CONNECTION_KEY || '''), pgp_sym_decrypt(dbpass, ''' || V_CONNECTION_KEY || '''), dbport, current_user FROM regla_db_connection WHERE dbconn_pkid = ' || V_CONNECTION_ID || ' ' 
            INTO V_SERVERHOST, V_DBNAME, V_DBUSER, V_DBPASS, V_DBPORT, V_DBUSERDEST;
            V_SERVERLINK := 'link_' || lower(V_DBNAME);
            ----- END GET CONNECTION DETAIL -----

            ----- START DROP SERVER DBLINK IF EXISTS -----
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'DROP SERVER IF EXISTS ' || V_SERVERLINK || ' CASCADE;';
            ----
            EXECUTE (V_STR_QUERY);
            ----- END DROP SERVER DBLINK IF EXISTS -----

            ----- START CREATE SERVER DBLINK -----
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE SERVER ' || V_SERVERLINK || '
            FOREIGN DATA WRAPPER dblink_fdw
            OPTIONS (host ''' || V_SERVERHOST || ''', dbname ''' || V_DBNAME || ''', port ''' || V_DBPORT || ''');';
            ----
            EXECUTE (V_STR_QUERY);
            ----- END CREATE SERVER DBLINK -----

            ----- START GRANT USAGE ON FOREIGN SERVER -----
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'GRANT USAGE ON FOREIGN SERVER ' || V_SERVERLINK || ' TO ' || V_DBUSERDEST || ';';
            ----
            EXECUTE (V_STR_QUERY);
            ----- END GRANT USAGE ON FOREIGN SERVER -----

            ----- START CREATE USER MAPPING -----
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'CREATE USER MAPPING
            FOR ' || V_DBUSERDEST || '
            SERVER ' || V_SERVERLINK || '
            OPTIONS (user ''' || V_DBUSER || ''', password ''' || V_DBPASS || ''');';
            ----
            EXECUTE (V_STR_QUERY);
            ----- END CREATE USER MAPPING -----

            ----- START CONNECT DBLINK -----
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'SELECT dblink_connect(''' || 'conn_' || V_SERVERLINK || ''', ''' || V_SERVERLINK || ''');';
            ----
            EXECUTE (V_STR_QUERY);
            ----- END CONNECT DBLINK -----

            ------------------ START CHECK TABLE DESTINATION EXISTS ------------------
            EXECUTE 'SELECT COUNT(*) FROM IFRS_MASTER_ACCOUNT_ACV WHERE ' || V_TABLE_CONDITION_RESULT || '' INTO V_CHECKEXISTS;
            ------------------ END CHECK TABLE DESTINATION EXISTS ------------------

            V_STR_QUERY := '';
            V_STR_QUERY := 'SELECT * FROM dblink(''' || V_SERVERLINK || ''',';
            V_STR_QUERY := V_STR_QUERY || '''SELECT COUNT(*) FROM ' || V_TABLESOURCE || ' WHERE ';
            V_STR_QUERY := V_STR_QUERY || '' || V_TABLE_CONDITION || ''') AS ROW(RESULT BIGINT); ';
            EXECUTE (V_STR_QUERY) INTO V_GETRECORD;

            IF V_CHECKEXISTS = 0 THEN
                IF V_GETRECORD > 0 THEN
                    ------------------ START GET DATA FROM SOURCE TABLE ------------------
                    V_STR_QUERY := '';
                    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEDEST || ' ';
                    V_STR_QUERY := V_STR_QUERY || 'SELECT * FROM dblink(''' || V_SERVERLINK || ''',';
                    V_STR_QUERY := V_STR_QUERY || '''SELECT * FROM ' || V_TABLESOURCE || ' WHERE ' ;
                    V_STR_QUERY := V_STR_QUERY || '' || V_TABLE_CONDITION || ''') ';
                    V_STR_QUERY := V_STR_QUERY || 'as table_source(';
                    FOR COLUMN_RECORD IN 
                    SELECT a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
                    a.attnum as attnum, e.max_attnum as max_attnum FROM pg_catalog.pg_attribute a
                    INNER JOIN 
                    (SELECT c.oid, n.nspname, c.relname
                    FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname ~ ('^('|| V_TABLEDEST ||')$')
                    AND pg_catalog.pg_table_is_visible(c.oid)
                    ORDER BY 2, 3) b
                    ON a.attrelid = b.oid
                    INNER JOIN 
                    (SELECT a.attrelid,
                    MAX(a.attnum) as max_attnum
                    FROM pg_catalog.pg_attribute a
                    WHERE a.attnum > 0 
                    AND NOT a.attisdropped
                    GROUP BY a.attrelid) e
                    ON a.attrelid=e.attrelid
                    WHERE a.attnum > 0 
                    AND NOT a.attisdropped
                    ORDER BY a.attnum
                    LOOP
                        IF column_record.attnum < column_record.max_attnum THEN
                            V_STR_QUERY := V_STR_QUERY || '' || COLUMN_RECORD.COLUMN_NAME || ' ' || COLUMN_RECORD.COLUMN_TYPE || '';
                            V_STR_QUERY := V_STR_QUERY || ',';
                        ELSE
                            V_STR_QUERY := V_STR_QUERY || '' || COLUMN_RECORD.COLUMN_NAME || ' ' || COLUMN_RECORD.COLUMN_TYPE || '';
                        END IF;
                    END LOOP;
                    V_STR_QUERY := V_STR_QUERY || ')';
                    -- RAISE NOTICE '%', V_STR_QUERY;
                    EXECUTE (V_STR_QUERY);
                END IF;
            END IF;
            ------------------ END GET DATA FROM SOURCE TABLE ------------------

            ----- START GET RECORD AFFECTED -----
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
            ----- END GET RECORD AFFECTED -----

            ---------------- START DELETE FROM SOURCE ------------------
            V_STR_QUERY := '';
            V_STR_QUERY := 'SELECT * FROM dblink(''' || V_SERVERLINK || ''',';
            V_STR_QUERY := V_STR_QUERY || '''DELETE FROM ' || V_TABLESOURCE || ' WHERE ';
            V_STR_QUERY := V_STR_QUERY || '' || V_TABLE_CONDITION || ''') AS ROW(RESULT TEXT); ';
            ----
            EXECUTE (V_STR_QUERY);
            ---------------- END DELETE FROM SOURCE ------------------

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_ARCHIVING_LOGS (RETENTION_ID, TABLE_SOURCE, TABLE_DEST, SEQUENCE_ID, AFFECTED_RECORD, OPERATION_TYPE) ';
            V_STR_QUERY := V_STR_QUERY || 'VALUES (''' || V_RETENTION_ID || ''', ''' || V_TABLESOURCE || ''', ''' || V_TABLEDEST || ''', ' || V_SEQUENCE_PARAM || ', ' || V_RETURNROWS2 || ', ''ARCHIVING'');';
            ----
            EXECUTE (V_STR_QUERY);

            ----- START DISCONNECT DBLINK -----
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'SELECT dblink_disconnect(''' || 'conn_' || V_SERVERLINK || ''');';
            ----
            EXECUTE (V_STR_QUERY);
            ----- END DISCONNECT DBLINK -----

            V_COLUMNDEST = '-';
            V_OPERATION = 'ARCHIVING';
            
            -- -------- ====== LOG ======
            CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
            -- -------- ====== LOG ======

        END LOOP;

        -------- ====== RESULT ======
        V_QUERYS = 'SELECT * FROM ' || V_TABLEDEST || ' WHERE ' || V_TABLE_CONDITION_RESULT || '';
        CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, V_RETURNROWS2, P_RUNID);
        -------- ====== RESULT ======

        RAISE NOTICE 'SP_IFRS_ARCHIVING_PROCESS | SUCCESSFULY';
        -------- ====== BODY ======
    ELSE
        RAISE NOTICE 'SP_IFRS_ARCHIVING_PROCESS | NO PARAMETER FOUND';
    END IF;

END;

$$;