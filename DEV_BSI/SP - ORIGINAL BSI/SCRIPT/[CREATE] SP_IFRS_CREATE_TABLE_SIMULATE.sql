CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE (
    P_TABLE_NAME   IN VARCHAR2,
    P_TABLE_COPY   IN VARCHAR2,
    P_DOWNLOAD_DATE IN DATE DEFAULT NULL
)
AUTHID CURRENT_USER
AS
    V_SQL        CLOB;
    V_EXIST      NUMBER;
    V_SEQ_NAME   VARCHAR2(200);
    P_SCHEMA     VARCHAR2(50);
    V_HAS_PKID   NUMBER := 0;
    V_CURRDATE   DATE;
BEGIN
    -- GET CURRENT SCHEMA
    P_SCHEMA := 'PSAK413';

    V_SEQ_NAME := 'SEQ_' || UPPER(P_TABLE_COPY);

    -- =========================
    -- CEK ADA KOLOM PKID ATAU TIDAK
    -- =========================
    SELECT COUNT(*)
    INTO V_HAS_PKID
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = UPPER(P_TABLE_NAME)
    AND OWNER = UPPER(P_SCHEMA)
    AND COLUMN_NAME = 'PKID';

    -- =========================
    -- DROP TABLE JIKA ADA
    -- =========================
    SELECT COUNT(*)
    INTO V_EXIST
    FROM ALL_TABLES
    WHERE TABLE_NAME = UPPER(P_TABLE_COPY)
    AND OWNER = UPPER(P_SCHEMA);

    IF V_EXIST > 0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE ' || P_SCHEMA || '.' || P_TABLE_COPY || ' CASCADE CONSTRAINTS';
    END IF;

    -- =========================
    -- CREATE TABLE (CTAS)
    -- =========================
    IF P_DOWNLOAD_DATE IS NULL THEN
        V_SQL := '
        CREATE TABLE ' || P_SCHEMA || '.' || P_TABLE_COPY || '
        AS SELECT * FROM ' || P_SCHEMA || '.' || P_TABLE_NAME || ' WHERE 0=1';
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_SQL := '
        CREATE TABLE ' || P_SCHEMA || '.' || P_TABLE_COPY || '
        AS SELECT * FROM ' || P_SCHEMA || '.' || P_TABLE_NAME || '
        WHERE DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';
    END IF;

    DBMS_OUTPUT.PUT_LINE(V_SQL);

    EXECUTE IMMEDIATE V_SQL;

    -- =========================
    -- CLONE INDEX DARI TABEL ASAL
    -- =========================
    DECLARE
        V_IDX_SQL   CLOB;
        V_NEW_INDEX VARCHAR2(200);
    BEGIN

        FOR IDX IN (
            SELECT INDEX_NAME,
                   UNIQUENESS
            FROM ALL_INDEXES
            WHERE TABLE_OWNER = UPPER(P_SCHEMA)
              AND TABLE_NAME = UPPER(P_TABLE_NAME)
              AND GENERATED = 'N'
              AND INDEX_TYPE = 'NORMAL'
        )
        LOOP

            V_NEW_INDEX :=
                CASE
                    WHEN LENGTH(IDX.INDEX_NAME) > 20 THEN
                        SUBSTR(IDX.INDEX_NAME,1,20) || '_' ||
                        SUBSTR(P_TABLE_COPY,1,8)
                    ELSE
                        IDX.INDEX_NAME || '_' ||
                        SUBSTR(P_TABLE_COPY,1,8)
                END;

            V_IDX_SQL :=
                'CREATE ' ||
                CASE
                    WHEN IDX.UNIQUENESS = 'UNIQUE'
                    THEN 'UNIQUE '
                    ELSE ''
                END ||
                'INDEX ' || P_SCHEMA || '.' || V_NEW_INDEX ||
                ' ON ' || P_SCHEMA || '.' || P_TABLE_COPY || ' (';

            FOR COL IN (
                SELECT COLUMN_NAME,
                       COLUMN_POSITION
                FROM ALL_IND_COLUMNS
                WHERE INDEX_OWNER = UPPER(P_SCHEMA)
                  AND INDEX_NAME = IDX.INDEX_NAME
                ORDER BY COLUMN_POSITION
            )
            LOOP
                V_IDX_SQL := V_IDX_SQL || COL.COLUMN_NAME || ',';
            END LOOP;

            V_IDX_SQL := RTRIM(V_IDX_SQL, ',') || ')';

            BEGIN
                EXECUTE IMMEDIATE V_IDX_SQL;

                DBMS_OUTPUT.PUT_LINE(
                    'INDEX CREATED : ' || V_NEW_INDEX
                );

            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(
                        'FAILED INDEX : ' ||
                        V_NEW_INDEX || ' - ' || SQLERRM
                    );
            END;

        END LOOP;

    END;

    -- =========================
    -- CLONE PRIMARY KEY
    -- =========================
    DECLARE
        V_PK_SQL     CLOB;
        V_PK_NAME    VARCHAR2(200);
        V_COL_LIST   VARCHAR2(4000);
    BEGIN

        FOR PK IN (
            SELECT CONSTRAINT_NAME
            FROM ALL_CONSTRAINTS
            WHERE OWNER = UPPER(P_SCHEMA)
              AND TABLE_NAME = UPPER(P_TABLE_NAME)
              AND CONSTRAINT_TYPE = 'P'
        )
        LOOP

            V_COL_LIST := NULL;

            SELECT LISTAGG(COLUMN_NAME, ',')
                   WITHIN GROUP (ORDER BY POSITION)
            INTO V_COL_LIST
            FROM ALL_CONS_COLUMNS
            WHERE OWNER = UPPER(P_SCHEMA)
              AND CONSTRAINT_NAME = PK.CONSTRAINT_NAME;

            V_PK_NAME :=
                CASE
                    WHEN LENGTH(PK.CONSTRAINT_NAME) > 20 THEN
                        SUBSTR(PK.CONSTRAINT_NAME,1,20) || '_PK'
                    ELSE
                        PK.CONSTRAINT_NAME || '_PK'
                END;

            V_PK_SQL :=
                   'ALTER TABLE '
                || P_SCHEMA
                || '.'
                || P_TABLE_COPY
                || ' ADD CONSTRAINT '
                || V_PK_NAME
                || ' PRIMARY KEY ('
                || V_COL_LIST
                || ')';

            BEGIN
                EXECUTE IMMEDIATE V_PK_SQL;

                DBMS_OUTPUT.PUT_LINE(
                    'PRIMARY KEY CREATED : ' || V_PK_NAME
                );

            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(
                        'FAILED PRIMARY KEY : '
                        || V_PK_NAME
                        || ' - '
                        || SQLERRM
                    );
            END;

        END LOOP;

    END;

    -- =========================
    -- JIKA ADA PKID → BUAT SEQUENCE
    -- =========================
    IF V_HAS_PKID > 0 THEN

        -- DROP SEQUENCE JIKA ADA
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE ' || P_SCHEMA || '.' || V_SEQ_NAME;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        -- CREATE SEQUENCE
        V_SQL := '
            CREATE SEQUENCE ' || P_SCHEMA || '.' || V_SEQ_NAME || '
            START WITH 1
            INCREMENT BY 1
            NOCACHE
            NOCYCLE
        ';
        EXECUTE IMMEDIATE V_SQL;

        -- SET DEFAULT PKID
        BEGIN
            EXECUTE IMMEDIATE '
                ALTER TABLE ' || P_SCHEMA || '.' || P_TABLE_COPY || '
                MODIFY PKID DEFAULT ' || P_SCHEMA || '.' || V_SEQ_NAME || '.NEXTVAL
            ';
        EXCEPTION
            WHEN OTHERS THEN 
                DBMS_OUTPUT.PUT_LINE('ERROR SET DEFAULT: ' || SQLERRM);
        END;

        -- =========================
        -- SYNC SEQUENCE
        -- =========================
        DECLARE
            V_MAX_ID NUMBER;
        BEGIN
            EXECUTE IMMEDIATE '
                SELECT NVL(MAX(PKID),0) FROM ' || P_SCHEMA || '.' || P_TABLE_COPY
            INTO V_MAX_ID;

            EXECUTE IMMEDIATE '
                ALTER SEQUENCE ' || P_SCHEMA || '.' || V_SEQ_NAME || ' INCREMENT BY ' || (V_MAX_ID + 1);

            EXECUTE IMMEDIATE '
                SELECT ' || P_SCHEMA || '.' || V_SEQ_NAME || '.NEXTVAL FROM DUAL';

            EXECUTE IMMEDIATE '
                ALTER SEQUENCE ' || P_SCHEMA || '.' || V_SEQ_NAME || ' INCREMENT BY 1';
        END;

        DBMS_OUTPUT.PUT_LINE('SEQUENCE CREATED: ' || P_SCHEMA || '.' || V_SEQ_NAME);

    ELSE
        DBMS_OUTPUT.PUT_LINE('PKID NOT FOUND → SKIP SEQUENCE');
    END IF;

    DBMS_OUTPUT.PUT_LINE('SUCCESS CTAS: ' || P_SCHEMA || '.' || P_TABLE_COPY);

END;