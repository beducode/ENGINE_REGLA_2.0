CREATE OR REPLACE PROCEDURE SP_IFRS_CREATE_TABLE_SIMULATE (
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
    SELECT USERNAME INTO P_SCHEMA FROM USER_USERS;

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
        AS SELECT * FROM ' || P_SCHEMA || '.' || P_TABLE_NAME;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_SQL := '
        CREATE TABLE ' || P_SCHEMA || '.' || P_TABLE_COPY || '
        AS SELECT * FROM ' || P_SCHEMA || '.' || P_TABLE_NAME || '
        WHERE DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';
    END IF;

    EXECUTE IMMEDIATE V_SQL;

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