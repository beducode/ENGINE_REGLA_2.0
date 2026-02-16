CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_IA_TO_CA_BCA (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_IA_TO_CA_BCA';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLEINSERT3  VARCHAR2(100);
    V_TABLEINSERT4  VARCHAR2(100);
    V_TABLEINSERT5  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLESELECT2  VARCHAR2(100);
    V_TABLESELECT3  VARCHAR2(100);
    V_TABLESELECT4  VARCHAR2(100);
    V_TABLECONFIG   VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_RULE_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);
    V_PD_RULES_QRY_RESULT CLOB;


    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID         VARCHAR2(30);
    V_SYSCODE       VARCHAR2(10);
    V_PRC           VARCHAR2(5);
    V_CONSTNAME     VARCHAR2(100) := 'THRESHOLD';

    -- RESULT QUERY
    V_QUERYS        CLOB;
    


BEGIN

    ----------------------------------------------------------------
    -- GET OWNER
    ----------------------------------------------------------------
    SELECT USERNAME INTO V_OWNER FROM USER_USERS;

    ----------------------------------------------------------------
    -- INSERT VCURRDATE DETERMINATION IF NULL
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE_AMORT;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE_AMORT HAS NO CURRDATE ROW');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_RUNID := NVL(P_RUNID, 'P_00000_0000');
    V_SYSCODE := NVL(P_SYSCODE, '0');
    V_MODEL_ID := V_SYSCODE;
    V_PRC := NVL(P_PRC, 'P');

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_IA_OVERRIDEH_HIST_' || V_RUNID;
        V_TABLEINSERT2 := 'TBLT_PAYMENTEXPECTED_' || V_RUNID;
        V_TABLEINSERT3 := 'TBLT_PAYMENTEXPECTEDH_' || V_RUNID;
        V_TABLEINSERT4 := 'IFRS_IA_OVERRIDED_' || V_RUNID;
        V_TABLEINSERT5 := 'IFRS_IA_OVERRIDEH_' || V_RUNID;
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_GEN_QUERY_' || V_RUNID;
        V_TABLESELECT2 := 'GTMP_IFRS_MASTER_ACCOUNT_' || V_RUNID;
        V_TABLESELECT3 := 'IFRS_MASTER_ACCOUNT_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_IA_OVERRIDEH_HIST';
        V_TABLEINSERT2 := 'TBLT_PAYMENTEXPECTED';
        V_TABLEINSERT3 := 'TBLT_PAYMENTEXPECTEDH';
        V_TABLEINSERT4 := 'IFRS_IA_OVERRIDED';
        V_TABLEINSERT5 := 'IFRS_IA_OVERRIDEH';
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_GEN_QUERY';
        V_TABLESELECT2 := 'GTMP_IFRS_MASTER_ACCOUNT';
        V_TABLESELECT3 := 'IFRS_MASTER_ACCOUNT';
    END IF;

    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    ----------------------------------------------------------------
    -- PRE-PROCESSING SIMULATION TABLES
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN
        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_IA_OVERRIDEH_HIST WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TBLT_PAYMENTEXPECTED WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT3);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT3;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT3 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TBLT_PAYMENTEXPECTEDH WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
        COMMIT;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT4);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT4;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT4 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_IA_OVERRIDED WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;


        -- DROP TABLE IF EXISTS
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT5);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT5;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT5 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_IA_OVERRIDEH WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;


    ----------------------------------------------------------------
    -- EXECUTE DATA PROCEDURE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN IFRS9_BCA.SP_IFRS_GENERATE_RULE_BCA(:1, :2, :3, :4, :5); END;';

    EXECUTE IMMEDIATE V_STR_QUERY
    USING V_RUNID, V_CURRDATE, V_SYSCODE, V_PRC, V_CONSTNAME;
    COMMIT;
    
    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------

    V_STR_QUERY := 'SELECT PD_RULES_QRY_RESULT FROM ' || V_OWNER || '.' || V_TABLESELECT1;

    BEGIN
        EXECUTE IMMEDIATE V_STR_QUERY 
        INTO V_PD_RULES_QRY_RESULT;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_PD_RULES_QRY_RESULT := '1=0'; -- DEFAULT TO ALL IF NO RULES RETURNED
        END;
    COMMIT;

    V_STR_QUERY := 'TRUNCATE TABLE ' || V_OWNER || '.' || V_TABLESELECT2;
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT; 

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT2 || '
                (
                    PKID,
                    DOWNLOAD_DATE,
                    MASTERID,
                    MASTER_ACCOUNT_CODE,
                    CUSTOMER_NUMBER,
                    ACCOUNT_NUMBER,
                    IMPAIRED_FLAG
                )
                SELECT DISTINCT A.PKID,
                    A.DOWNLOAD_DATE,
                    A.MASTERID,
                    '' '' MASTER_ACCOUNT_CODE,
                    A.CUSTOMER_NUMBER,
                    A.ACCOUNT_NUMBER,
                    ''C'' AS IMPAIRED_FLAG
                FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
                JOIN ' || V_OWNER || '.' || V_TABLEINSERT5 || ' B
                ON A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                AND B.EFFECTIVE_DATE <= A.DOWNLOAD_DATE
                AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLESELECT2 || '
                WHERE CUSTOMER_NUMBER IN
                (SELECT CUSTOMER_NUMBER FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
                 WHERE A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                 AND A.OUTSTANDING > 0
                 AND A.ACCOUNT_STATUS = ''A''
                 AND (' || V_PD_RULES_QRY_RESULT || '))';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
    USING
    (
        SELECT DISTINCT A2.PKID FROM ' || V_OWNER || '.' || V_TABLEINSERT5 || ' A2
        JOIN ' || V_OWNER || '.' || V_TABLESELECT2 || ' B2
        ON A2.CUSTOMER_NUMBER = B2.CUSTOMER_NUMBER
        AND A2.EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    ) B
    ON (A.PKID = B.PKID)
    WHEN MATCHED THEN
    UPDATE SET
        A.RESERVED_VARCHAR_1 = ''C'',
        A.RESERVED_DATE_1 = SYSDATE,
        A.RESERVED_DATE_2 = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || '
    WHERE OVERRIDEID IN
    (
        SELECT DISTINCT A2.PKID FROM ' || V_OWNER || '.' || V_TABLEINSERT5 || ' A2
        JOIN ' || V_OWNER || '.' || V_TABLESELECT2 || ' B2
        ON A2.CUSTOMER_NUMBER = B2.CUSTOMER_NUMBER
        AND A2.EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    )';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
    WHERE OVERRIDEID IN
    (
        SELECT DISTINCT A2.PKID FROM ' || V_OWNER || '.' || V_TABLEINSERT5 || ' A2
        JOIN ' || V_OWNER || '.' || V_TABLESELECT2 || ' B2
        ON A2.CUSTOMER_NUMBER = B2.CUSTOMER_NUMBER
        AND A2.EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    )';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT4 || '
    WHERE OVERRIDEID IN
    (
        SELECT DISTINCT A2.PKID FROM ' || V_OWNER || '.' || V_TABLEINSERT5 || ' A2
        JOIN ' || V_OWNER || '.' || V_TABLESELECT2 || ' B2
        ON A2.CUSTOMER_NUMBER = B2.CUSTOMER_NUMBER
        AND A2.EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    )';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT5 || ' A
    WHERE CUSTOMER_NUMBER IN
    (SELECT CUSTOMER_NUMBER FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ' B2
     WHERE A.CUSTOMER_NUMBER = B2.CUSTOMER_NUMBER)
    AND EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------

    
    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9_BCA.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1 ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (' || CASE WHEN V_MODEL_ID = '0' THEN '1=1' ELSE 'PD_RULE_ID = ' || V_MODEL_ID END || ')';

    IFRS9_BCA.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        IF C_RULE%ISOPEN THEN
            CLOSE C_RULE;
        END IF;

        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;