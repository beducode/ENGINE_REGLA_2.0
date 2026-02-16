CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_UNPROCESSED_IA_BCA (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_UNPROCESSED_IA_BCA';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
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
        V_TABLEINSERT1 := 'IFRS_ECL_RESULT_DETAIL_CALC_' || V_RUNID;
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_GEN_QUERY_' || V_RUNID;
        V_TABLESELECT2 := 'GTMP_IFRS_MASTER_ACCOUNT_' || V_RUNID;
        V_TABLESELECT3 := 'IFRS_MASTER_ACCOUNT_' || V_RUNID;
        V_TABLESELECT4 := 'IFRS_ECL_RESULT_DETAIL_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ECL_RESULT_DETAIL_CALC';
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_GEN_QUERY';
        V_TABLESELECT2 := 'GTMP_IFRS_MASTER_ACCOUNT';
        V_TABLESELECT3 := 'IFRS_MASTER_ACCOUNT';
        V_TABLESELECT4 := 'IFRS_ECL_RESULT_DETAIL';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_ECL_RESULT_DETAIL_CALC WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
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
                (PKID,
                DOWNLOAD_DATE,
                MASTERID,
                MASTER_ACCOUNT_CODE,
                CUSTOMER_NUMBER,
                ACCOUNT_NUMBER,
                FAIR_VALUE_AMOUNT
                )
                SELECT A.PKID,
                    A.DOWNLOAD_DATE,
                    A.MASTERID,
                    '' '' MASTER_ACCOUNT_CODE,
                    A.CUSTOMER_NUMBER,
                    A.ACCOUNT_NUMBER,
                    A.FAIR_VALUE_AMOUNT
                FROM ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
                WHERE A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                AND A.ACCOUNT_STATUS = ''A''
                AND A.CUSTOMER_NUMBER NOT IN (SELECT CUSTOMER_NUMBER FROM IFRS_IA_OVERRIDEH)
                AND A.OUTSTANDING > 0
                AND (' || V_PD_RULES_QRY_RESULT || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND ECL_MODEL_ID = ''' || V_MODEL_ID || '''
    AND MASTERID IN
    (SELECT MASTERID FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ')
    AND COUNTER_PAYSCHD > 1';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
    USING ' || V_OWNER || '.' || V_TABLESELECT2 || ' B
    ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND A.ECL_MODEL_ID = ''' || V_MODEL_ID || '''
    AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        A.PD_RATE = 1,
        A.LGD_RATE = 1,
        A.DISCOUNT_RATE = 1,
        A.ECL_AMOUNT = B.FAIR_VALUE_AMOUNT';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT4 || ' A
    USING
    (SELECT A2.MASTERID, B2.ECL_AMOUNT, A2.IMPAIRED_FLAG
        FROM ' || V_OWNER || '.' || V_TABLESELECT2 || ' A2
        JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B2
        ON B2.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        AND B2.ECL_MODEL_ID = ''' || V_MODEL_ID || '''
        AND A2.MASTERID = B2.MASTERID
    ) B
    ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND A.ECL_MODEL_ID = ''' || V_MODEL_ID || '''
    AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        A.ECL_AMOUNT = B.ECL_AMOUNT,
        A.IMPAIRED_FLAG = ''I'',
        A.SPECIAL_REASON = ''UNPROCESSED INDIVIDUAL''';

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