CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_INSERT_DCF_BCA (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S',
    P_UPLOADID      IN NUMBER DEFAULT 0
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_INSERT_DCF_BCA';
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
    V_UPLOADID      NUMBER(18);

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
        V_TABLESELECT1 := 'TBLU_DCF_INFO_' || V_RUNID;
        V_TABLESELECT2 := 'IFRS_IA_OVERRIDEH_' || V_RUNID;
        V_TABLESELECT3 := 'TBLU_DCF_DETAIL_' || V_RUNID;
        V_TABLESELECT4 := 'IFRS_IA_OVERRIDED_' || V_RUNID;
    ELSE 
        V_TABLESELECT1 := 'TBLU_DCF_INFO';
        V_TABLESELECT2 := 'IFRS_IA_OVERRIDEH';
        V_TABLESELECT3 := 'TBLU_DCF_DETAIL';
        V_TABLESELECT4 := 'IFRS_IA_OVERRIDED';
    END IF;

    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;

    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || '
    (
        OVERRIDEID,
        FILENAME,
        UPLOAD_STATUS,
        CUSTOMER_NUMBER,
        UPLOADBY,
        UPLOADDATE,
        UPLOADHOST,
        APPROVEDBY,
        APPROVEDDATE,
        APPROVEDHOST
    )
    SELECT DISTINCT B.PKID OVERRIDEID,
        C.FILENAME,
        C.STATUS UPLOAD_STATUS,
        B.CUSTOMER_NUMBER,
        A.UPLOADBY,
        A.UPLOADDATE,
        A.UPLOADHOST,
        A.APPROVEDBY,
        A.APPROVEDDATE,
        A.APPROVEDHOST
    FROM ' || V_OWNER || '.TBLU_DCF_BULK A
    JOIN ' || V_OWNER || '.' || V_TABLESELECT2 || ' B
    ON A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER
    AND A.EFFECTIVE_DATE = B.EFFECTIVE_DATE
    AND A.UPLOADID = ' || V_UPLOADID || '
    JOIN ' || V_OWNER || '.TBLT_UPLOAD_POOL C
    ON A.UPLOADID = C.PKID';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT3 || '
    (
        UPLOADID,
        OVERRIDEID,
        ROW_NUMBER,
        CUSTOMER_NUMBER,
        ACCOUNT_NUMBER,
        MASTERID,
        ESTIMATED_REALIZE_DATE,
        ESTIMATED_CF_PERCENT,
        DISCOUNT_RATE_TRS,
        DISCOUNT_RATE_TRF,
        CREATEDBY,
        CREATEDDATE,
        CREATEDHOST,
        UPDATEDBY,
        UPDATEDDATE,
        UPDATEDHOST
    )
    SELECT DISTINCT ' || V_UPLOADID || ' AS UPLOADID,
        B.OVERRIDEID,
        A.ROW_NUMBER,
        A.CUSTOMER_NUMBER,
        B.ACCOUNT_NUMBER,
        B.MASTERID,
        A.EXPECTED_PERIOD,
        A.EXPECTED_CF_PERCENT,
        A.DISCOUNT_RATE_TRS,
        A.DISCOUNT_RATE_TRF,
        B.CREATEDBY,
        B.CREATEDDATE,
        B.CREATEDHOST,
        B.UPDATEDBY,
        B.UPDATEDDATE,
        B.UPDATEDHOST
    FROM
    (
        SELECT ROW_NUMBER() OVER(ORDER BY A2.CUSTOMER_NUMBER, A2.EXPECTED_PERIOD) ROW_NUMBER, A2.*
        FROM ' || V_OWNER || '.TBLU_DCF_BULK A2
        WHERE A2.UPLOADID = ' || V_UPLOADID || '
    ) A
    JOIN ' || V_OWNER || '.' || V_TABLESELECT4 || ' B
    ON A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER
    JOIN ' || V_OWNER || '.' || V_TABLESELECT2 || ' C
    ON A.EFFECTIVE_DATE = C.EFFECTIVE_DATE
    AND B.OVERRIDEID = C.PKID
    ORDER BY A.ROW_NUMBER, B.ACCOUNT_NUMBER';

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