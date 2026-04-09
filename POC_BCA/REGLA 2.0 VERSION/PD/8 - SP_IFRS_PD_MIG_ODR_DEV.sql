CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_MIG_ODR_DEV (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_MIG_ODR_DEV';
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
    V_TABLEPDCONFIG VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_DETAIL_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);


    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);
    V_SYSCODE      VARCHAR2(10);
    V_PRC         VARCHAR2(5);

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
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
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
        V_TABLEINSERT1 := 'IFRS_PD_MIG_ODR_' || V_RUNID;
        V_TABLESELECT1 := 'TMP_IFRS_PD_RUNNING_DATE_' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_PD_MIG_ODR';
        V_TABLESELECT1 := 'TMP_IFRS_PD_RUNNING_DATE';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_PD_MIG_ODR';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- UNLOCK & CLEAN TARGET TABLE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLESELECT1 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLESELECT1;

    ----------------------------------------------------------------
    -- BUILD DYNAMIC CURSOR QUERY
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || '
    (
        EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        BUCKET_GROUP,
        MIG_LOSS_BUCKET
    )
    SELECT
        TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AS EFF_DATE,
		LAST_DAY(ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''), A.INCREMENT_PERIOD * -1)) AS BASE_DATE,
	    A.PKID AS PD_RULE_ID,
		A.BUCKET_GROUP,
		A.MIG_LOSS_BUCKET
    FROM ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' A
    WHERE NVL(A.ACTIVE_FLAG,0) = 1
	AND IS_DELETED = 0
	AND PD_METHOD =''MIG''
	AND A.DERIVED_PD_MODEL IS NULL';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    

    V_STR_QUERY := 'DELETE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
    AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    (
        PKID,
        EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        ODR,
        CREATEDBY,
        CREATEDDATE,
        CREATEDHOST
    )
    SELECT
        SEQ_IFRS_PD_MIG_ODR.NEXTVAL AS PKID,
        A.EFF_DATE,
        A.BASE_DATE,
        A.PD_RULE_ID,
        CASE 
            WHEN B.SUM_CALC_AMOUNT = 0 THEN 0
            ELSE A.SUM_CALC_AMOUNT / B.SUM_CALC_AMOUNT
        END AS ODR,
        A.CREATEDBY,
        A.CREATEDDATE,
        A.CREATEDHOST
    FROM
    (
        SELECT
            A2.EFF_DATE,
            A2.BASE_DATE,
            A2.PD_RULE_ID,
            SUM(A2.CALC_AMOUNT) AS SUM_CALC_AMOUNT,
            ''SYSTEM'' AS CREATEDBY,
            SYSDATE AS CREATEDDATE,
            ''SYSTEM'' AS CREATEDHOST
        FROM ' || V_OWNER || '.IFRS_PD_MIG_ENR A2
        JOIN ' || V_OWNER || '.' || V_TABLESELECT1 || ' B2
            ON A2.PD_RULE_ID = B2.PD_RULE_ID
        AND A2.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET C2
            ON A2.BUCKET_GROUP = C2.BUCKET_GROUP
        AND A2.BUCKET_TO = C2.MAX_BUCKET_ID
        GROUP BY
            A2.EFF_DATE,
            A2.BASE_DATE,
            A2.PD_RULE_ID
    ) A
    JOIN
    (
        SELECT
            A2.EFF_DATE,
            A2.BASE_DATE,
            A2.PD_RULE_ID,
            SUM(A2.CALC_AMOUNT) AS SUM_CALC_AMOUNT
        FROM ' || V_OWNER || '.IFRS_PD_MIG_ENR A2
        JOIN ' || V_OWNER || '.' || V_TABLESELECT1 || ' B2
            ON A2.PD_RULE_ID = B2.PD_RULE_ID
        AND A2.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        GROUP BY
            A2.EFF_DATE,
            A2.BASE_DATE,
            A2.PD_RULE_ID
    ) B
        ON A.EFF_DATE   = B.EFF_DATE
    AND A.BASE_DATE  = B.BASE_DATE
    AND A.PD_RULE_ID = B.PD_RULE_ID';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    (
        PKID,
        EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        ODR
    )
    SELECT
        SEQ_IFRS_PD_MIG_ODR.NEXTVAL AS PKID,
        X.EFF_DATE,
        X.BASE_DATE,
        X.PD_RULE_ID,
        X.ODR
    FROM
    (
        SELECT DISTINCT
            A.EFF_DATE,
            A.BASE_DATE,
            A.PD_RULE_ID,
            0 AS ODR
        FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
        WHERE A.PD_RULE_ID NOT IN
        (
            SELECT B.PD_RULE_ID
            FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
            WHERE B.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
        )
    ) X';
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

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
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

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