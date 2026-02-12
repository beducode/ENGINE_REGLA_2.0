CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_UPDATE_STAGE_BCA (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_UPDATE_STAGE_BCA';
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
    V_CONSTNAME     VARCHAR2(100);

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
    V_CONSTNAME := 'STAGE';

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_MASTER_ACCOUNT_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_MASTER_ACCOUNT';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
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

    V_STR_QUERY := 'BEGIN IFRS9_BCA.SP_IFRS_RULE_DATA_BCA(:1, :2, :3, :4); END;';

    EXECUTE IMMEDIATE V_STR_QUERY
    USING V_RUNID, V_CURRDATE, V_SYSCODE, V_PRC;
    COMMIT;
    ----------------------------------------------------------------

    BEGIN
        EXECUTE IMMEDIATE
            'SELECT DISTINCT SICR_RULE_ID
            FROM IFRS_ECL_MODEL_CONFIG
            WHERE ECL_MODEL_ID = :1'
        INTO V_RULE_ID
        USING V_SYSCODE;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            V_RULE_ID := 0;
    END;

    V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
                    SET CR_STAGE = NULL
                    WHERE DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'')';
    
    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
        USING (  SELECT DOWNLOAD_DATE,
                        CUSTOMER_NUMBER,
                        MAX (RULE_TYPE) AS CR_STAGE
                   FROM ' || V_OWNER || '.GTMP_IFRS_SCENARIO_DATA
                  WHERE RULE_ID = :1
                        AND (ACCOUNT_STATUS = ''A''
                             OR (    DATA_SOURCE = ''CRD''
                                 AND ACCOUNT_STATUS = ''C''
                                 AND OUTSTANDING > 0))
               --        AND GROUP_SEGMENT != ''BANK_BTRD''
               GROUP BY DOWNLOAD_DATE, CUSTOMER_NUMBER) B
           ON (A.DOWNLOAD_DATE = TO_DATE(:2,''YYYY-MM-DD'')
               AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
    WHEN MATCHED
    THEN
      UPDATE SET A.CR_STAGE = B.CR_STAGE';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_RULE_ID, V_CURRDATE;
    COMMIT;

    V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    SET CR_STAGE = ''1''
    WHERE DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'') AND (GROUP_SEGMENT = ''BANK_BTRD'')';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
    COMMIT;

    V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    SET CR_STAGE = ''1''
    WHERE     DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'')
            AND DATA_SOURCE = ''LIMIT''
            AND CR_STAGE IS NULL';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
    COMMIT;
    

    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
    USING (SELECT DISTINCT CUSTOMER_NUMBER
                FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || '
            WHERE (GROUP_SEGMENT IN
                        (''COMMERCIAL'',
                        ''COMMERCIAL BG'',
                        ''CORPORATE'',
                        ''CORPORATE BG'')
                    AND DATA_SOURCE = ''ILS''
                    AND BI_COLLECTABILITY = ''1''
                    AND RESERVED_FLAG_4 = 1)
                    AND DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'')
                    AND CUSTOMER_NUMBER NOT IN
                            (SELECT CUSTOMER_NUMBER FROM ' || V_OWNER || '.TBLU_WORSTCASE_LIST)) B
        ON (A.DOWNLOAD_DATE = TO_DATE(:2,''YYYY-MM-DD'')
            AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER
            AND A.GROUP_SEGMENT IN
                    (''COMMERCIAL'',
                    ''COMMERCIAL BG'',
                    ''CORPORATE'',
                    ''CORPORATE BG'')
            AND A.DATA_SOURCE = ''ILS''
            AND A.BI_COLLECTABILITY = ''1''
            AND A.RESERVED_FLAG_4 = 1)
    WHEN MATCHED
    THEN
      UPDATE SET CR_STAGE = 1';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE, V_CURRDATE;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
        USING ' || V_OWNER || '.TBLU_WORSTCASE_LIST B
           ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
               AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
    WHEN MATCHED
    THEN
        UPDATE SET CR_STAGE = 2
                WHERE A.DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'') AND A.CR_STAGE = 1';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
    COMMIT;


    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
        USING (SELECT DISTINCT A.CUSTOMER_NUMBER, B.STAGE_OVERRIDE
                 FROM    ' || V_OWNER || '.IFRS_STAGE_OVERRIDE_H A
                      JOIN
                         ' || V_OWNER || '.IFRS_STAGE_OVERRIDE_D B
                      ON A.PKID = B.MASTERID AND A.IGNORE_OVERRIDE = 0) B
           ON (A.DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'')
               AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
    WHEN MATCHED
    THEN
        UPDATE SET CR_STAGE = B.STAGE_OVERRIDE';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
    COMMIT;


    V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
    SET CR_STAGE = NULL
    WHERE DOWNLOAD_DATE = TO_DATE(:1,''YYYY-MM-DD'')
          AND TRIM (NVL (PRODUCT_CODE, ''-'')) NOT IN
                 (SELECT PRD_CODE FROM ' || V_OWNER || '.IFRS_MASTER_PRODUCT_PARAM)';

    EXECUTE IMMEDIATE V_STR_QUERY USING V_CURRDATE;
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