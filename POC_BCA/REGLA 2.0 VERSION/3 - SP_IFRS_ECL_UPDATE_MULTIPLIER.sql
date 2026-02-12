CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_UPDATE_MULTIPLIER_BCA (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_UPDATE_MULTIPLIER_BCA';
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
    V_RULE_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);
    V_PD_RULES_QRY_RESULT CLOB;


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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;


    V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || ' IMA
    SET IMA.RESERVED_RATE_8 = 1
    WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
         USING (SELECT A2.RULE_ID, A2.MULTIPLIER
                  FROM ' || V_OWNER || '.IFRS_ECL_MULTIPLIER A2
                       JOIN (  SELECT RULE_ID,
                                      MAX (EFFECTIVE_DATE) MAX_EFFECTIVE_DATE
                                 FROM ' || V_OWNER || '.IFRS_ECL_MULTIPLIER
                                WHERE EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                             GROUP BY RULE_ID) B2
                          ON A2.RULE_ID = B2.RULE_ID
                             AND A2.EFFECTIVE_DATE = B2.MAX_EFFECTIVE_DATE
                       JOIN (  SELECT RULE_ID,
                                      EFFECTIVE_DATE,
                                      MAX (CREATEDDATE) MAX_CREATEDDATE
                                 FROM ' || V_OWNER || '.IFRS_ECL_MULTIPLIER
                                WHERE EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                             GROUP BY RULE_ID, EFFECTIVE_DATE) C2
                          ON     A2.RULE_ID = C2.RULE_ID
                             AND A2.EFFECTIVE_DATE = C2.EFFECTIVE_DATE
                             AND A2.CREATEDDATE = C2.MAX_CREATEDDATE) B
            ON (B.RULE_ID = A.SEGMENT_RULE_ID
                AND A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))
     WHEN MATCHED
     THEN
       UPDATE SET A.RESERVED_RATE_8 = B.MULTIPLIER';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

    
     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
         USING (SELECT DISTINCT
                       SUBSTR (SWIFT_CODE, 1, 8) SWIFT_CODE,
                       NVL (MULTIPLIER, 1) MULTIPLIER
                  FROM ' || V_OWNER || '.TBLU_RATING_BANK
                 WHERE DOWNLOAD_DATE =
                          (SELECT MAX (DOWNLOAD_DATE)
                             FROM ' || V_OWNER || '.TBLU_RATING_BANK
                            WHERE DOWNLOAD_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))) B
            ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_1, 1, 8)
                AND A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))
    WHEN MATCHED
    THEN
       UPDATE SET A.RESERVED_RATE_8 = B.MULTIPLIER
               WHERE ( (A.DATA_SOURCE = ''KTP''
                        AND PRODUCT_CODE LIKE ''PLACEMENT%'')
                      OR (A.DATA_SOURCE = ''RKN''))';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
        USING (SELECT DISTINCT
                      SUBSTR (SWIFT_CODE, 1, 8) SWIFT_CODE,
                      NVL (MULTIPLIER, 1) MULTIPLIER
                 FROM ' || V_OWNER || '.TBLU_RATING_BANK
                WHERE DOWNLOAD_DATE =
                         (SELECT MAX (DOWNLOAD_DATE)
                            FROM ' || V_OWNER || '.TBLU_RATING_BANK
                           WHERE DOWNLOAD_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))) B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_15, 1, 8)
               AND A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''))
   WHEN MATCHED
   THEN
      UPDATE SET
         A.RESERVED_RATE_8 = B.MULTIPLIER
              WHERE (DATA_SOURCE = ''BTRD''
                     AND RESERVED_VARCHAR_23 IN (''2'', ''3''))
                    OR (    DATA_SOURCE = ''BTRD''
                        AND RESERVED_VARCHAR_23 IN (''4'', ''5'')
                        AND RESERVED_FLAG_10 = 1)
                    OR DATA_SOURCE IN (''ILS'', ''LIMIT'')';
    
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A
         USING (SELECT *
                  FROM ' || V_OWNER || '.IFRS_MDL_NONGOV
                 WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')) B
            ON (    A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                AND A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                AND A.RESERVED_VARCHAR_28 = B.FUND_CODE)
     WHEN MATCHED
     THEN
         UPDATE SET A.RESERVED_RATE_7 = B.NONGOVRATE * 100';

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