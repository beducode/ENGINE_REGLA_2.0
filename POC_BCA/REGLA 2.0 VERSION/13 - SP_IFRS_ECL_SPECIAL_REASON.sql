CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_SPECIAL_REASON_BCA (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_SPECIAL_REASON_BCA';
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
    V_DATE_CKPN365  DATE;

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
    V_DATE_CKPN365 := '31-DEC-2024';

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    IF V_PRC = 'S' THEN
        V_TABLEINSERT1 := 'TMP_IMA_CKPN365_' || V_RUNID;
        V_TABLEINSERT2 := 'TBLU_CKPN365_PARAM_' || V_RUNID;
        V_TABLEINSERT3 := 'TMP_CKPN_365_100_' || V_RUNID;
        V_TABLESELECT1 := 'GTMP_IFRS_MASTER_ACCOUNT_' || V_RUNID;
        V_TABLESELECT2 := 'IFRS_ECL_RESULT_DETAIL_' || V_RUNID;
        V_TABLESELECT3 := 'IFRS_ECL_RESULT_DETAIL_CALC_' || V_RUNID;
        V_TABLESELECT4 := 'IFRS_MASTER_ACCOUNT_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'TMP_IMA_CKPN365';
        V_TABLEINSERT2 := 'TBLU_CKPN365_PARAM';
        V_TABLEINSERT3 := 'TMP_CKPN_365_100';
        V_TABLESELECT1 := 'GTMP_IFRS_MASTER_ACCOUNT';
        V_TABLESELECT2 := 'IFRS_ECL_RESULT_DETAIL';
        V_TABLESELECT3 := 'IFRS_ECL_RESULT_DETAIL_CALC';
        V_TABLESELECT4 := 'IFRS_MASTER_ACCOUNT';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IMA_CKPN365 WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TBLU_CKPN365_PARAM WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_CKPN_365_100 WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------
    V_STR_QUERY := 'TRUNCATE TABLE ' || V_OWNER || '.' || V_TABLESELECT1;
    EXECUTE IMMEDIATE V_STR_QUERY;

    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLESELECT1 || '
    (PKID,
     DOWNLOAD_DATE,
     MASTERID,
     MASTER_ACCOUNT_CODE,
     CUSTOMER_NUMBER,
     ACCOUNT_NUMBER,
     CR_STAGE,
     OUTSTANDING,
     RESERVED_VARCHAR_1)
    SELECT A.PKID,
           A.DOWNLOAD_DATE,
           A.MASTERID,
           '' '' AS MASTER_ACCOUNT_CODE,
           A.CUSTOMER_NUMBER,
           A.ACCOUNT_NUMBER,
           A.CR_STAGE,
           A.OUTSTANDING,
           CASE
               WHEN NVL(BTB_FLAG, 0) = 1 THEN ''BACK-T0-BACK, NO IMPAIRMENT''
               WHEN NVL(A.RESERVED_FLAG_6, 0) = 1 THEN ''CKPN 100%''
               END AS SPECIAL_REASON
    FROM ' || V_OWNER || '.' || V_TABLESELECT4 || ' A
    WHERE A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
      AND (A.ACCOUNT_STATUS = ''A'' OR (A.DATA_SOURCE = ''CRD'' AND A.ACCOUNT_STATUS = ''C'' AND A.OUTSTANDING > 0))
      AND A.DATA_SOURCE IN (''ILS'', ''BTRD'', ''KTP'', ''CRD'', ''LIMIT'')
      AND 1 = (
        CASE
            WHEN NVL(BTB_FLAG, 0) = 1 OR NVL(A.RESERVED_FLAG_6, 0) = 1 THEN 1
            ELSE 0
            END
        )';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

     V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLESELECT3 || '
     WHERE DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
       AND ECL_MODEL_ID = ' || V_MODEL_ID || '
       AND MASTERID IN
           (SELECT MASTERID FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ')
       AND COUNTER_PAYSCHD > 1';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
     USING ' || V_OWNER || '.' || V_TABLESELECT1 || ' B
     ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
         AND A.ECL_MODEL_ID = ' || V_MODEL_ID || '
         AND A.MASTERID = B.MASTERID)
     WHEN MATCHED THEN
         UPDATE
         SET A.PD_RATE       = 0,
             A.LGD_RATE      = 1,
             A.DISCOUNT_RATE = 1,
             A.ECL_AMOUNT    = 0
         WHERE B.RESERVED_VARCHAR_1 = ''BACK-T0-BACK, NO IMPAIRMENT''';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
     USING ' || V_OWNER || '.' || V_TABLESELECT1 || ' B
     ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
         AND A.ECL_MODEL_ID = ' || V_MODEL_ID || '
         AND A.MASTERID = B.MASTERID)
     WHEN MATCHED THEN
         UPDATE
         SET A.PD_RATE = 1,
             A.LGD_RATE = 1,
             A.DISCOUNT_RATE = 1,
             A.ECL_AMOUNT = A.EAD_AMOUNT
         WHERE B.RESERVED_VARCHAR_1 = ''CKPN 100%''';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT2 || ' A
     USING
         (SELECT A2.MASTERID,
                 B2.ECL_AMOUNT,
                 A2.RESERVED_VARCHAR_1 AS SPECIAL_REASON
          FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A2
                   JOIN ' || V_OWNER || '.' || V_TABLESELECT3 || ' B2
                        ON B2.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                            AND B2.ECL_MODEL_ID = ' || V_MODEL_ID || '
                            AND A2.MASTERID = B2.MASTERID) B
     ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
         AND A.ECL_MODEL_ID = ' || V_MODEL_ID || '
         AND A.MASTERID = B.MASTERID)
     WHEN MATCHED THEN
         UPDATE
         SET A.ECL_AMOUNT = B.ECL_AMOUNT,
             A.SPECIAL_REASON = B.SPECIAL_REASON';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

     V_STR_QUERY := 'TRUNCATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT1;
     EXECUTE IMMEDIATE V_STR_QUERY;

     V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
     WITH IMA AS (SELECT A.PKID,
     A.DOWNLOAD_DATE,
     A.MASTERID,
     A.CUSTOMER_NUMBER,
     A.ACCOUNT_NUMBER,
     A.DATA_SOURCE,
     A.ACCOUNT_STATUS,
     A.CR_STAGE,
     A.OUTSTANDING,
     A.SEGMENT,
     A.PRODUCT_CODE,
     A.DAY_PAST_DUE,
     A.RATING_CODE DELINQUENCY
     FROM ' || V_OWNER || '.' || V_TABLESELECT4 || ' A
     WHERE A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
     AND (A.ACCOUNT_STATUS = ''A'' OR
     (A.DATA_SOURCE = ''CRD'' AND A.ACCOUNT_STATUS = ''C'' AND A.OUTSTANDING > 0))
     AND A.DATA_SOURCE IN (''ILS'', ''BTRD'', ''KTP'', ''CRD'', ''LIMIT'')
     AND NVL(A.RESERVED_VARCHAR_9, '' '') NOT LIKE ''%H%''),
     PARAM AS (SELECT DISTINCT A.OPTION_GROUPING,
     A.SEGMENT,
     A.PRODUCT_CODE,
     NVL(A.DPD, 0)     AS DPD,
     A.DLQ,
     NVL(A.CKPN365, 0) AS CKPN365
     FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
     WHERE A.DOWNLOAD_DATE =(SELECT MAX(DOWNLOAD_DATE) DOWNLOAD_DATE FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ')
     ),
     RANKED_JOIN AS (SELECT IMA.PKID,
     IMA.DOWNLOAD_DATE,
     IMA.MASTERID,
     IMA.CUSTOMER_NUMBER,
     IMA.ACCOUNT_NUMBER,
     IMA.DATA_SOURCE,
     IMA.ACCOUNT_STATUS,
     IMA.PRODUCT_CODE,
     IMA.SEGMENT,
     IMA.CR_STAGE,
     IMA.OUTSTANDING,
     IMA.DAY_PAST_DUE,
     IMA.DELINQUENCY,
     PARAM.CKPN365,
     PARAM.DPD,
     PARAM.DLQ,
     PARAM.OPTION_GROUPING,
     ROW_NUMBER() OVER (
     PARTITION BY IMA.PKID
     ORDER BY
     CASE
         WHEN NVL(PARAM.SEGMENT,''-'') <> ''-'' AND NVL(PARAM.PRODUCT_CODE,''-'') <> ''-'' THEN 1
         WHEN NVL(PARAM.SEGMENT,''-'') <> ''-'' AND NVL(PARAM.PRODUCT_CODE,''-'') = ''-'' THEN 2
         WHEN NVL(PARAM.SEGMENT,''-'') = ''-'' AND  NVL(PARAM.PRODUCT_CODE,''-'') <> ''-'' THEN 3
         END
     ) AS MATCH_RANK
     FROM IMA
     INNER JOIN PARAM ON (PARAM.SEGMENT = IMA.SEGMENT OR NVL(PARAM.SEGMENT,''-'') = ''-'')
     AND (PARAM.PRODUCT_CODE = IMA.PRODUCT_CODE OR NVL(PARAM.PRODUCT_CODE,''-'') = ''-''))
         SELECT PKID,
             DOWNLOAD_DATE,
             MASTERID,
             CUSTOMER_NUMBER,
             ACCOUNT_NUMBER,
             DATA_SOURCE,
             ACCOUNT_STATUS,
             SEGMENT,
             PRODUCT_CODE,
             CR_STAGE,
             OUTSTANDING,
             CASE WHEN CKPN365 = 1 THEN ''CKPN 100%'' ELSE ''CKPN 365'' END AS SPECIAL_REASON,
             OPTION_GROUPING,
             DAY_PAST_DUE,
             CASE WHEN OPTION_GROUPING = ''DLQ'' THEN DELINQUENCY ELSE NULL END AS DELINQUENCY,
             CKPN365
         FROM RANKED_JOIN
         WHERE MATCH_RANK = 1
         AND ((OPTION_GROUPING = ''DLQ'' AND DELINQUENCY >= DLQ) OR (NVL(OPTION_GROUPING, ''DPD'') = ''DPD'' AND DAY_PAST_DUE >= DPD))';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


     V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT1 || '
     SET CKPN365 = 1,
         SPECIAL_REASON=''CKPN 100%''
     WHERE MASTERID IN (
         SELECT MASTERID
         FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
         WHERE DOWNLOAD_DATE BETWEEN ''' || V_DATE_CKPN365 || ''' AND ADD_MONTHS(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD''),-1) AND DAY_PAST_DUE>=365
         GROUP BY MASTERID HAVING COUNT(1)=MONTHS_BETWEEN(LAST_DAY(TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')),LAST_DAY(''' || V_DATE_CKPN365 || '''))
     )';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


     V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || ' WHERE DOWNLOAD_DATE=TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT3 || '
    SELECT DOWNLOAD_DATE,MASTERID,DAY_PAST_DUE
    FROM ' || V_OWNER || '.' || V_TABLESELECT4 || '
    WHERE DOWNLOAD_DATE=TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND SEGMENT IN (''SME'',''KUK'',''KPR'') AND DAY_PAST_DUE>=365';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT3 || ' A
     USING ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
     ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
         AND A.ECL_MODEL_ID = ' || V_MODEL_ID || '
         AND A.MASTERID = B.MASTERID)
     WHEN MATCHED THEN
         UPDATE
         SET A.PD_RATE = 1,
             A.LGD_RATE = 1,
             A.DISCOUNT_RATE = 1,
             A.ECL_AMOUNT = A.EAD_AMOUNT * B.CKPN365
         WHERE B.SPECIAL_REASON IN (''CKPN 365'', ''CKPN 100%'')';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLESELECT2 || ' A
     USING
         (SELECT A2.MASTERID,
                 B2.ECL_AMOUNT,
                 A2.SPECIAL_REASON AS SPECIAL_REASON
          FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || ' A2
                   JOIN ' || V_OWNER || '.' || V_TABLESELECT3 || ' B2
                        ON B2.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                            AND B2.ECL_MODEL_ID = ' || V_MODEL_ID || '
                            AND A2.MASTERID = B2.MASTERID) B
     ON (A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
         AND A.ECL_MODEL_ID = ' || V_MODEL_ID || '
         AND A.MASTERID = B.MASTERID)
     WHEN MATCHED THEN
         UPDATE
         SET A.ECL_AMOUNT = B.ECL_AMOUNT,
             A.SPECIAL_REASON = B.SPECIAL_REASON';

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