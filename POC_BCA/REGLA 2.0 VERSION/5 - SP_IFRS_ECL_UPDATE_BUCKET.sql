CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_UPDATE_BUCKET_BCA (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_UPDATE_BUCKET_BCA';
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
    V_TABLECONFIG   VARCHAR2(100);


    -- CURSOR
    TYPE REF_CURSOR IS REF CURSOR;
    C_RULE        REF_CURSOR;

    -- FETCH VARIABLES
    V_RULE_ID     VARCHAR2(250);
    V_RULE_TYPE VARCHAR2(25);
    V_TABLE_NAME  VARCHAR2(100);
    V_DATASOURCE   VARCHAR2(30) := 'ALL';


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
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE_AMORT HASNO CURRDATE ROW');
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
        V_TABLEINSERT2 := 'GTMP_IFRS_MASTER_ACCOUNT_' || V_RUNID;
        V_TABLECONFIG := 'IFRS_ECL_MODEL_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT2 := 'GTMP_IFRS_MASTER_ACCOUNT';
        V_TABLECONFIG := 'IFRS_ECL_MODEL_CONFIG';
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

    ----------------------------------------------------------------
    -- MAIN PROCESSING
    ----------------------------------------------------------------

    ----------------------------------------------------------------
    -- EXECUTE DATA PROCEDURE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN IFRS9_BCA.SP_IFRS_INSERT_GTMP_FROM_IMA_BCA(:1, :2, :3, :4, :5); END;';

    EXECUTE IMMEDIATE V_STR_QUERY
    USING V_RUNID, V_CURRDATE, V_SYSCODE, V_PRC, V_DATASOURCE;
    COMMIT;
    ----------------------------------------------------------------

    V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT2 || '
    SET BUCKET_GROUP = NULL,
        BUCKET_ID = NULL
    WHERE CREATEDBY <> ''DKP''';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' IMA
     USING
     (
         SELECT
         A.MASTERID, C.BUCKET_ID, C.BUCKET_GROUP
         FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
         JOIN ' || V_OWNER || '.' || V_TABLECONFIG || ' B
         ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
         AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
         JOIN ' || V_OWNER || '.VW_IFRS_BUCKET C
         ON B.BUCKET_GROUP = C.BUCKET_GROUP
         AND C.OPTION_GROUPING IN (''DPD'',''DLQ'')
         AND CASE WHEN C.OPTION_GROUPING = ''DPD'' THEN A.DAY_PAST_DUE ELSE TO_NUMBER(NVL(A.RATING_CODE,1)) END BETWEEN C.RANGE_START AND C.RANGE_END
         WHERE B.ECL_MODEL_ID = ' || V_MODEL_ID || '
     ) TMP
     ON (IMA.MASTERID = TMP.MASTERID)
     WHEN MATCHED THEN
         UPDATE SET
         IMA.BUCKET_ID = TMP.BUCKET_ID,
         IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

      V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' IMA
      USING
      (
          SELECT
              A.MASTERID, C.BUCKET_ID, C.BUCKET_GROUP
          FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
          JOIN ' || V_OWNER || '.' || V_TABLECONFIG || ' B
          ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
          AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
          JOIN ' || V_OWNER || '.VW_IFRS_BUCKET C
          ON B.BUCKET_GROUP = C.BUCKET_GROUP
          AND C.OPTION_GROUPING IN (''IR'',''ER'',''PEF'',''SNP'',''SNPFI'',''PEFFI'')
          AND NVL(UPPER(A.RATING_CODE), ''X'') = C.BUCKET_NAME
          WHERE B.ECL_MODEL_ID = ' || V_MODEL_ID || '
      ) TMP
      ON (IMA.MASTERID = TMP.MASTERID)
      WHEN MATCHED THEN
          UPDATE SET
          IMA.BUCKET_ID = TMP.BUCKET_ID,
          IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

      EXECUTE IMMEDIATE V_STR_QUERY;
      COMMIT;

      V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' IMA
      USING
      (
          SELECT
              A.MASTERID, 12 BUCKET_ID , ''BR9_1'' BUCKET_GROUP
          FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
          JOIN ' || V_OWNER || '.' || V_TABLECONFIG || ' B
          ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
          AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
          WHERE B.ECL_MODEL_ID = ' || V_MODEL_ID || '
          AND A.SEGMENT IN (''PLACEMENT'', ''NOSTRO'', ''BANK_BTRD'')
          AND A.RATING_CODE = ''UNK''
      ) TMP
      ON (IMA.MASTERID = TMP.MASTERID)
      WHEN MATCHED THEN
          UPDATE SET
          IMA.BUCKET_ID = TMP.BUCKET_ID,
          IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

      EXECUTE IMMEDIATE V_STR_QUERY;
      COMMIT;

      V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' IMA
      USING
      (
          SELECT
              A.MASTERID, C.BUCKET_ID, C.BUCKET_GROUP , A.RATING_CODE
          FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
          JOIN ' || V_OWNER || '.VW_IFRS_BUCKET C
          ON C.BUCKET_GROUP = ''BR9_1''
          AND A.RATING_CODE LIKE ''%BR%''
          AND C.OPTION_GROUPING IN (''IR'',''ER'',''PEF'',''SNP'',''SNPFI'',''PEFFI'')
          AND NVL(A.RATING_CODE, ''X'') = C.BUCKET_NAME
          WHERE GROUP_SEGMENT IN (''PLACEMENT'', ''NOSTRO'')
      ) TMP
      ON (IMA.MASTERID = TMP.MASTERID)
      WHEN MATCHED THEN
          UPDATE SET
          IMA.BUCKET_ID = TMP.BUCKET_ID,
          IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

      EXECUTE IMMEDIATE V_STR_QUERY;
      COMMIT;

     V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT2 || '
     SET BUCKET_ID = ''12'',
         BUCKET_GROUP = ''BR9_1''
     WHERE RESERVED_VARCHAR_26 = ''MUTFUND''';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

     V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' IMA
     USING
     (
         SELECT
             A.MASTERID, 12 BUCKET_ID, ''IR11_1'' BUCKET_GROUP
         FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
         JOIN ' || V_OWNER || '.' || V_TABLECONFIG || ' B
         ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
         AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
         WHERE B.ECL_MODEL_ID = ' || V_MODEL_ID || '
         AND A.SEGMENT IN (''BOND_CORPORATE'')
         AND A.SUB_SEGMENT = ''BOND_CORPORATE - INTERNAL''
         AND A.RATING_CODE = ''UNK''
         AND A.DATA_SOURCE = ''KTP''
     ) TMP
     ON (IMA.MASTERID = TMP.MASTERID)
     WHEN MATCHED THEN
         UPDATE SET
         IMA.BUCKET_ID = TMP.BUCKET_ID,
         IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

      V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || ' IMA
      USING
      (
          SELECT
              A.MASTERID, E.BUCKET_GROUP, E.MAX_BUCKET_ID BUCKET_ID
          FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
          JOIN ' || V_OWNER || '.' || V_TABLECONFIG || ' B
          ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
          AND A.SEGMENT_RULE_ID = B.PF_SEGMENT_ID
          JOIN
          (
              SELECT CUSTOMER_NUMBER, MAX(PKID) MAX_OVERRIDEID
              FROM ' || V_OWNER || '.IFRS_IA_OVERRIDEH
              WHERE EFFECTIVE_DATE <= TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
              GROUP BY CUSTOMER_NUMBER
          ) C
          ON A.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
          JOIN ' || V_OWNER || '.TBLT_PAYMENTEXPECTEDH D
          ON A.ACCOUNT_NUMBER = D.ACCOUNT_NUMBER
          AND C.MAX_OVERRIDEID = D.OVERRIDEID
          JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET E
          ON B.BUCKET_GROUP = E.BUCKET_GROUP
          WHERE B.ECL_MODEL_ID = ' || V_MODEL_ID || '
          AND A.BUCKET_GROUP IS NULL
      ) TMP
      ON (IMA.MASTERID = TMP.MASTERID)
      WHEN MATCHED THEN
          UPDATE SET
          IMA.BUCKET_ID = TMP.BUCKET_ID,
          IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

      EXECUTE IMMEDIATE V_STR_QUERY;
      COMMIT;

     V_STR_QUERY := 'UPDATE ' || V_OWNER || '.' || V_TABLEINSERT2 || '
     SET BUCKET_ID = NULL,
         BUCKET_GROUP = NULL
     WHERE DATA_SOURCE = ''BTRD'' AND RESERVED_VARCHAR_23 IN (''0'',''1'')';

     EXECUTE IMMEDIATE V_STR_QUERY;
     COMMIT;

    V_STR_QUERY := 'MERGE INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' IMA
    USING ' || V_OWNER || '.' || V_TABLEINSERT2 || ' TMP
    ON (IMA.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') AND IMA.MASTERID = TMP.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        IMA.BUCKET_ID = TMP.BUCKET_ID,
        IMA.BUCKET_GROUP = TMP.BUCKET_GROUP';

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