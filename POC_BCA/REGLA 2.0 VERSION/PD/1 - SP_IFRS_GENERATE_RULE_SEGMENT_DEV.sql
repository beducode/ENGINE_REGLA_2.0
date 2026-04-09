CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_GENERATE_RULE_SEGMENT_DEV (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S',
    P_CONSTNAME     IN VARCHAR2 DEFAULT ' '
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_GENERATE_RULE_SEGMENT_DEV';
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
    V_GROUP_SEGMENT VARCHAR2(50);
    V_SEGMENT VARCHAR2(50);
    V_SUB_SEGMENT VARCHAR2(100);
    V_SEGMENT_TYPE VARCHAR2(25);
    V_SEQUENCE NUMBER(10);
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
        V_TABLEINSERT1 := 'GTMP_SCENARIO_SEGMENT_GENQUERY_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'GTMP_SCENARIO_SEGMENT_GENQUERY';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.GTMP_SCENARIO_SEGMENT_GENQUERY';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- UNLOCK & CLEAN TARGET TABLE
    ----------------------------------------------------------------
    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT1 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT1;

    ----------------------------------------------------------------
    -- BUILD DYNAMIC CURSOR QUERY
    ----------------------------------------------------------------
    V_STR_QUERY := 'SELECT DISTINCT
           B.RULE_ID,
           A.GROUP_SEGMENT,
           A.SEGMENT,
           A.SUB_SEGMENT,
           A.SEGMENT_TYPE,
           A.SEQUENCE,
           B.TABLE_NAME
    FROM IFRS_MSTR_SEGMENT_RULES_HEADER A
    JOIN IFRS_MSTR_SEGMENT_RULES_DETAIL B
      ON A.PKID = B.RULE_ID
      WHERE A.IS_DELETED = 0
      AND (NVL(:1,'' '') = '' '' OR A.SEGMENT_TYPE = :2)';

    ----------------------------------------------------------------
    -- OPEN DYNAMIC CURSOR
    ----------------------------------------------------------------
OPEN C_RULE FOR V_STR_QUERY
USING NULLIF(TRIM(P_CONSTNAME), ''),
      NULLIF(TRIM(P_CONSTNAME), '');

LOOP
    FETCH C_RULE INTO
        V_RULE_ID,
        V_GROUP_SEGMENT,
        V_SEGMENT,
        V_SUB_SEGMENT,
        V_SEGMENT_TYPE,
        V_SEQUENCE,
        V_TABLE_NAME;

    EXIT WHEN C_RULE%NOTFOUND;

    ----------------------------------------------------------------
    -- INSERT PER ROW (DYNAMIC)
    ----------------------------------------------------------------
    V_STR_QUERY := '
        INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || ' (
            RULE_ID,
            GROUP_SEGMENT,
            SEGMENT,
            SUB_SEGMENT,
            SEGMENT_TYPE,
            SEQUENCE,
            TABLE_NAME,
            CONDITION,
            PRC_CODE
        )
        VALUES (
            :1, :2, :3, :4, :5, :6, :7,
            FN_GENERATE_RULE_SEGMENT(:8),
            :9
        )';

    DBMS_OUTPUT.PUT_LINE('Executing INSERT for RULE_ID = ' || V_RULE_ID);

    EXECUTE IMMEDIATE V_STR_QUERY
    USING V_RULE_ID,
          V_GROUP_SEGMENT,
          V_SEGMENT,
          V_SUB_SEGMENT,
          V_SEGMENT_TYPE,
          V_SEQUENCE,
          V_TABLE_NAME,
          V_RULE_ID, -- FOR THE FUNCTION PARAMETER
          V_PRC;

    V_RETURNROWS2 := V_RETURNROWS2 + SQL%ROWCOUNT;

END LOOP;

    CLOSE C_RULE;

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