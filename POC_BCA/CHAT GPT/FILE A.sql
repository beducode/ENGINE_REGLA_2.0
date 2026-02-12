CREATE OR REPLACE PROCEDURE IFRS9_BCA.XXX (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    -- DATES / COUNTERS
    V_CURRDATE      DATE;
    V_PREVDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- DYNAMIC SQL (USE VARCHAR2 LARGE)
    V_STR_QUERY     VARCHAR2(32767);

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'IFRS9_BCA';
    V_TABLEINSERT1  VARCHAR2(100);
    V_TABLEINSERT2  VARCHAR2(100);
    V_TABLESELECT1  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- MISC
    V_RETURNROWS    NUMBER := 0;
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);
    V_SYSCODE      VARCHAR2(10);
    V_PRC         VARCHAR2(5);

    -- RESULT QUERY
    V_QUERYS        CLOB;

    -- HELPER TO PRINT LONG TEXT
    PROCEDURE PRINT_LONG(p_txt CLOB) IS
        v_pos INTEGER := 1;
        v_len INTEGER := DBMS_LOB.getlength(p_txt);
        v_step INTEGER := 30000;
        v_part VARCHAR2(32767);
    BEGIN
        WHILE v_pos <= v_len LOOP
            v_part := DBMS_LOB.SUBSTR(p_txt, v_step, v_pos);
            DBMS_OUTPUT.PUT_LINE(v_part);
            v_pos := v_pos + v_step;
        END LOOP;
    END PRINT_LONG;

BEGIN
    V_SP_NAME := 'XXX';

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
        V_TABLEINSERT1 := 'XXX_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'XXX';
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
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.XXX';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    ----------------------------------------------------------------
    -- START BODY OF PROCEDURE
    ----------------------------------------------------------------

    V_STR_QUERY := 'UPDATE ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || '
                    SET COL1 = ''VALUE1'',
                        COL2 = ''VALUE2''
                    WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || '
                    WHERE COL3 IS NULL
                    AND EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')';
    
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 || ' (COL1, COL2, COL3, EFF_DATE)
                    SELECT COL1, COL2, COL3, TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                    FROM ' || V_TAB_OWNER || '.SOURCE_TABLE
                    WHERE CONDITION_COL = ''SOME_CONDITION''';
    
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY,1,30000));

    ----------------------------------------------------------------
    -- END BODY OF PROCEDURE
    ----------------------------------------------------------------


    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9_BCA.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1 ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (' || CASE WHEN V_MODEL_ID = '0' THEN '1=1' ELSE 'PD_RULE_ID = ' || V_MODEL_ID END || ')';

    IFRS9_BCA.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20002, 'XXX FAILED: ' || SQLERRM);
END;