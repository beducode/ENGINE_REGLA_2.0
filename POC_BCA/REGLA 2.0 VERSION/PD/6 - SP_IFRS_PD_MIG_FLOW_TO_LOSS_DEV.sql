CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_MIG_FLOW_TO_LOSS_DEV (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_MIG_FLOW_TO_LOSS_DEV';
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
    V_RETURNROWS        NUMBER := 0;
    V_RETURNROWS2       NUMBER := 0;
    V_TABLEDEST         VARCHAR2(100);
    V_COLUMNDEST        VARCHAR2(100);
    V_OPERATION         VARCHAR2(100);
    V_RUNID             VARCHAR2(30);
    V_SYSCODE           VARCHAR2(10);
    V_PRC               VARCHAR2(5);
    V_CURSOR            NUMBER;
    V_PD_RULE_ID        VARCHAR2(30);
    V_MIG_LOSS_BUCKET   VARCHAR2(30);
    V_TEMP              NUMBER;
    V_MAX_ID            NUMBER;


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
        V_TABLEINSERT1 := 'TMP_IFRS_PD_RUNNING_DATE_' || V_RUNID;
        V_TABLEINSERT2 := 'TMP_IFRS_PD_MIG_FLOW_TO_LOSS_' || V_RUNID;
        V_TABLEINSERT3 := 'TMP_IFRS_PD_MAX_BUCKET_' || V_RUNID;
        V_TABLEINSERT4 := 'IFRS_PD_MIG_FLOW_TO_LOSS_' || V_RUNID;
        V_TABLESELECT1 := 'IFRS_PD_MIG_FLOWRATE_' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'TMP_IFRS_PD_RUNNING_DATE';
        V_TABLEINSERT2 := 'TMP_IFRS_PD_MIG_FLOW_TO_LOSS';
        V_TABLEINSERT3 := 'TMP_IFRS_PD_MAX_BUCKET';
        V_TABLEINSERT4 := 'IFRS_PD_MIG_FLOW_TO_LOSS';
        V_TABLESELECT1 := 'IFRS_PD_MIG_FLOWRATE';
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
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_MIG_FLOW_TO_LOSS';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_IFRS_PD_MAX_BUCKET';
        EXECUTE IMMEDIATE V_STR_QUERY;


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
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_PD_MIG_FLOW_TO_LOSS';
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
    -- BUILD DYNAMIC QUERY
    ----------------------------------------------------------------
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 ||
    ' (
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


    V_STR_QUERY := 'DELETE FROM ' || V_OWNER || '.' || V_TABLEINSERT4 ||
                   ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                   ' AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM ' || V_OWNER || '.' || V_TABLEINSERT1 || ')';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT3 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT3 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT3;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT3 || '
    (
    PKID,
    PD_RULE_ID,
    MAX_BUCKET_ID
    )
    SELECT ROWNUM,
        PD_RULE_ID,
        MAX_BUCKET_ID
    FROM
    (
        SELECT A.PD_RULE_ID,
            MAX(A.BUCKET_FROM) AS MAX_BUCKET_ID
        FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
        INNER JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
        ON A.PD_RULE_ID = B.PD_RULE_ID
            AND A.BASE_DATE = B.BASE_DATE
            AND A.EFF_DATE = B.EFF_DATE
        GROUP BY A.PD_RULE_ID
    ) A';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_CURSOR := 1;
    EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM ' || V_OWNER || '.' || V_TABLEINSERT3 INTO V_COUNT;

    WHILE V_CURSOR <= V_COUNT LOOP

        EXECUTE IMMEDIATE 'SELECT PD_RULE_ID, MAX_BUCKET_ID
        FROM ' || V_OWNER || '.' || V_TABLEINSERT3 || '
        WHERE PKID = ' || V_CURSOR || '' INTO V_PD_RULE_ID, V_MAX_ID;

        V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'END;';
        EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT2;

        V_TEMP := V_MAX_ID;
        WHILE V_TEMP > 0 LOOP

            V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || '
            (
                BASE_DATE,
                EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FLOW_TO_LOSS
            )
              SELECT C.BASE_DATE,
                    C.EFF_DATE,
                    A.PD_RULE_ID,
                    A.BUCKET_GROUP,
                    A.BUCKET_FROM,
                     CASE
                       WHEN A.BUCKET_FROM = ' || V_MAX_ID || ' THEN
                        1
                       ELSE
                        CASE
                          WHEN NVL(B.FLAG_DEFAULT, 0) = 1 THEN
                           NVL(B.PD_DEFAULT, 0)
                          ELSE
                           SUM(NVL(A.FLOWRATE, 0) * NVL(D.FLOW_TO_LOSS, 0))
                        END
                     END FLOW_TO_LOSS
                FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
                JOIN ' || V_OWNER || '.IFRS_BUCKET_DETAIL B
                ON A.BUCKET_GROUP = B.BUCKET_GROUP
                AND A.EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
                AND A.PD_RULE_ID = ' || V_PD_RULE_ID || '
                AND A.BUCKET_FROM = B.BUCKET_ID
                AND A.BUCKET_FROM = ' || V_TEMP || '
                AND A.BUCKET_TO >= ' || V_TEMP || '
                JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' C
                ON A.PD_RULE_ID = C.PD_RULE_ID
                LEFT JOIN ' || V_OWNER || '.' || V_TABLEINSERT2 || ' D
                  ON A.PD_RULE_ID = D.PD_RULE_ID
                 AND A.BUCKET_TO = D.BUCKET_ID
               GROUP BY C.BASE_DATE,
                    C.EFF_DATE,
                    A.PD_RULE_ID,
                    A.EFF_DATE,
                    A.BUCKET_GROUP,
                    A.BUCKET_FROM,
                    B.FLAG_DEFAULT,
                    B.PD_DEFAULT';

            EXECUTE IMMEDIATE V_STR_QUERY;
            COMMIT;

            V_TEMP := V_TEMP - 1;

        END LOOP;

        V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT4 || '
        (
            PKID,
            EFF_DATE,
            BASE_DATE,
            PD_RULE_ID,
            BUCKET_GROUP,
            BUCKET_ID,
            FLOW_TO_LOSS,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        SELECT
            SEQ_IFRS_PD_MIG_FLOW_TO_LOSS.NEXTVAL AS PKID,
            A.EFF_DATE,
            A.BASE_DATE,
            A.PD_RULE_ID,
            A.BUCKET_GROUP,
            A.BUCKET_ID,
            A.FLOW_TO_LOSS,
            ''SYSTEM'' AS CREATEDBY,
            SYSDATE AS CREATEDDATE,
            ''SYSTEM'' AS CREATEDHOST
        FROM ' || V_OWNER || '.' || V_TABLEINSERT2 || ' A
        JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
            ON A.PD_RULE_ID = B.PD_RULE_ID
        AND A.EFF_DATE   = B.EFF_DATE
        AND A.BASE_DATE  = B.BASE_DATE';

        EXECUTE IMMEDIATE V_STR_QUERY; 
        COMMIT;

        V_CURSOR := V_CURSOR + 1;

    END LOOP;


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