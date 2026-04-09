CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_TM_SCENARIO_DATA_DEV (
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
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_TM_SCENARIO_DATA_DEV';
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
        V_TABLEINSERT1 := 'TMP_SCENARIO_DATA_' || V_RUNID;
        V_TABLEINSERT2 := 'IFRS_PD_SCENARIO_DATA_' || V_RUNID;
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_DATA_' || V_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || V_RUNID;
    ELSE 
        V_TABLEINSERT1 := 'TMP_SCENARIO_DATA';
        V_TABLEINSERT2 := 'IFRS_PD_SCENARIO_DATA';
        V_TABLESELECT1 := 'GTMP_IFRS_SCENARIO_DATA';
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
                       ' AS SELECT * FROM ' || V_OWNER || '.TMP_SCENARIO_DATA';
        EXECUTE IMMEDIATE V_STR_QUERY;

        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_OWNER
          AND TABLE_NAME = UPPER(V_TABLEINSERT2);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_OWNER || '.' || V_TABLEINSERT2;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_OWNER || '.' || V_TABLEINSERT2 ||
                       ' AS SELECT * FROM ' || V_OWNER || '.IFRS_PD_SCENARIO_DATA';
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
    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT1 || '
	(
        DOWNLOAD_DATE,
        RULE_ID,
        RULE_TYPE,
        MASTERID,
        GROUP_SEGMENT,
        SEGMENT,
        SUB_SEGMENT,
        RATING_CODE,
        DAY_PAST_DUE,
        BI_COLLECTABILITY,
        WRITEOFF_FLAG,
        ACCOUNT_NUMBER,
        ACCOUNT_STATUS,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        EXCHANGE_RATE,
        IMPAIRED_FLAG,
        OUTSTANDING,
        KEY_TMP_IMA
	)
	SELECT A.DOWNLOAD_DATE,
		  B.PKID RULE_ID,
		  A.RULE_TYPE,
		  A.MASTERID,
		  A.GROUP_SEGMENT,
		  A.SEGMENT,
		  A.SUB_SEGMENT,
		  A.RATING_CODE,
		  A.DAY_PAST_DUE,
		  A.BI_COLLECTABILITY,
		  A.WRITEOFF_FLAG,
		  A.ACCOUNT_NUMBER,
		  A.ACCOUNT_STATUS,
		  A.CUSTOMER_NUMBER,
		  A.CUSTOMER_NAME,
		  A.EXCHANGE_RATE,
		  A.IMPAIRED_FLAG,
		  A.OUTSTANDING,
		  CASE WHEN B.CALC_METHOD IN (''CNOC'', ''COS'', ''CVS'') THEN
                A.CUSTOMER_NUMBER
            WHEN B.CALC_METHOD IN (''ANOA'', ''AOS'', ''AVS'') THEN
                A.ACCOUNT_NUMBER
            ELSE
                TO_CHAR(A.MASTERID)
          END KEY_TMP_IMA
	FROM ' || V_OWNER || '.' || V_TABLESELECT1 || ' A
	JOIN ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' B
	ON A.RULE_ID = B.SEGMENTATION_ID
	AND B.PD_METHOD = ''MIG''
	AND NVL(B.IS_DELETED,0) = 0
	AND NVL(B.ACTIVE_FLAG,1) = 1';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;


    V_STR_QUERY := 'BEGIN ' ||
                   'DBMS_STATS.UNLOCK_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'DBMS_STATS.DELETE_TABLE_STATS(:1, ''' || V_TABLEINSERT2 || '''); ' ||
                   'END;';
    EXECUTE IMMEDIATE V_STR_QUERY USING V_OWNER;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE  ' || V_OWNER || '.' || V_TABLEINSERT2;


    V_STR_QUERY := 'INSERT INTO ' || V_OWNER || '.' || V_TABLEINSERT2 || '
	(
	  EFF_DATE
	  , PD_RULE_ID
	  , RULE_ID
	  , BUCKET_GROUP
	  , MASTERID
	  , ACCOUNT_NUMBER
	  , CUSTOMER_NUMBER
	  , CUSTOMER_NAME
	  , PD_UNIQUE_ID
	  , PD_METHOD
	  , CALC_METHOD
	  , CALC_AMOUNT
	  , BUCKET_ID
	  , OUTSTANDING
	  , ACCOUNT_STATUS
	  , IMPAIRED_FLAG
	  , IS_IMPAIRED
	  , WRITEOFF_FLAG
	 )
	 SELECT B.DOWNLOAD_DATE
	   , A.PKID
	   , A.SEGMENTATION_ID
	   , A.BUCKET_GROUP
	   , B.MASTERID
	   , B.ACCOUNT_NUMBER
	   , B.CUSTOMER_NUMBER
	   , B.CUSTOMER_NAME
	   , B.KEY_TMP_IMA AS PD_UNIQUE_ID
	   , A.PD_METHOD
	   , A.CALC_METHOD
	   , CASE WHEN A.CALC_METHOD = ''MNOA'' THEN /*Add New Calc Method By YY 20180708*/
			1
		 WHEN A.CALC_METHOD = ''ANOA'' THEN
			 CASE WHEN B.MASTERID = MAX(B.MASTERID) OVER (PARTITION BY A.PKID,B.DOWNLOAD_DATE,B.ACCOUNT_NUMBER) THEN
			 		   1
				  ELSE 0
			 END
		 WHEN A.CALC_METHOD = ''CNOC'' THEN
			 CASE WHEN B.MASTERID = MAX(B.MASTERID) OVER (PARTITION BY A.PKID,B.DOWNLOAD_DATE,B.CUSTOMER_NUMBER) THEN
			 		   1
				  ELSE 0
			 END
		 ELSE
			B.OUTSTANDING * B.EXCHANGE_RATE
		 END CALC_AMOUNT
       , CASE WHEN C.BUCKET_ID > D.MAX_BUCKET_ID THEN
            D.MAX_BUCKET_ID
         ELSE
            C.BUCKET_ID
         END BUCKET_ID
	   , B.OUTSTANDING * B.EXCHANGE_RATE OUTSTANDING
	   , B.ACCOUNT_STATUS
	   , B.IMPAIRED_FLAG
	   , 0 AS IS_IMPAIRED
	   , B.WRITEOFF_FLAG
	 FROM ' || V_OWNER || '.' || V_TABLEPDCONFIG || ' A
	 JOIN ' || V_OWNER || '.' || V_TABLEINSERT1 || ' B
	 ON A.PKID = B.RULE_ID
	 LEFT JOIN ' || V_OWNER || '.VW_IFRS_BUCKET C
	 ON C.BUCKET_GROUP=A.BUCKET_GROUP
	  AND
	  (
	   (CASE WHEN C.OPTION_GROUPING = ''DPD'' THEN
		 B.DAY_PAST_DUE
		WHEN C.OPTION_GROUPING = ''BIC'' THEN
		 TO_NUMBER(B.BI_COLLECTABILITY)
		WHEN C.OPTION_GROUPING = ''DLQ'' THEN
         TO_NUMBER(B.RATING_CODE)
		END >= C.RANGE_START
		AND
		CASE WHEN C.OPTION_GROUPING = ''DPD'' THEN
		 B.DAY_PAST_DUE
		WHEN C.OPTION_GROUPING = ''BIC'' THEN
		 TO_NUMBER(B.BI_COLLECTABILITY)
		WHEN C.OPTION_GROUPING = ''DLQ'' THEN
         TO_NUMBER(B.RATING_CODE)
		END <= C.RANGE_END
	   )
	   OR
	   (C.OPTION_GROUPING IN (''IR'',''ER'') AND C.BUCKET_NAME = B.RATING_CODE
	   )
	  )
	 JOIN ' || V_OWNER || '.VW_IFRS_MAX_BUCKET D
	 ON D.BUCKET_GROUP = C.BUCKET_GROUP';
    
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