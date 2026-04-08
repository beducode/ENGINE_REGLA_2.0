CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_PD_TERM_STRUCTURE
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AS
    -- Dates / counters
    V_CURRDATE      DATE;
    V_PREVDATE      DATE;
    V_MODEL_ID      VARCHAR2(22);
    V_COUNT         NUMBER;

    -- Dynamic SQL
    V_STR_QUERY     CLOB;

    -- Table names
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_GTMP_PD_TERM_STRUCTURE1  VARCHAR2(100);
    V_IFRS_PD_MAA_MMULT 	VARCHAR2(100);
    V_IFRS_PD_TERM_STRUCTURE  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);

    -- misc
    V_RETURNROWS    NUMBER;
    V_RETURNROWS2   NUMBER;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);

    -- result query
    V_QUERYS        CLOB;
BEGIN
    -- set procedure name
    V_SP_NAME := 'SP_IFRS_PD_TERM_STRUCTURE';
    
    -- handle default download date
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE
          INTO V_CURRDATE
          FROM PSAK413.IFRS_PRC_DATE; 
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    IF P_SYSCODE <> '0' THEN
   		V_MODEL_ID := '1';
    ELSE
    	V_MODEL_ID := '0';
    END IF;

    IF P_PRC = 'S' THEN
        V_GTMP_PD_TERM_STRUCTURE1  := 'GTMP_IFRS_PD_TERM_STRUCTURE1_' || P_RUNID;
        V_IFRS_PD_MAA_MMULT  := 'IFRS_PD_MAA_MMULT_' || P_RUNID;
        V_IFRS_PD_TERM_STRUCTURE  := 'IFRS_PD_TERM_STRUCTURE_' || P_RUNID;
        V_TABLEPDCONFIG := 'GTMP_IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_GTMP_PD_TERM_STRUCTURE1  := 'GTMP_IFRS_PD_TERM_STRUCTURE1';
        V_IFRS_PD_MAA_MMULT  := 'IFRS_PD_MAA_MMULT';
        V_IFRS_PD_TERM_STRUCTURE  := 'IFRS_PD_TERM_STRUCTURE';
        V_TABLEPDCONFIG := 'GTMP_IFRS_PD_RULES_CONFIG';
    END IF;

   PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, 0, SYSDATE);
   COMMIT;

  	EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLEPDCONFIG || '' ; 
	 
    V_STR_QUERY := 'INSERT INTO ' || V_TABLEPDCONFIG || ' (PKID, SYSCODE_PD, PD_RULE_NAME, SEGMENTATION_ID, PD_METHOD, START_HISTORICAL_DATE,' || 
						'CALC_METHOD, HISTORICAL_DATA, EXPECTED_LIFE, INCLUDE_INDIVIDUAL_ACCOUNT, INCLUDE_WO, INCLUDE_CLOSE, DEFAULT_RATIO_BY, ' ||
						'DEFAULT_RULE_ID, BUCKET_GROUP, ME_CODE, PERIOD_START_DATE, PERIOD_END_DATE, ' ||
					 	'IS_DELETED, INCREMENT_PERIOD, IS_ACTIVE, CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE)' ||
               'SELECT PKID, SYSCODE_PD, PD_RULE_NAME, SEGMENTATION_ID, PD_METHOD, START_HISTORICAL_DATE,' || 
						'CALC_METHOD, HISTORICAL_DATA, EXPECTED_LIFE, INCLUDE_INDIVIDUAL_ACCOUNT, INCLUDE_WO, INCLUDE_CLOSE, DEFAULT_RATIO_BY, ' ||
						'DEFAULT_RULE_ID, BUCKET_GROUP, ME_CODE, PERIOD_START_DATE, PERIOD_END_DATE, ' ||
					 	'IS_DELETED, INCREMENT_PERIOD, IS_ACTIVE, CREATED_BY, CREATED_DATE, UPDATED_BY, UPDATED_DATE' || 
					 	' FROM IFRS_PD_RULES_CONFIG ' ||
               'WHERE PD_METHOD = ''MAA'' AND IS_DELETED = 0 AND (  
                 UPPER(TRIM(SYSCODE_PD)) IN ( 
                   SELECT UPPER(TRIM(REGEXP_SUBSTR(:p1, ''[^;]+'', 1, LEVEL)))
                   FROM DUAL
                   CONNECT BY REGEXP_SUBSTR(:p1, ''[^;]+'', 1, LEVEL) IS NOT NULL
                 )
                 OR :p2 = ''0'' 
               )';
    DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
    EXECUTE IMMEDIATE V_STR_QUERY USING P_SYSCODE, P_SYSCODE, P_SYSCODE;
    ----------------------------------------------------------------
    -- PRE-SIMULATION TABLE: create/drop temp tables if P_PRC = 'S'
    ----------------------------------------------------------------
    IF P_PRC = 'S' THEN
        -- drop table if exists
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_GTMP_PD_TERM_STRUCTURE1);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_TERM_STRUCTURE1;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_TERM_STRUCTURE1 ||
                       ' AS SELECT *  FROM ' || V_TAB_OWNER || '.GTMP_IFRS_PD_TERM_STRUCTURE1 WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;

        -- second table
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_IFRS_PD_TERM_STRUCTURE);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.IFRS_PD_TERM_STRUCTURE WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
        
        -- config
        SELECT COUNT(*) INTO V_COUNT
        FROM ALL_TABLES
        WHERE OWNER = V_TAB_OWNER
          AND TABLE_NAME = UPPER(V_TABLEPDCONFIG);

        IF V_COUNT > 0 THEN
            V_STR_QUERY := 'DROP TABLE ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG;
            EXECUTE IMMEDIATE V_STR_QUERY;
        END IF;

        V_STR_QUERY := 'CREATE TABLE ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG ||
                       ' AS SELECT * FROM ' || V_TAB_OWNER || '.GTMP_IFRS_PD_RULES_CONFIG WHERE 1=0';
        EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
    COMMIT;

    V_STR_QUERY := 'TRUNCATE TABLE ' || V_TAB_OWNER || '.' || V_GTMP_PD_TERM_STRUCTURE1;
    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    --------------------------------------------------------------------
    -- Dynamic DELETE
    --------------------------------------------------------------------
    V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE || ' A ' ||
             'WHERE A.PD_METHOD = ''MAA'' ' ||
             '  AND A.MODEL_ID = 0 ' ||
             '  AND (A.PD_RULE_ID IN (SELECT PKID FROM ' || V_TABLEPDCONFIG || ')) ' ||
             '  AND A.EFF_DATE = :eff_date';

    EXECUTE IMMEDIATE V_STR_QUERY
        USING V_CURRDATE;
    --------------------------------------------------------------------
    -- Conditional DELETE
    --------------------------------------------------------------------
    IF V_MODEL_ID = '0' THEN
        V_STR_QUERY := ' DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE || ' A
        WHERE A.EFF_DATE  = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') 
          AND A.PD_METHOD = ''MAA''
          AND A.MODEL_ID  = 0';
    EXECUTE IMMEDIATE V_STR_QUERY;
    END IF;
   
    COMMIT;

    V_STR_QUERY := 'INSERT INTO ' || V_TAB_OWNER || '.' || V_GTMP_PD_TERM_STRUCTURE1 || '
        (EFF_DATE, BASE_DATE,SCENARIO_NO, MODEL_ID, PD_RULE_ID, FL_SEQ, INCREMENT_PERIOD, BUCKET_GROUP, BUCKET_FROM, MMULT, PD_METHOD,
         PD_RULE_NAME, MAX_BUCKET_ID)
        SELECT A.EFF_DATE, A.BASE_DATE, A.SCENARIO_NO, A.MODEL_ID,
            A.PD_RULE_ID, A.FL_SEQ, B.INCREMENT_PERIOD, A.BUCKET_GROUP, A.BUCKET_FROM, A.MMULT - NVL(D.MMULT,0),
            B.PD_METHOD, B.PD_RULE_NAME, C.MAX_BUCKET_ID
        FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_MMULT || ' A
        JOIN ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG || ' B 
             ON A.PD_RULE_ID = B.PKID
        JOIN VW_IFRS_MAX_BUCKET C
             ON A.BUCKET_GROUP = C.BUCKET_GROUP
            AND A.BUCKET_TO    = C.MAX_BUCKET_ID
        LEFT JOIN ' || V_TAB_OWNER || '.' || V_IFRS_PD_MAA_MMULT || ' D
             ON A.PD_RULE_ID = D.PD_RULE_ID
            AND A.MODEL_ID   = D.MODEL_ID
            AND A.SCENARIO_NO = D.SCENARIO_NO
            AND A.EFF_DATE   = D.EFF_DATE
            AND A.BUCKET_FROM = D.BUCKET_FROM
            AND A.BUCKET_TO   = D.BUCKET_TO
            AND A.FL_SEQ      = D.FL_SEQ + 1
        WHERE A.EFF_DATE = :currdate
          AND (A.PD_RULE_ID = B.PKID
               OR (:model_id = 0 AND B.PD_METHOD = ''MAA''))
        ORDER BY A.BUCKET_FROM, A.FL_SEQ
    ';
	
    EXECUTE IMMEDIATE V_STR_QUERY
        USING V_CURRDATE, V_MODEL_ID;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_QUERY, 1, 30000));
   V_STR_QUERY := 'DECLARE
	    V_MONTH_SEQ NUMBER;
	    V_MMULT_FIN BINARY_DOUBLE;
	BEGIN
	    FOR rec IN (
	        SELECT A.EFF_DATE, A.BASE_DATE, A.SCENARIO_NO, A.MODEL_ID, A.PD_RULE_ID, 
	               A.FL_SEQ, A.INCREMENT_PERIOD, A.BUCKET_GROUP, A.BUCKET_FROM, A.MMULT,
	               A.PD_METHOD, A.PD_RULE_NAME, A.MAX_BUCKET_ID
	        FROM ' || V_TAB_OWNER || '.' || V_GTMP_PD_TERM_STRUCTURE1 || ' A
	        ORDER BY A.BUCKET_FROM, A.FL_SEQ
	    ) LOOP
	
	        V_MONTH_SEQ := 1;
	
	        WHILE V_MONTH_SEQ <= 12 LOOP
	
	            IF V_MONTH_SEQ = 1 THEN
	                V_MMULT_FIN := 1 - POWER(1 - rec.MMULT, (V_MONTH_SEQ / 12));
	            ELSE
	                V_MMULT_FIN := 1 - POWER(1 - rec.MMULT, (V_MONTH_SEQ / 12))
	                               - (1 - POWER(1 - rec.MMULT, ((V_MONTH_SEQ - 1)/12)));
	            END IF;
	
	            IF rec.BUCKET_FROM = rec.MAX_BUCKET_ID THEN
	                IF V_MONTH_SEQ = 1 AND rec.FL_SEQ = 0 THEN
	                    V_MMULT_FIN := 1;
	                ELSE
	                    V_MMULT_FIN := 0;
	                END IF;
	            END IF;
	
	            INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE || ' (
	                EFF_DATE, BASE_DATE, SCENARIO_NO, PD_RULE_ID, PD_METHOD, PD_RULE_NAME, MODEL_ID,
	                BUCKET_GROUP, BUCKET_ID, FL_SEQ, FL_YEAR, FL_MONTH, PD, PD_OVERRIDE, CREATEDBY, CREATEDDATE
	            ) VALUES (
	                rec.EFF_DATE, rec.BASE_DATE, rec.SCENARIO_NO, rec.PD_RULE_ID, rec.PD_METHOD, rec.PD_RULE_NAME,
	                rec.MODEL_ID, rec.BUCKET_GROUP, rec.BUCKET_FROM,
	                (rec.FL_SEQ * 12) + V_MONTH_SEQ - 1,
	                ((rec.FL_SEQ * 12) / 12) + 1,
	                MOD((rec.FL_SEQ * 12) + V_MONTH_SEQ - 1, 12) + 1,
	                V_MMULT_FIN, V_MMULT_FIN,
	                ''SP_IFRS_PD_TERM_STRUCTURE1'', SYSDATE
	            );
	
	            V_MONTH_SEQ := V_MONTH_SEQ + 1;
	        END LOOP;
	
	    END LOOP;
	END;';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
 
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;  
   
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE ||
                ' WHERE EFF_DATE = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')' ||
                ' AND (PD_RULE_ID IN (SELECT PKID FROM ' || V_TABLEPDCONFIG || ') )';

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

END