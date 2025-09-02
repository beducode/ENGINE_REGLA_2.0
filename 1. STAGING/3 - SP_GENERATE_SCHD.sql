---- DROP PROCEDURE SP_GENERATE_SCHD;

CREATE OR REPLACE PROCEDURE SP_GENERATE_SCHD(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_PRD_TYPE_VAL_CS VARCHAR(100);
    
    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;

    --- VARIABLE
    V_SP_NAME VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
BEGIN 
    -------- ====== VARIABLE ======
	GET DIAGNOSTICS STACK = PG_CONTEXT;
	FCESIG := substring(STACK from 'function (.*?) line');
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'IMA_SCHD_EXCLUDE_' || P_RUNID || '';
        V_TABLEINSERT1 := 'STG_DELTA_LOAN_SCHEDULE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IMA_SCHD_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IMA_SCHD_EXCLUDE';
        V_TABLEINSERT1 := 'STG_DELTA_LOAN_SCHEDULE';
        V_TABLEINSERT2 := 'IMA_SCHD';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            STG_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;

    IF V_CURRDATE IS NULL THEN 
        SELECT CURRDATE INTO V_CURRDATE 
        FROM IFRS_PRC_DATE;
    END IF;

    IF V_PREVDATE IS NULL THEN 
        SELECT PREVDATE INTO V_PREVDATE 
        FROM IFRS_PRC_DATE; 
    END IF;

    SELECT VALUE1 INTO V_PRD_TYPE_VAL_CS
    FROM TBLM_COMMONCODEDETAIL 
    WHERE COMMONCODE = 'PRD_TYPE_CS';
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLENAME || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLENAME || ' AS SELECT * FROM IMA_SCHD_EXCLUDE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLENAME_MON || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLENAME_MON || ' AS SELECT * FROM STG_DELTA_LOAN_SCHEDULE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
        
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IMA_SCHD WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLENAME || ' 
        (
            BUSS_DATE,  
            BRANCH,  
            DEAL_TYPE,  
            DEAL_REF,  
            CCY,  
            PRINCIPAL,  
            COUNT,  
            REMARKS,  
            CREATEDBY 
        ) SELECT 
            BUSS_DATE  
            ,BRANCH  
            ,DEAL_TYPE  
            ,DEAL_REF  
            ,CCY  
            ,CASE WHEN MOVE_TYPE = ''P'' THEN ISNULL(AMOUNT_RECEIVE, 0) END AS PRINCIPAL  
            ,COUNT(1) AS COUNT  
            ,''DOUBLE SOURCE'' AS REMARKS  
            ,''SP_GENERATE_SCHD'' AS CREATEDBY  
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE MOVE_TYPE IN (''P'') -- Just Take Principal and Interest
        AND MOVE_SUBTYPE IN (''C'') -- Just Take Repayment for Principal and Blank '' for Interest
        AND BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SCHEDULE_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCE_SYSTEM <> ''T24'' 
        GROUP BY BUSS_DATE, BRANCH, DEAL_TYPE, DEAL_REF, CCY, MOVE_TYPE, ISNULL(AMOUNT_RECEIVE, 0)  
        HAVING COUNT(1) > 1
        ';
    EXECUTE (V_STR_QUERY);
    --END EXCEPTION DOUBLE SOURCE

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  
        (
            BUSS_DATE  
            ,BRANCH  
            ,DEAL_TYPE  
            ,DEAL_REF  
            ,SCHEDULE_DATE  
            ,CCY  
            ,ISNULL(AMOUNT_PAY, 0) AMOUNT_PAY  
            ,CASE WHEN MOVE_TYPE = ''P'' THEN ISNULL(AMOUNT_RECEIVE, 0) END AS PRINCIPAL  
            ,CASE WHEN MOVE_TYPE = ''I'' THEN ISNULL(AMOUNT_RECEIVE, 0) END AS INTEREST  
            ,CASE WHEN MOVE_SUBTYPE IN (''R'') THEN ISNULL(AMOUNT_PAY, 0) END AS OSPRN  
            ,STATUS  
        )
        INTO #SCHD
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE ISNULL(MOVE_TYPE,'') IN (''P'', ''I'') -- Just Take Principal and Interest
        AND ISNULL(MOVE_SUBTYPE,'') IN (''R'', '') -- Just Take Repayment for Principal and Blank '' for Interest
        AND BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SCHEDULE_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCE_SYSTEM <> ''T24''  
        ORDER BY SCHEDULE_DATE
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(1) FROM #SCHD';
    EXECUTE (V_STR_QUERY);

    IF(COUNT > 0)
    BEGIN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT * INTO #SCHD2' || V_TABLEINSERT2 || ' WITH(NOLOCK) WHERE 1=2
            WHERE BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND CREATEDBY = ''SP_GENERATE_SCHD'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE #SCHD2' || ' 
            ADD PRODUCT_CODE VARCHAR(20) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE #SCHD2' || ' 
            ADD BRANCH_CODE VARCHAR(20) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE #SCHD2' || ' 
            ADD ACCOUNT_NUMBER VARCHAR(20) ';
        EXECUTE (V_STR_QUERY);
        
        -- Sorting SCHD By Counter
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || SCHD2 || '   
            (
                DOWNLOAD_DATE  
                ,ACCOUNT_NUMBER  
                ,MASTERID  
                ,PMTDATE  
                ,INTEREST_RATE  
                ,OSPRN  
                ,PRINCIPAL  
                ,INTEREST  
                ,COUNTER  
                ,SOURCE_PROCESS  
                ,PRODUCT_CODE  
                ,BRANCH_CODE  
            )
            SELECT DISTINCT  
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE  
                ,DEAL_REF AS ACCOUNT_NUMBER  
                ,DEAL_TYPE || ''_'' || DEAL_REF || ''_'' || BRANCH AS MASTERID  
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS PMTDATE  
                ,NULL AS INTEREST_RATE  
                ,0 AS OSPRN  
                ,0 AS PRINCIPAL  
                ,0 AS INTEREST  
                ,0 AS COUNTER  
                ,''SP_GENERATE_SCHD'' AS SOURCE_PROCESS  
                ,DEAL_TYPE AS PRODUCT_CODE  
                ,BRANCH AS BRANCH_CODE
            FROM #SCHD
            GROUP BY DEAL_REF, DEAL_TYPE, BRANCH
            UNION ALL
            SELECT  
                BUSS_DATE AS DOWNLOAD_DATE  
                ,DEAL_REF AS ACCOUNT_NUMBER  
                ,DEAL_TYPE || ''_'' || DEAL_REF || ''_'' || BRANCH AS MASTERID  
                ,SCHEDULE_DATE AS PMTDATE  
                ,NULL AS INTEREST_RATE  
                ,MAX(ISNULL(OSPRN,0)) AS OSPRN  
                ,MAX(ISNULL(PRINCIPAL,0)) AS PRINCIPAL  
                ,MAX(ISNULL(INTEREST,0)) AS INTEREST  
                ,RANK () OVER (PARTITION BY BUSS_DATE, DEAL_TYPE || ''_'' || DEAL_REF || ''_'' || BRANCH ORDER BY SCHEDULE_DATE) AS COUNTER  
                ,''SCHD'' AS SOURCE_PROCESS  
                ,DEAL_TYPE AS PRODUCT_CODE  
                ,BRANCH AS BRANCH_CODE
            FROM #SCHD
            GROUP BY BUSS_DATE, BRANCH, DEAL_TYPE, DEAL_REF, SCHEDULE_DATE
            ';
        EXECUTE (V_STR_QUERY);
        
        -- Sum Up Principle to get OSPRN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT ' || '   
            (
                DOWNLOAD_DATE  
                ,BRANCH_CODE  
                ,PRODUCT_CODE  
                ,ACCOUNT_NUMBER  
                ,SUM(PRINCIPAL) AS PRINCIPAL  
            )
            INTO #OSPRN
            FROM #SCHD2 WITH(NOLOCK)  
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND PMTDATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY DOWNLOAD_DATE, BRANCH_CODE, PRODUCT_CODE, ACCOUNT_NUMBER
            ';
        EXECUTE (V_STR_QUERY);
        
        -- Sum Up Principle to get OSPRN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || ' A
            SET OSPRN = B.PRINCIPAL
            FROM #SCHD2 A
            JOIN #OSPRN B 
            ON A.MASTERID = B.PRODUCT_CODE || ''_'' || B.ACCOUNT_NUMBER || ''_'' || B.BRANCH_CODE
            AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
            WHERE A.COUNTER = 0
            ';
        EXECUTE (V_STR_QUERY);
        -- End Sum Up Principle to get OSPRN 

        -- Sum Up Principle to get OSPRN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT ' || '(
            DATA_SOURCE
            ,BRANCH_CODE
            ,PRODUCT_CODE
            ,COSTUMER_NUMBER
            , ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,ACCOUNT_NUMBER
            ,INTEREST_RATE
            ,OUTSTANDING
            ,PLAFOND
            --DOWNLOAD_DATE, ACCOUNT_NUMBER, INTEREST_RATE, OUTSTANDING, PLAFOND  
            INTO #' || IMA || '
            FROM ' || IMA_LENDING || ' WITH(NOLOCK)
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND ACCOUNT_STATUS = ''A''
            ';
        EXECUTE (V_STR_QUERY);

         -- Updating Additional Data From IMA 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'Y
            SET 
                Y.OUTSTANDING = X.OUTSTANDING
                ,Y.PLAFOND = X.PLAFOND
                ,Y.INTEREST_RATE = X.INTEREST_RATE
                ,Y.MASTERID = X.CUSTOMER_NUMBER + ''_'' + Y.ACCOUNT_NUMBER + ''_'' + X.PRODUCT_CODE 
            FROM (
                SELECT OUTSTANDING, PLAFOND, BRANCH_CODE, PRODUCT_CODE, CUSTOMER_NUMBER, DOWNLOAD_DATE, ACCOUNT_NUMBER, INTEREST_RATE 
                FROM #' || IMA || '
            ) X
            JOIN (
                OUTSTANDING, PLAFOND, MASTERID, DOWNLOAD_DATE, ACCOUNT_NUMBER, INTEREST_RATE, PRODUCT_CODE, BRANCH_CODE 
                FROM #' || SCHD2 || '
                WHERE DOWNLAOD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ) Y
            ON X.PRODUCT_CODE + ''_'' + X.ACCOUNT_NUMBER + ''_'' + X.BRANCH_CODE = Y.MASTERID + ''_'' + Y.ACCOUNT_NUMBER + ''_'' + Y.BRANCH_CODE
            ';
        EXECUTE (V_STR_QUERY);

        -- Sum Sisa Principal 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT ' || '(
            DOWNLOAD_DATE
            ,MASTERID
            ,SUM(PRINCIPAL) AS PRINCIPAL
            )
            INTO #' || SCHD3 || '
            FROM #' || SCHD2 || '
            WHERE PMTDATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY DOWNLOAD_DATE, MASTERID
            ';
        EXECUTE (V_STR_QUERY);

        -- START UPDATING OSPRN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DECLARE ' || ' @START INT, @END INT ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT @START = 0, @END = MAX(COUNTER) FROM #' || SCHD2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'WHILE @START <= @END 
            BEGIN 
            UPDATE Y SET OSPRN = CASE WHEN Z.DOWNLOAD_DATE = Y.PMTDATE THEN Z.PRINCIPAL ELSE X.OSPRN - CASE WHEN @START = 0 THEN X.PRINCIPAL ELSE 0 END - Y.PRINCIPAL END
            FROM (
                SELECT * FROM #' || SCHD2 || ' WHERE COUNTER = @start
            ) X
            JOIN (
                SELECT * FROM #' || SCHD2 || ' WHERE COUNTER = @start + 1
            ) Y
            ON X.DOWNLOAD_DATE = Y.DOWNLOAD_DATE AND X.MASTERID = Y.MASTERID
            LEFT JOIN #' || SCHD3 || ' Z ON Y.PMTDATE = Z.DOWNLOAD_DATE AND X.MASTERID = Z.MASTERID
            WHERE Y.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SET @START = @START + 1 ';
        EXECUTE (V_STR_QUERY);

        -- END UPDATING OSPRN 
    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || '   
            (
                DOWNLOAD_DATE  
                ,MASTERID  
                ,PMTDATE  
                ,INTEREST_RATE  
                ,OSPRN  
                ,PRINCIPAL  
                ,INTEREST   
                ,PLAFOND      
                ,COUNTER      
                ,OUTSTANDING  
                ,SOURCE_PROCESS 
            )
            SELECT 
                DOWNLOAD_DATE  
                ,MASTERID  
                ,PMTDATE  
                ,INTEREST_RATE  
                ,OSPRN  
                ,PRINCIPAL  
                ,INTEREST  
                ,PLAFOND       
                ,COUNTER      
                ,OUTSTANDING  
                ,SOURCE_PROCESS  
            FROM #' || SCHD2 || ' 
            ';
        EXECUTE (V_STR_QUERY);

    END IF;

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_GENERATE_SCHD | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_GENERATE_SCHD';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;