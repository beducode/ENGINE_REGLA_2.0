---- DROP PROCEDURE SP_GENERATE_SCHD_LIABILITIES;

CREATE OR REPLACE PROCEDURE SP_GENERATE_SCHD_LIABILITIES(
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
    V_TABLEINSERT VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    
    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);
    V_COUNT INT;

    ---- RESULT
    V_QUERYS TEXT;

    --- VARIABLE
    V_SP_NAME VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
    V_START INT;
    V_END INT;
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
        V_TABLEINSERT := 'IMA_SCHD_EXCLUDE_' || P_RUNID || '';
        V_TABLEINSERT1 := 'STG_DELTA_DEPOSITO_SCHEDULE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'SCHD_' || P_RUNID || '';
        V_TABLEINSERT3 := 'SCHD2_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IMA_SCHD_LIABILITIES_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IMA_SCHD_EXCLUDE';
        V_TABLEINSERT1 := 'STG_DELTA_DEPOSITO_SCHEDULE';
        V_TABLEINSERT2 := 'SCHD';
        V_TABLEINSERT3 := 'SCHD2';
        V_TABLEINSERT4 := 'IMA_SCHD_LIABILITIES';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM STG_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN



    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || ' (
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
            BUSS_DATE,            
            BRANCH,            
            LTRIM(RTRIM(DEAL_TYPE)) AS DEAL_TYPE,            
            LTRIM(RTRIM(DEAL_REF)) AS DEAL_REF,            
            CCY,            
            CASE WHEN MOVE_TYPE = ''P'' THEN COALESCE(AMOUNT_PAY, 0) END AS PRINCIPAL,            
            COUNT(1) AS COUNT,            
            ''DOUBLE SOURCE'' AS REMARKS,            
            ''SP_GENERATE_SCHD_LIABILITIES'' AS CREATEDBY
        FROM ' || V_TABLEINSERT1 || ' WITH(NOLOCK)
        WHERE MOVE_TYPE IN (''P'')
        AND MOVE_SUBTYPE IN (''M'')
        AND BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE --AND SCHEDULE_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        GROUP BY BUSS_DATE, BRANCH, DEAL_TYPE, DEAL_REF, CCY, MOVE_TYPE, COALESCE(AMOUNT_PAY, 0)
        HAVING COUNT(1) > 1
        ';
    EXECUTE (V_STR_QUERY);
    --END EXCEPTION DOUBLE SOURCE 

    -- INSERT STG_DELTA_LOAN_SCHEDULE TO TEMPTABLE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT 
            BUSS_DATE,            
            BRANCH,            
            LTRIM(RTRIM(DEAL_TYPE)) AS DEAL_TYPE,            
            LTRIM(RTRIM(DEAL_REF)) AS DEAL_REF,            
            SCHEDULE_DATE,            
            CCY,            
            COALESCE(AMOUNT_PAY, 0) AS AMOUNT_PAY,            
            CASE WHEN MOVE_TYPE = ''P'' THEN COALESCE(AMOUNT_PAY, 0) END AS PRINCIPAL,            
            CASE WHEN MOVE_TYPE = ''I'' THEN COALESCE(AMOUNT_PAY, 0) END AS INTEREST,            
            0 AS OSPRN,            
            STATUS
        FROM ' || V_TABLEINSERT1 || ' WITH(NOLOCK)
        WHERE MOVE_TYPE IN (''P'', ''I'')
        AND MOVE_SUBTYPE IN ('',''M'')
        AND BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE --AND SCHEDULE_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ORDER BY SCHEDULE_DATE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(1) FROM ' || V_TABLEINSERT2 || '';
    EXECUTE (V_STR_QUERY) INTO V_COUNT;

    -- Checking IF #SCHD Have Result, Then Go To Next Step      
    -- If no result then Stop

    IF V_COUNT > 0
    THEN

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT * INTO #' || V_TABLEINSERT3 || '
            FROM #' || V_TABLEINSERT4 || ' WITH(NOLOCK) WHERE 1=2 
        ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE #' || V_TABLEINSERT3 || '
            ADD PRODUCT_CODE VARCHAR(20)
        ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE #' || V_TABLEINSERT3 || '
            ADD BRANCH_CODE VARCHAR(20)
        ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'ALTER TABLE #' || V_TABLEINSERT3 || '
            ADD ACCOUNT_NUMBER VARCHAR(20)
        ';
        EXECUTE (V_STR_QUERY);

        -- Sorting SCHD By Counter
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT3 || ' 
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
            ) SELECT 
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE      
                ,DEAL_REF AS ACCOUNT_NUMBER      
                ,DEAL_REF AS MASTERID      
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS PMTDATE      
                ,NULL AS INTEREST_RATE      
                ,0 AS OSPRN      
                ,0 AS PRINCIPAL      
                ,0 AS INTEREST      
                ,0 AS COUNTER      
                ,''SP_GENERATE_SCHD_LIABILITIES'' AS SOURCE_PROCESS      
                ,DEAL_TYPE AS PRODUCT_CODE      
                ,BRANCH AS BRANCH_CODE
            FROM #' || V_TABLEINSERT2 || '
            GROUP BY DEAL_REF, DEAL_TYPE, BRANCH
            UNION ALL
            SELECT 
                BUSS_DATE AS DOWNLOAD_DATE      
                ,DEAL_REF AS ACCOUNT_NUMBER      
                ,DEAL_REF AS MASTERID      
                ,SCHEDULE_DATE AS PMTDATE      
                ,NULL AS INTEREST_RATE      
                ,MAX(COALESCE(OSPRN,0)) AS OSPRN      
                ,MAX(COALESCE(PRINCIPAL,0)) AS PRINCIPAL      
                ,MAX(COALESCE(INTEREST,0)) AS INTEREST      
                ,RANK () OVER (PARTITION BY BUSS_DATE, DEAL_REF ORDER BY SCHEDULE_DATE) AS COUNTER      
                ,''SCHD_LIABILITIES'' AS SOURCE_PROCESS      
                ,DEAL_TYPE AS PRODUCT_CODE      
                ,BRANCH AS BRANCH_CODE
            FROM #' || V_TABLEINSERT2 || '
            GROUP BY BUSS_DATE, BRANCH, DEAL_TYPE, DEAL_REF, SCHEDULE_DATE, CCY
            ) ';
        EXECUTE (V_STR_QUERY);

        -- Sum Up Principle to get OSPRN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT 
                DOWNLOAD_DATE            
                ,BRANCH_CODE      
                ,PRODUCT_CODE            
                ,ACCOUNT_NUMBER            
                ,SUM(PRINCIPAL) AS PRINCIPAL
            INTO #' || OSPRN || '
            FROM #' || V_TABLEINSERT3 || ' WITH(NOLOCK)      
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AND PMTDATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY DOWNLOAD_DATE, BRANCH_CODE, PRODUCT_CODE, ACCOUNT_NUMBER ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE A
            SET A.OSPRN = B.PRINCIPAL  
            FROM #' || V_TABLEINSERT3 || ' A
            JOIN #' || OSPRN || ' B
            ON A.MASTERID = B.ACCOUNT_NUMBER AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
            WHERE A.COUNTER = 0';
        EXECUTE (V_STR_QUERY);
        -- End Sum Up Principle to get OSPRN

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT
            DATA_SOURCE
            ,BRANCH_CODE
            ,PRODUCT_CODE
            ,CUSTOMER_NUMBER
            ,DOWNLOAD_DATE
            ,ACCOUNT_NUMBER
            ,INTEREST_RATE
            ,OUTSTANDING
            INTO #' || IMA || '
            FROM ' || IMA_LIABILITIES || ' WITH(NOLOCK)
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
        EXECUTE (V_STR_QUERY);

        -- Updating Additional Data From IMA 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE Y
            SET Y.OUTSTANDING = X.OUTSTANDING, Y.INTEREST_RATE = X.INTEREST_RATE
            FROM (SELECT OUTSTANDING, BRANCH_CODE, PRODUCT_CODE, CUSTOMER_NUMBER, DOWNLOAD_DATE, ACCOUNT_NUMBER, INTEREST_RATE 
                FROM #' || IMA || ') X
            JOIN (SELECT OUTSTANDING, DOWNLOAD_DATE, MASTERID, ACCOUNT_NUMBER, INTEREST_RATE, PRODUCT_CODE, BRANCH_CODE 
                FROM #' || V_TABLEINSERT3 || ' WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE) Y
            ON -- X.DOWNLOAD_DATE = EOMONTH(Y.DOWNLOAD_DATE) --COMMENT BY SAID SUDAH 1 TANGGAL  
            X.ACCOUNT_NUMBER = Y.ACCOUNT_NUMBER';
        EXECUTE (V_STR_QUERY);

        -- Sum Sisa Principal  
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT
            DOWNLOAD_DATE
            ,MASTERID
            ,SUM(PRINCIPAL) AS PRINCIPAL
            INTO #' || SCHD3 || '
            FROM #' || V_TABLEINSERT3 || '
            WHERE PMTDATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY DOWNLOAD_DATE, MASTERID';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT 0, MAX(COUNTER) 
            FROM #' || V_TABLEINSERT3 || '';  
        EXECUTE (V_STR_QUERY) INTO V_START, V_END;

        WHILE V_START <= V_END
        LOOP
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE Y
                SET Y.OSPRN = CASE WHEN Z.DOWNLOAD_DATE = Y.PMTDATE THEN Z.PRINCIPAL ELSE ABS(X.OSPRN - CASE WHEN ' || V_START || ' = 0 THEN X.PRINCIPAL ELSE 0 END - Y.PRINCIPAL) END
                FROM (SELECT * FROM #' || V_TABLEINSERT3 || ' WHERE COUNTER = ' || V_START || ') X
                JOIN (SELECT * FROM #' || V_TABLEINSERT3 || ' WHERE COUNTER = ' || V_START || ' + 1) Y
                ON X.DOWNLOAD_DATE = Y.DOWNLOAD_DATE AND X.MASTERID = Y.MASTERID
                LEFT JOIN #' || SCHD3 || ' Z ON Y.PMTDATE = Z.DOWNLOAD_DATE AND X.MASTERID = Z.MASTERID
                WHERE Y.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
            EXECUTE (V_STR_QUERY);

            V_START := V_START + 1;
            
        END LOOP;
        -- END UPDATING OSPRN  
        
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE'; 
        EXECUTE (V_STR_QUERY);
        
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (
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
            FROM #' || V_TABLEINSERT3 || '';     
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'EXEC (''USE IFRS9; dbcc shrinkfile (''''IFRS9_log'''', 0)'')';
        V_STR_QUERY := V_STR_QUERY || 'EXEC (''USE IFRS9_STG; dbcc shrinkfile (''''IFRS9_STG_log'''', 0)'')';
        EXECUTE (V_STR_QUERY);


    END IF;

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
	

    RAISE NOTICE 'SP_GENERATE_SCHD_LIABILITIES | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_GENERATE_SCHD_LIABILITIES';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======
	
	CALL SP_GENERATE_SCHD_LIABILITIES();
END;

$$;


