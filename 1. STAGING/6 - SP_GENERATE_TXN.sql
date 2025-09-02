---- DROP PROCEDURE SP_GENERATE_TXN;

CREATE OR REPLACE PROCEDURE SP_GENERATE_TXN(
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
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TMPTABLE VARCHAR(100);
    
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
        V_TABLEINSERT := 'IMA_LIABILITIES_' || P_RUNID || '';
        V_TABLEINSERT1 := 'TXN_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IMA_TXN_' || P_RUNID || '';
        V_TABLEINSERT3 := 'NCI_IMA_TXN_' || P_RUNID || '';
        V_TABLEINSERT4 := 'STG_ADDINFO_AMORTIZE_' || P_RUNID || '';
        V_TABLEINSERT5 := 'STG_FEE_COST_JFMF_' || P_RUNID || '';
        V_TABLEINSERT6 := 'TBL_MASTER_EXCHANGE_RATE_' || P_RUNID || '';
        V_TABLEINSERT7 := 'STG_LOAN_OPEN_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IMA_LIABILITIES';
        V_TABLEINSERT1 := 'TXN';
        V_TABLEINSERT2 := 'IMA_TXN';
        V_TABLEINSERT3 := 'NCI_IMA_TXN';
        V_TABLEINSERT4 := 'STG_ADDINFO_AMORTIZE';
        V_TABLEINSERT5 := 'STG_FEE_COST_JFMF';
        V_TABLEINSERT6 := 'TBL_MASTER_EXCHANGE_RATE';
        V_TABLEINSERT7 := 'STG_LOAN_OPEN';
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

    IF 

    IF V_CURRDATE IS NULL THEN 
        SELECT CURRDATE INTO V_CURRDATE 
        FROM IFRS_PRC_DATE;
    END IF;

    IF V_PREVDATE IS NULL THEN 
        SELECT PREVDATE INTO V_PREVDATE 
        FROM IFRS_PRC_DATE;
    END IF;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT || ' AS SELECT * FROM IMA_LIABILITIES WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT * INTO #' || V_TABLEINSERT1 || '
        FROM ' || V_TABLEINSERT2 || '
        WHERE 1 = 2 
    ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE NONCLUSTERED INDEX #' || V_TABLEINSERT3 || ' ON #' || V_TABLEINSERT1 || ' (DOWNLOAD_DATE ASC, SOURCE_TABLE ASC) 
    WITH (PAD_INDEX = OFF, ALLOW_PAGE_LOCKS = ON, ALLOW_ROW_LOCKS = ON, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, SORT_IN_TEMPDB = OFF, FILLFACTOR =100) ON PRIMARY
    ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT1 || ' (
        DOWNLOAD_DATE        
        ,EFFECTIVE_DATE        
        ,MATURITY_DATE        
        ,MASTERID        
        ,ACCOUNT_NUMBER        
        ,FACILITY_NUMBER        
        ,CUSTOMER_NUMBER        
        ,BRANCH_CODE        
        ,DATA_SOURCE        
        ,PRD_TYPE        
        ,PRD_CODE        
        ,TRX_CODE        
        ,CCY        
        ,EVENT_CODE        
        ,TRX_REFERENCE_NUMBER        
        ,ORG_CCY_AMT        
        ,EQV_LCY_AMT        
        ,DEBET_CREDIT_FLAG        
        ,TRX_SOURCE        
        ,INTERNAL_NO        
        ,REVOLVING_FLAG        
        ,CREATED_DATE        
        ,SOURCE_TABLE        
        ,TRX_LEVEL 
    )
    SELECT 
         A.BUSS_DATE AS DOWNLOAD_DATE       
        ,A.BUSS_DATE AS EFFECTIVE_DATE       
        ,A.BUSS_DATE AS MATURITY_DATE       
        ,A.CIF + ''_'' + A.DEAL_REF + ''_'' + A.DEAL_TYPE AS MASTERID       
        ,A.DEAL_REF AS ACCOUNT_NUMBER         
        ,NULL AS FACILITY_NUMBER       
        ,A.CIF AS CUSTOMER_NUMBER       
        ,A.BRANCH AS BRANCH_CODE       
        ,''LOAN'' AS DATA_SOURCE       
        ,NULL AS PRD_TYPE       
        ,A.DEAL_TYPE AS PRD_CODE       
        ,T_CODE AS TRX_CODE       
        ,A.CCY AS CCY       
        ,NULL AS EVENT_CODE       
        ,NULL AS TRX_REFERENCE_NUMBER       
        ,NILAI_AWAL AS ORG_CCY_AMT       
        ,NILAI_AWAL * ISNULL(B.SPOT_RATE,1) AS EQV_LCY_AMT       
        ,NULL AS DEBET_CREDIT_FLAG       
        ,NULL AS TRX_SOURCE       
        ,NULL AS INTERNAL_NO       
        ,NULL AS REVOLVING_FLAG       
        ,GETDATE() AS CREATED_DATE       
        ,''STG_ADDINFO_AMORTIZE'' AS SOURCE_TABLE       
        ,NULL AS TRX_LEVEL
    FROM ' || V_TABLEINSERT4 || ' A
    LEFT JOIN '|| V_TABLEINSERT6 || ' B
    ON A.CCY = B.CCY_CODE AND A.BUSS_DATE = B.BUSS_DATE
    --INNER JOIN ' || V_TABLEINSERT7 || ' C ON A.CIF = C.CIF AND A.DEAL_REF = C.DEAL_REF AND A.DEAL_TYPE = C.DEAL_TYPE AND A.BUSS_DATE = C.BUSS_DATE
    WHERE A.BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    AND A.START_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE

    ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT1 || ' (
        DOWNLOAD_DATE    
        ,EFFECTIVE_DATE    
        ,MATURITY_DATE    
        ,MASTERID    
        ,ACCOUNT_NUMBER    
        ,FACILITY_NUMBER    
        ,CUSTOMER_NUMBER    
        ,BRANCH_CODE    
        ,DATA_SOURCE    
        ,PRD_TYPE    
        ,PRD_CODE    
        ,TRX_CODE    
        ,CCY    
        ,EVENT_CODE    
        ,TRX_REFERENCE_NUMBER    
        ,ORG_CCY_AMT    
        ,EQV_LCY_AMT    
        ,DEBET_CREDIT_FLAG    
        ,TRX_SOURCE    
        ,INTERNAL_NO    
        ,REVOLVING_FLAG    
        ,CREATED_DATE    
        ,SOURCE_TABLE    
        ,TRX_LEVEL
    )
    SELECT 
        A.BUSS_DATE AS DOWNLOAD_DATE    
        ,A.BUSS_DATE AS EFFECTIVE_DATE    
        ,A.BUSS_DATE AS MATURITY_DATE    
        ,A.CIF + ''_'' + A.DEAL_REF + ''_'' + A.DEAL_TYPE AS MASTERID   
        ,A.DEAL_REF AS ACCOUNT_NUMBER     
        ,NULL AS FACILITY_NUMBER    
        ,A.CIF AS CUSTOMER_NUMBER    
        ,A.BRANCH AS BRANCH_CODE    
        ,''LOAN'' AS DATA_SOURCE    
        ,NULL AS PRD_TYPE    
        ,A.DEAL_TYPE AS PRD_CODE  
        ,T_CODE AS TRX_CODE    
        ,A.CCY AS CCY    
        ,NULL AS EVENT_CODE    
        ,NULL AS TRX_REFERENCE_NUMBER    
        ,NILAI_AWAL AS ORG_CCY_AMT    
        ,NILAI_AWAL * ISNULL(B.SPOT_RATE,1) AS EQV_LCY_AMT    
        ,NULL AS DEBET_CREDIT_FLAG    
        ,NULL AS TRX_SOURCE    
        ,NULL AS INTERNAL_NO    
        ,NULL AS REVOLVING_FLAG    
        ,GETDATE() AS CREATED_DATE    
        ,' || V_TABLEINSERT5 || ' AS SOURCE_TABLE    
        ,NULL AS TRX_LEVEL 
    FROM ' || V_TABLEINSERT5 || ' A
    LEFT JOIN '|| V_TABLEINSERT6 || ' B
    ON A.CCY = B.CCY_CODE AND A.BUSS_DATE = B.BUSS_DATE
    INNER JOIN ' || V_TABLEINSERT7 || ' C ON A.CIF = C.CIF AND A.DEAL_REF = C.DEAL_REF AND A.DEAL_TYPE = C.DEAL_TYPE AND A.BUSS_DATE = C.BUSS_DATE
    WHERE A.BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    -- ND A.START_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE

    ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND SOURCE_TABLE IN (' || V_TABLEINSERT4  ||', '|| V_TABLEINSERT5 ||')
    ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' (
        DOWNLOAD_DATE       
        ,EFFECTIVE_DATE       
        ,MATURITY_DATE       
        ,MASTERID       
        ,ACCOUNT_NUMBER       
        ,FACILITY_NUMBER       
        ,CUSTOMER_NUMBER       
        ,BRANCH_CODE       
        ,DATA_SOURCE       
        ,PRD_TYPE       
        ,PRD_CODE       
        ,TRX_CODE       
        ,CCY       
        ,EVENT_CODE       
        ,TRX_REFERENCE_NUMBER       
        ,ORG_CCY_AMT       
        ,EQV_LCY_AMT       
        ,DEBET_CREDIT_FLAG       
        ,TRX_SOURCE       
        ,INTERNAL_NO       
        ,REVOLVING_FLAG       
        ,CREATED_DATE       
        ,SOURCE_TABLE       
        ,TRX_LEVEL
    )
    SELECT 
         DOWNLOAD_DATE       
        ,EFFECTIVE_DATE       
        ,MATURITY_DATE       
        ,MASTERID       
        ,ACCOUNT_NUMBER       
        ,FACILITY_NUMBER       
        ,CUSTOMER_NUMBER       
        ,BRANCH_CODE       
        ,DATA_SOURCE       
        ,PRD_TYPE       
        ,PRD_CODE       
        ,TRX_CODE       
        ,CCY       
        ,EVENT_CODE       
        ,TRX_REFERENCE_NUMBER       
        ,ORG_CCY_AMT       
        ,EQV_LCY_AMT       
        ,DEBET_CREDIT_FLAG       
        ,TRX_SOURCE       
        ,INTERNAL_NO       
        ,REVOLVING_FLAG       
        ,CREATED_DATE       
        ,SOURCE_TABLE       
        ,TRX_LEVEL
    FROM #' || V_TABLEINSERT1 || '';
    EXECUTE (V_STR_QUERY);
    
    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_GENERATE_TXN | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_GENERATE_TXN';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;