---- DROP PROCEDURE SP_GENERATE_IMA_WO;

CREATE OR REPLACE PROCEDURE SP_GENERATE_IMA_WO(
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
    V_TABLEINSERT8 VARCHAR(100);
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    V_TABLEINSERT11 VARCHAR(100);
    V_TABLEINSERT12 VARCHAR(100);
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
        V_TABLEINSERT := 'IMA_WO_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IMA_WO_RECOVERY_' || P_RUNID || '';
        V_TABLEINSERT2 := 'WO_CRMS_' || P_RUNID || '';
        V_TABLEINSERT3 := 'STG_M_WO_RMS_' || P_RUNID || '';
        V_TABLEINSERT4 := 'STG_M_WO_CRMS_' || P_RUNID || '';
        V_TABLEINSERT5 := 'STG_M_WO_CZ_' || P_RUNID || '';
        V_TABLEINSERT6 := 'WO_CZ_' || P_RUNID || '';
        V_TABLEINSERT7 := 'STG_CIF_' || P_RUNID || '';
        V_TABLEINSERT8 := 'WO_CZ_PREV_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IMA_AN_' || P_RUNID || '';
        V_TABLEINSERT10 := 'WO_CZ_PREV_' || P_RUNID || '';
        V_TABLEINSERT11 := 'WO_CRMS_PREV_' || P_RUNID || '';
        V_TABLEINSERT12 := 'WO_RMS_PREV_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IMA_WO';
        V_TABLEINSERT1 := 'IMA_WO_RECOVERY';
        V_TABLEINSERT2 := 'WO_CRMS';
        V_TABLEINSERT3 := 'STG_M_WO_RMS';
        V_TABLEINSERT4 := 'STG_M_WO_CRMS';
        V_TABLEINSERT5 := 'STG_M_WO_CZ';
        V_TABLEINSERT6 := 'WO_CZ';
        V_TABLEINSERT7 := 'STG_CIF';
        V_TABLEINSERT8 := 'WO_CZ_PREV';
        V_TABLEINSERT9 := 'IMA_AN';
        V_TABLEINSERT10 := 'WO_CZ_PREV';
        V_TABLEINSERT11 := 'WO_CRMS_PREV';
        V_TABLEINSERT12 := 'WO_RMS_PREV';
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

    /**
    EFS MIGRATION
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  * 
        INTO #' || V_TABLEINSERT2 || '
        FROM ' || V_TABLEINSERT3 || '
        WHERE EOMONTH(BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND START_DATE_WO > ''31 DEC 2011' || ' 
        AND START_DATE_WO <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  * 
        INTO #' || V_TABLEINSERT2 || '
        FROM ' || V_TABLEINSERT4 || '
        WHERE EOMONTH(BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND START_DATE_WO > ''31 DEC 2011' || ' 
        AND START_DATE_WO <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);

    -- CR CREDIT CARD JENIUS -- 
    ------ BEFORE ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  * 
        INTO #' || V_TABLEINSERT2 || '
        FROM ' || V_TABLEINSERT5 || '
        WHERE EOMONTH(BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);
    ------ END
    **/

    ------ AFTER ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT BUSS_DATE
        ,ACCOUNT_KEY
        ,PERSON_KEY
        ,CIF
        ,COALESCE(TRY_CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE, 103),CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE)) AS ACCOUNTING_MODULE_ENTRY_DATE
        ,DAYS_IN_ACCOUNTING_MODULE
        ,INITIAL_DPD
        ,INITIAL_WRITEOFF_CAPITAL
        ,INITIAL_WRITEOFF_INTEREST
        ,INITIAL_WRITEOFF_EXPENSES
        ,COLLECTABILITY_BEFORE_WRITEOFF
        ,CURRENT_BRANCH
        ,PRODUCT_KEY
        ,CURRENCY
        ,CURRENT_CAPITAL
        ,CURRENT_INTEREST
        ,CURRENT_EXPENSES
        ,TOTAL_RECOVERIES
        ,WRITE_OFF_STATUS
        ,VAM_NUMBER
        ,COALESCE(TRY_CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE, 103),CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE)) AS LAST_PAYMENT_DATE
        ,LAST_PAYMENT_AMOUNT
        ,SECTOR_OF_ECONOMY
        ,RM_CODE
        ,COST_CENTER
        ,COMMITMENT_REF
        ,SOURCE_SYSTEM
        ,YTD_RECOVERY_AMOUNT
        ,YTD_RECOVERY_CAPITAL
        ,YTD_RECOVERY_INTEREST
        ,YTD_RECOVERY_EXPENSES
        INTO #' || V_TABLEINSERT2 || '
        FROM ' || V_TABLEINSERT5 || '
        WHERE EOMONTH(BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);

    ------ END AFTER ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE
    --END --

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    ';EXECUTE (V_STR_QUERY
    );

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    ';EXECUTE (V_STR_QUERY
    );

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    ';EXECUTE (V_STR_QUERY
    );

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT * INTO #' || V_TABLEINSERT || '
        FROM ' || V_TABLEINSERT || '
        WHERE 1 - 2
    ';EXECUTE (V_STR_QUERY
    );

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT * INTO #' || V_TABLEINSERT1 || '
        FROM ' || V_TABLEINSERT1 || '
        WHERE 1 - 2
    ';EXECUTE (V_STR_QUERY
    );

    ------ END AFTER ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT * INTO #' || V_TABLEINSERT9 || '
        FROM ' || IMA_LENDING || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    ';EXECUTE (V_STR_QUERY
    );

    ----------------- WO SECTION ------------------ 
    /** EFS MIGRATION **/ 
    ---------- INSERT WO ACCOUNT FROM TABLE WO RMS-----  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS               
            ,BRANCH_CODE              
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE               
            ,DAY_PAST_DUE              
            ,EXCHANGE_RATE                
            ,LOAN_DUE_DATE               
            ,LOAN_START_DATE               
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PLAFOND              
            ,PRODUCT_CODE              
            ,TENOR              
            ,WRITEOFF_DATE                
            ,WRITEOFF_FLAG                
            ,SOURCE_TABLE              
            ,PRODUCT_ENTITY              
            ,SUFFIX              
            ,CO_FLAG 
        ) SELECT 
            EOMONTH(START_DATE_WO) AS DOWNLOAD_DATE               
            ,A.DEAL_REF AS ACCOUNT_NUMBER               
            ,''W'' AS ACCOUNT_STATUS              
            ,A.BRANCH AS BRANCH_CODE                
            ,''IDR'' AS CURRENCY               
            ,A.CUST_FULL_NAME AS CUSTOMER_NAME               
            ,A.CIF AS CUSTOMER_NUMBER               
            ,''LOAN'' AS DATA_SOURCE              
            ,A.TOTAL_DAYS_PASTDUE AS DAY_PAST_DUE              
            ,1 AS EXCHANGE_RATE                
            ,A.MATURITY_DATE AS LOAN_DUE_DATE               
            ,A.ACCOUNT_LOAN_OPEN AS LOAN_START_DATE              
            ,CONCAT(A.CIF, ''_'', A.DEAL_REF, ''_'', A.DEAL_TYPE) AS MASTERID              
            ,''5'' AS BI_COLLECTABILITY               
            ,A.AMOUNT_WO  AS OUTSTANDING_WO              
            ,A.PLAFOND AS PLAFOND              
            ,A.DEAL_TYPE AS PRODUCT_CODE           
            ,A.TENOR AS TENOR              
            ,START_DATE_WO AS WRITEOFF_DATE                
            ,1 AS WRITEOFF_FLAG                
            ,''STG_M_WO_RMS'' AS SOURCE_TABLE              
            ,''C'' AS PRODUCT_ENTITY              
            ,SUFIX AS SUFFIX              
            ,FLAG_HAPUSTAGIH AS CO_FLAG 
        FROM #' || WO_RMS || ' A (NOCLOCK)
        WHERE EOMONTH(CAST(A.START_DATE_WO) AS DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);

    ---------- INSERT WO ACCOUNT FROM TABLE WO CRMS-----  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE                
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE           
            ,EXCHANGE_RATE                
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PRODUCT_CODE            
            ,WRITEOFF_DATE               
            ,WRITEOFF_FLAG                
            ,SOURCE_TABLE              
            ,PRODUCT_ENTITY              
            ,SUFFIX              
            ,CO_FLAG 
        ) SELECT 
            EOMONTH(START_DATE_WO) AS DOWNLOAD_DATE               
            ,A.DEAL_REF AS ACCOUNT_NUMBER               
            ,''W'' AS ACCOUNT_STATUS              
            ,A.BRANCH AS BRANCH_CODE                
            ,''IDR'' AS CURRENCY               
            ,A.CUST_FULL_NAME AS CUSTOMER_NAME               
            ,A.CIF AS CUSTOMER_NUMBER               
            ,''LOAN'' AS DATA_SOURCE               
            ,1 AS EXCHANGE_RATE                
            ,A.MATURITY_DATE AS LOAN_DUE_DATE               
            ,A.ACCOUNT_LOAN_OPEN AS LOAN_START_DATE              
            ,CONCAT(A.CIF, ''_'', A.DEAL_REF, ''_'', A.DEAL_TYPE) AS MASTERID              
            ,''5'' AS BI_COLLECTABILITY               
            ,A.AMOUNT_WO  AS OUTSTANDING_WO              
            ,A.DEAL_TYPE AS PRODUCT_CODE           
            ,START_DATE_WO AS WRITEOFF_DATE                
            ,1 AS WRITEOFF_FLAG                
            ,''STG_M_WO_CRMS'' AS SOURCE_TABLE              
            ,''C'' AS PRODUCT_ENTITY              
        FROM #' || WO_RMS || ' A (NOCLOCK)
        WHERE EOMONTH(CAST(A.START_DATE_WO) AS DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);

    /* --CREDIT CARD JENIUS --*/
    ----- INSERT WO ACCOUNT FROM TABLE WO CZ----- 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE                
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE            
            ,EXCHANGE_RATE                
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PRODUCT_CODE            
            ,WRITEOFF_DATE               
            ,WRITEOFF_FLAG                
            ,SOURCE_TABLE              
            ,PRODUCT_ENTITY              
            ,SUFFIX              
            ,CO_FLAG 
        ) SELECT 
            EOMONTH(A.ACCOUNTING_MODULE_ENTRY_DATE) AS DOWNLOAD_DATE    
            ,SUBSTRING(A.ACCOUNT_KEY,7,13) AS ACCOUNT_NUMBER              
            ,''W'' AS ACCOUNT_STATUS              
            ,A.CURRENT_BRANCH AS BRANCH_CODE            
            ,''IDR'' AS CURRENCY              
            ,B.CUST_FULL_NAME AS CUSTOMER_NAME    
            ,A.CIF AS CUSTOMER_NUMBER              
            ,''LOAN'' AS DATA_SOURCE               
            ,1 AS EXCHANGE_RATE              
            ,CONCAT(A.CIF,''_'',SUBSTRING(A.ACCOUNT_KEY,7,13),''_'',A.PRODUCT_KEY) AS MASTERID
            ,''5'' AS BIL_COLLECTABILITY               
            ,A.INITIAL_WRITEOFF_CAPITAL  AS OUTSTANDING_WO              
            ,A.PRODUCT_KEY AS PRODUCT_CODE          
            ,A.ACCOUNTING_MODULE_ENTRY_DATE AS WRITEOFF_DATE  
            ,1 AS WRITEOFF_FLAG                
            ,''STG_M_WO_CZ'' AS SOURCE_TABLE              
            ,''C'' AS PRODUCT_ENTITY              
            ,'''' AS SUFIX           
            ,0 AS CO_FLAG  
        FROM #' || V_TABLEINSERT6 || ' A (NOCLOCK)
        LEFT JOIN #' || V_TABLEINSERT7 || ' B ON A.BUSS_DATE = B.BUSS_DATE AND A.CIF = B.CIF
        WHERE EOMONTH( CAST(A.ACCOUNTING_MODULE_ENTRY_DATE) AS DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);
    --END--

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE #' || V_TABLEINSERT || ' 
        WHERE MASTERID IN (
            SELECT MASTERID 
            FROM #' || V_TABLEINSERT || ' 
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE                
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE            
            ,EXCHANGE_RATE                
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PRODUCT_CODE            
            ,WRITEOFF_DATE               
            ,WRITEOFF_FLAG                
            ,SOURCE_TABLE              
            ,PRODUCT_ENTITY              
            ,SUFFIX              
            ,CO_FLAG 
        ) SELECT 
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE              
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE              
            ,DAY_PAST_DUE              
            ,EXCHANGE_RATE                
            ,LOAN_DUE_DATE           
            ,LOAN_START_DATE               
            ,MASTERID                
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PLAFOND              
            ,LTRIM(RTRIM(PRODUCT_CODE)) AS PRODUCT_CODE             
            ,TENOR              
            ,WRITEOFF_DATE                
            ,WRITEOFF_FLAG                
            ,SOURCE_TABLE              
            ,PRODUCT_ENTITY              
            ,SUFFIX              
            ,1 AS NPL_FLAG              
            ,CO_FLAG  
        FROM #' || V_TABLEINSERT || ' 
        ) ';
    EXECUTE (V_STR_QUERY);
    ----------------- END WO SECTION ------------------

    /**
    EFS MIGRATION
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  * 
        INTO #' || V_TABLEINSERT12 || '
        FROM ' || V_TABLEINSERT3 || '
        WHERE EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  * 
        INTO #' || V_TABLEINSERT11 || '
        FROM ' || V_TABLEINSERT4 || '
        WHERE EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE
        ';
    EXECUTE (V_STR_QUERY);

    -- CREDIT CARD JENIUS WO RECOVERY--
    ------ BEFORE ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT  * 
        INTO #' || V_TABLEINSERT10 || '
        FROM ' || V_TABLEINSERT5 || '
        WHERE EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE
        ';
    EXECUTE (V_STR_QUERY);
    ------ END BEFORE ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE   
    **/

    ------ AFTER ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT BUSS_DATE
        ,ACCOUNT_KEY
        ,PERSON_KEY
        ,CIF
        ,COALESCE(TRY_CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE, 103),CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE)) AS ACCOUNTING_MODULE_ENTRY_DATE
        ,DAYS_IN_ACCOUNTING_MODULE
        ,INITIAL_DPD
        ,INITIAL_WRITEOFF_CAPITAL
        ,INITIAL_WRITEOFF_INTEREST
        ,INITIAL_WRITEOFF_EXPENSES
        ,COLLECTABILITY_BEFORE_WRITEOFF
        ,CURRENT_BRANCH
        ,PRODUCT_KEY
        ,CURRENCY
        ,CURRENT_CAPITAL
        ,CURRENT_INTEREST
        ,CURRENT_EXPENSES
        ,TOTAL_RECOVERIES
        ,WRITE_OFF_STATUS
        ,VAM_NUMBER
        ,COALESCE(TRY_CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE, 103),CONVERT(DATE, ACCOUNTING_MODULE_ENTRY_DATE)) AS LAST_PAYMENT_DATE
        ,LAST_PAYMENT_AMOUNT
        ,SECTOR_OF_ECONOMY
        ,RM_CODE
        ,COST_CENTER
        ,COMMITMENT_REF
        ,SOURCE_SYSTEM
        ,YTD_RECOVERY_AMOUNT
        ,YTD_RECOVERY_CAPITAL
        ,YTD_RECOVERY_INTEREST
        ,YTD_RECOVERY_EXPENSES
        INTO #' || V_TABLEINSERT2 || '
        FROM ' || V_TABLEINSERT5 || '
        WHERE EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE
        ';
    EXECUTE (V_STR_QUERY);
    ------ END AFTER ADJUSTMENT FIELD ACCOUNTING_MODULE_ENTRY_DATE & LAST_PAYMENT_DATE ALTER TO VARCHAR(8) FROM DATE

    /** EFS MIGRATION 
    ------ INSERT WO RECOVERY ACCOUNT FROM TABLE WO RMS-----
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT1 || '   (               
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS               
            ,BRANCH_CODE              
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE               
            ,DAY_PAST_DUE              
            ,EXCHANGE_RATE                
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PLAFOND              
            ,PRODUCT_CODE             
            ,SOURCE_TABLE              
            ,RECOVERY_AMOUNT              
            ,TOTAL_RECOVERY              
        )               
        SELECT                
            EOMONTH(A.BUSS_DATE) AS DOWNLOAD_DATE     
            ,A.DEAL_REF AS ACCOUNT_NUMBER               
            ,''W'' AS ACCOUNT_STATUS              
            ,A.BRANCH AS BRANCH_CODE                
            ,''IDR'' AS CURRENCY               
            ,A.CUST_FULL_NAME AS CUSTOMER_NAME               
            ,A.CIF AS CUSTOMER_NUMBER               
            ,''LOAN'' AS DATA_SOURCE               
            ,A.TOTAL_DAYS_PASTDUE AS DAY_PAST_DUE              
            ,1 AS EXCHANGE_RATE              
            ,CONCAT(A.CIF, ''_'', A.DEAL_REF, ''_'', A.DEAL_TYPE) AS MASTERID              
            ,''5'' AS BI_COLLECTABILITY               
            ,A.AMOUNT_WO  AS OUTSTANDING_WO              
            ,A.PLAFOND AS PLAFOND              
            ,A.DEAL_TYPE AS PRODUCT_CODE             
            ,''STG_M_WO_RMS'' AS SOURCE_TABLE              
            ,(A.TOTAL_RECOVERY - COALESCE(C.RECOVERY_AMOUNT, 0)) AS RECOVERY_AMOUNT              
            ,A.TOTAL_RECOVERY  
        FROM #' || WO_RMS || ' A (NOLOCK)            
        LEFT JOIN         
        (        
            SELECT EOMONTH(BUSS_DATE) AS DOWNLOAD_DATE
            ,CONCAT(CIF, ''_'', DEAL_REF, ''_'', DEAL_TYPE) AS MASTERID
            ,TOTAL_RECOVERY AS RECOVERY_AMOUNT 
            FROM #' || V_TABLEINSERT12 || ' A (NOLOCK)            
        ) C ON EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE
        WHERE EOMONTH(A.BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE      
        AND (A.TOTAL_RECOVERY - COALESCE(C.RECOVERY_AMOUNT, 0)) <> 0 
    ';EXECUTE (V_STR_QUERY);  


    --  INSERT WO ACCOUNT FROM TABLE WO CRMS-
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT1 || ' (               
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE                
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE              
            ,EXCHANGE_RATE                
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PRODUCT_CODE              
            ,SOURCE_TABLE              
            ,RECOVERY_AMOUNT              
            ,TOTAL_RECOVERY              
        )                
        SELECT               
            EOMONTH(A.BUSS_DATE) AS DOWNLOAD_DATE              
            ,A.DEAL_REF AS ACCOUNT_NUMBER              
            ,''W'' AS ACCOUNT_STATUS              
            ,A.BRANCH AS BRANCH_CODE                
            ,''IDR'' AS CURRENCY              
            ,A.CUST_FULL_NAME AS CUSTOMER_NAME              
            ,A.CIF AS CUSTOMER_NUMBER              
            ,''LOAN'' AS DATA_SOURCE                
            ,1 AS EXCHANGE_RATE              
            ,CONCAT(A.CIF, ''_'', A.DEAL_REF, ''_'', A.DEAL_TYPE) AS MASTERID              
            ,''5'' AS BIL_COLLECTABILITY            
            ,A.AMOUNT_WO  AS OUTSTANDING_WO              
            ,A.DEAL_TYPE AS PRODUCT_CODE                 
            ,''STG_M_WO_CRMS'' AS SOURCE_TABLE              
            ,(A.TOTAL_RECOVERY - COALESCE(C.RECOVERY_AMOUNT, 0)) AS RECOVERY_AMOUNT              
            ,A.TOTAL_RECOVERY   
        FROM #' || V_TABLEINSERT2 || ' A (NOLOCK)
        LEFT JOIN        
        (        
            SELECT EOMONTH(BUSS_DATE) AS DOWNLOAD_DATE, CONCAT(CIF, ''_'', DEAL_REF, ''_'', DEAL_TYPE) AS MASTERID, TOTAL_RECOVERY AS RECOVERY_AMOUNT 
            FROM #' || V_TABLEINSERT11 || ' 
        ) C ON EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE
        WHERE EOMONTH(A.BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND (A.TOTAL_RECOVERY - COALESCE(C.RECOVERY_AMOUNT, 0)) <> 0 
    ';EXECUTE (V_STR_QUERY); 

    **/ 

    ----------------- WO RECOVERY SECTION ------------------

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE                
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE              
            ,EXCHANGE_RATE                
            ,MASTERID              
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PRODUCT_CODE              
            ,SOURCE_TABLE              
            ,RECOVERY_AMOUNT              
            ,TOTAL_RECOVERY 
        ) SELECT 
            EOMONTH(A.BUSS_DATE) AS DOWNLOAD_DATE              
            ,SUBSTRING(A.ACCOUNT_KEY,7,13) AS ACCOUNT_NUMBER              
            ,''W'' AS ACCOUNT_STATUS              
            ,A.CURRENT_BRANCH AS BRANCH_CODE                
            ,''IDR'' AS CURRENCY              
            ,B.CUST_FULL_NAME AS CUSTOMER_NAME          
            ,A.CIF AS CUSTOMER_NUMBER              
            ,''LOAN'' AS DATA_SOURCE                
            ,1 AS EXCHANGE_RATE              
            ,CONCAT(A.CIF,''_'',SUBSTRING(A.ACCOUNT_KEY,7,13),''_'',A.PRODUCT_KEY) AS MASTERID
            ,''5'' AS BIL_COLLECTABILITY            
            ,A.INITIAL_WRITEOFF_CAPITAL AS OUTSTANDING_WO  
            ,A.PRODUCT_KEY AS PRODUCT_CODE                 
            ,''STG_M_WO_CZ'' AS SOURCE_TABLE              
            ,A.LAST_PAYMENT_AMOUNT AS RECOVERY_AMOUNT  
            ,A.TOTAL_RECOVERIES  
        FROM #' || V_TABLEINSERT6 || ' A (NOLOCK)  
        LEFT JOIN #' || V_TABLEINSERT7 || ' B ON A.BUSS_DATE = B.BUSS_DATE AND A.CIF = B.CIF
        LEFY JOIN (
            SELECT EOMONTH(BUSS_DATE) AS DOWNLOAD_DATE,
            ,ACCOUNT_KEY AS MASTERID
            ,TOTAL_RECOVERIES AS RECOVERY_AMOUNT
            FROM #' || V_TABLEINSERT10 || '
        ) C ON EOMONTH(BUSS_DATE) = EOMONTH(''' || CAST(DATEADD(MM, -1, V_CURRDATE) AS VARCHAR(10)) || ''')::DATE AND A.ACCOUNT_KEY = C.MASTERID 
        WHERE EOMONTH(A.BUSS_DATE) = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.LAST_PAYMENT_AMOUNT <> 0
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE #' || V_TABLEINSERT1 || ' 
        WHERE MASTERID IN (
            SELECT MASTERID 
            FROM #' || V_TABLEINSERT9 || ' 
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO #' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE              
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE               
            ,DAY_PAST_DUE              
            ,EXCHANGE_RATE                
            ,MASTERID                
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PLAFOND              
            ,PRODUCT_CODE            
            ,SOURCE_TABLE              
            ,RECOVERY_AMOUNT              
            ,TOTAL_RECOVERY 
        ) SELECT 
            DOWNLOAD_DATE               
            ,ACCOUNT_NUMBER               
            ,ACCOUNT_STATUS              
            ,BRANCH_CODE              
            ,CURRENCY               
            ,CUSTOMER_NAME               
            ,CUSTOMER_NUMBER               
            ,DATA_SOURCE               
            ,DAY_PAST_DUE              
            ,EXCHANGE_RATE     
            ,MASTERID                
            ,BI_COLLECTABILITY              
            ,OUTSTANDING_WO              
            ,PLAFOND              
            ,LTRIM(RTRIM(PRODUCT_CODE)) AS PRODUCT_CODE             
            ,SOURCE_TABLE              
            ,RECOVERY_AMOUNT              
            ,TOTAL_RECOVERY 
        FROM #' || V_TABLEINSERT1 || '
        ) ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
	

    RAISE NOTICE 'SP_GENERATE_IMA_WO | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_GENERATE_IMA_WO';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

	CALL SP_GENERATE_IMA_WO();

END;

$$;


