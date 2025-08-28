---- DROP PROCEDURE SP_FETCH_IFRS_MASTER_ACCOUNT;

CREATE OR REPLACE PROCEDURE SP_FETCH_IFRS_MASTER_ACCOUNT(
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
        V_TABLEINSERT := 'IFRS_MASTER_ACCOUNT_' || P_RUNID || '';
        V_TABLEINSERT1 := 'TBLU_CUST_GRADE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_JF_PORTION_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'TBLU_CUST_GRADE';
        V_TABLEINSERT2 := 'IFRS_JF_PORTION_PARAM';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            DBLINK('ifrs9_stg', 'SELECT CURRDATE, PREVDATE FROM STG_PRC_DATE') AS STG_PRC_DATE(CURRDATE DATE, PREVDATE DATE);
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT || ' AS SELECT * FROM IFRS_MASTER_ACCOUNT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT || '
        WHERE DOWNLOAD_DATE = ''' || V_CURRDATE || '''
    ';
    EXECUTE (V_STR_QUERY);

    IF OBJECT_ID('TEMPDB.#' || V_TABLEINSERT1 || '')' IS NOT NULL DROP TABLE # ' || V_TABLEINSERT1 || 
    THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT DISTINCT CUSTOMER_NUMBER
            INTO #' || V_TABLEINSERT1 || '
            FROM DBLINK(''ifrs9_stg'', ''SELECT CUSTOMER_NUMBER FROM TBLU_CUSTOMER_GRADING'')
            WHERE DOWNLOAD_DATE = ''' || V_CURRDATE || '''::DATE
            ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || ' 
        (
            ACCOUNT_NUMBER        
            ,ACCOUNT_STATUS        
            ,BRANCH_CODE       
            ,CURRENCY        
            ,CUSTOMER_NAME        
            ,CUSTOMER_NUMBER        
            ,DATA_SOURCE        
            ,DAY_PAST_DUE        
            ,DOWNLOAD_DATE        
            ,ECONOMIC_SECTOR        
            ,EXCHANGE_RATE      
            ,INTEREST_RATE      
            ,LOAN_DUE_DATE        
            ,LOAN_START_DATE        
            ,MASTERID      
            ,MASTER_ACCOUNT_CODE       
            ,NPL_FLAG        
            ,BI_COLLECTABILITY        
            ,OUTSTANDING      
            ,PREVIOUS_ACCOUNT_NUMBER        
            ,PRODUCT_CODE        
            ,PRODUCT_GROUP        
            ,PRODUCT_TYPE        
            ,PRODUCT_TYPE_1       
            ,TENOR      
            ,PLAFOND      
            ,PRODUCT_ENTITY      
            ,SUFFIX      
            ,OUTSTANDING_PASTDUE      
            ,OUTSTANDING_PROFIT_DUE      
            ,NEXT_PAYMENT_DATE      
            ,LAST_PAYMENT_DATE      
            ,INSTALLMENT_AMOUNT      
            ,AO_CODE      
            ,INTEREST_ACCRUED      
            ,FACILITY_NUMBER      
            ,INITIAL_OUTSTANDING      
            ,INTEREST_CALCULATION_CODE      
            ,RESTRUCTURE_COLLECT_FLAG      
            ,IAS_CLASS      
            ,CREATEDBY      
            ,CREATEDDATE      
            ,ACCOUNT_TYPE      
            ,CUSTOMER_TYPE      
            ,DPD_FINAL      
            ,SOURCE_SYSTEM    
            ,SANDI_BANK  
            ,SEGMENT_FLAG
            ,LOB_CODE    
        )        
        SELECT      
            ACCOUNT_NUMBER        
            ,ACCOUNT_STATUS        
            ,BRANCH_CODE        
            ,CURRENCY        
            ,CUSTOMER_NAME        
            ,A.CUSTOMER_NUMBER        
            ,DATA_SOURCE        
            ,DAY_PAST_DUE        
            ,DOWNLOAD_DATE        
            ,ECONOMIC_SECTOR      
            ,EXCHANGE_RATE      
            ,INTEREST_RATE        
            ,LOAN_DUE_DATE        
            ,LOAN_START_DATE        
            ,MASTERID        
            ,MASTERID AS MASTER_ACCOUNT_CODE      
            ,ISNULL(NPL_FLAG, 0) AS NPL_FLAG        
            ,BI_COLLECTABILITY        
            ,CASE WHEN B.PRODUCT_CODE IS NULL THEN OUTSTANDING ELSE OUTSTANDING * (B.PRINCIPAL_PORTION / 100) END AS OUTSTANDING        
            ,PREVIOUS_ACCOUNT_NUMBER        
            ,A.PRODUCT_CODE        
            ,PRODUCT_GROUP        
            ,PRODUCT_TYPE        
            ,PRODUCT_TYPE_1      
            ,TENOR      
            ,CASE WHEN B.PRODUCT_CODE IS NULL THEN PLAFOND ELSE PLAFOND * (B.PRINCIPAL_PORTION / 100) END AS PLAFOND      
            ,PRODUCT_ENTITY      
            ,SUFFIX      
            ,CASE WHEN B.PRODUCT_CODE IS NULL THEN OUTSTANDING_PASTDUE ELSE OUTSTANDING_PASTDUE * (B.PRINCIPAL_PORTION / 100) END AS    OUTSTANDING_PASTDUE      
            ,CASE WHEN B.PRODUCT_CODE IS NULL THEN OUTSTANDING_PROFIT_DUE ELSE OUTSTANDING_PROFIT_DUE * (B.INTEREST_PORTION / 100) END AS       OUTSTANDING_PROFIT_DUE      
            ,NEXT_PAYMENT_DATE      
            ,LAST_PAYMENT_DATE      
            ,INSTALLMENT_AMOUNT      
            ,AO_CODE      
            ,CASE WHEN B.PRODUCT_CODE IS NULL THEN INTEREST_ACCRUED ELSE INTEREST_ACCRUED * (B.INTEREST_PORTION / 100) END AS INTEREST_ACCRUED    
            ,FACILITY_NUMBER      
            ,CASE WHEN B.PRODUCT_CODE IS NULL THEN INITIAL_OUTSTANDING ELSE INITIAL_OUTSTANDING * (B.PRINCIPAL_PORTION / 100) END AS    INITIAL_OUTSTANDING      
            ,A.INTEREST_CALCULATION_CODE      
            ,RESTRUCTURE_COLLECT_FLAG      
            ,''A'' AS IAS_CLASS      
            ,''SP_FETCH_IFRS_MASTER_ACCOUNT'' AS CREATEDBY        
            ,GETDATE() AS CREATEDDATE      
            ,ACCOUNT_TYPE      
            ,CUSTOMER_TYPE      
            ,DPD_FINAL      
            ,SOURCE_SYSTEM    
            ,SANDI_BANK
            ,SEGMENT_FLAG
            ,'' AS LOB_CODE
        FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM IMA_LENDING'') 
        AS IFRS_STG_IMA_LENDING (
            ,ACCOUNT_NUMBER
            ,ACCOUNT_STATUS
            ,ACCOUNT_STATUS_ORG
            ,BRANCH_CODE
            ,CURRENCY
            ,CUSTOMER_NAME
            ,CUSTOMER_NUMBER
            ,DATA_SOURCE
            ,DAY_PAST_DUE
            ,DOWNLOAD_DATE
            ,ECONOMIC_SECTOR
            ,EXCHANGE_RATE
            ,INTEREST_RATE
            ,LOAN_DUE_DATE
            ,LOAN_START_DATE
            ,MASTERID
            ,NPL_FLAG
            ,BI_COLLECTABILITY
            ,OUTSTANDING
            ,PLAFOND
            ,PREVIOUS_ACCOUNT_NUMBER
            ,PRODUCT_CODE
            ,PRODUCT_GROUP
            ,PRODUCT_TYPE
            ,TENOR
            ,SUFFIX
            ,PRODUCT_ENTITY
            ,OUTSTANDING_PASTDUE
            ,OUTSTANDING_PROFIT_DUE
            ,NEXT_PAYMENT_DATE
            ,LAST_PAYMENT_DATE
            ,INSTALLMENT_AMOUNT
            ,AO_CODE
            ,INTEREST_ACCRUED
            ,SOURCE_TABLE
            ,FACILITY_NUMBER
            ,INITIAL_OUTSTANDING
            ,INTEREST_CALCULATION_CODE
            ,RESTRUCTURE_COLLECT_FLAG
            ,ACCOUNT_TYPE
            ,CUSTOMER_TYPE
            ,DPD_FINAL
            ,PRODUCT_TYPE_1
            ,SOURCE_SYSTEM
            ,SANDI_BANK
            ,SEGMENT_FLAG
        )
        LEFT JOIN '|| V_TABLEINSERT2 || ' B
        ON A.PRODUCT_CODE = B.PRODUCT_CODE
        WHERE SOURCE_SYSREM <> ''T24''
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ';
    EXECUTE (V_STR_QUERY);

    ------ CROSS SEGMENT PROFILING

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SET @SQLRATING_CODE = ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SET @SQLRATING_CODE = 
        ''UPDATE A
        SET A.SEGMENT_FLAG = CASE WHEN B.CUSTOMER_NUMBER IS NULL THEN ''N/A'' ELSE A.SEGMENT_FLAG END
        FROM '|| V_TABLEINSERT ||' A
        LEFT JOIN #' || V_TABLEINSERT1 || ' B ON A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ' '+ CONVERT(VARCHAR(10), '' AND A.SEGMENT_FLAG <> ''N/A''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'EXEC SP_EXECUTESQL @SQLRATING_CODE  
        ----PRINT @SQLRATING_CODE
 
        ------ CROSS SEGMENT PROFILING   
            
        ---- REMARK DEVELOPMENT CBS BY @BEDU    
        --------  IF @V_CURRDATE = EOMONTH(@V_CURRDATE)      
        --------  BEGIN      
        --------      EXEC SP_FETCH_IFRS_MASTER_ACCOUNT_T24 @V_CURRDATE      
        -- ------EXEC SP_FETCH_IFRS_MASTER_TREASURY @V_CURRDATE    
        --------  END       
            
        ------DEVELOPMENT CBS    
        ----IMA LENDING #CBSPROJECT';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'EXEC 
        SP_FETCH_IMA_LENDING_CBS 
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'EXEC 
        SP_FETCH_IFRS_MASTER_TREASURY_CBS 
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'EXEC 
        SP_GENERATE_IMA_COLLATERAL_CBS 
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'EXEC 
        SP_FETCH_IFRS_MASTER_COLLATERAL_CBS 
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''
        ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_FETCH_IFRS_MASTER_ACCOUNT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_FETCH_IFRS_MASTER_ACCOUNT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;