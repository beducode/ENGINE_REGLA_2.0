---- DROP PROCEDURE SP_GENERATE_IMA_LIABILITIES;

CREATE OR REPLACE PROCEDURE SP_GENERATE_IMA_LIABILITIES(
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
        V_TABLEINSERT := 'IMA_LIABILITIES_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT := 'IMA_LIABILITIES';
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
    /**
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || V_TABLEINSERT || '
        WHERE DOWNLOAD_DATE = ''' || V_CURRDATE || '''
    ';
    EXECUTE (V_STR_QUERY);
    **/

    /**
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE            
            ,DATA_SOURCE            
            ,BRANCH_CODE            
            ,MASTERID            
            ,ACCOUNT_NUMBER            
            ,CUSTOMER_NUMBER            
            ,CUSTOMER_NAME            
            ,START_DATE            
            ,SETTLE_DATE            
            ,MATURITY_DATE            
            ,LAST_PAYMENT_DATE            
            ,NEXT_PAYMENT_DATE            
            ,ACCOUNT_STATUS            
            ,CURRENCY            
            ,EXCHANGE_RATE            
            ,OUTSTANDING            
            ,INTEREST_RATE            
            ,INTEREST_TYPE            
            ,PRODUCT_CODE            
            ,INTEREST_CALCULATION_CODE            
            ,SOURCE_TABLE            
            ,ACCOUNT_TYPE            
            ,CUSTOMER_TYPE  
        ) SELECT 
            A.BUSS_DATE AS DOWNLOAD_DATE            
            ''ISSUED_BONDS'' AS DATA_SOURCE            
            A.BRANCH AS BRANCH_CODE            
            LTRIM(RTRIM(DEAL_REF)) AS MASTERID            
            LTRIM(RTRIM(DEAL_REF)) AS ACCOUNT_NUMBER            
            LTRIM(RTRIM(A.CIF)) AS CUSTOMER_NUMBER            
            LTRIM(RTRIM(A.CUST_SHORT_NAME)) AS CUSTOMER_NAME            
            START_DATE            
            CONTRACT_DATE AS SETTLE_DATE            
            MAT_DATE AS MATURITY_DATE            
            SCHEDULE_DATE AS LAST_PAYMENT_DATE            
            NEXT_INT_PAYMENT_DATE AS NEXT_PAYMENT_DATE            
            ''A'' AS ACCOUNT_STATUS            
            A.CCY AS CURRENCY            
            COALESCE(B.SPOT_RATE,1) AS EXCHANGE_RATE            
            AMOUNT AS OUTSTANDING            
            INT_RATE AS INTEREST_RATE            
            NULL AS INTEREST_TYPE            
            DEAL_TYPE AS PRODUCT_CODE            
            NULL AS INTEREST_CALCULATION_CODE            
            ''STG_DEPOSITO'' AS SOURCE_TABLE            
            C.ACCOUNT_TYPE            
            D.CUSTOMER_TYPE  
        FROM ' || STG_DEPOSITO || ' A
        LEFT JOIN TBL_MASTER_EXCHANGE_RATE B
        ON A.CCY = B.CCY_CODE AND A.BUSS_DATE = B.BUSS_DATE
        LEFT JOIN TBL_MASTER_PRODUCT_BANKWIDE C
        ON A.DEAL_TYPE = C.PRODUCT_CODE
        LEFT JOIN STG_CIF D
        ON A.CIF = D.CIF 
        AND A.SOURCE_SYSTEM = D.SOURCE_SYSTEM
        AND A.BUSS_DATE = D.BUSS_DATE
        WHERE DEAL_TYPE = ''TOB'' AND A.BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);
    **/

    /**
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT || ' 
        (
            DOWNLOAD_DATE            
            ,DATA_SOURCE            
            ,BRANCH_CODE            
            ,MASTERID            
            ,ACCOUNT_NUMBER            
            ,CUSTOMER_NUMBER            
            ,CUSTOMER_NAME            
            ,START_DATE            
            ,SETTLE_DATE            
            ,MATURITY_DATE            
            ,CURRENCY            
            ,EXCHANGE_RATE            
            ,OUTSTANDING            
            ,INTEREST_RATE            
            ,INTEREST_TYPE            
            ,PRODUCT_CODE            
            ,INTEREST_CALCULATION_CODE            
            ,SOURCE_TABLE            
            ,ACCOUNT_TYPE            
            ,CUSTOMER_TYPE  
        ) SELECT 
            A.BUSS_DATE AS DOWNLOAD_DATE            
            ,''BORROWING'' AS DATA_SOURCE            
            ,A.BRANCH AS BRANCH_CODE            
            ,LTRIM(RTRIM(DEAL_REF)) AS MASTERID            
            ,LTRIM(RTRIM(DEAL_REF)) AS ACCOUNT_NUMBER            
            ,LTRIM(RTRIM(CIF_BFEQ)) AS CUSTOMER_NUMBER            
            ,LTRIM(RTRIM(A.CUST_SHORT_NAME)) AS CUSTOMER_NAME            
            ,DEAL_DATE AS START_DATE            
            ,A.VALUE_DATE AS SETTLE_DATE            
            ,MATURITY_DATE AS MATURITY_DATE            
            ,A.CCY AS CURRENCY            
            ,COALESCE(B.SPOT_RATE,1) AS EXCHANGE_RATE            
            ,NOTIONAL_AMOUNT AS OUTSTANDING            
            ,TINGKAT_SUKU_BUNGA AS INTEREST_RATE            
            ,JENIS_SUKU_BUNGA AS INTEREST_TYPE            
            ,A.PRODUCT_CODE AS PRODUCT_CODE            
            ,BASIS_PERHITUNGAN_BUNGA AS INTEREST_CALCULATION_CODE            
            ,''STG_MM'' AS SOURCE_TABLE            
            ,C.ACCOUNT_TYPE            
            ,D.CUSTOMER_TYPE  
        FROM ' || STG_MM || ' A
        LEFT JOIN TBL_MASTER_EXCHANGE_RATE B
        ON A.CCY = B.CCY_CODE AND A.BUSS_DATE = B.BUSS_DATE
        LEFT JOIN TBL_MASTER_PRODUCT_BANKWIDE C
        ON A.PRODUCT_CODE = C.PRODUCT_CODE            
        LEFT JOIN STG_CIF D
        ON A.CIF = D.CIF 
        AND A.BUSS_DATE = D.BUSS_DATE
        WHERE A.PRODUCT_CODE <> ''PLACE'' 
        AND A.BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ) ';
    EXECUTE (V_STR_QUERY);
    **/

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_GENERATE_IMA_LIABILITIES | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_GENERATE_IMA_LIABILITIES';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;