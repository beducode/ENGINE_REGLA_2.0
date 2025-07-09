---- DROP PROCEDURE SP_FETCH_IFRS_LI_MASTER_ACCOUNT;

CREATE OR REPLACE PROCEDURE SP_FETCH_IFRS_LI_MASTER_ACCOUNT(
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
    V_TABLEINSERT1 VARCHAR(100);
    
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
        V_TABLEINSERT1 := 'IFRS_LI_MASTER_ACCOUNT_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_LI_MASTER_ACCOUNT';
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

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_LI_MASTER_ACCOUNT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            DOWNLOAD_DATE 
            ,DATA_SOURCE 
            ,BRANCH_CODE 
            ,MASTER_ACCOUNT_CODE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,CUSTOMER_NUMBER 
            ,CUSTOMER_NAME 
            ,LOAN_START_DATE 
            ,SETTLE_DATE 
            ,LOAN_DUE_DATE 
            ,LAST_PAYMENT_DATE 
            ,NEXT_PAYMENT_DATE 
            ,NEXT_INT_PAYMENT_DATE 
            ,ACCOUNT_STATUS 
            ,CURRENCY 
            ,EXCHANGE_RATE 
            ,OUTSTANDING 
            ,INTEREST_RATE 
            ,INTEREST_TYPE 
            ,PRODUCT_CODE 
            ,INTEREST_CALCULATION_CODE 
            ,CREATEDBY 
            ,CREATEDDATE 
            ,ACCOUNT_TYPE 
            ,CUSTOMER_TYPE 
        ) SELECT 
            DOWNLOAD_DATE 
            ,DATA_SOURCE 
            ,BRANCH_CODE 
            ,MASTERID AS MASTER_ACCOUNT_CODE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,CUSTOMER_NUMBER 
            ,CUSTOMER_NAME 
            ,START_DATE AS LOAN_START_DATE 
            ,SETTLE_DATE 
            ,MATURITY_DATE AS LOAN_DUE_DATE 
            ,LAST_PAYMENT_DATE 
            ,NEXT_PAYMENT_DATE 
            ,NEXT_PAYMENT_DATE AS NEXT_INT_PAYMENT_DATE 
            ,ACCOUNT_STATUS 
            ,CURRENCY 
            ,EXCHANGE_RATE 
            ,OUTSTANDING 
            ,INTEREST_RATE 
            ,INTEREST_TYPE 
            ,PRODUCT_CODE 
            ,INTEREST_CALCULATION_CODE 
            ,CREATEDBY 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,ACCOUNT_TYPE 
            ,CUSTOMER_TYPE 
        FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM IMA_LIABILITIES'') 
        AS A ( 
            DOWNLOAD_DATE DATE 
            ,DATA_SOURCE VARCHAR(20) 
            ,BRANCH_CODE VARCHAR(20) 
            ,MASTERID VARCHAR(50) 
            ,ACCOUNT_NUMBER VARCHAR(20) 
            ,CUSTOMER_NUMBER VARCHAR(20) 
            ,CUSTOMER_NAME VARCHAR(100) 
            ,START_DATE DATE 
            ,SETTLE_DATE DATE 
            ,MATURITY_DATE DATE 
            ,CURRENCY VARCHAR(3) 
            ,EXCHANGE_RATE NUMERIC(32,6) 
            ,OUTSTANDING NUMERIC(32,6) 
            ,INTEREST_RATE NUMERIC(32,6) 
            ,INTEREST_TYPE BPCHAR(1) 
            ,PRODUCT_CODE VARCHAR(20) 
            ,INTEREST_CALCULATION_CODE VARCHAR(10) 
            ,SOURCE_TABLE VARCHAR(50) 
            ,CREATEDBY VARCHAR(20) 
            ,CREATEDDATE TIMESTAMP 
            ,LAST_PAYMENT_DATE DATE 
            ,NEXT_PAYMENT_DATE DATE 
            ,ACCOUNT_STATUS VARCHAR(1) 
            ,ACCOUNT_TYPE VARCHAR(5) 
            ,CUSTOMER_TYPE VARCHAR(5) 
            ,SANDI_BANK VARCHAR(10) 
        ) WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_FETCH_IFRS_LI_MASTER_ACCOUNT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_FETCH_IFRS_LI_MASTER_ACCOUNT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;