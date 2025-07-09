---- DROP PROCEDURE SP_FETCH_IFRS_MASTER_TRANSACTION_DAILY;

CREATE OR REPLACE PROCEDURE SP_FETCH_IFRS_MASTER_TRANSACTION_DAILY(
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
        V_TABLEINSERT1 := 'IFRS_TRANSACTION_DAILY_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_TRANSACTION_DAILY';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_TRANSACTION_DAILY WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND SOURCE_TABLE IN (''STG_FEE_COST_JFMF'', ''STG_ADDINFO_AMORTIZE'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
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
        ) SELECT 
            DOWNLOAD_DATE 
            ,EFFECTIVE_DATE 
            ,MATURITY_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,FACILITY_NUMBER 
            ,CUSTOMER_NUMBER 
            ,BRANCH_CODE 
            ,A.DATA_SOURCE 
            ,A.PRD_TYPE 
            ,A.PRD_CODE 
            ,A.TRX_CODE 
            ,A.CCY 
            ,EVENT_CODE 
            ,TRX_REFERENCE_NUMBER 
            ,CASE 
                WHEN B.PRODUCT_CODE IS NULL 
                THEN ORG_CCY_AMT 
                ELSE ORG_CCY_AMT * (CASE C.IFRS_TXN_CLASS WHEN ''FEE'' THEN TRANSACTION_FEE_PORTION WHEN ''COST'' THEN TRANSACTION_COST_PORTION END/100) 
            END AS ORG_CCY_AMT 
            ,CASE 
                WHEN B.PRODUCT_CODE IS NULL 
                THEN EQV_LCY_AMT 
                ELSE EQV_LCY_AMT * (CASE C.IFRS_TXN_CLASS WHEN ''FEE'' THEN TRANSACTION_FEE_PORTION WHEN ''COST'' THEN TRANSACTION_COST_PORTION END/100) 
            END AS EQV_LCY_AMT 
            ,DEBET_CREDIT_FLAG 
            ,TRX_SOURCE 
            ,INTERNAL_NO 
            ,REVOLVING_FLAG 
            ,CREATED_DATE 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        FROM DBLINK(''ifrs9_stg'', ''SELECT * FROM IMA_TXN'') 
        AS A ( 
            DOWNLOAD_DATE DATE 
            ,EFFECTIVE_DATE DATE 
            ,MATURITY_DATE DATE 
            ,MASTERID VARCHAR(100) 
            ,ACCOUNT_NUMBER VARCHAR(50) 
            ,FACILITY_NUMBER VARCHAR(50) 
            ,CUSTOMER_NUMBER VARCHAR(50) 
            ,BRANCH_CODE VARCHAR(20) 
            ,DATA_SOURCE VARCHAR(20) 
            ,PRD_TYPE VARCHAR(20) 
            ,PRD_CODE VARCHAR(20) 
            ,TRX_CODE VARCHAR(20) 
            ,CCY VARCHAR(3) 
            ,EVENT_CODE VARCHAR(50) 
            ,TRX_REFERENCE_NUMBER VARCHAR(100) 
            ,ORG_CCY_AMT NUMERIC(32,6) 
            ,EQV_LCY_AMT NUMERIC(32,6) 
            ,DEBET_CREDIT_FLAG VARCHAR(1) 
            ,TRX_SOURCE VARCHAR(50) 
            ,INTERNAL_NO VARCHAR(70) 
            ,REVOLVING_FLAG VARCHAR(3) 
            ,TRX_LEVEL VARCHAR(5) 
            ,SOURCE_TABLE VARCHAR(100) 
            ,CREATED_DATE TIMESTAMP 
        ) LEFT JOIN IFRS_JF_PORTION_PARAM B 
        ON A.PRD_CODE = B.PRODUCT_CODE 
        LEFT JOIN IFRS_MASTER_TRANS_PARAM C 
        ON A.TRX_CODE = C.TRX_CODE 
        AND C.IS_DELETE = 0 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    RAISE NOTICE 'SP_FETCH_IFRS_MASTER_TRANSACTION_DAILY | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_FETCH_IFRS_MASTER_TRANSACTION_DAILY';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;