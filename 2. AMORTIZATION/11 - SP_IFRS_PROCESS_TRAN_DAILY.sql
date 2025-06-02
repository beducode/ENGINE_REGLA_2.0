---- DROP PROCEDURE SP_IFRS_PROCESS_TRAN_DAILY;

CREATE OR REPLACE PROCEDURE SP_IFRS_PROCESS_TRAN_DAILY(
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
    V_TABLEINSERT3 VARCHAR(100);
    
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
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_PREV_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_CLOSED_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_PREV';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT3 := 'IFRS_ACCT_CLOSED';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ACCT_CLOSED WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_PROCESS_TRAN_DAILY', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'IFRS_ACCT_COST_FEE' || ' WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DAATE ';
    EXECUTE (V_STR_QUERY);

    ---- INSERT FOR RESTRUCTURE ACCOUNT AS TRANSACTION

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'IFRS_ACCT_COST_FEE' || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,PRD_TYPE 
            ,PRD_CODE 
            ,TRX_CODE 
            ,CCY 
            ,FLAG_CF 
            ,FLAG_REVERSE 
            ,METHOD 
            ,STATUS 
            ,SRCPROCESS 
            ,AMOUNT 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,TRX_REFF_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        ) SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.BRANCH_CODE 
            ,A.CUSTOMER_NUMBER 
            ,A.FACILITY_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,A.DATA_SOURCE 
            ,A.PRODUCT_TYPE 
            ,A.PRODUCT_CODE 
            ,''RESTRU'' AS TRX_CODE 
            ,A.CURRENCY 
            ,''F'' AS FLAG_CF 
            ,''N'' AS FLAG_REVERSE 
            ,A.AMORT_TYPE 
            ,''ACT'' AS STATUS 
            ,''RESTRU'' AS SRCPROCESS 
            ,B.PRORATE_UNAMORT_FEE 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,''' || V_SP_NAME || ''' AS CREATEDBY 
            ,''RESTRU - '' || B.PREV_MASTERID AS TRX_REFF_NUMBER 
            ,''' || 'IFRS_ACCT_AMORT_RESTRU' || ''' AS SOURCE_TABLE 
            ,NULL AS TRX_LEVEL 
        FROM ' || 'IFRS_IMA_AMORT_CURR' || ' A 
        JOIN ' || 'IFRS_ACCT_AMORT_RESTRU' || ' B 
        ON A.MASTERID = B.MASTERID 
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 

        UNION ALL 

        SELECT 
            A,DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.BRANCH_CODE 
            ,A.CUSTOMER_NUMBER 
            ,A.FACILITY_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,A.DATA_SOURCE 
            ,A.PRODUCT_TYPE 
            ,A.PRODUCT_CODE 
            ,''RESTRU'' AS TRX_CODE 
            ,A.CURRENCY 
            ,''C'' AS FLAG_CF 
            ,''N'' AS FLAG_REVERSE 
            ,A.AMORT_TYPE 
            ,''ACT'' AS STATUS 
            ,''RESTRU'' AS SRCPROCESS 
            ,B.PRORATE_UNAMORT_COST 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,''' || V_SP_NAME || ''' AS CREATEDBY 
            ,''RESTRU - '' || B.PREV_MASTERID AS TRX_REFF_NUMBER 
            ,''' || 'IFRS_ACCT_AMORT_RESTRU' || ''' AS SOURCE_TABLE 
            ,NULL AS TRX_LEVEL 
        FROM ' || 'IFRS_IMA_AMORT_CURR' || ' A 
        JOIN ' || 'IFRS_ACCT_AMORT_RESTRU' || ' B 
        ON A.MASTERID = B.MASTERID 
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    ---- END RESTRUCTURE ACCOUNT 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'IFRS_ACCT_COST_FEE' || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,PRD_TYPE 
            ,PRD_CODE 
            ,TRX_CODE 
            ,CCY 
            ,FLAG_CF 
            ,FLAG_REVERSE 
            ,METHOD 
            ,STATUS 
            ,SRCPROCESS 
            ,AMOUNT 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,TRX_REFF_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        ) SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.BRANCH_CODE 
            ,NULL AS CIFNO 
            ,A.FACILITY_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,A.DATA_SOURCE 
            ,A.PRODUCT_TYPE 
            ,A.PRODUCT_CODE 
            ,A.TRX_CODE 
            ,A.CCY 
            ,LEFT(COALESCE(B.IFRS_TXN_CLASS, ''F''), 1) AS FLAG_CF 
            ,LEFT(COALESCE(A.DEBET_CREDIT_FLAG, ''X''), 1) AS FLAG_REVERSE 
            ,''X'' AS METHOD 
            ,''ACT'' AS STATUS 
            ,''TRAN_DAILY'' AS SRCPROCESS 
            ,A.ORG_CCY_AMT 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,''' || V_SP_NAME || ''' AS CREATEDBY 
            ,TRX_REFFERENCE_NUMBER 
            ,SOURCE_TABLE 
            ,NULL AS TRX_LEVEL
            '

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_PROCESS_TRAN_DAILY', '');

    RAISE NOTICE 'SP_IFRS_PROCESS_TRAN_DAILY | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_PROCESS_TRAN_DAILY';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;