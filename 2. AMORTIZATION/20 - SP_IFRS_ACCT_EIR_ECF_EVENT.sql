---- DROP PROCEDURE SP_IFRS_ACCT_EIR_ECF_EVENT;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_ECF_EVENT(
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
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    V_TABLEINSERT11 VARCHAR(100);
    V_TABLEINSERT12 VARCHAR(100);
    V_TABLEINSERT13 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_EFFDATEFLAG VARCHAR(1);
    
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
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_EVENT_CHANGES_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_EVENT_CHANGES_DETAILS_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACT_ECF_TMP_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_IMA_AMORT_PREV_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_ACCT_AMORT_RESTRU_' || P_RUNID || '';
        V_TABLEINSERT11 := 'IFRS_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT12 := 'IFRS_TRANSACTION_DAILY_' || P_RUNID || '';
        V_TABLEINSERT13 := 'IFRS_ACCT_CLOSED_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT2 := 'IFRS_EVENT_CHANGES';
        V_TABLEINSERT3 := 'IFRS_EVENT_CHANGES_DETAILS';
        V_TABLEINSERT4 := 'IFRS_ACT_ECF_TMP';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_ECF';
        V_TABLEINSERT6 := 'IFRS_IMA_AMORT_PREV';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_ECF';
        V_TABLEINSERT8 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT9 := 'IFRS_PRODUCT_PARAM';
        V_TABLEINSERT10 := 'IFRS_ACCT_AMORT_RESTRU';
        V_TABLEINSERT11 := 'IFRS_ACCT_SWITCH';
        V_TABLEINSERT12 := 'IFRS_TRANSACTION_DAILY';
        V_TABLEINSERT13 := 'IFRS_ACCT_CLOSED';
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

    SELECT COMMONUSAGE INTO V_EFFDATEFLAG 
    FROM TBLM_COMMONCODEHEADER
    WHERE COMMONCODE = 'SCM004';
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_EVENT_CHANGES WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_EVENT_CHANGES_DETAILS WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_ACT_ECF_TMP WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_EIR_ECF_EVENT', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            EIR_STATUS = ''''
            ,ECF_STATUS = '''' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
        SET 
            EIR_STATUS = '''' 
            ,ECF_STATUS = '''' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (MASTERID)
        SELECT DISTINCT MASTERID 
        FROM ' || V_TABLEINSERT5 || ' 
        WHERE AMORTSTOPDATE IS NULL 
        AND DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,CASE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''1'' THEN ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''2'' THEN CAST(A.NEXT_PAYMENT_DATE AS VARCHAR(10))::DATE 
                ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            END 
            ,C.INTEREST_RATE 
            ,A.INTEREST_RATE 
            ,0 
            ,''INTEREST RATE CHANGES'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        JOIN ' || V_TABLEINSERT4 || ' B 
        ON A.MASTERID = B.MASTERID 
        JOIN ' || V_TABLEINSERT6 || ' C 
        ON A.MASTERID = C.MASTERID 
        WHERE (
            A.INTEREST_RATE <> C.INTEREST_RATE 
            OR A.INTEREST_RATE_IDC <> C.INTEREST_RATE_IDC
        ) AND (
            ABS(A.UNAMORT_COST_AMT) <> 0
            OR ABS(A.UNAMORT_FEE_AMT) <> 0
        ) AND COALESCE(A.INTEREST_RATE, 0) > 0 
        AND A.LOAN_DUE_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.STAFF_LOAN_FLAG = ''N'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,CASE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''1'' THEN ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''2'' THEN CAST(A.NEXT_PAYMENT_DATE AS VARCHAR(10))::DATE 
                ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            END 
            ,C.INTEREST_RATE 
            ,A.INTEREST_RATE 
            ,0 
            ,''INTEREST RATE CHANGES'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN (SELECT DISTINCT MASTERID FROM ' || V_TABLEINSERT7 || ' WHERE AMORTSTOPDATE IS NULL) B 
        ON A.MASTERID = B.MASTERID 
        INNER JOIN ' || V_TABLEINSERT6 || ' C 
        ON A.MASTERID = C.MASTERID 
        WHERE ( 
            A.INTEREST_RATE <> C.INTEREST_RATE 
            OR A.INTEREST_RATE_IDC <> C.INTEREST_RATE_IDC 
        ) AND COALESCE(A.INTEREST_RATE, 0) > 0 
        AND A.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND A.STAFF_LOAN_FLAG = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,CASE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''1'' THEN ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''2'' THEN CAST(A.NEXT_PAYMENT_DATE AS VARCHAR(10))::DATE 
                ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            END 
            ,C.LOAN_DUE_DATE 
            ,A.LOAN_DUE_DATE 
            ,6 
            ,''LOAN DUE DATE CHANGES'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN ' || V_TABLEINSERT4 || ' B 
        ON A.MASTERID = B.MASTERID 
        INNER JOIN ' || V_TABLEINSERT6 || ' C 
        ON A.MASTERID = C.MASTERID 
        WHERE A.LOAN_DUE_DATE <> C.LOAN_DUE_DATE 
        AND (
            ABS(A.UNAMORT_COST_AMT) <> 0 
            OR ABS(A.UNAMORT_FEE_AMT) <> 0 
        ) AND A.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT DISTINCT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,CASE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''1'' THEN ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                WHEN ''' || V_EFFDATEFLAG || ''' = ''2'' THEN CAST(A.NEXT_PAYMENT_DATE AS VARCHAR(10))::DATE 
                ELSE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            END 
            ,0 
            ,B.AMOUNT 
            ,2 
            ,B.FLAG_CF || '' - '' || B.FLAG_REVERSE || '' - '' || B.TRX_CODE 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN ' || V_TABLEINSERT8 || ' B 
        ON A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        WHERE B.STATUS = ''ACT'' 
        AND B.METHOD = ''EIR'' 
        AND A.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,0 
            ,0 
            ,3 
            ,''NEW STAFFLOAN ACCOUNT'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN ' || V_TABLEINSERT9 || ' B 
        ON A.DATA_SOURCE = B.DATA_SOURCE 
        AND A.PRODUCT_TYPE = B.PRD_TYPE 
        AND A.PRODUCT_CODE = B.PRD_CODE 
        AND (A.CURRENCY = B.CCY OR B.CCY = ''ALL'') 
        WHERE A.LOAN_START_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND (B.IS_STAF_LOAN IN (''1'', ''Y'') OR A.STAFF_LOAN_FLAG = ''Y'') 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT DISTINCT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,B.PREV_MASTERID 
            ,B.MASTERID 
            ,6 
            ,''RESTRUCTURE - '' || B.PREV_MASTERID 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN ' || V_TABLEINSERT10 || ' B 
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND A.MASTERID = B.MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,B.PREV_ACCTNO 
            ,B.ACCTNO 
            ,5 
            ,''CHANGE_BRANCH'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN ' || V_TABLEINSERT11 || ' B 
        ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND A.MASTERID = B.MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,0 
            ,B.ORG_CCY_AMT 
            ,6 
            ,''PARTIAL PAYMENT ACCOUNT'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        INNER JOIN ' || V_TABLEINSERT12 || ' B 
        ON A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND B.TRX_CODE = ''PREPAYMENT'' 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.ACCOUNT_STATUS = ''A'' 
        AND A.OUTSTANDING > 0 ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,EFFECTIVE_DATE 
            ,BEFORE_VALUE 
            ,AFTER_VALUE 
            ,EVENT_ID 
            ,REMARKS 
            ,CREATEDBY 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,0 
            ,COALESCE(A.EARLY_PAYMENT, 0) 
            ,6 
            ,''EARLY PAYMENT'' 
            ,''SP_IFRS_ACCT_EIR_ECF_EVENT'' 
        FROM ' || V_TABLEINSERT1 || ' A 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.EARLY_PAYMENT_FLAG = ''Y''
        AND A.ACCOUNT_STATUS = ''A'' 
        AND A.OUTSTANDING > 0 ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' IMA 
        SET 
            EIR_STATUS = ''Y'' 
            ,ECF_STATUS = ''Y'' 
        FROM ' || V_TABLEINSERT2 || ' RES 
        WHERE RES.MASTERID = IMA.MASTERID 
        AND IMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND RES.EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND IMA.MASTERID NOT IN (
            SELECT DISTINCT MASTERID 
            FROM ' || V_TABLEINSERT13 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) ';
    EXECUTE (V_STR_QUERY);
    
    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_EIR_ECF_EVENT', '');

    RAISE NOTICE 'SP_IFRS_ACCT_EIR_ECF_EVENT | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_EIR_ECF_EVENT';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;