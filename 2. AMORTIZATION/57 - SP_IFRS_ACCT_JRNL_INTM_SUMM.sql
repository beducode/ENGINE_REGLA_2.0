---- DROP PROCEDURE SP_IFRS_PAYM_SCHD_MTM;

CREATE OR REPLACE PROCEDURE SP_IFRS_PAYM_SCHD_MTM(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_ENDOFMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_COUNTERPAY INT;
    V_MAX_COUNTERPAY INT;
    V_NEXT_COUNTERPAY INT;
    V_PMT_DATE DATE;
    V_NEXT_START_DATE DATE;
    V_CUT_OFF_DATE DATE;
    V_ROUND INT;
    V_FUNCROUND INT;
    V_LOG_ID INT;
    V_PARAM_CALC_TO_LAST_PAYMENT INT;
    V_CALC_IDAYS INT;
    V_PARAM_EIR_THRESHOLD INT;
    V_PARAM_INT_THRESHOLD INT;
    
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
        V_TABLEINSERT1 := 'IFRS_EIR_ADJUSTMENT_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_EXCEPTION_DETAILS_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_MASTER_ACCOUNT_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_MASTER_PAYMENT_SETTING_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_PAYM_SCHD_ALL_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_PAYM_SCHD_MTM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_EIR_ADJUSTMENT';
        V_TABLEINSERT2 := 'IFRS_EXCEPTION_DETAILS';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT4 := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT5 := 'IFRS_MASTER_PAYMENT_SETTING';
        V_TABLEINSERT6 := 'IFRS_PAYM_SCHD_ALL';
        V_TABLEINSERT7 := 'IFRS_PAYM_SCHD_MTM';
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

    V_ENDOFMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_CUT_OFF_DATE := '2016-01-01'::DATE;
    V_MAX_COUNTERPAY := 0;
    V_COUNTERPAY := 0;
    V_NEXT_COUNTERPAY := 1;
    V_ROUND := 6;
    V_FUNCROUND = 1;
    V_LOG_ID := 912;
    V_CALC_IDAYS := 0;
    V_PARAM_EIR_THRESHOLD := 0;
    V_PARAM_INT_THRESHOLD := 0;

    SELECT CAST(VALUE1 AS INT), CAST(VALUE2 AS INT)
    INTO V_ROUND, V_FUNCROUND
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SCM003';

    SELECT COMMONUSAGE INTO V_CALC_IDAYS
    FROM TBLM_COMMONCODEHEADER
    WHERE COMMONCODE = 'SCM004';

    SELECT CASE WHEN COMMONUSAGE = 'Y' THEN 1 ELSE 0 END 
    INTO V_PARAM_CALC_TO_LAST_PAYMENT 
    FROM TBLM_COMMONCODEHEADER
    WHERE COMMONCODE = 'SCM005';
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_JOURNAL_INTM_SUMM', '');

    V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || IFRS_ACCT_JOURNAL_INTM_SUMM || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_JRNL1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_JRNL1' || ' 
        (    
          FACNO  
          ,CIFNO  
          ,DATASOURCE  
          ,PRDCODE  
          ,TRXCODE  
          ,CCY  
          ,JOURNALCODE  
          ,STATUS  
          ,REVERSE  
          ,N_AMOUNT  
          ,MASTERID  
          ,ACCTNO  
          ,FLAG_CF  
          ,BRANCH  
          ,IS_PNL  
          ,JOURNALCODE2  
          ,PRDTYPE    
        ) SELECT 
          FACNO  
          ,CIFNO  
          ,DATASOURCE  
          ,PRDCODE  
          ,TRXCODE  
          ,CCY  
          ,JOURNALCODE  
          ,STATUS  
          ,REVERSE  
          ,N_AMOUNT  
          ,MASTERID  
          ,ACCTNO  
          ,FLAG_CF  
          ,BRANCH  
          ,IS_PNL  
          ,JOURNALCODE2  
          ,PRDTYPE   
        FROM ' || IFRS_ACCT_JOURNAL_INTM_SUMM || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UNION ALL ' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY ' 
        SELECT 
          FACNO  
          ,CIFNO  
          ,DATASOURCE  
          ,PRDCODE  
          ,TRXCODE  
          ,CCY  
          ,JOURNALCODE  
          ,STATUS  
          ,REVERSE  
          ,N_AMOUNT  
          ,MASTERID  
          ,ACCTNO  
          ,FLAG_CF  
          ,BRANCH  
          ,IS_PNL  
          ,JOURNALCODE2  
          ,PRDTYPE 
        FROM ' || IFRS_ACCT_JOURNAL_INTM || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        )';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'TMP INSERT', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_ACCT_JOURNAL_INTM_SUMM || ' (
        DOWNLOAD_DATE
        ,FACNO
        ,CIFNO
        ,DATASOURCE
        ,PRDCODE
        ,TRXCODE
        ,CCY
        ,JOURNALCODE
        ,STATUS
        ,REVERSE
        ,N_AMOUNT
        ,MASTERID
        ,N_AMOUNT
        ,MASTERID
        ,CREATEDATE
        ,ACCTNO
        ,FLAG_CF
        ,BRANCH
        ,IS_PNL
        ,JOURNALCODE2
        ,PRDTYPE
        ) SELECT 
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
        ,FACNO  
        ,CIFNO  
        ,DATASOURCE  
        ,PRDCODE  
        ,TRXCODE  
        ,CCY  
        ,JOURNALCODE  
        ,STATUS  
        ,REVERSE  
        ,AMOUNT  
        ,MASTERID  
        ,CREATEDDATE  
        ,ACCTNO  
        ,FLAG_CF  
        ,BRANCH  
        ,IS_PNL  
        ,JOURNALCODE2  
        ,PRDTYPE
        FROM (
            SELECT ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
                FACNO  
                ,CIFNO  
                ,DATASOURCE  
                ,PRDCODE  
                ,TRXCODE  
                ,CCY  
                ,JOURNALCODE  
                ,STATUS  
                ,REVERSE  
                ,SUM(N_AMOUNT) AS N_AMOUNT  
                ,MASTERID  
                ,CURRENT_TIMESTAMP AS CREATEDDATE  
                ,ACCTNO  
                ,FLAG_CF  
                ,BRANCH  
                ,IS_PNL  
                ,JOURNALCODE2  
                ,PRDTYPE
            FROM ' || 'TMP_JRNL1' || '
            GROUP BY 
                FACNO
                ,CIFNO
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,MASTERID
                ,ACCTNO
                ,FLAG_CF
                ,BRANCH
                ,IS_PNL
                ,JOURNALCODE2
                ,PRDTYPE
        )';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SUMM INSERT', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_AP || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_AP || ' (  
        MASTERID  
        ,FLAG_CF  
        ,AMOUNT  
        )
        SELECT MASTERID
        ,FLAG_CF
        ,SUM(CASE WHEN REVERSE = ''Y'' THEN -1 * N_AMOUNT ELSE N_AMOUNT END) AS AMOUNT_AMOUNT
        FROM ' || IFRS_ACCT_JOURNAL_INTM_SUMM || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE   
        AND JOURNALCODE IN (''ACCRU'', ''ACCRU_SL'', ''AMORT'')
        AND TRXCODE <> (''BENEFIT'')
        GROUP BY MASTERID';
    EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_ACCT_COST_FEE_SUMM || ' 
        SET AMORT_COST = B.AMOUNT
        FROM ' || TMP_AP || ' B
        WHERE ' || IFRS_ACCT_COST_FEE_SUMM || '.MASTERID = B.MASTERID''
        AND ' || IFRS_ACCT_COST_FEE_SUMM || '.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND B.FLAG_CF = ''C'''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_AP || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_AP || ' (  
        MASTERID  
        ,FLAG_CF  
        ,AMOUNT  
        )
        SELECT MASTERID
        ,FLAG_CF
        ,SUM(CASE WHEN REVERSE = ''Y'' THEN -1 * N_AMOUNT ELSE N_AMOUNT END) AS AMOUNT_AMOUNT
        FROM ' || IFRS_ACCT_JOURNAL_INTM_SUMM || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE   
        AND JOURNALCODE IN (''ACCRU'', ''ACCRU_SL'', ''AMORT'')
        AND TRXCODE = (''BENEFIT'')
        GROUP BY MASTERID,
        FLAG_CF';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_STAFF_BENEFIT_SUMM || ' (  
        SET AMORT_FEE = B.AMOUNT
        FROM ' || TMP_AP || ' B
        WHERE ' || IFRS_LBM_STAFF_BENEFIT_SUMM || '.MASTERID = B.MASTERID''
        AND ' || IFRS_LBM_STAFF_BENEFIT_SUMM || '.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND B.FLAG_CF = ''F'''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_STAFF_BENEFIT_SUMM || ' (  
        SET AMORT_COST = B.AMOUNT
        FROM ' || TMP_AP || ' B
        WHERE ' || IFRS_LBM_STAFF_BENEFIT_SUMM || '.MASTERID = B.MASTERID''
        AND ' || IFRS_LBM_STAFF_BENEFIT_SUMM || '.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND B.FLAG_CF = ''C'''
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'UPDATE AMORT AMTT', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_IMA_AMORT_CURR || ' (  
        SET INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST  
        ,INITIAL_UNAMORT_ORG_FEE = B.AMOUNT_FEE 
        FROM ' || IFRS_ACCT_COST_FEE_SUMM || ' B
        WHERE ' || IFRS_IMA_AMORT_CURR || '.MASTERID = B.MASTERID''
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND ISNULL (
        ' || IFRS_IMA_AMORT_CURR || '.FACILITY_NUMBER, '''') = ISNULL(B.FACNO,' ''') 
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_MASTER_ACCOUNT || '  
        SET INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST  
        ,INITIAL_UNAMORT_ORG_FEE = B.AMOUNT_FEE 
        FROM ' || IFRS_ACCT_COST_FEE_SUMM || ' B
        WHERE ' || IFRS_MASTER_ACCOUNT || '.MASTERID = B.MASTERID''
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND ' || IFRS_MASTER_ACCOUNT || '.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        UPDATE ' || IFRS_IMA_AMORT_CURR || ' 
        SET INITIAL_UNAMORT_TXN_COST = B.AMOUNT_COST
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || '
      WHERE ' || IFRS_IMA_AMORT_CURR || '.MASTERID = B.MASTERID''
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
    ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_MASTER_ACCOUNT || ' 
        SET INITIAL_BENEFIT = B.AMOUNT_FEE
        FROM ' || IFRS_LBM_STAFF_BENEFIT_SUMM || ' B
        WHERE ' || IFRS_MASTER_ACCOUNT || '.MASTERID = B.MASTERID''
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND ISNULL (
        ' || IFRS_MASTER_ACCOUNT || '.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        )';

    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'UPDATE INITIAL AMT', '');


    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_JOURNAL_INTM_SUMM', '');

    RAISE NOTICE 'SP_IFRS_PAYM_SCHD_MTM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT5;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_PAYM_SCHD_MTM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT5 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;