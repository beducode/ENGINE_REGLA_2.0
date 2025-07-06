---- DROP PROCEDURE SP_IFRS_LBM_ACCT_EIR_SWITCH;

CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_SWITCH(
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
    V_TABLEINSERT2 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    
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
        V_TABLEINSERT1 := 'IFRS_LBM_ACCT_EIR_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_LBM_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_ACCT_COST_FEE_SUMM_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_ACF_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_PRC_DATE_AMORT_' || P_RUNID || '';
        V_TABLEINSERT9 := 'TMP_T1_' || P_RUNID || '';
        V_TABLEINSERT9 := 'TMP_T2_' || P_RUNID || '';
        V_TABLEINSERT11 := 'TMP_TF_' || P_RUNID || '';
        V_TABLEINSERT12 := 'TMP_T3_' || P_RUNID || '';
        V_TABLEINSERT13 := 'TMP_TC_' || P_RUNID || '';
        V_TABLEINSERT14 := 'TMP_SW1_' || P_RUNID || '';
        V_TABLEINSERT15 := 'TMP_P1_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_LBM_ACCT_EIR_COST_FEE_PREV';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_EIR_ECF';
        V_TABLEINSERT3 := 'IFRS_LBM_ACCT_SWITCH';
        V_TABLEINSERT4 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF';
        V_TABLEINSERT5 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV';
        V_TABLEINSERT6 := 'IFRS_ACCT_COST_FEE_SUMM';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_ACF';
        V_TABLEINSERT8 := 'IFRS_PRC_DATE_AMORT';
        V_TABLEINSERT9 := 'TMP_T1';
        V_TABLEINSERT9 := 'TMP_T2';
        V_TABLEINSERT11 := 'TMP_TF';
        V_TABLEINSERT12 := 'TMP_T3';
        V_TABLEINSERT13 := 'TMP_TC';
        V_TABLEINSERT14 := 'TMP_SW1';
        V_TABLEINSERT15 := 'TMP_P1';
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
    
    SELECT CAST(VALUE1 AS INT), CAST(VALUE2 AS INT) INTO V_ROUND, V_FUNCROUND
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SCM003';
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_LBM_ACCT_EIR_SWITCH', '');
    
    --RESET

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''ACT''
        WHERE STATUS = ''REV''
        AND CREATEDBY = ''EIR_SWITCH''
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
         ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''ACT''
        WHERE STATUS = ''REV2''
        AND CREATEDBY = ''EIR_SWITCH''
        AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
         ';
    EXECUTE (V_STR_QUERY);

    -- EXIST PROC IF NO NEED TO PROCESS EIR SWITCH

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SELECT ' || CAST(V_NUM = COUNT(*) AS VARCHAR(10)) || '
        FROM ' || V_TABLEINSERT3 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND PREV_EIR_ECF = ''Y'''
    EXECUTE (V_STR_QUERY);

    IF V_NUM <= 0 
    BEGIN 
        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_LBM_ACCT_EIR_SWITCH', '');
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' (
        DOWNLOAD_DATE    
        ,MASTERID    
        ,N_LOAN_AMT    
        ,N_INT_RATE    
        ,N_EFF_INT_RATE    
        ,STARTAMORTDATE    
        ,ENDAMORTDATE    
        ,GRACEDATE    
        ,PAYMENTCODE    
        ,INTCALCCODE    
        ,PAYMENTTERM    
        ,ISGRACE    
        ,PREV_PMT_DATE    
        ,PMT_DATE    
        ,I_DAYS    
        ,I_DAYS2    
        ,N_OSPRN_PREV    
        ,N_INSTALLMENT    
        ,N_PRN_PAYMENT    
        ,N_INT_PAYMENT    
        ,N_OSPRN    
        ,N_FAIRVALUE_PREV    
        ,N_EFF_INT_AMT    
        ,N_FAIRVALUE    
        ,N_UNAMORT_AMT_PREV    
        ,N_AMORT_AMT    
        ,N_UNAMORT_AMT    
        ,N_COST_UNAMORT_AMT_PREV    
        ,N_COST_AMORT_AMT    
        ,N_COST_UNAMORT_AMT    
        ,N_FEE_UNAMORT_AMT_PREV    
        ,N_FEE_AMORT_AMT    
        ,N_FEE_UNAMORT_AMT    
        ,AMORTSTOPDATE    
        ,AMORTSTOPMSG    
        ,N_DAILY_AMORT_COST    
        ,N_DAILY_AMORT_FEE    
        ,N_EFF_INT_AMT0    
        ,N_EFF_INT_RATE0    
        ,N_DAILY_INT_ADJ_AMT    
        ,N_INT_ADJ_AMT    
        -- SWITCH ADJUST CARRY FORWARD    
        ,SW_ADJ_COST    
        ,SW_ADJ_FEE    
        ,NOCF_OSPRN    
        ,NOCF_OSPRN_PREV    
        ,NOCF_INT_RATE    
        ,NOCF_PRN_PAYMENT    
        ,NOCF_EFF_INT_AMT    
        ,NOCF_UNAMORT_AMT_PREV    
        ,NOCF_AMORT_AMT    
        ,NOCF_UNAMORT_AMT
        ) SELECT 
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,A.MASTERID    
        ,B.N_LOAN_AMT    
        ,B.N_INT_RATE    
        ,B.N_EFF_INT_RATE    
        ,B.STARTAMORTDATE    
        ,B.ENDAMORTDATE    
        ,B.GRACEDATE    
        ,B.PAYMENTCODE    
        ,B.INTCALCCODE    
        ,B.PAYMENTTERM    
        ,B.ISGRACE    
        ,B.PREV_PMT_DATE    
        ,B.PMT_DATE    
        ,B.I_DAYS    
        ,B.I_DAYS2    
        ,B.N_OSPRN_PREV    
        ,B.N_INSTALLMENT    
        ,B.N_PRN_PAYMENT    
        ,B.N_INT_PAYMENT    
        ,B.N_OSPRN    
        ,B.N_FAIRVALUE_PREV    
        ,B.N_EFF_INT_AMT    
        ,B.N_FAIRVALUE    
        ,B.N_UNAMORT_AMT_PREV    
        ,B.N_AMORT_AMT    
        ,B.N_UNAMORT_AMT    
        ,B.N_COST_UNAMORT_AMT_PREV    
        ,B.N_COST_AMORT_AMT    
        ,B.N_COST_UNAMORT_AMT    
        ,B.N_FEE_UNAMORT_AMT_PREV    
        ,B.N_FEE_AMORT_AMT    
        ,B.N_FEE_UNAMORT_AMT    
        ,B.AMORTSTOPDATE    
        ,''EIR_SWITCH_2''    
        ,B.N_DAILY_AMORT_COST    
        ,B.N_DAILY_AMORT_FEE    
        ,B.N_EFF_INT_AMT0    
        ,B.N_EFF_INT_RATE0    
        ,B.N_DAILY_INT_ADJ_AMT    
        ,B.N_INT_ADJ_AMT    
        -- SWITCH ADJUST CARRY FORWARD    
        ,B.SW_ADJ_COST    
        ,B.SW_ADJ_FEE    
        ,B.NOCF_OSPRN    
        ,B.NOCF_OSPRN_PREV    
        ,B.NOCF_INT_RATE    
        ,B.NOCF_PRN_PAYMENT    
        ,B.NOCF_EFF_INT_AMT    
        ,B.NOCF_UNAMORT_AMT_PREV    
        ,B.NOCF_AMORT_AMT    
        ,B.NOCF_UNAMORT_AMT
        FROM ' || V_TABLEINSERT3 || ' A
        JOIN ' || V_TABLEINSERT2 || ' B ON B.AMORTSTOPDATE IS NULL
        AND B.MASTERID = A.PREV_MASTERID
        WHERE B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND PREV_EIR_ECF = ''Y'''
    EXECUTE (V_STR_QUERY);

    -- COPY OLD COST FEE ECF TO NEW ACCT  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (
        DOWNLOAD_DATE    
        ,ECFDATE    
        ,MASTERID    
        ,BRCODE    
        ,CIFNO    
        ,FACNO    
        ,ACCTNO    
        ,DATASOURCE    
        ,CCY    
        ,PRDCODE    
        ,TRXCODE    
        ,FLAG_CF    
        ,FLAG_REVERSE    
        ,METHOD    
        ,STATUS    
        ,SRCPROCESS    
        ,AMOUNT    
        ,CREATEDDATE    
        ,CREATEDBY    
        ,SEQ    
        ,AMOUNT_ORG    
        ,ORG_CCY    
        ,ORG_CCY_EXRATE    
        ,PRDTYPE    
        ,CF_ID
        ) SELECT C.DOWNLOAD_DATE
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,A.MASTERID    
        ,A.BRCODE    
        ,A.CIFNO    
        ,A.FACNO    
        ,A.ACCTNO    
        ,A.DATASOURCE    
        ,C.CCY    
        ,A.PRDCODE    
        ,C.TRXCODE    
        ,C.FLAG_CF    
        ,C.FLAG_REVERSE    
        ,C.METHOD    
        ,C.STATUS    
        ,C.SRCPROCESS    
        ,C.AMOUNT    
        ,CURRENT_TIMESTAMP    
        ,''EIR_SWITCH''    
        ,C.SEQ    
        ,C.AMOUNT_ORG    
        ,C.ORG_CCY    
        ,C.ORG_CCY_EXRATE    
        ,A.PRDTYPE    
        ,C.CF_ID   
        FROM ' || V_TABLEINSERT3 || ' A
        JOIN ' || V_TABLEINSERT2 || ' B ON B.AMORTSTOPDATE IS NULL
        AND B.MASTERID = A.PREV_MASTERID
        AND B.PREV_PMT_DATE = B.PMT_DATE
        AND B.DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        JOIN ' || V_TABLEINSERT4 || ' C ON C.ECFDATE = B.DOWNLOAD_DATE
        AND C.MASTERID = B.MASTERID AND C.STATUS = ''ACT''
        WHERE B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND PREV_EIR_ECF = ''Y'''
    EXECUTE (V_STR_QUERY);

    --REV OLD COST FEE PREV  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET STATUS  = CASE
            WHEN ' || V_TABLEINSERT1 || '.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                THEN ''REV2''
                ELSE ''ACT''
            END
        ,CREATEDBY = ''EIR_SWITCH''
        FROM (
            SELECT C.MASTERID
            ,C.DOWNLOAD_DATE
            ,C.SEQ
            FROM ' || V_TABLEINSERT3 || ' A
            JOIN ' || V_TABLEINSERT8 || ' P ON P.CURRDATE = A.DOWNLOAD_DATE
            JOIN VW_LBM_LAST_EIR_CF_PREV C ON C.MASTERID = A.PREV_MASTERID 
            WHERE A.PREV_EIR_ECF = ''Y''
        ) C
        WHERE ' || V_TABLEINSERT1 || '.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
        AND ' || V_TABLEINSERT1 || '.MASTERID = C.MASTERID
        AND ' || V_TABLEINSERT1 || '.SEQ = C.SEQ';
    EXECUTE (V_STR_QUERY);

    --REV OLD COST FEE PREV  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' (
        DOWNLOAD_DATE
        ,ECFDATE
        ,MASTERID
        ,BRCODE
        ,CIFNO    
        ,FACNO    
        ,ACCTNO    
        ,DATASOURCE    
        ,CCY    
        ,PRDCODE    
        ,TRXCODE    
        ,FLAG_CF    
        ,FLAG_REVERSE    
        ,METHOD    
        ,STATUS    
        ,SRCPROCESS    
        ,AMOUNT    
        ,CREATEDDATE    
        ,CREATEDBY    
        ,ISUSED    
        ,SEQ    
        ,AMOUNT_ORG    
        ,ORG_CCY    
        ,ORG_CCY_EXRATE    
        ,PRDTYPE    
        ,CF_ID  
        )
        SELECT ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,A.MASTERID    
        ,A.BRCODE    
        ,A.CIFNO    
        ,A.FACNO    
        ,A.ACCTNO    
        ,A.DATASOURCE    
        ,D.CCY    
        ,A.PRDCODE    
        ,D.TRXCODE    
        ,D.FLAG_CF    
        ,D.FLAG_REVERSE    
        ,D.METHOD    
        ,''ACT'' STATUS    
        ,D.SRCPROCESS    
        ,D.AMOUNT    
        ,CURRENT_TIMESTAMP    
        ,''EIR_SWITCH''    
        ,D.ISUSED    
        ,''0'' SEQ_NEW    
        ,D.AMOUNT_ORG    
        ,D.ORG_CCY    
        ,D.ORG_CCY_EXRATE    
        ,A.PRDTYPE    
        ,D.CF_ID
        FROM ' || V_TABLEINSERT3 || ' A
        JOIN VW_LBM_LAST_EIR_CF_PREV C ON C.MASTERID = A.PREV_MASTERID
        JOIN ' || V_TABLEINSERT1 || ' D ON D.DOWNLOAD_DATE = C.DOWNLOAD_DATE  
        AND D.MASTERID = C.MASTERID 
        AND D.SEQ = C.SEQ  
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.PREV_EIR_ECF = ''Y''
       ';
    EXECUTE (V_STR_QUERY);

    --REV OLD COST FEE PREV  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' 
        SET AMORTSTOPDATE  = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,AMORTSTOPMSG = ''EIR_SWITCH''
        WHERE AMORTSTOPDATE IS NULL
        FROM ' || V_TABLEINSERT3 || '
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.PREV_EIR_ECF = ''Y''
        ';
    EXECUTE (V_STR_QUERY);

    -- HANDLE ACF THAT IS DOING ACCRU YESTERDAY
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT9 ||
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT9 || ' (MASTERID)
        SELECT PREV_MASTERID AS MASTERID
        FROM ' || V_TABLEINSERT3 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND PREV_EIR_ECF = ''Y''
        ';
    EXECUTE (V_STR_QUERY);

    -- NO ACCRU IF YESTERDAY IS DOING AMORT  
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT9 ||

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT9 || ' (MASTERID)
        SELECT DISTINCT MASTERID
        FROM ' || V_TABLEINSERT7 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND DO_AMORT = ''Y''
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT12 ||
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT12 || ' (MASTERID)
        SELECT A.MASTERID
        FROM ' || V_TABLEINSERT9 || ' A
        ';
    EXECUTE (V_STR_QUERY);

    --LEFT JOIN V_TABLEINSERT9 B ON B.MASTERID=A.MASTERID    
    --WHERE B.MASTERID IS NULL    
    -- GET LAST ACF WITH DO_AMORT=N 
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT15 ||
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT15 || ' (ID)
        SELECT MAX(ID) AS ID
        FROM ' || V_TABLEINSERT7 || '
        WHERE MASTERID IN (
            SELECT MASTERID
            FROM ' || V_TABLEINSERT12 || '
        )
        AND DO_AMORT = ''N''
        GROUP BY MASTERID
        ';
    EXECUTE (V_STR_QUERY);

    --UPDATE SW ADJ COST/FEE FOR LBM  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || '
        SET SW_ADJ_COST = B.N_ACCRU_COST    
            ,SW_ADJ_FEE = B.N_ACCRU_FEE
        FROM (
            SELECT B.MASTERID
            ,B.ACCTNO
            ,B.N_ACCRU_COST
            ,B.N_ACCRU_FEE
            ,C.CURRDATE
            FROM ' || V_TABLEINSERT5 || ' B
            CROSS JOIN ' || V_TABLEINSERT8 || ' C
            WHERE B.ID IN (
                SELECT ID
                FROM ' || V_TABLEINSERT15 || '
            ) B
        WHERE ' || V_TABLEINSERT3 || '.DOWNLOAD_DATE = C.CURRDATE
        AND ' || V_TABLEINSERT3 || '.PREV_EIR_ECF =''Y''
        AND ' || V_TABLEINSERT3 || '.PREV_MASTERID = B.MASTERID  
        AND ' || V_TABLEINSERT3 || '.PREV_ACCTNO = B.ACCTNO  
        )
        --END ADD LBM 20180823 
        ';
    EXECUTE (V_STR_QUERY);

    -- GET FEE SUMMARY 
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT11 ||
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT11 || ' (    
        SUM_AMT    
        ,DOWNLOAD_DATE    
        ,MASTERID    
        )
        SELECT SUM(A.N_AMOUNT) AS SUM_AMT
        ,A.DOWNLOAD_DATE
        ,A.MASTERID
        FROM (
            SELECT CASE
                WHEN A.FLAG_REVRESE = ''Y'' THEN -1 * A.N_AMOUNT
                ELSE A.N_AMOUNT
                END AS N_AMOUNT
            ,A.ECFDATE DOWNLOAD_DATE
            ,A.MASTERID
            FROM ' || V_TABLEINSERT4 || ' A
            WHERE A.MASTERID IN (
                SELECT MASTERID
                FROM ' || V_TABLEINSERT12 || '
            )
            AND A.STATUS = ''ACT''
            AND A.FLAG_CF = ''F''
            AND A.METHOD = ''EIR''
        ) A
        GROUP BY A.DOWNLOAD_DATE
        ,A.MASTERID
        ';
    EXECUTE (V_STR_QUERY);

    -- GET COST SUMMARY
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT13 ||
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT13 || ' (    
        SUM_AMT    
        ,DOWNLOAD_DATE    
        ,MASTERID    
        )
        SELECT SUM(A.N_AMOUNT) AS SUM_AMT
        ,A.DOWNLOAD_DATE
        ,A.MASTERID
        FROM (
            SELECT CASE
                WHEN A.FLAG_REVRESE = ''Y'' THEN -1 * A.N_AMOUNT
                ELSE A.N_AMOUNT
                END AS N_AMOUNT
            ,A.ECFDATE DOWNLOAD_DATE
            ,A.MASTERID
            FROM ' || V_TABLEINSERT4 || ' A
            WHERE A.MASTERID IN (
                SELECT MASTERID
                FROM ' || V_TABLEINSERT12 || '
            )
            AND A.STATUS = ''ACT''
            AND A.FLAG_CF = ''C''
            AND A.METHOD = ''EIR''
        ) A
        GROUP BY A.DOWNLOAD_DATE
        ,A.MASTERID
        ';
    EXECUTE (V_STR_QUERY);

    --INSERT FEE 1 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' (    
        FACNO    
        ,CIFNO    
        ,DOWNLOAD_DATE    
        ,ECFDATE    
        ,DATASOURCE    
        ,PRDCODE    
        ,TRXCODE    
        ,CCY    
        ,AMOUNT    
        ,STATUS    
        ,CREATEDDATE    
        ,ACCTNO    
        ,MASTERID    
        ,FLAG_CF    
        ,FLAG_REVERSE    
        ,AMORTDATE    
        ,SRCPROCESS    
        ,ORG_CCY    
        ,ORG_CCY_EXRATE    
        ,PRDTYPE    
        ,CF_ID    
        )
        SELECT A.FACNO    
        ,A.CIFNO
        , ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        ,A.ECFDATE    
        ,A.DATASOURCE    
        ,B.PRDCODE    
        ,B.TRXCODE    
        ,B.CCY    
        ,CAST(CAST(CASE     
            WHEN B.FLAG_REVERSE = ''Y''    
            THEN - 1 * B.AMOUNT    
            ELSE B.AMOUNT    
            END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS NUMERIC(32, 20)) * A.N_ACCRU_FEE AS N_AMOUNT    
        ,B.STATUS    
        ,CURRENT_TIMESTAMP    
        ,A.ACCTNO    
        ,A.MASTERID    
        ,B.FLAG_CF    
        ,''N''    
        ,NULL AS AMORTDATE    
        ,''SW''    
        ,B.ORG_CCY    
        ,B.ORG_CCY_EXRATE    
        ,B.PRDTYPE    
        ,B.CF_ID
        FROM ' || V_TABLEINSERT2 || ' A
        JOIN ' || V_TABLEINSERT4 || '
        B ON B.ECFDATE = A.ECFDATE
        AND B.MASTERID = A.MASTERID
        AND B.STATUS = ''ACT''
        JOIN ' || V_TABLEINSERT11 || ' C ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
        WHERE A.ID IN (
            SELECT MASTERID
            FROM ' || V_TABLEINSERT9|| '
        )

        ';
    EXECUTE (V_STR_QUERY);

    /*    
    AND A.MASTERID NOT IN (    
    SELECT MASTERID FROM IFRS_ACCT_EIR_ECF    
    WHERE PMT_DATE = @V_CURRDATE    
    AND AMORTSTOPDATE IS NULL    
    )    
        
    */    
    --COST 1 

    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' (    
        FACNO    
        ,CIFNO    
        ,DOWNLOAD_DATE    
        ,ECFDATE    
        ,DATASOURCE    
        ,PRDCODE    
        ,TRXCODE    
        ,CCY    
        ,AMOUNT    
        ,STATUS    
        ,CREATEDDATE    
        ,ACCTNO    
        ,MASTERID    
        ,FLAG_CF    
        ,FLAG_REVERSE    
        ,AMORTDATE    
        ,SRCPROCESS    
        ,ORG_CCY    
        ,ORG_CCY_EXRATE    
        ,PRDTYPE    
        ,CF_ID    
        )
        SELECT A.FACNO    
        ,A.CIFNO
        , ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        ,A.ECFDATE    
        ,A.DATASOURCE    
        ,B.PRDCODE    
        ,B.TRXCODE    
        ,B.CCY    
        ,CAST(CAST(CASE     
            WHEN B.FLAG_REVERSE = ''Y''    
            THEN - 1 * B.AMOUNT    
            ELSE B.AMOUNT    
            END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS NUMERIC(32, 20)) * A.N_ACCRU_COST AS N_AMOUNT    
        ,B.STATUS    
        ,CURRENT_TIMESTAMP    
        ,A.ACCTNO    
        ,A.MASTERID    
        ,B.FLAG_CF    
        ,''N''    
        ,NULL AS AMORTDATE    
        ,''SW''    
        ,B.ORG_CCY    
        ,B.ORG_CCY_EXRATE    
        ,B.PRDTYPE    
        ,B.CF_ID
        FROM ' || V_TABLEINSERT2 || ' A
        JOIN ' || V_TABLEINSERT4 || '
        B ON B.ECFDATE = A.ECFDATE
        AND B.MASTERID = A.MASTERID
        AND B.FLAG_CD = ''C''
        AND B.STATUS = ''ACT''
        JOIN ' || V_TABLEINSERT13 || ' C ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
        WHERE A.ID IN (
            SELECT MASTERID
            FROM ' || V_TABLEINSERT9|| '
        )

        ';
    EXECUTE (V_STR_QUERY);

    --STOP OLD ACCRU BEFORE CURRDATE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' 
        SET STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE STATUS = ''ACT''
        --AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND MASTERID IN (
            SELECT PREV_MASTERID
            FROM ' || V_TABLEINSERT3 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND PREV_EIR_ECF = ''Y''
        )
        ';
    EXECUTE (V_STR_QUERY);

    -- GET COST SUMMARY
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT14 ||
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT14 || ' (    
        MASTERID    
        ,PMTDATE    
        )
        SELECT A.MASTERID
        ,MIN(A.PMT_DATE) AS PMTDATE
        FROM ' || V_TABLEINSERT2 || ' A
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND B.PREV_EIR_ECF = ''Y''
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.AMORTSTOPDATE IS NULL
        AND A.PMT_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND A.PMT_DATE <> A.PREV_PMT_DATE 
        GROUP BY A.MASTERID
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' 
        SET SW_ADJ_COST = COALESCE('''|| V_TABLEINSERT2 ||| '''.SW_ADJ_COST, 0) + X.SW_ADJ_COST 
            SW_ADJ_FEE = COALESCE('''|| V_TABLEINSERT2 ||| '''.SW_ADJ_FEE, 0) + X.SW_ADJ_FEE 
            ,SW_ADJ_FEE = B.N_ACCRU_FEE
        FROM (
        SELECT A.*
        ,B.SW_ADJ_COST
        ,B.SW_ADJ_FEE
        ,C.CURRDATE
        FROM ' || V_TABLEINSERT14 || ' A    
        JOIN ' || V_TABLEINSERT3 || ' B ON B.MASTERID = A.MASTERID
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        JOIN ' || V_TABLEINSERT8 || ' C ON C.CURRDATE = B.DOWNLOAD_DATE
        WHERE B.PREV_EIR_ECF = ''Y''
        ) X
        WHERE ' || V_TABLEINSERT2 || '.DOWNLOAD_DATE = X.
        AND ' || V_TABLEINSERT2 || '.MASTERID = X.MASTERID
        AND ' || V_TABLEINSERT2 || '.PMT_DATE = X.PMTDATE  
        ';
    EXECUTE (V_STR_QUERY);

     /*FOR LBM NO NEED UPDATE TO V_TABLEINSERT6 20180823  
    -- UPDATE COST FEE SUMM
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' 
        SET SW_ADJ_COST = COALESCE('''|| V_TABLEINSERT6 ||| '''.AMOUNT_FEE, 0) + COALESCE('''|| V_TABLEINSERT6 ||| '''.AMOUNT_FEE, 0)
            SW_ADJ_FEE = COALESCE('''|| V_TABLEINSERT6 ||| '''.AMOUNT_COST, 0) + COALESCE('''|| V_TABLEINSERT6 ||| '''.AMORT_COST, 0)
            ,SW_ADJ_FEE = B.N_ACCRU_FEE
        FROM (
        SELECT B.DOWNLOAD_DATE    
        ,B.MASTERID
        FROM ' || V_TABLEINSERT3 || ' B   
        WHERE B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND B.MASTERID IN (
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT3 || '    
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND PREV_EIR_ECF = ''Y''
        ) X
        WHERE ' || V_TABLEINSERT6 || '.DOWNLOAD_DATE = X.DOWNLOAD_DATE 
        AND ' || V_TABLEINSERT6 || '.MASTERID = X.MASTERID
        ';
    EXECUTE (V_STR_QUERY);    
    */  

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_LBM_ACCT_EIR_SWITCH', '');

    RAISE NOTICE 'SP_IFRS_LBM_ACCT_EIR_SWITCH | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_LBM_ACCT_EIR_SWITCH';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;