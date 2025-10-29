CREATE OR REPLACE PROCEDURE SP_IFRS_JRNL_ACF_ABN_ADJ(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    V_TABLEINSERT11 VARCHAR(100);
    V_TABLEINSERT12 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_PARAM_DISABLE_ACCRU_PREV INT;
    V_SL_METHOD VARCHAR(40);
    V_ROUND INT;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_SUMM_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_EIR_ACF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_ACCT_JOURNAL_INTM_SUMM_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT11 := 'IFRS_ACCT_SL_ECF_' || P_RUNID || '';
        V_TABLEINSERT12 := 'IFRS_CFID_JOURNAL_INTM_SUMM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_SUMM';
        V_TABLEINSERT3 := 'IFRS_ACCT_EIR_ACF';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_COST_FEE_PREV';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_ECF';
        V_TABLEINSERT7 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT8 := 'IFRS_ACCT_JOURNAL_INTM_SUMM';
        V_TABLEINSERT9 := 'IFRS_ACCT_SL_ACF';
        V_TABLEINSERT10 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEINSERT11 := 'IFRS_ACCT_SL_ECF';
        V_TABLEINSERT12 := 'IFRS_CFID_JOURNAL_INTM_SUMM';
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

    SELECT VALUE1 INTO V_SL_METHOD
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SL_METHOD';

    IF V_SL_METHOD IS NULL OR V_SL_METHOD NOT IN ('ECF', 'NO_ECF') THEN 
        V_SL_METHOD := 'ECF';
    END IF;

    V_PARAM_DISABLE_ACCRU_PREV := 0;

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
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_JRNL_ACF_ABN_ADJ', '');

    -- CLEAN UP  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT8 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCEPROCESS IN (
            ''ACFABN_ADJ''
            ,''REV_ADJ''
            ,''REV_ADJ2''
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT7 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        AND SOURCEPROCESS IN (
            ''ACFABN_ADJ''
            ,''REV_ADJ''
            ,''REV_ADJ2''
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
        SELECT MASTERID
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND CREATEDBY = ''SP_EIR_LAST_ACF_ABN'' 

        UNION

        SELECT MASTERID
        FROM ' || V_TABLEINSERT9 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND CREATEDBY = ''SP_SL_LAST_ACF_ABN'' ';
    EXECUTE (V_STR_QUERY);


    --20180108 REVERSAL RESONA WILL USE SIMILAR LOGIC FOR SIMPLICITY  
    --20180108 CF REVERSAL WILL EXCLUDE CF AND ITS CF REV PAIR FROM ECF GENERATION  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T2' || ' (MASTERID) 
        SELECT DISTINCT MASTERID
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE FLAG_REVERSE = ''Y''
            AND CF_ID_REV IS NOT NULL
            AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND MASTERID NOT IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || ' 
            ) --PREVENT DOUBLE PROCESSING  
            --20180310 ALSO PROCESS TODAY NEW EIR ECF HAVING AMORT AMT <= CURRDATE  

        UNION

        SELECT MASTERID
        FROM ' || V_TABLEINSERT5 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND AMORTSTOPDATE IS NULL
            AND PMT_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND MASTERID NOT IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || ' 
            ) --PREVENT DOUBLE PROCESSING  
        GROUP BY MASTERID
        HAVING SUM(N_AMORT_AMT) <> 0
        --20180328 ALSO PROCESS TODAY NEW SL ECF HAVING AMORT AMT <= CURRDATE  

        UNION

        SELECT MASTERID
        FROM ' || V_TABLEINSERT11 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND AMORTSTOPDATE IS NULL
            AND PMTDATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND MASTERID NOT IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || ' 
            ) --PREVENT DOUBLE PROCESSING  
        GROUP BY MASTERID
        HAVING SUM(N_AMORT_FEE) <> 0
            OR SUM(N_AMORT_COST) <> 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,FACNO
            ,CIFNO
            ,ACCTNO
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,JOURNALCODE2
            ,STATUS
            ,REVERSE
            ,FLAG_CF
            ,N_AMOUNT
            ,SOURCEPROCESS
            ,CREATEDDATE
            ,CREATEDBY
            ,BRANCH
            ,PRDTYPE
            ,IS_PNL
            ,CF_ID
            ,METHOD
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,A.MASTERID
            ,B.FACNO
            ,B.CIFNO
            ,B.ACCTNO
            ,B.DATASOURCE
            ,B.PRD_CODE
            ,B.TRX_CODE
            ,B.CCY
            ,''AMORT''
            ,CASE 
                WHEN B.METHOD = ''SL''
                THEN ''ACCRU_SL''
                ELSE ''ACCRU''
            END
            ,''ACT''
            ,''N''
            ,B.FLAG_CF
            ,- 1 * A.UNAMORT_AMT
            ,''ACFABN_ADJ''
            ,CURRENT_TIMESTAMP
            ,''ACFABN_ADJ''
            ,B.BRCODE
            ,B.PRD_TYPE
            ,''''
            ,B.CF_ID
            ,B.METHOD
        FROM ' || V_TABLEINSERT12 || ' A
        JOIN ' || V_TABLEINSERT1 || ' B 
            ON B.CF_ID = A.CF_ID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || ' 
            )
            AND A.UNAMORT_AMT <> 0 ';
    EXECUTE (V_STR_QUERY);

    --20180108 REV ADJ  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,FACNO
            ,CIFNO
            ,ACCTNO
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,JOURNALCODE2
            ,STATUS
            ,REVERSE
            ,FLAG_CF
            ,N_AMOUNT
            ,SOURCEPROCESS
            ,CREATEDDATE
            ,CREATEDBY
            ,BRANCH
            ,PRDTYPE
            ,IS_PNL
            ,CF_ID
            ,METHOD
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,A.MASTERID
            ,B.FACNO
            ,B.CIFNO
            ,B.ACCTNO
            ,B.DATASOURCE
            ,B.PRD_CODE
            ,B.TRX_CODE
            ,B.CCY
            ,''AMORT''
            ,CASE 
                WHEN B.METHOD = ''SL''
                THEN ''ACCRU_SL''
                ELSE ''ACCRU''
            END
            ,''ACT''
            ,C.FLAG_REVERSE --20180305 FOLLOW FLAG REV FROM COST FEE  
            ,B.FLAG_CF
            ,CASE 
                WHEN C.FLAG_REVERSE = ''N''
                THEN - 1 * A.UNAMORT_AMT
                ELSE A.UNAMORT_AMT
            END --20180305 FOLLOW FLAG REV FROM COST FEE  
            ,''REV_ADJ''
            ,CURRENT_TIMESTAMP
            ,''ACFABN_ADJ''
            ,B.BRCODE
            ,B.PRD_TYPE
            ,''''
            ,B.CF_ID
            ,B.METHOD
        FROM ' || V_TABLEINSERT12 || ' A
        JOIN ' || V_TABLEINSERT1 || ' B 
            ON (
                B.CF_ID = A.CF_ID
                OR B.CF_ID_REV = A.CF_ID
            ) --ONLY PROCESS TODAY CF_ID REV AND ITS PAIR  
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.FLAG_REVERSE = ''Y''
            AND B.CF_ID_REV IS NOT NULL
        JOIN ' || V_TABLEINSERT1 || ' C 
            ON C.CF_ID = A.CF_ID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T2' || ' 
            )
            AND A.UNAMORT_AMT <> 0 ';
    EXECUTE (V_STR_QUERY);

    --20180109 ADJ DIFF BETWEEN ALREADY AMORTIZED VS CURRENT FOR RESONA REV CF RECALC START FROM LOAN_START_DATE   
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,FACNO
            ,CIFNO
            ,ACCTNO
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,JOURNALCODE2
            ,STATUS
            ,REVERSE
            ,FLAG_CF
            ,N_AMOUNT
            ,SOURCEPROCESS
            ,CREATEDDATE
            ,CREATEDBY
            ,BRANCH
            ,PRDTYPE
            ,IS_PNL
            ,CF_ID
            ,METHOD
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,A.MASTERID
            ,B.FACNO
            ,B.CIFNO
            ,B.ACCTNO
            ,B.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''AMORT''
            ,CASE 
                WHEN B.METHOD = ''SL''
                THEN ''ACCRU_SL''
                ELSE ''ACCRU''
            END
            ,''ACT''
            ,''N''
            ,B.FLAG_CF
            ,(CASE 
                WHEN B.FLAG_REVERSE = ''Y''
                THEN - 1 * B.AMOUNT
                ELSE B.AMOUNT
            END) - A.UNAMORT_AMT
            ,''REV_ADJ2''
            ,CURRENT_TIMESTAMP
            ,''ACFABN_ADJ''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''''
            ,B.CF_ID
            ,B.METHOD
        FROM ' || V_TABLEINSERT12 || ' A
        JOIN VW_LAST_SL_CF_PREV C 
            ON C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
            AND C.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT10 || ' B ON B.CF_ID = A.CF_ID
            AND B.DOWNLOAD_DATE = C.DOWNLOAD_DATE
            AND B.MASTERID = C.MASTERID
            AND B.SEQ = C.SEQ
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T2' || ' 
            ) ';
    EXECUTE (V_STR_QUERY);

    --20180305 EIR ADJ DIFF BETWEEN ALREADY AMORTIZED VS CURRENT FOR RESONA REV CF RECALC START FROM LOAN_START_DATE   
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,FACNO
            ,CIFNO
            ,ACCTNO
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,JOURNALCODE2
            ,STATUS
            ,REVERSE
            ,FLAG_CF
            ,N_AMOUNT
            ,SOURCEPROCESS
            ,CREATEDDATE
            ,CREATEDBY
            ,BRANCH
            ,PRDTYPE
            ,IS_PNL
            ,CF_ID
            ,METHOD
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,A.MASTERID
            ,B.FACNO
            ,B.CIFNO
            ,B.ACCTNO
            ,B.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''AMORT''
            ,CASE 
                WHEN B.METHOD = ''SL''
                THEN ''ACCRU_SL''
                ELSE ''ACCRU''
            END
            ,''ACT''
            ,''N''
            ,B.FLAG_CF
            ,(CASE 
                WHEN B.FLAG_REVERSE = ''Y''
                THEN - 1 * B.AMOUNT
                ELSE B.AMOUNT
            END) - A.UNAMORT_AMT
            ,''REV_ADJ2''
            ,CURRENT_TIMESTAMP
            ,''ACFABN_ADJ''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''''
            ,B.CF_ID
            ,B.METHOD
        FROM ' || V_TABLEINSERT12 || ' A
        JOIN ' || 'TMP_LAST_EIR_CF_PREV' || ' C 
            ON C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
            AND C.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.CF_ID = A.CF_ID
            AND B.DOWNLOAD_DATE = C.DOWNLOAD_DATE
            AND B.MASTERID = C.MASTERID
            AND B.SEQ = C.SEQ
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T2' || ' 
            ) ';
    EXECUTE (V_STR_QUERY);

    /* UPDATE BY RISWANTO 03-12-2015 */
    -- NOT REVERSE  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET N_AMOUNT_IDR = N_AMOUNT * COALESCE(RATE_AMOUNT, 1)
        FROM ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' B
        WHERE A.CCY = B.CURRENCY
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.REVERSE = ''N''
            AND A.SOURCEPROCESS IN (
                ''ACFABN_ADJ''
                ,''REV_ADJ''
                ,''REV_ADJ2''
            ) ';
    EXECUTE (V_STR_QUERY);

    -- INTM REVERSE DATA  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET N_AMOUNT_IDR = A.N_AMOUNT * COALESCE(RATE_AMOUNT, 1)
        FROM ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' B
        WHERE A.CCY = B.CURRENCY
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.REVERSE = ''Y''
            AND A.SOURCEPROCESS IN (
                ''ACFABN_ADJ''
                ,''REV_ADJ''
                ,''REV_ADJ2''
            ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT8 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,FACNO
            ,CIFNO
            ,ACCTNO
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,FLAG_CF
            ,N_AMOUNT
            ,SOURCEPROCESS
            ,CREATEDDATE
            ,CREATEDBY
            ,BRANCH
            ,IS_PNL
            ,JOURNALCODE2
            ,PRDTYPE
        ) SELECT 
            DOWNLOAD_DATE
            ,MASTERID
            ,FACNO
            ,CIFNO
            ,ACCTNO
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,FLAG_CF
            ,N_AMOUNT
            ,SOURCEPROCESS
            ,CREATEDDATE
            ,CREATEDBY
            ,BRANCH
            ,IS_PNL
            ,JOURNALCODE2
            ,PRDTYPE
        FROM ' || V_TABLEINSERT7 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND SOURCEPROCESS IN (
                ''ACFABN_ADJ''
                ,''REV_ADJ''
                ,''REV_ADJ2''
            ) ';
    EXECUTE (V_STR_QUERY);

    -- ADDED 03122015 UPDATE AMORT AMOUNT ON COST FEE SUMM  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_AP' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_AP' || ' 
        (
            MASTERID
            ,FLAG_CF
            ,AMOUNT
        ) SELECT 
            MASTERID
            ,FLAG_CF
            ,SUM(CASE 
                WHEN REVERSE = ''Y''
                THEN - 1 * N_AMOUNT
                ELSE N_AMOUNT
            END) AS AMORT_AMOUNT
        FROM ' || V_TABLEINSERT8 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND JOURNALCODE IN (
                ''ACCRU''
                ,''ACCRU_SL''
                ,''AMORT''
            )
            AND TRXCODE <> ''BENEFIT''
            AND MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || ' 
                
                UNION ALL
                
                SELECT MASTERID
                FROM ' || 'TMP_T2' || ' 
            )
        GROUP BY 
            MASTERID
            ,FLAG_CF ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET AMORT_FEE = B.AMOUNT
        FROM ' || 'TMP_AP' || ' B
        WHERE A.MASTERID = B.MASTERID
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.FLAG_CF = ''F''
            AND A.MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || '
                
                UNION ALL
                
                SELECT MASTERID
                FROM ' || 'TMP_T2' || '
            ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET AMORT_COST = B.AMOUNT
        FROM ' || 'TMP_AP' || ' B
        WHERE A.MASTERID = B.MASTERID
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.FLAG_CF = ''C''
            AND A.MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || '
                
                UNION ALL
                
                SELECT MASTERID
                FROM ' || 'TMP_T2' || '
            ) ';
    EXECUTE (V_STR_QUERY);

    --UPDATE IFRS_CFID_JOURNAL_INTM_SUMM  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT12 || ' A 
        SET 
            AMORT_AMT = ITRCG_AMT * - 1
            ,UNAMORT_AMT = 0
        WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T1' || ' 
            )
            AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    --20180108 UPDATE IFRS_CFID_JOURNAL_INTM_SUMM FOR REV_ADJ  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT12 || ' A 
        SET 
            AMORT_AMT = ITRCG_AMT * - 1
            ,UNAMORT_AMT = 0
        WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || 'TMP_T2' || ' 
            )
            AND CF_ID IN (
                SELECT CF_ID
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
                
                UNION ALL
                
                SELECT CF_ID_REV
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
            )
            AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    --20180109 UPDATE IFRS_CFID_JOURNAL_INTM_SUMM FOR REV_ADJ2  
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT12 || ' A 
        SET 
            AMORT_AMT = AMORT_AMT + B.N_AMOUNT
            ,UNAMORT_AMT = UNAMORT_AMT + B.N_AMOUNT
        FROM ' || V_TABLEINSERT7 || ' B
        WHERE A.MASTERID IN (
                SELECT MASTERID
                FROM TMP_T2
            )
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.SOURCEPROCESS = ''REV_ADJ2''
            AND B.CF_ID = A.CF_ID ';
    EXECUTE (V_STR_QUERY);

    --- 20180827 DELETE JOURNAL WHEN AMOUNT = 0 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT7 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND N_AMOUNT = 0 ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_JRNL_ACF_ABN_ADJ', '');

    RAISE NOTICE 'SP_IFRS_JRNL_ACF_ABN_ADJ | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_JRNL_ACF_ABN_ADJ';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
