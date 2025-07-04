---- DROP PROCEDURE SP_IFRS_PAYM_SCHD_SRC;

CREATE OR REPLACE PROCEDURE SP_IFRS_PAYM_SCHD_SRC(
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
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_COUNTER_PAY INT;
    V_MAX_COUNTERPAY INT;
    V_NEXT_COUNTER_PAY INT;
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
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_SUMM_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_PAYM_CORE_SRC_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_PAYM_SCHD_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_PAYM_SCHD_ALL_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_SUMM';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT5 := 'IFRS_PAYM_CORE_SRC';
        V_TABLEINSERT6 := 'IFRS_PAYM_SCHD';
        V_TABLEINSERT7 := 'IFRS_PAYM_SCHD_ALL';
        V_TABLEINSERT8 := 'IFRS_PRODUCT_PARAM';
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

    V_CUT_OFF_DATE := '2016-01-01'::DATE;
    V_MAX_COUNTERPAY := 0;
    V_COUNTER_PAY := 0;
    V_NEXT_COUNTER_PAY := 1;
    V_ROUND := 6;
    V_FUNCROUND = 1;
    V_LOG_ID := 911;
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
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_PAYM_CORE_SRC WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT6 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT6 || ' AS SELECT * FROM IFRS_PAYM_SCHD WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT7 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT7 || ' AS SELECT * FROM IFRS_PAYM_SCHD_ALL WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || IFRS_LBM_ACCT_EIR_ACCRU_PREV || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND SRCPROCESS = ''ECF''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_ACCT_COST_FEE || '
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''PNL''
        AND CREATEDBY = ''EIRECF1''
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''PNL''
        AND CREATEDBY = ''EIRECF2''
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''PNL2''
        AND CREATEDBY = ''EIRECF2''
        AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'TRUNCARE TABLE ' || TMP_T7 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T7 || ' 
        (
            MID  
            ,STAFFLOAN  
            ,PKID  
            ,NPVRATE 
        ) SELECT 
            A.MASTERID  
            ,CASE   
            WHEN COALESCE(STAFF_LOAN_FLAG, ''N'') IN (  
                ''N''  
                ,''''  
                )  
                THEN 0  
            ELSE 1  
            END  
            ,A.ID  
            ,CASE   
            WHEN STAFF_LOAN_FLAG = ''Y''  
                THEN COALESCE(P.MARKET_RATE, 0)  
            ELSE 0  
            END MARKET_RATE  
        FROM ' || IFRS_IMA_AMORT_CURR || ' 
        LEFT JOIN ' || IFRS_PRODUCT_PARAM || ' P ON P.DATA_SOURCE = A.DATA_SOURCE 
        AND P.PRD_TYPE = A.PRODUCT_TYPE  
        AND P.PRD_CODE = A.PRODUCT_CODE
        AND (  
        P.CCY = A.CURRENCY  
        OR ISNULL(P.CCY, ''ALL'') = ''ALL''  
        )
        WHERE A.EIR_STATUS = ''Y''
        AND A.AMORT_TYPE <> ''SL''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'TRUNCARE TABLE ' || IFRS_LBM_ACCT_EIR_CF_ECF || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'SELECT DISTINCT MASTERID
        INTO #TODAYREV
        FROM ' || IFRS_ACCT_COST_FEE || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND FLAG_REVERSE = ''Y''
            AND CF_ID_REV IS NOT NULL
            ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        (
             MASTERID  
            ,FEE_AMT  
            ,COST_AMT  
            ,FEE_AMT_ACRU  
            ,COST_AMT_ACRU  
            ,STAFFLOAN  
            ,PKID  
            ,NPV_RATE  
            ,GAIN_LOSS_CALC --20180226 SET N   
        ) SELECT 
            A.MID  
            ,SUM(COALESCE(CASE   
                WHEN C.FLAG_CF = ''F''  
                THEN CASE   
                    WHEN C.FLAG_REVERSE = ''Y''  
                    THEN - 1 * C.AMOUNT  
                    ELSE C.AMOUNT  
                    END  
                ELSE 0  
                END, 0))  
            ,SUM(COALESCE(CASE   
                WHEN C.FLAG_CF = ''C''  
                THEN CASE   
                    WHEN C.FLAG_REVERSE = ''Y''  
                    THEN - 1 * C.AMOUNT  
                    ELSE C.AMOUNT  
                    END  
                ELSE 0  
                END, 0))  
            ,0  
            ,0  
            ,A.STAFFLOAN  
            ,A.PKID  
            ,A.NPVRATE  
            ,''N'' --20180226 
        FROM ' || TMP_T7 || ' A 
        LEFT JOIN ' || IFRS_ACCT_COST_FEE || ' C
            ON C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND C.MSATERID = A.MID
            AND C.STATUS = ''ACT''
            AND C.METHOD = ''EIR''
            AND C.CF_ID NOT IN (
                SELECT CF_ID
                FROM ' || IFRS_ACCT_COST_FEE ||'
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
                UNION ALL
                SELECT CF_ID_REV
                FROM ' || IFRS_ACCT_COST_FEE || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
            )
        GROUP BY A.MID
            ,A.STAFFLOAN
            ,A.PKID
            ,A.NPVRATE';
    EXECUTE (V_STR_QUERY);

    --20180226 FILL TO COLUMN FOR NEW COST/FEE  
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET NEW_FEE_AMT = ISNULL(FEE_AMT, 0) 
            ,NEW_COST_AMT = ISNULL(COST_AMT, 0) 
            ,NEW_FEE_AMT_ACRU = ISNULL(FEE_AMT_ACRU, 0) 
            ,NEW_COST_AMT_ACRU = ISNULL(COST_AMT_ACRU, 0)';
    EXECUTE (V_STR_QUERY);

    -- SISA UNAMORT            
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T10 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T10 || ' 
        (
            MASTERID  
            ,FEE_AMT  
            ,COST_AMT 
        ) SELECT 
            B.MASTERID  
            ,SUM(COALESCE(CASE   
                WHEN B.FLAG_CF = 'F'  
                THEN CASE   
                    WHEN B.FLAG_REVERSE = 'Y'  
                    THEN - 1 * CASE   
                    WHEN CFREV.MASTERID IS NULL  
                        THEN B.AMOUNT  
                    ELSE B.AMOUNT  
                    END  
                    ELSE CASE   
                    WHEN CFREV.MASTERID IS NULL  
                    THEN B.AMOUNT  
                    ELSE B.AMOUNT  
                    END  
                    END  
                ELSE 0  
                END, 0)) AS FEE_AMT  
            ,SUM(COALESCE(CASE   
                WHEN B.FLAG_CF = ''C''  
                THEN CASE   
                    WHEN B.FLAG_REVERSE = ''Y''  
                    THEN - 1 * CASE   
                    WHEN CFREV.MASTERID IS NULL  
                        THEN B.AMOUNT  
                    ELSE B.AMOUNT  
                    END  
                    ELSE CASE   
                    WHEN CFREV.MASTERID IS NULL  
                    THEN B.AMOUNT  
                    ELSE B.AMOUNT  
                    END  
                    END  
                ELSE 0  
                END, 0)) AS COST_AMT  
        FROM ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || ' B
        JOIN ' || VW_LBM_LAST_EIR_CF_PREV || ' X ON X.MASTERID = B.MASTERID 
            AND X.DOWNLOAD_DATE = B.DOWNLOAD_DATE
            AND B.SEQ = X.SEQ
        --20160407 EIR STOP REV            
        LEFT JOIN (
            SELECT DISTINCT MASTERID
            FROM '|| IFRS_LBM_ACCT_EIR_STOP_REV || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            ) A ON A.MASTERID = B.MASTERID
             --20180116 RESONA REQ            
        LEFT JOIN #TODAYREV CFREV ON CFREV.MASTERID = B.MASTERID 
        WHERE ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,WHERE ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
        )
        AND B.STATUS = ''ACT''  
        AND A.MASTERID IS NULL
        --20180116 EXCLUDE CF REVERSAL AND ITS PAIR     
        AND B.CF_ID NOT IN (
            SELECT CF_ID
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
            )
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU 
        AND CASE
            WHEN B.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
                AND B.SEQ <> ''2''
                THEN 0
            ELSE 1
            END = 1
        GROUP BY B.MASTERID';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
    SET 
        FEE_AMT = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.FEE_AMT + B.FEE_AMT
        ,COST_AMT = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.COST_AMT + B.COST_AMT
    FROM ' || TMP_T10 || ' B
    WHERE B.MASTERID = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID';
    EXECUTE (V_STR_QUERY);

    IF PARAM_DISABLE_ACCRU_PREV != 0
    BEGIN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T1 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T1 || ' 
            (
                MASTERID
                ,ACCTNO
            ) SELECT DISTINCT MSATERID
                ,ACCTNO
            FROM ' || IFRS_LBM_ACCT_EIR_ACF || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND DO_AMORT = ''Y''';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T3 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T3 || ' 
            (
                MASTERID
            ) SELECT MSATERID
            FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || '
            WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || TMP_T1 || '
            )';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_P1 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_P1 || ' 
            (
                ID
            ) SELECT MAX(ID) AS ID
            FROM ' || IFRS_LBM_ACCT_EIR_ACF || '
            WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || TMP_T3 || '
            AND DO_AMORT = ''N''
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY MASTERID
            )';

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
            SET 
                FEE_AMT = FEE_AMT - B.N_ACCRU_FEE
                ,COST_AMT = COST_AMT - B.N_ACCRU_COST
            FROM (
                SELECT * 
                FROM ' || IFRS_LBM_ACCT_EIR_ACF || '
                WHERE ID IN (
                    SELECT ID
                    FROM ' || TMP_P1 || '
                )
            ) B
            WHERE B.MASTERID = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID
                AND ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID NOT IN (
                    SELECT MASTERID
                    FROM ' || IFRS_LBM_ACCT_EIR_STOP_REV || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    )
                AND ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID NOT IN (
                    SELECT DISTINCT MASTERID
                    FROM ' || IFRS_ACCT_SWITCH || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    )
            )';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T10 || '';
    EXECUTE (V_STR_QUERY);
            
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T10 || ' 
        (
            MASTERID  
            ,FEE_AMT  
            ,COST_AMT 
        ) SELECT 
            B.MASTERID  
            ,SUM(COALESCE(CASE   
                WHEN B.FLAG_CF = ''F''  
                THEN CASE   
                    WHEN B.FLAG_REVERSE = ''Y''  
                    THEN - 1 * B.AMOUNT  
                    ELSE B.AMOUNT  
                    END  
                ELSE 0  
                END, 0)) AS FEE_AMT
                THEN CASE   
                    WHEN B.FLAG_REVERSE = ''Y''  
                    THEN - 1 * B.AMOUNT  
                    ELSE B.AMOUNT  
                    END  
                ELSE 0  
                END, 0)) AS COST_AMT
        FROM ' || IFRS_LBM_ACCT_EIR_ACCRU_PREV || ' B 
        WHERE B.STATUS = ''ACT''
            AND B.CF_ID NOT IN (  
        SELECT CF_ID  
        FROM ' || IFRS_ACCT_COST_FEE || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND FLAG_REVERSE = ''Y''  
            AND CF_ID_REV IS NOT NULL
        UNION ALL
        SELECT CF_ID_REV
        FROM ' || IFRS_ACCT_COST_FEE || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND FLAG_REVERSE = ''Y''  
            AND CF_ID_REV IS NOT NULL
        )
        GROUP BY B.MASTERID';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET 
            FEE_AMT = FEE_AMT + B.FEE_AMT
            ,COST_AMT = COST_AMT + B.COST_AMT
        FROM ' || TMP_T10 || ' B
        WHERE B.MASTERID = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID';
    EXECUTE (V_STR_QUERY);
    -- UPDATE TOTAL            
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET TOTAL = ROUND (FEE_ANT + COST_AMT, 0)
        ,TOTAL_AMT_ACRU = ROUND (FEE_AMT + COST_AMT + FEE_AMT_ACRU + COST_AMT_ACRU, 0) 
           ';
    EXECUTE (V_STR_QUERY);
    -- UPDATE PREV EIR    
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T13 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T13 || ' 
        (
            MASTERID  
            ,N_EFF_INT_RATE  
            ,ENDAMORTDATE  
            )  
            SELECT B.MASTERID  
            ,B.N_EFF_INT_RATE  
            ,B.ENDAMORTDATE  
            FROM ' || IFRS_LBM_ACCT_EIR_ECF || ' B  
            WHERE B.AMORTSTOPDATE IS NULL  
            AND B.PMT_DATE = B.PREV_PMT_DATE  
        ';
    EXECUTE (V_STR_QUERY);
        
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET PREV_EIR = N_EFF_INT_RATE
            ,PREV_ENDAMORTDATE = B.ENDAMORTDATE 
        FROM ' || TMP_T13 || ' B
        WHERE (
            B.MASTERID = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID           ';
    EXECUTE (V_STR_QUERY);

    --20180226 SET GAIN_LOSS_CALC TO Y IF PREPAYMENT EVENT DETECTED WITHOUT OTHER EVENT (SIMPLIFY FOR NOW)          
    --PARTIAL PAYMENT EVENTID IS 6  

    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET GAIN_LOSS_CALC = ''Y''
        WHERE MASTERID IN (
            SELECT MASTERID
            FROM ' || IFRS_LBM_EVENT_CHANGES || '
            WHERE EVENT_ID = '6'
                AND EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        )
        AND MASTERID NOT IN (
            SELECT MASTERID
            FROM ' || IFRS_LBM_EVENT_CHANGES || '
            WHERE EVENT_ID IN ('0', '1', '2', '3')
                AND EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        )';
    EXECUTE (V_STR_QUERY);

    --20180226 IF DONT HAVE PREV EIR THEN SET BACK TO N          
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET GAIN_LOSS_CALC = ''N''  
        WHERE PREV_EIR IS NULL  
        AND GAIN_LOSS_CALC = ''Y''';
    EXECUTE (V_STR_QUERY);

    -- DO FULL AMORT IF SUM COST FEE ZERO AND DONT CREATE NEW ECF
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_ACCT_COST_FEE || ' 
        SET STATUS = ''PNL''
            ,CREATEDBY = ''EIRECF1''
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND MASTERID IN (
                SELECT MASTERID
                FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || '
                WHERE TOTAL_AMT = '0'
                    OR TOTAL_AMT_ACRU = '0'
            )  
        AND STATUS = ''ACT''
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY ACF_ABN
        AND CF_ID NOT IN (
            SELECT CF_ID
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
        )';
    EXECUTE (V_STR_QUERY);

    -- IF LAST COST FEE PREV IS CURRDATE
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T11 || ''
    EXECUTE (V_STR_QUERY);


    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T11 || ' 
        (
            MASTERID  
            ,DOWNLOAD_DATE  
            ,SEQ  
            ,CURRDATE 
        ) SELECT B.MASTERID  
            ,B.DOWNLOAD_DATE  
            ,B.SEQ  
            ,P.CURRDATE
        FROM VW_LBM_LAST_EIR_CF_PREV B  
        CROSS JOIN ' || IFRS_PRC_DATE_AMORT || 'P  
        WHERE B.MASTERID IN (  
        SELECT MASTERID  
        FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        WHERE TOTAL_AMT = 0  
            OR TOTAL_AMT_ACRU = 0  
        )   
        ';
    EXECUTE (V_STR_QUERY);
        
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || ' 
        SET STATUS = CASE
            WHEN STATUS = ''ACT''
                THEN ''PNL''
            ELSE STATUS
            END
            ,CREATEDBY = ''EIRECF2''
        FROM ' || TMP_T11 || ' B
        WHERE ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.DOWNLOAD_DATE = B.CURRDATE
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.MASTERID = B.MASTERID
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.SEQ = B.SEQ
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN  
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.CF_ID NOT IN (
            SELECT CF_ID
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
        )';
    EXECUTE (V_STR_QUERY);

     -- IF LAST COST FEE PREV IS PREVDATE
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T12 || ''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T12 || ' 
        (
             MASTERID  
            ,DOWNLOAD_DATE  
            ,SEQ  
            ,PREVDATE
        ) SELECT B.MASTERID  
            ,B.DOWNLOAD_DATE  
            ,B.SEQ  
            ,P.PREVDATE
        FROM VW_LBM_LAST_EIR_CF_PREV B  
        CROSS JOIN ' || IFRS_PRC_DATE_AMORT || 'P  
        WHERE B.MASTERID IN (  
            SELECT MASTERID  
            FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
            WHERE TOTAL_AMT = '0'  
                OR TOTAL_AMT_ACRU = '0'  
        )';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || ' 
        SET STATUS = CASE
            WHEN STATUS = ''ACT''
                THEN ''PNL2''
            ELSE STATUS
            END
            ,CREATEDBY = ''EIRECF2''
        FROM ' || TMP_T12 || ' B
        WHERE ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.DOWNLOAD_DATE = B.PREVDATE
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.MASTERID = B.MASTERID
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.SEQ = B.SEQ
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN  
        AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.CF_ID NOT IN (
            SELECT CF_ID
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM ' || IFRS_ACCT_COST_FEE || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL
        )
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU 
        AND CASE
            WHEN ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
                AND ' || IFRS_LBM_ACCT_EIR_COST_FEE_PREV || '.SEQ <> ''2''
                THEN 0
            ELSE 1
        END = 1
        ';
    EXECUTE (V_STR_QUERY);

    IF PARAM_DISABLE_ACCRU_PREV != 0
    BEGIN
    -- INSERT ACCRU PREV ONLY FOR PNL ED            
    -- GET LAST ACF WITH DO_AMORT=N    

        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_P1 || ''
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_P1 || ' 
            (
                ID
            ) SELECT MAX(ID) AS ID
            WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || TMP_T3 || '
            )
            AND DO_AMORT = ''N''
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            -- ADD FILTER PNL ED ACCTNO   
            AND MASTERID IN (
                SELECT MASTERID
                FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || '
                WHERE TOTAL_AMT = 0  
                    OR TOTAL_AMT_ACRU = 0
            )
            GROUP BY MASTERID';
        
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_TF || ''
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_TF || ' 
            (
                SUM_AMT  
                ,DOWNLOAD_DATE  
                ,MASTERID
            ) SELECT SUM(A.N_AMOUNT) AS SUM_AMT
                ,A.DOWNLOAD_DATE  
                ,A.MASTERID
            FROM (
                SELECT CASE 
                    WHEN A.FLAG_REVERSE = ''Y''
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE  
                ,A.MASTERID
            FROM ' || IFRS_LBM_ACCT_EIR_ACCRU_PREV || ' A 
            WHERE A.MASTERID IN (
                SELECT MASTERID
                FROM ' || TMP_T3 || '
            )
            AND A.STATUS = ''ACT''
            AND A.FLAG_CF = ''F''
            AND A.METHOD = ''EIR''
            ) A
            GROUP BY A.DOWNLOAD_DATE  
                ,A.MASTERID';
        EXECUTE (V_STR_QUERY); 

        -- GET COST SUMMARY
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_TC || ''
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_TC || ' 
            (
                SUM_AMT  
                ,DOWNLOAD_DATE  
                ,MASTERID
            ) SELECT SUM(A.N_AMOUNT) AS SUM_AMT
                ,A.DOWNLOAD_DATE  
                ,A.MASTERID
            FROM (
                SELECT CASE 
                    WHEN A.FLAG_REVERSE = ''Y''
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE  
                ,A.MASTERID
            FROM ' || IFRS_LBM_ACCT_EIR_COST_FEE_ECF || ' A 
            WHERE A.MASTERID IN (
                SELECT MASTERID
                FROM ' || TMP_T3 || '
            )
            AND A.STATUS = ''ACT''
            AND A.FLAG_CF = ''c''
            AND A.METHOD = ''EIR''
            ) A
            GROUP BY A.DOWNLOAD_DATE  
                ,A.MASTERID';
        EXECUTE (V_STR_QUERY); 

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_ACCT_EIR_ACCRU_PREV || ' 
            (
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
                ,METHOD
            ) SELECT A.FACNO  
                ,A.CIFNO  
                , ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ,A.ECFDATE  
                ,A.DATASOURCE  
                ,B.PRDCODE  
                ,B.TRXCODE  
                ,B.CCY  
                ,ROUND(CAST(CAST(CASE   
                    WHEN B.FLAG_REVERSE = ''Y''  
                        THEN - 1 * B.AMOUNT  
                    ELSE B.AMOUNT  
                    END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS DECIMAL(32, 20)) * A.N_ACCRU_FEE, @V_ROUND, @V_FUNCROUND) AS N_AMOUNT  
                ,B.STATUS  
                ,CURRENT_TIMESTAMP  
                ,A.ACCTNO  
                ,A.MASTERID  
                ,B.FLAG_CF  
                ,''N''  
                ,NULL AS AMORTDATE  
                ,''ECF''  
                ,B.ORG_CCY  
                ,B.ORG_CCY_EXRATE  
                ,B.PRDTYPE  
                ,B.CF_ID  
                ,B.METHOD
            FROM ' || IFRS_LBM_ACCT_EIR_ACF || ' A 
            JOIN ' || IFRS_LBM_ACCT_EIR_COST_FEE_ECF || ' B ON B.ECFDATE = A.ECFDATE  
                AND A.MASTERID = B.MASTERID  
                AND B.FLAG_CF = ''F'' AND B.STATUS = ''ACT''
            JOIN ' || TMP_TF || ' C ON C.DOWNLOAD_DATE = A.ECFDATE
                AND C.MASTERID = A.MASTERID
            WHERE A.ID IN (
                SELECT ID
                FROM ' || TMP_P1 || '
            )
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (
                SELECT CF_ID
                FROM ' || IFRS_ACCT_COST_FEE || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
                UNION ALL
                SELECT CF_ID_REV
                FROM ' || IFRS_ACCT_COST_FEE || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
            )
            ';
        EXECUTE (V_STR_QUERY); 

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_ACCT_EIR_ACCRU_PREV || ' 
            (
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
                ,METHOD
            ) SELECT A.FACNO  
                ,A.CIFNO  
                , ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ,A.ECFDATE  
                ,A.DATASOURCE  
                ,B.PRDCODE  
                ,B.TRXCODE  
                ,B.CCY  
                ,ROUND(CAST(CAST(CASE   
                    WHEN B.FLAG_REVERSE = 'Y'  
                        THEN - 1 * B.AMOUNT  
                    ELSE B.AMOUNT  
                    END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS DECIMAL(32, 20)) * A.N_ACCRU_COST, @V_ROUND, @V_FUNCROUND) AS N_AMOUNT  
                ,B.STATUS  
                ,CURRENT_TIMESTAMP  
                ,A.ACCTNO  
                ,A.MASTERID  
                ,B.FLAG_CF  
                ,''N''  
                ,NULL AS AMORTDATE  
                ,''ECF''  
                ,B.ORG_CCY  
                ,B.ORG_CCY_EXRATE  
                ,B.PRDTYPE  
                ,B.CF_ID  
                ,B.METHOD
            FROM ' || IFRS_LBM_ACCT_EIR_ACF || ' A 
            JOIN ' || IFRS_LBM_ACCT_EIR_COST_FEE_ECF || ' B ON B.ECFDATE = A.ECFDATE  
                AND A.MASTERID = B.MASTERID  
                AND B.FLAG_CF = ''C'' AND B.STATUS = ''ACT''
            JOIN ' || TMP_TC || ' C ON C.DOWNLOAD_DATE = A.ECFDATE
                AND C.MASTERID = A.MASTERID
            WHERE A.ID IN (
                SELECT ID
                FROM ' || TMP_P1 || '
            )
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (
                SELECT CF_ID
                FROM ' || IFRS_ACCT_COST_FEE || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
                UNION ALL
                SELECT CF_ID_REV
                FROM ' || IFRS_ACCT_COST_FEE || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                    AND FLAG_REVERSE = ''Y''
                    AND CF_ID_REV IS NOT NULL
            )
            ';
        EXECUTE (V_STR_QUERY); 
    END IF;

    -- AMORT ACRU
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_ACCRU_PREV || ' 
        SET STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE STATUS = ''ACT''
            AND MASTERID IN (
                SELECT MASTERID
                FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || '
                WHERE TOTAL_AMT = '0'
                    OR TOTAL_AMT_ACRU = '0'
            )';
    EXECUTE (V_STR_QUERY);

    -- STOP OLD ECF
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_ECF || ' 
        SET AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,AMORTSTOPMSG = ''SP_ACCT_EIR_ECF''  
        WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || '
            )
        AND AMORTSTOPDATE IS NULL
            ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_AMORT_LOG || ' (
        DOWNLOAD_DATE  
        ,DTM  
        ,OPS  
        ,PROCNAME  
        ,REMARK 
        ) VALUES (
        ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        ,CURRENT_TIMESTAMP
        ,''DEBUG''
        ,''SP_IFRS_LBM_ACCT_EIR_ECF_MAIN''
        ,''2''';
    EXECUTE (V_STR_QUERY);

    /* END GET LAST OR START DATE FOR ASSIGN FIRST PAYM DATE  --RIDWAN*/            
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TT_PSAK_LAST_PAYM_DATE || ''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TT_PSAK_LAST_PAYM_DATE || ' 
        (
            MASTERID ,            
            CURRDATE ,            
            LAST_PAYMENT_DATE_SCHD ,            
            LOAN_START_DATE
        ) SELECT A.MASTERID
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,B.PMTDATE 
            ,A.LOAN_START_DATE            
        FROM ' || IFRS_IMA_AMORT_CURR || ' A
        LEFT JOIN (
            SELECT MASTERID, MAX(PMTDATE) AS PMTDATE
            FROM ' || PSAK_PAYM_SCHD || '
            WHERE PMTDATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            GROUP BY MASTERID
        ) B ON A.MASTERID = B.MASTERID
        WHERE A.EIRECF = ''Y''
            AND A.FLAG_AL N (
                ''A''
            )
        UPDATE ' || TT_PSAK_LAST_PAYM_DATE || '
        SET ' || LAST_PAYMENT_DATE_ASSIGN || ' = CASE WHEN LAST_PAYMENT_DATE_SCHD IS NULL 
            THEN LOAN_START_DATE 
            ELSE LAST_PAYMENT_DATE_SCHD END
        END
        ';
    EXECUTE (V_STR_QUERY); 

    /* END GET LAST OR START DATE FOR ASSIGN FIRST PAYM DATE  --RIDWAN*/            

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_PAYM_CORE_SRC || ' 
        (
            MASTERID ,            
            ACCTNO ,            
            PREV_PMT_DATE ,            
            PMT_DATE ,            
            INTEREST_RATE ,            
            PRN_AMT ,            
            INT_AMT ,            
            ICC ,            
            GRACE_DATE
        ) SELECT DISTINCT
            A.MASTERID 
            ,A.ACCTNO 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE        
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
            ,B.INT_RATE
            ,0           
            ,0          
            ,A.ICC           
            ,A.GRACE_DATE            
        FROM ' || IFRS_LBM_PAYM_CORE_SRC || ' A
            ,' || TMP_T1 || ' B
        WHERE B.MASTERID = A.MASTERID
        ';
    EXECUTE (V_STR_QUERY); 

    --UPDATE DISB AMOUNT 20160428         
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T2 || ''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T2 || ' 
        (
            MASTERID
            ,DOWNLOAD_DATE 
            ,LAST_PAYMENT_DATE_SCHD  
            ,LOAN_START_DATE
        ) SELECT A.MASTERID
            ,MAX(A.PMTDATE) DOWNLOAD_DATE           
        FROM ' || PSAK_PAYM_SCHD || ' A
            , ' || TMP_T1 || ' B
        WHERE A.MASTERID = B.MASTERID
            AND WHERE PMTDATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        GROUP BY A.MASTERID
        ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_PAYM_CORE_SRC || ' A
        USING (
            SELECT A.MASTERID MASTERID
                   ,A.DISB_PERCENTAGE
                   ,A.DISB_AMOUNT
                   ,A.PLAFOND
            FROM PSAK_PAYM_SCHD A
                 ,TMP_T2 B
            WHERE A.MASTERID = B.MASTERID            
                  AND A.PMTDATE = B.DOWNLOAD_DATE
        ) B
        ON ( 
            A.MASTERID = B.MASTERID            
            AND A.PMT_DATE = A.PREV_PMT_DATE
        )
        WHEN MATCHED
            THEN
        UPDATE SET
            A.DISB_PERCENTAGE = B.DISB_PERCENTAGE 
            ,A.DISB_AMOUNT = B.DISB_AMOUNT 
            ,A.PLAFOND = B.PLAFOND
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_T1 || ''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_T1 || ' 
        (
            MASTERID
        ) SELECT MASTERID          
        FROM ' || IFRS_LBM_PAYM_CORE_SRC || ' 
        WHERE PREV_PMT_DATE = PMT_DATE  
            AND MASTERID IN (  
            SELECT B.MASTERID  
            FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || 'B  
            WHERE (  
                (  
                B.TOTAL_AMT <> 0  
                AND B.TOTAL_AMT_ACRU <> 0  
                )  
                OR B.STAFFLOAN = 1             
                OR (  
                B.MASTERID IN (  
                SELECT DISTINCT MASTERID  
                FROM ' || IFRS_LBM_EVENT_CHANGES || '
                WHERE DOWNLOAD_DATE = @V_CURRDATE  
                    AND EVENT_ID = 4  
                )  
                )  
                )  
            )  
        ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || IFRS_LBM_GS_MASTERID || ''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_GS_MASTERID || ' 
        (
            MASTERID
        ) SELECT A.MASTERID          
        FROM ' || TMP_T1 || ' A
        ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || IFRS_LBM_ACCT_EIR_PAYM || ''
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT
            VMIN_ID = MIN(ID)
            ,VMAX_ID = MAX(ID)
        ) SELECT A.MASTERID          
        FROM ' || IFRS_LBM_GS_MASTERID || '
        ';
    EXECUTE (V_STR_QUERY);

    SET VX = VMIN_ID  
    SET VX_INC = 500000

    WHILE VX <= VMAX_ID
    BEGIN
        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_LBM_PAYM_CORE_PROCESS', '');

        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || IFRS_LBM_PAYM_CORE || ''
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || IFRS_LBM_PAYM_CORE || ' 
            (
                MASTERID  
                ,ACCTNO  
                ,PREV_PMT_DATE  
                ,PMT_DATE  
                ,INT_RATE  
                ,I_DAYS  
                ,COUNTER  
                ,OS_PRN_PREV  
                ,PRN_AMT  
                ,INT_AMT  
                ,OS_PRN  
                ,DISB_PERCENTAGE  
                ,DISB_AMOUNT  
                ,PLAFOND  
                ,ICC  
                ,GRACE_DATE
            ) 
            /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
            SELECT MASTERID  
                ,ACCTNO  
                ,PREV_PMT_DATE  
                ,PMT_DATE  
                ,INTEREST_RATE  
                ,I_DAYS  
                ,COUNTER  
                ,OS_PRN_PREV  
                ,PRN_AMT  
                ,INT_AMT  
                ,OS_PRN  
                ,DISB_PERCENTAGE  
                ,DISB_AMOUNT  
                ,PLAFOND  
                ,ICC  
                ,GRACE_DATE          
            FROM ' || IFRS_LBM_PAYM_CORE_SRC || '
            WHERE MASTERID IN (
                SELECT MASTERID
                FROM ' || IFRS_LBM_GS_MASTERID || '
                WHERE ID >= ' || VX || '
                    AND ID < ' || VX + VX_INC || '
            )
            EXEC SP_IFRS_EXEC_AND_LOG 'SP_IFRS_LBM_PAYM_CORE_PROC_NOP'

            SET VX = VX + VX_INC
            ';
        EXECUTE (V_STR_QUERY);
    END

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN', '');

    -- UPDATE NPV RATE FOR STAFF LOAN 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_PAYM || ' A
        SET NPV_RATE = B.NPV_RATE
        FROM ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' B
        WHERE B.STAFFLOAN - 1
        AND ' || IFRS_LBM_ACCT_EIR_PAYM || '.MASTERID = B.MASTERID
        AND COALESCE(B.NPV_RATE, 0) > 0 ';
    EXECUTE (V_STR_QUERY);

    -- UPDATE NPV_INSTALLMENT FOR STAFF LOAN 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_PAYM || '
        SET NPV_INSTALLMENT = CASE
            WHEN ROUND(dbo.FN_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE) / 30, 0, 1) = 0  
            THEN N_INSTALLMENT / (POWER(1 + NULLIF(NPV_RATE, 0) / 360 / 100, dbo.FN_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE)))  
            ELSE N_INSTALLMENT / NULLIF((POWER(1 + NULLIF(NPV_RATE, 0) / 12 / 100, ROUND(dbo.FN_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE) / 30, 0, 1))), 0)  
        END
        WHERE   > 0';
    EXECUTE (V_STR_QUERY);

    -- CALC STAFF LOAN BENEFIT 
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_B1 || ''
    EXECUTE (V_STR_QUERY);
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_B2 || ''
    EXECUTE (V_STR_QUERY);
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || TMP_B3 || ''
    EXECUTE (V_STR_QUERY);

    -- GET OS
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_B1 || ' 
        (
            MASTERID
            ,N_OSPRN 
        ) SELECT MASTERID          
        FROM ' || IFRS_LBM_ACCT_EIR_PAYM || ' 
        WHERE DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND PREV_PMT_DATE = PMT_DATE  
            AND NPV_RATE > 0
        ';
    EXECUTE (V_STR_QUERY);

    --GET NPV SUM 
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_B2 || ' 
        (
            MASTERID
            ,TMP_B2 
        ) SELECT MASTERID 
            ,SUM(COALESCE(NPV_INSTALLMENT, 0)) AS NPV_SUM          
        FROM ' || IFRS_LBM_ACCT_EIR_PAYM || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND NPV_RATE > 0
        GROUP BY MASTERID
        ';
    EXECUTE (V_STR_QUERY);

    --GET BENEFIT
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || TMP_B3 || ' 
        (
            MASTERID
            ,N_OSPRN  
            ,NPV_SUM  
            ,BENEFIT 
        ) SELECT MASTERID 
            ,A.N_OSPRN  
            ,B.NPV_SUM  
            ,B.NPV_SUM - A.N_OSPRN AS BENEFIT         
        FROM ' || TMP_B1 || ' A
        JOIN ' || TMP_B2 || ' B ON B.MASTERID = A.MASTERID
        ';
    EXECUTE (V_STR_QUERY);

    --UPDATE BACK
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || IFRS_LBM_ACCT_EIR_CF_ECF || ' 
        SET BENEFIT = A.BENEFIT
        FROM ' || TMP_B3 || '  A
        WHERE A.MASTERID = ' || IFRS_LBM_ACCT_EIR_CF_ECF || '.MASTERID  S
        ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN', '3A');


    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_PAYM_SCHD_SRC', '');

    RAISE NOTICE 'SP_IFRS_PAYM_SCHD_SRC | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT5;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_PAYM_SCHD_SRC';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT5 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;