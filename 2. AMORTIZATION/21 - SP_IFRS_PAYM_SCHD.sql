---- DROP PROCEDURE SP_IFRS_PAYM_SCHD;

CREATE OR REPLACE PROCEDURE SP_IFRS_PAYM_SCHD(
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
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'IFRS_BATCH_LOG_DETAILS' || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND BATCH_ID_HEADER = ''' || V_LOG_ID || ''' 
        AND BATCH_NAME = ''PMTSCHD'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_MAIN' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'IFRS_PAYM_SCHD' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'IFRS_PAYM_CORE_SRC' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_MAIN' || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,ACCOUNT_NUMBER 
            ,BRANCH_CODE 
            ,PRODUCT_CODE 
            ,START_DATE 
            ,DUE_DATE 
            ,START_AMORTIZATION_DATE 
            ,END_AMORTIZATION_DATE 
            ,FIRST_PMT_DATE 
            ,CURRENCY 
            ,OUTSTANDING 
            ,PLAFOND 
            , 
            --HOLD_AMOUNT, 
            INTEREST_RATE 
            ,TENOR 
            ,PAYMENT_TERM 
            ,PAYMENT_CODE 
            ,INTEREST_CALCULATION_CODE 
            ,NEXT_PMTDATE 
            ,NEXT_COUNTER_PAY 
            ,SCH_FLAG 
            ,GRACE_DATE 
        ) SELECT 
            PMA.DOWNLOAD_DATE 
            ,PMA.MASTERID 
            ,PMA.ACCOUNT_NUMBER 
            ,PMA.BRANCH_CODE 
            ,PMA.PRODUCT_CODE 
            ,PMA.LOAN_START_DATE 
            ,PMA.LOAN_DUE_DATE 
            ,CASE 
                WHEN ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0 
                THEN ''' || V_CURRDATE || ''' 
                ELSE CASE 
                    WHEN ECF.MASTERID IS NOT NULL 
                    THEN ECF.LAST_PAYMENT_DATE_ECF
                    ELSE CASE 
                        WHEN COALESCE(PMA.LAST_PAYMENT_DATE, PMA.LOAN_START_DATE) <= PMA.LOAN_START_DATE 
                        THEN PMA.LOAN_START_DATE 
                        WHEN PMV.MASTERID IS NULL 
                        THEN PMA.LOAN_START_DATE 
                        ELSE CASE 
                            WHEN PMA.LAST_PAYMENT_DATE >= COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)
                            THEN COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) - INTERVAL ''1 MONTH'' 
                            ELSE PMA.LAST_PAYMENT_DATE 
                        END
                    END
                END
            END AS START_AMORTIZATION_DATE 
            ,PMA.LOAN_DUE_DATE 
            ,CASE 
                WHEN PMA.NEXT_PAYMENT_DATE >= COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) 
                    OR TO_CHAR(PMA.NEXT_PAYMENT_DATE, ''YYYYMMDD'') = TO_CHAR(COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE), ''YYYYMMDD'')
                THEN COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) 
                ELSE PMA.NEXT_PAYMENT_DATE 
            END AS FIRST_PMT_DATE
            ,PMA.CURRENCY 
            ,PMA.OUTSTANDING 
            ,PMA.PLAFOND 
            ,PMA.INTEREST_RATE 
            ,CASE 
                WHEN COALESCE(PMA.TENOR, 0) > (
                    (EXTRACT(YEAR FROM COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)) - EXTRACT(YEAR FROM PMA.LOAN_START_DATE)) * 12 +
                    (EXTRACT(MONTH FROM COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)) - EXTRACT(MONTH FROM PMA.LOAN_START_DATE))
                ) THEN COALESCE(PMA.TENOR, 0) 
                ELSE ((EXTRACT(YEAR FROM COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)) - EXTRACT(YEAR FROM PMA.LOAN_START_DATE)) * 12 +
                    (EXTRACT(MONTH FROM COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)) - EXTRACT(MONTH FROM PMA.LOAN_START_DATE))
                ) + 2
            END AS TENOR 
            ,PMA.PAYMENT_TERM 
            ,PMA.PAYMENT_CODE 
            ,PMA.INTEREST_CALCULATION_CODE 
            ,CASE 
                WHEN PMA.NEXT_PAYMENT_DATE >= COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) 
                    OR TO_CHAR(PMA.NEXT_PAYMENT_DATE, ''YYYYMMDD'') = TO_CHAR(COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE), ''YYYYMMDD'')
                THEN COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) 
                ELSE PMA.NEXT_PAYMENT_DATE 
            END AS NEXT_PMTDATE
            ,0
            ,''N''
            ,PMA.INSTALLMENT_GRACE_PERIOD 
        FROM ' || 'IFRS_MASTER_ACCOUNT' || ' PMA 
        JOIN ' || 'IFRS_IMA_AMORT_CURR' || ' PMC 
            ON PMA.MASTERID = PMC.MASTERID 
            AND PMA.DOWNLOAD_DATE = PMC.DOWNLOAD_DATE 
        LEFT JOIN ' || 'IFRS_IMA_AMORT_PREV' || ' PMV 
            ON PMC.MASTERID = PMV.MASTERID 
            AND PMV.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
        LEFT JOIN ( 
            SELECT MASTERID, MAX(PMT_DATE) AS LAST_PAYMENT_DATE_ECF
            FROM ' || 'IFRS_ACCT_EIR_ECF' || ' 
            WHERE AMORTSTOPDATE IS NULL 
            AND PMT_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY MASTERID 
        ) ECF 
            ON ECF.MASTERID = PMA.MASTERID 
        JOIN ( 
            SELECT DISTINCT MASTERID 
            FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) PYM 
            ON PMA.MASTERID = PYM.MASTERID 
        WHERE PMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND PMC.ECF_STATUS = ''Y'' 
            AND PMA.ACCOUNT_STATUS = ''A'' 
            AND PMA.IAS_CLASS IN (''A'', ''O'') 
            AND PMA.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND PMA.AMORT_TYPE = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_PY0' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_PY1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_PY2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_PY3' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_PY4' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_PY5' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY0' || ' 
        SELECT * 
        FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' PY0 
        WHERE PY0.COMPONENT_TYPE = ''0'' 
        AND PY0.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PY0.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY1' || ' 
        SELECT * 
        FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' PY1 
        WHERE PY1.COMPONENT_TYPE = ''1'' 
        AND PY1.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PY1.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY2' || ' 
        SELECT * 
        FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' PY2 
        WHERE PY2.COMPONENT_TYPE = ''2'' 
        AND PY2.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PY2.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY3' || ' 
        SELECT * 
        FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' PY3 
        WHERE PY3.COMPONENT_TYPE = ''3'' 
        AND PY3.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PY3.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY4' || ' 
        SELECT * 
        FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' PY4 
        WHERE PY4.COMPONENT_TYPE = ''4'' 
        AND PY4.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PY4.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY5' || ' 
        SELECT * 
        FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' PY5 
        WHERE PY5.COMPONENT_TYPE = ''5'' 
        AND PY5.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PY5.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_CURR_HIST' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_PREV_HIST' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_CURR' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_PREV' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_CURR' || ' 
        (
            MASTERID 
            ,ACCOUNT_NUMBER 
            ,INTEREST_RATE 
            ,PMTDATE 
            ,OSPRN 
            ,PRINCIPAL 
            ,INTEREST 
            ,DISB_PERCENTAGE 
            ,DISB_AMOUNT 
            ,PLAFOND 
            ,I_DAYS 
            ,COUNTER 
            ,DATE_START 
            ,DATE_END 
            ,TENOR 
            ,PAYMENT_CODE 
            ,ICC 
            ,NEXT_PMTDATE 
            ,NEXT_COUNTER_PAY 
            ,SCH_FLAG 
            ,GRACE_DATE 
        ) SELECT 
            A.MASTERID 
            ,A.ACCOUNT_NUMBER 
            ,A.INTEREST_RATE 
            ,A.START_AMORTIZATION_DATE 
            ,A.OUTSTANDING 
            ,0 AS PRINCIPAL 
            ,0 AS INTEREST 
            ,COALESCE(PY5.AMOUNT, 0) AS DISB_PERCENTAGE 
            ,A.OUTSTANDING AS DISB_AMOUNT 
            ,A.PLAFOND AS PLAFOND 
            ,0 AS I_DAYS 
            ,0 COUNTER 
            ,A.FIRST_PMT_DATE AS DATE_START 
            ,A.END_AMORTIZATION_DATE 
            ,A.TENOR 
            ,A.PAYMENT_CODE 
            ,A.INTEREST_CALCULATION_CODE 
            ,A.NEXT_PMTDATE AS NEXT_PMTDATE 
            ,A.NEXT_COUNTER_PAY + 1 
            ,A.SCH_FLAG 
            ,A.GRACE_DATE 
        FROM ' || 'TMP_SCHEDULE_MAIN' || ' A 
        LEFT JOIN ' || 'TMP_PY5' || ' PY5 
        ON A.MASTERID = PY5.MASTERID 
        AND A.DOWNLOAD_DATE BETWEEN PY5.DATE_START AND PY5.DATE_END 
        AND (
            (EXTRACT(YEAR FROM AGE(PY5.DATE_START, A.DOWNLOAD_DATE)) * 12) +
            EXTRACT(MONTH FROM AGE(PY5.DATE_START, A.DOWNLOAD_DATE))
        ) % PY5.INCREMENTS = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'IFRS_PAYM_SCHD' || ' 
        (
            MASTERID 
            ,PMTDATE 
            ,INTEREST_RATE 
            ,OSPRN 
            ,PRINCIPAL 
            ,INTEREST 
            ,DISB_PERCENTAGE 
            ,DISB_AMOUNT 
            ,PLAFOND 
            ,I_DAYS 
            ,ICC 
            ,COUNTER 
            ,DOWNLOAD_DATE 
            ,SCH_FLAG 
            ,GRACE_DATE 
        ) SELECT 
            MASTERID 
            ,PMTDATE 
            ,INTEREST_RATE 
            ,OSPRN 
            ,PRINCIPAL 
            ,INTEREST 
            ,DISB_PERCENTAGE 
            ,DISB_AMOUNT 
            ,PLAFOND 
            ,I_DAYS 
            ,ICC 
            ,COUNTER 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE  
            ,SCH_FLAG 
            ,GRACE_DATE 
        FROM ' || 'TMP_SCHEDULE_CURR' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MAX(TENOR) FROM ' || 'TMP_SCHEDULE_MAIN' || ' ';
    EXECUTE (V_STR_QUERY) INTO V_MAX_COUNTERPAY;

    WHILE V_COUNTER_PAY <= V_MAX_COUNTERPAY 
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_MIN_MAX_DATE' || ' (
            MASTERID VARCHAR(100) 
            ,MIN_DATE DATE 
        ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_MIN_MAX_DATE' || ' 
            SELECT 
                A.MASTERID 
                ,MIN(A.DATE_START) AS MIN_DATE 
            FROM ' || 'IFRS_MASTER_PAYMENT_SETTING' || ' A 
            INNER JOIN ' || 'TMP_SCHEDULE_CURR' || ' B 
            ON A.MASTERID = B.MASTERID 
            AND A.DATE_START > B.NEXT_PMTDATE 
            GROUP BY A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_COUNTER_PAY := V_COUNTER_PAY + 1;
        V_NEXT_COUNTER_PAY := V_COUNTER_PAY + 1;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_PREV' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_PREV' || ' 
            (
                MASTERID 
                ,ACCOUNT_NUMBER 
                ,INTEREST_RATE 
                ,PMTDATE 
                ,OSPRN 
                ,PRINCIPAL 
                ,INTEREST 
                ,DISB_PERCENTAGE 
                ,DISB_AMOUNT 
                ,PLAFOND 
                ,I_DAYS 
                ,COUNTER 
                ,DATE_START 
                ,DATE_END 
                ,TENOR 
                ,PAYMENT_CODE 
                ,ICC 
                ,NEXT_PMTDATE 
                ,NEXT_COUNTER_PAY 
                ,SCH_FLAG 
                ,GRACE_DATE 
            ) SELECT 
                A.MASTERID 
                ,A.ACCOUNT_NUMBER 
                ,COALESCE(PY3.AMOUNT, A.INTEREST_RATE) AS INTEREST_RATE 
                ,A.NEXT_PMTDATE AS NEW_PMTDATE 
                ,ROUND(
                    (
                        CASE
                            WHEN PY5.COMPONENT_TYPE = ''5'' 
                            THEN A.OSPRN + (PY5.AMOUNT / 100 * A.PLAFOND) 
                            ELSE A.OSPRN 
                        END - (
                            ROUND(
                                (
                                    CASE 
                                        WHEN A.GRACE_DATE >= A.NEXT_PMTDATE 
                                            AND A.GRACE_DATE IS NOT NULL 
                                        THEN 0 -- BIBD FOR GRACE PERIOD 
                                        ELSE CASE 
                                            WHEN A.NEXT_PMTDATE >= A.DATE_END 
                                            THEN A.OSPRN 
                                            ELSE CASE 
                                                WHEN PY0.COMPONENT_TYPE = 0 
                                                THEN CASE -- FIX PRINCIPAL
                                                    WHEN A.OSPRN <= PY0.AMOUNT 
                                                    THEN A.OSPRN 
                                                    ELSE PY0.AMOUNT 
                                                END 
                                                WHEN PY2.COMPONENT_TYPE = 2 
                                                THEN CASE -- FIX PRINCIPAL 
                                                    WHEN A.OSPRN <= PY2.AMOUNT 
                                                    THEN A.OSPRN 
                                                    ELSE PY2.AMOUNT - (
                                                        ROUND(
                                                            (
                                                                CASE 
                                                                    WHEN PY1.COMPONENT_TYPE = ''1'' 
                                                                    THEN PY1.AMOUNT -- FIX INTEREST 
                                                                    ELSE CASE 
                                                                        WHEN A.ICC = ''1'' -- ACTUAL/360
                                                                        THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360 
                                                                        WHEN A.ICC = ''2'' -- ACTUAL/365 
                                                                        THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365 
                                                                        WHEN A.ICC = ''6'' -- 30/360 
                                                                        THEN CASE 
                                                                            WHEN ' || V_CALC_IDAYS || ' = 0 
                                                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * COALESCE(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360 
                                                                            WHEN ' || V_CALC_IDAYS || ' = 1 
                                                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                                            ELSE A.INTEREST_RATE / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                                        END 
                                                                        ELSE 0 
                                                                    END 
                                                                END
                                                            ), V_ROUND 
                                                        )
                                                    ) 
                                                END 
                                                WHEN PY4.COMPONENT_TYPE = 4 
                                                THEN CASE -- INSTALLMENT 
                                                    WHEN A.OSPRN <= PY4.AMOUNT 
                                                    THEN A.OSPRN 
                                                    ELSE PY4.AMOUNT - (
                                                        ROUND(
                                                            (
                                                                CASE 
                                                                    WHEN A.ICC = ''1'' 
                                                                    THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360 
                                                                    WHEN A.ICC = ''2'' 
                                                                    THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365 
                                                                    WHEN A.ICC = ''6'' 
                                                                    THEN CASE 
                                                                        WHEN ' || V_CALC_IDAYS || ' = 0 
                                                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * COALESCE(PY4.INCREMENTS, PY2.INCREMENTS) * 30 / 360 
                                                                        WHEN ' || V_CALC_IDAYS || ' = 1 
                                                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                                        ELSE COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                                    END 
                                                                    ELSE 0 
                                                                END
                                                            ), V_ROUND 
                                                        )
                                                    )
                                                END 
                                                ELSE 0 
                                            END 
                                        END 
                                    END 
                                ), V_ROUND 
                            )
                        )
                    ), V_ROUND
                ) AS NEW_OSPRN 
                ,ROUND(
                    (
                        CASE 
                            WHEN A.GRACE_DATE >= A.NEXT_PMTDATE 
                                AND A.GRACE_DATE IS NOT NULL 
                            THEN 0 -- BIBD FOR GRACE PERIOD 
                            ELSE CASE 
                                WHEN A.NEXT_PMTDATE >= A.DATE_END 
                                THEN A.OSPRN 
                                ELSE CASE 
                                    WHEN PY0.COMPONENT_TYPE = 0 
                                    THEN CASE -- FIX PRINCIPAL
                                        WHEN A.OSPRN <= PY0.AMOUNT 
                                        THEN A.OSPRN 
                                        ELSE PY0.AMOUNT 
                                    END 
                                    WHEN PY2.COMPONENT_TYPE = 2 
                                    THEN CASE -- INSTALLMENT 
                                        WHEN A.OSPRN <= PY2.AMOUNT 
                                        THEN A.OSPRN 
                                        ELSE PY2.AMOUNT - (
                                            ROUND(
                                                (
                                                    CASE 
                                                        WHEN PY1.COMPONENT_TYPE = ''1'' 
                                                        THEN PY1.AMOUNT -- FIX INTEREST 
                                                        ELSE CASE 
                                                            WHEN A.ICC = ''1'' -- ACTUAL/360
                                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360 
                                                            WHEN A.ICC = ''2'' -- ACTUAL/365 
                                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365 
                                                            WHEN A.ICC = ''6'' -- 30/360 
                                                            THEN CASE 
                                                                WHEN ' || V_CALC_IDAYS || ' = 0 
                                                                THEN A.INTEREST_RATE / 100 * A.OSPRN * COALESCE(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360 
                                                                WHEN ' || V_CALC_IDAYS || ' = 1 
                                                                THEN A.INTEREST_RATE / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                                ELSE A.INTEREST_RATE / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                            END 
                                                            ELSE 0 
                                                        END
                                                    END
                                                ), V_ROUND 
                                            )
                                        )
                                    END 
                                    WHEN PY4.COMPONENT_TYPE = 4 
                                    THEN CASE -- INSTALLMENT 
                                        WHEN A.OSPRN <= PY4.AMOUNT 
                                        THEN A.OSPRN 
                                        ELSE PY4.AMOUNT - (
                                            ROUND(
                                                (
                                                    CASE 
                                                        WHEN A.ICC = ''1'' 
                                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360 
                                                        WHEN A.ICC = ''2'' 
                                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365 
                                                        WHEN A.ICC = ''6'' 
                                                        THEN CASE 
                                                            WHEN ' || V_CALC_IDAYS || ' = 0 
                                                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * COALESCE(PY4.INCREMENTS, PY2.INCREMENTS) * 30 / 360 
                                                            WHEN ' || V_CALC_IDAYS || ' = 1 
                                                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                            ELSE COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * F_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360 
                                                        END 
                                                        ELSE 0 
                                                    END
                                                ), V_ROUND 
                                            )
                                        )
                                    END 
                                    ELSE 0 
                                END 
                            END 
                        END 
                    ), V_ROUND 
                ) AS NEW_PRINCIPAL 
                ,ROUND(
                    (
                        CASE 
                            WHEN A.GRACE_DATE >= A.NEXT_PMTDATE 
                                AND A.GRACE_DATE IS NOT NULL 
                            THEN 0 -- BIBD FOR GRACE PERIOD 
                            ELSE CASE 
                                WHEN PY1.COMPONENT_TYPE = ''1'' 
                                THEN CASE -- FIX INTEREST 
                                    WHEN PY1.AMOUNT = 0 
                                    THEN CASE 
                                        WHEN A.ICC = ''1'' -- ACTUAL/360
                                        THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360 
                                        WHEN A.ICC = ''2'' -- ACTUAL/365 
                                        THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365 
                                        WHEN A.ICC = ''6'' -- 30/360 
                                        THEN CASE 
                                            WHEN ' || V_CALC_IDAYS || ' = 0 
                                            THEN CASE 
                                                WHEN (
                                                    ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0 
                                                    AND A.ICC = ''6'' 
                                                    AND ' || V_COUNTER_PAY || ' = 1 
                                                ) 
                                                THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360 
                                                ELSE A.INTEREST_RATE / 100 * A.OSPRN * PY1.INCREMENTS * 30 / 360 
                                            END 
                                            WHEN ' || V_CALC_IDAYS || ' = 1 
                                            THEN CASE 
                    )
                )
         ';
        EXECUTE (V_STR_QUERY);
    END LOOP;
    
    RAISE NOTICE 'SP_IFRS_PAYM_SCHD | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_PAYM_SCHD';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;