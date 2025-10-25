CREATE OR REPLACE PROCEDURESP_IFRS_PAYM_SCHD_MTM(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_ENDOFMONTH DATE;

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
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_EIR_ADJUSTMENT_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_EXCEPTION_DETAILS_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_MASTER_PAYMENT_SETTING_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_PAYM_SCHD_ALL_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_PAYM_SCHD_MTM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_EIR_ADJUSTMENT';
        V_TABLEINSERT2 := 'IFRS_EXCEPTION_DETAILS';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR';
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

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_EIR_ADJUSTMENT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT7 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT7 || ' AS SELECT * FROM IFRS_PAYM_SCHD_MTM WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_PAYM_SCHD_MTM', '');

    IF V_ENDOFMONTH = V_CURRDATE 
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT7 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
            (    
                MASTERID    
                ,ACCOUNT_NUMBER  
                ,PMTDATE    
                ,INTEREST_RATE    
                ,OSPRN    
                ,PRINCIPAL    
                ,INTEREST    
                ,DISB_PERCENTAGE    
                ,DISB_AMOUNT    
                ,PLAFOND    
                ,I_DAYS    
                ,COUNTER    
                ,ICC    
                ,OUTSTANDING    
                ,DOWNLOAD_DATE    
                ,SCH_FLAG    
                ,GRACE_DATE    
            ) SELECT 
                A.MASTERID    
                ,B.ACCOUNT_NUMBER  
                ,PMTDATE    
                ,A.INTEREST_RATE    
                ,A.OSPRN    
                ,A.PRINCIPAL    
                ,A.INTEREST    
                ,A.DISB_PERCENTAGE    
                ,A.DISB_AMOUNT    
                ,A.PLAFOND    
                ,A.I_DAYS    
                ,ROW_NUMBER() OVER( PARTITION BY A.MASTERID ORDER BY A.PMTDATE ASC) COUNTER    
                ,A.ICC    
                ,A.OUTSTANDING    
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE   
                ,A.SCH_FLAG    
                ,A.GRACE_DATE    
            FROM ' || V_TABLEINSERT6 || ' A    
            JOIN ' || V_TABLEINSERT3 || ' B 
            ON A.MASTERID = B.MASTERID    
            WHERE  B.IFRS9_CLASS IN (''FVOCI'',''FVTPL'')    
            AND B.MARKET_RATE <> 0   
            AND PMTDATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND END_DATE IS NULL     
            ORDER BY PMTDATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
            SET 
                PV_CF = (A.PRINCIPAL + A.INTEREST) / POWER((1 + Z.MARKET_RATE / 12 / 100), A.COUNTER)
                ,MARKET_RATE = Z.MARKET_RATE   
            FROM ' || V_TABLEINSERT3 || ' Z 
            WHERE A.MASTERID = Z.MASTERID    
            AND Z.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (    
                DOWNLOAD_DATE    
                ,MASTERID    
                ,ACCOUNT_NUMBER    
                ,IFRS9_CLASS    
                ,LOAN_START_DATE    
                ,LOAN_DUE_DATE    
                ,OUTSTANDING    
                ,INTEREST_RATE    
                ,EIR    
                ,MARKET_RATE    
                ,FAIR_VALUE_AMT    
                ,TOTAL_PV_CF    
                ,--GANTI NAMA    
                TOT_ADJUST --- TOTAL_PV_CF - COALESCE(FAIRVALUEAMT, OUTSTANDING)    
            )    
            SELECT 
                A.DOWNLOAD_DATE    
                ,A.MASTERID    
                ,A.ACCOUNT_NUMBER    
                ,A.IFRS9_CLASS    
                ,A.LOAN_START_DATE    
                ,A.LOAN_DUE_DATE    
                ,A.OUTSTANDING    
                ,A.INTEREST_RATE    
                ,A.EIR    
                ,A.MARKET_RATE    
                ,A.FAIR_VALUE_AMOUNT    
                ,B.TOTAL_PV_CF AS TOTAL_PV_CF    
                ,(B.TOTAL_PV_CF - COALESCE(A.FAIR_VALUE_AMOUNT, A.OUTSTANDING)) AS TOT_ADJUST    
            FROM ' || V_TABLEINSERT3 || ' A    
            JOIN (    
                SELECT 
                    X.MASTERID    
                    ,MIN(X.PMTDATE) AS MIN_PMTDATE    
                    ,MAX(X.PMTDATE) AS MAX_PMTDATE    
                    ,SUM(X.PRINCIPAL + X.INTEREST) AS TOT_INSTALMENT    
                    ,SUM((X.PRINCIPAL + X.INTEREST) / POWER((1 + Z.MARKET_RATE / 12 / 100), X.COUNTER)) AS TOTAL_PV_CF    
                FROM ' || V_TABLEINSERT7 || ' X    
                JOIN ' || V_TABLEINSERT3 || ' Z 
                ON X.MASTERID = Z.MASTERID    
                WHERE Z.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND X.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                GROUP BY X.MASTERID    
            ) B 
            ON A.MASTERID = B.MASTERID    
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE   
            AND A.IFRS9_CLASS IN (''FVTPL'', ''FVOCI'') --IFRS_CLASS    
            AND A.ACCOUNT_STATUS = ''A'' ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_MAIN_MTM' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT7 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_MAIN_MTM' || ' 
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
            ,INTEREST_RATE    
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
                THEN ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                ELSE CASE     
                    WHEN COALESCE(PMA.LAST_PAYMENT_DATE, PMA.LOAN_START_DATE) <= PMA.LOAN_START_DATE    
                    THEN PMA.LOAN_START_DATE    
                    ELSE CASE     
                        WHEN PMA.LAST_PAYMENT_DATE >= COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                        THEN (COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) - INTERVAL ''1 MONTH'')    
                        ELSE PMA.LAST_PAYMENT_DATE    
                    END    
                END    
            END START_AMORTIZATION_DATE    
            ,PMA.LOAN_DUE_DATE    
            ,CASE     
                WHEN PMA.NEXT_PAYMENT_DATE >= COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                    OR PMA.NEXT_PAYMENT_DATE = COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                THEN COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                ELSE PMA.NEXT_PAYMENT_DATE    
            END AS FIRST_PMT_DATE    
            ,PMA.CURRENCY    
            ,PMA.OUTSTANDING    
            ,PMA.PLAFOND    
            ,PMA.INTEREST_RATE    
            ,CASE     
                WHEN COALESCE(PMA.TENOR, 0) > (EXTRACT(YEAR FROM AGE(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE)) * 12 + EXTRACT(MONTH FROM AGE(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE)))    
                THEN COALESCE(PMA.TENOR, 0)    
                ELSE (EXTRACT(YEAR FROM AGE(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE)) * 12 + EXTRACT(MONTH FROM AGE(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE)))    
            END AS TENOR    
            ,PMA.PAYMENT_TERM    
            ,PMA.PAYMENT_CODE    
            ,PMA.INTEREST_CALCULATION_CODE    
            ,CASE     
                WHEN PMA.NEXT_PAYMENT_DATE >= COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                    OR PMA.NEXT_PAYMENT_DATE = COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                THEN COALESCE(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)    
                ELSE PMA.NEXT_PAYMENT_DATE    
            END    
            ,0    
            ,''N''    
            ,PMA.INSTALLMENT_GRACE_PERIOD AS GRACE_DATE -- BIBD GRACE PERIOD    
        FROM ' || V_TABLENAME || ' AS PMA    
        JOIN ' || V_TABLEINSERT3 || ' PMC 
            ON PMA.MASTERID = PMC.MASTERID    
            AND PMA.DOWNLOAD_DATE = PMC.DOWNLOAD_DATE    
        WHERE PMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
            AND PMA.IFRS9_CLASS IN (''FVTPL'', ''FVOCI'')    
            ---AND PMA.DATA_SOURCE = ''LOAN''    
            AND PMA.MARKET_RATE > 0    
            AND PMA.ACCOUNT_STATUS = ''A''    
            AND PMA.IAS_CLASS = ''A'' -----ADD YAHYA    
            AND PMA.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
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
        FROM ' || V_TABLEINSERT5 || ' PY0    
        WHERE PY0.COMPONENT_TYPE = ''0''    
        AND PY0.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND PY0.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY1' || ' 
        SELECT *    
        FROM ' || V_TABLEINSERT5 || ' PY1    
        WHERE PY1.COMPONENT_TYPE = ''1''    
        AND PY1.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND PY1.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY2' || ' 
        SELECT *    
        FROM ' || V_TABLEINSERT5 || ' PY2    
        WHERE PY2.COMPONENT_TYPE = ''2''    
        AND PY2.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND PY2.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY3' || ' 
        SELECT *    
        FROM ' || V_TABLEINSERT5 || ' PY3    
        WHERE PY3.COMPONENT_TYPE = ''3''    
        AND PY3.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND PY3.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY4' || ' 
        SELECT *    
        FROM ' || V_TABLEINSERT5 || ' PY4    
        WHERE PY4.COMPONENT_TYPE = ''4''    
        AND PY4.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND PY4.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_PY5' || ' 
        SELECT *    
        FROM ' || V_TABLEINSERT5 || ' PY5    
        WHERE PY5.COMPONENT_TYPE = ''5''    
        AND PY5.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND PY5.FREQUENCY IN (''M'', ''N'', ''D'') ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_CURR_MTM_HIST' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_PREV_MTM_HIST' || '';
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
            ,A.GRACE_DATE --BIBD FOR GRACE PERIOD    
        FROM ' || 'TMP_SCHEDULE_MAIN_MTM' || ' A    
        LEFT JOIN ' || 'TMP_PY5' || ' PY5 
        ON A.MASTERID = PY5.MASTERID    
        AND A.DOWNLOAD_DATE BETWEEN PY5.DATE_START    
        AND PY5.DATE_END    
        AND (EXTRACT(YEAR FROM AGE(PY5.DATE_START, A.DOWNLOAD_DATE)) * 12 + EXTRACT(MONTH FROM AGE(PY5.DATE_START, A.DOWNLOAD_DATE))) % PY5.INCREMENTS = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_CURR_MTM_HIST' || ' 
        SELECT * 
        FROM ' || 'TMP_SCHEDULE_CURR' || '';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (    
            MASTERID    
            --,ACCOUNT_NUMBER    
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
            --,ACCOUNT_NUMBER    
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
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
            ,SCH_FLAG    
            ,GRACE_DATE --BIBD FOR GRACE    
        FROM ' || 'TMP_SCHEDULE_CURR' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MAX(TENOR) FROM ' || 'TMP_SCHEDULE_MAIN_MTM' || ' ';
    EXECUTE (V_STR_QUERY) INTO V_MAX_COUNTERPAY;

    WHILE (V_COUNTERPAY <= V_MAX_COUNTERPAY) 
    LOOP 
        V_COUNTERPAY := V_COUNTERPAY + 1;
        V_NEXT_COUNTERPAY := V_NEXT_COUNTERPAY + 1;

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
                ,ROUND((CASE     
                    WHEN PY5.COMPONENT_TYPE = ''5''    
                    THEN A.OSPRN + (PY5.AMOUNT / 100 * A.PLAFOND)    
                    ELSE A.OSPRN    
                END - (    
                    ROUND((CASE     
                        WHEN A.GRACE_DATE >= A.NEXT_PMTDATE    
                            AND A.GRACE_DATE IS NOT NULL    
                        THEN 0 --BIBD FOR GRACE PERIOD      
                        ELSE CASE     
                            WHEN A.NEXT_PMTDATE >= A.DATE_END    
                            THEN A.OSPRN    
                            ELSE CASE     
                                WHEN PY0.COMPONENT_TYPE = 0    
                                THEN CASE --FIX PRINCIPAL    
                                    WHEN A.OSPRN <= PY0.AMOUNT    
                                    THEN A.OSPRN    
                                    ELSE PY0.AMOUNT    
                                END    
                                WHEN PY2.COMPONENT_TYPE = 2    
                                THEN CASE --INSTALMENT    
                                    WHEN A.OSPRN <= PY2.AMOUNT    
                                    THEN A.OSPRN    
                                    ELSE PY2.AMOUNT - (    
                                        ROUND((CASE     
                                            WHEN PY1.COMPONENT_TYPE = ''1''    
                                            THEN PY1.AMOUNT --FIX INTEREST    
                                            ELSE CASE     
                                                WHEN A.ICC = ''1''    --ACTUAL/360    
                                                THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                                WHEN A.ICC = ''2''    --ACTUAL/365    
                                                THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                                                WHEN A.ICC = ''6''    --30 / 360    
                                                THEN A.INTEREST_RATE / 100 * A.OSPRN * COALESCE(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360    
                                                ELSE 0    
                                            END    
                                        END    
                                        ), ' || V_ROUND || ')    
                                    )    
                                END    
                                WHEN PY4.COMPONENT_TYPE = 4    
                                THEN CASE --INSTALMENT
                                    WHEN A.OSPRN <= PY4.AMOUNT    
                                    THEN A.OSPRN    
                                    ELSE PY4.AMOUNT - (    
                                        ROUND((CASE     
                                            WHEN A.ICC = ''1''    --ACTUAL/360    
                                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                            WHEN A.ICC = ''2''    --ACTUAL/365    
                                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                                            WHEN A.ICC = ''6''    --ACTUAL/365    
                                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * COALESCE(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360    
                                            ELSE 0    
                                        END    
                                        ), ' || V_ROUND || ')    
                                    )    
                                END    
                                ELSE 0    
                            END    
                        END    
                    END    
                    ), ' || V_ROUND || ')    
                )), ' || V_ROUND || ') AS NEW_OSPRN    
                ,ROUND((CASE     
                    WHEN A.GRACE_DATE >= A.NEXT_PMTDATE    
                        AND A.GRACE_DATE IS NOT NULL    
                    THEN 0 --BIBD FOR GRACE PERIOD       
                    ELSE CASE     
                        WHEN A.NEXT_PMTDATE >= A.DATE_END    
                        THEN A.OSPRN    
                        ELSE CASE     
                            WHEN PY0.COMPONENT_TYPE = 0    
                            THEN CASE      --FIX PRINCIPAL    
                                WHEN A.OSPRN <= PY0.AMOUNT    
                                THEN A.OSPRN    
                                ELSE PY0.AMOUNT    
                            END    
                            WHEN PY2.COMPONENT_TYPE = 2    
                            THEN CASE --INSTALMENT    
                                WHEN A.OSPRN <= PY2.AMOUNT    
                                THEN A.OSPRN    
                                ELSE PY2.AMOUNT - (    
                                    ROUND((CASE     
                                        WHEN PY1.COMPONENT_TYPE = ''1''    
                                        THEN PY1.AMOUNT --FIX INTEREST    
                                        ELSE CASE     
                                            WHEN A.ICC = ''1''    --ACTUAL/360    
                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                            WHEN A.ICC = ''2''    --ACTUAL/365    
                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                                            WHEN A.ICC = ''6''    --30/360    
                                            THEN A.INTEREST_RATE / 100 * A.OSPRN * COALESCE(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360    
                                            ELSE 0    
                                        END    
                                    END    
                                    ), ' || V_ROUND || ')    
                                )    
                            END    
                            WHEN PY4.COMPONENT_TYPE = 4    
                            THEN CASE --INSTALMENT    
                                WHEN A.OSPRN <= PY4.AMOUNT    
                                THEN A.OSPRN    
                                ELSE PY4.AMOUNT - (    
                                    ROUND((CASE     
                                        WHEN A.ICC = ''1''    --ACTUAL/360    
                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                        WHEN A.ICC = ''2''    --ACTUAL/365    
                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                                        WHEN A.ICC = ''6''    --30/360    
                                        THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * COALESCE(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360    
                                        ELSE 0    
                                    END    
                                    ), ' || V_ROUND || ')    
                                )    
                            END    
                            ELSE 0    
                        END    
                    END    
                END    
                ), ' || V_ROUND || ') AS NEW_PRINCIPAL    
                ,ROUND((CASE     
                    WHEN A.GRACE_DATE >= A.NEXT_PMTDATE    
                        AND A.GRACE_DATE IS NOT NULL    
                    THEN 0     --BIBD FOR GRACE PERIOD     
                    ELSE CASE     
                        WHEN PY1.COMPONENT_TYPE = ''1''    --FIX INTEREST    
                        THEN CASE     
                            WHEN PY1.AMOUNT = 0    
                            THEN CASE     
                                WHEN A.ICC = ''1''    --ACTUAL/360    
                                THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                WHEN A.ICC = ''2''    --ACTUAL/365    
                                THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                                WHEN A.ICC = ''6''    --30/360    
                                THEN CASE
                                    -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF         
                                    WHEN (    
                                        ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0    
                                        AND A.ICC = ''6''    
                                        AND ' || V_COUNTERPAY || ' = 1    
                                        )    
                                    THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                    ELSE A.INTEREST_RATE / 100 * A.OSPRN * PY1.INCREMENTS * 30 / 360    
                                END    
                                ----END ADD YAHYA    
                                ELSE 0    
                            END    
                            ELSE PY1.AMOUNT    
                        END    
                        WHEN PY3.COMPONENT_TYPE = ''3''    
                        THEN CASE     
                            WHEN A.ICC = ''1''    --ACTUAL/360    
                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                            WHEN A.ICC = ''2''    --ACTUAL/365    
                            THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                            WHEN A.ICC = ''6''    --30/360    
                            THEN CASE
                                -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF    
                                WHEN (    
                                    ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0    
                                    AND A.ICC = ''6''    
                                    AND ' || V_COUNTERPAY || ' = 1    
                                    )    
                                    THEN COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                ELSE COALESCE(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * PY3.INCREMENTS * 30 / 360    
                            END    
                            ----END ADD YAHYA    
                            ELSE 0    
                        END    
                        ELSE CASE     
                            WHEN A.ICC = ''1''    --ACTUAL/360    
                            THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                            WHEN A.ICC = ''2''    --ACTUAL/365    
                            THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365    
                            WHEN A.ICC = ''6''     --30/360    
                            THEN CASE
                                -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF    
                                WHEN ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0    
                                    AND A.ICC = ''6''    
                                    AND ' || V_COUNTERPAY || ' = 1    
                                THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360    
                                ELSE A.INTEREST_RATE / 100 * A.OSPRN * COALESCE(PY2.INCREMENTS, 1) * 30 / 360    
                            END    
                            ----END ADD YAHYA    
                            ELSE 0    
                        END    
                    END    
                    /*  BCA DISABLE BPI  END */    
                END    
                ), ' || V_ROUND || ') AS NEW_INTEREST    
                ,COALESCE(PY5.AMOUNT, 0) AS DISB_PERCENTAGE    
                ,COALESCE(PY5.AMOUNT, 0) / 100 * A.PLAFOND AS DISB_AMOUNT    
                ,A.PLAFOND    
                ,CASE     
                    WHEN A.GRACE_DATE >= A.NEXT_PMTDATE    
                        AND A.GRACE_DATE IS NOT NULL    
                    THEN 0    
                    ELSE CASE     
                        WHEN A.ICC IN (''1'',''2'')    
                        THEN CASE     
                            WHEN PY1.COMPONENT_TYPE = ''1''    
                            THEN (A.NEXT_PMTDATE - A.PMTDATE)    
                            WHEN PY2.COMPONENT_TYPE = ''2''    
                            THEN (A.NEXT_PMTDATE - A.PMTDATE)    
                            WHEN PY3.COMPONENT_TYPE = ''3''    
                            THEN (A.NEXT_PMTDATE - A.PMTDATE)    
                            ELSE 0    
                        END    
                        WHEN A.ICC = ''6''    
                        THEN CASE
                            ---- ADD YAHYA TO CALCULATE I_DAYS IF MIGRATION IN CUTOFF    
                            WHEN (    
                                ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0    
                                AND A.ICC = ''6''    
                                AND ' || V_COUNTERPAY || ' = 1    
                                )    
                            THEN CASE     
                                WHEN PY1.COMPONENT_TYPE = ''1''    
                                THEN (A.NEXT_PMTDATE - A.PMTDATE)    
                                WHEN PY2.COMPONENT_TYPE = ''2''    
                                THEN (A.NEXT_PMTDATE - A.PMTDATE)    
                                WHEN PY3.COMPONENT_TYPE = ''3''    
                                THEN (A.NEXT_PMTDATE - A.PMTDATE)    
                                ELSE 0    
                            END    
                            ELSE CASE     
                                WHEN PY1.COMPONENT_TYPE = ''1''    
                                THEN COALESCE(PY1.INCREMENTS, 1) * 30    
                                WHEN PY2.COMPONENT_TYPE = ''2''    
                                THEN COALESCE(PY2.INCREMENTS, 1) * 30    
                                WHEN PY3.COMPONENT_TYPE = ''3''    
                                THEN COALESCE(PY3.INCREMENTS, 1) * 30    
                                ELSE 0    
                            END    
                        END    
                        --------- END ADD YAHYA    
                        ELSE 0 -- NOT IN 1,2,6    
                    END    
                END AS I_DAYS    
                ,' || V_COUNTERPAY || ' AS COUNTER    
                ,CASE     
                    WHEN PY1.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY1.DATE_END    
                    THEN B.MIN_DATE    
                    WHEN PY2.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY2.DATE_END    
                    THEN B.MIN_DATE    
                    WHEN PY3.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY3.DATE_END    
                    THEN B.MIN_DATE    
                    WHEN PY4.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY4.DATE_END    
                    THEN B.MIN_DATE    
                    WHEN PY5.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY5.DATE_END    
                    THEN B.MIN_DATE    
                    ELSE A.DATE_START    
                END DATE_START    
                ,A.DATE_END    
                ,A.TENOR    
                ,A.PAYMENT_CODE    
                ,A.ICC    
                ,CASE     
                    WHEN PY1.COMPONENT_TYPE = ''1''    
                    THEN CASE     
                        WHEN PY1.FREQUENCY = ''N''    
                        THEN EOMONTH((A.DATE_START + (PY1.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH'')))    
                        ELSE CASE     
                            ---START ADD YAHYA ---     
                            WHEN PY1.DATE_END IS NOT NULL    
                                AND A.NEXT_PMTDATE = PY1.DATE_END    
                            THEN B.MIN_DATE --- ADD YAHYA    
                            WHEN ISDATE(CONCAT (    
                                (A.DATE_START + (PY1.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY1.PMT_DATE    
                                )) = 1    
                            THEN CONVERT(DATE, CONCAT (    
                                (A.DATE_START + (PY1.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY1.PMT_DATE    
                                ))    
                            ELSE (A.DATE_START + (PY1.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                            ---END ADD YAHYA----    
                        END    
                    END    
                    WHEN PY2.COMPONENT_TYPE = ''2''    
                    THEN CASE     
                        WHEN PY2.FREQUENCY = ''N''    
                        THEN EOMONTH((A.DATE_START + (PY2.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH'')))    
                        ELSE CASE     
                            ---START ADD YAHYA ---     
                            WHEN PY2.DATE_END IS NOT NULL    
                                AND A.NEXT_PMTDATE = PY2.DATE_END    
                            THEN B.MIN_DATE    
                            WHEN ISDATE(CONCAT (    
                                (A.DATE_START + (PY2.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY2.PMT_DATE    
                                )) = 1    
                            THEN CONVERT(DATE, CONCAT (    
                                (A.DATE_START + (PY2.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY2.PMT_DATE    
                                ))    
                            ELSE (A.DATE_START + (PY2.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                            ---END ADD YAHYA----    
                        END    
                    END    
                    WHEN PY3.COMPONENT_TYPE = ''3''    
                    THEN CASE     
                        WHEN PY3.FREQUENCY = ''N''    
                        THEN EOMONTH((A.DATE_START + (PY3.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH'')))    
                        ELSE CASE     
                            ---START ADD YAHYA ---    
                            WHEN PY3.DATE_END IS NOT NULL    
                                AND A.NEXT_PMTDATE = PY3.DATE_END    
                            THEN B.MIN_DATE    
                            WHEN ISDATE(CONCAT (    
                                (A.DATE_START + (PY3.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY3.PMT_DATE    
                                )) = 1    
                            THEN CONVERT(DATE, CONCAT (    
                                (A.DATE_START + (PY3.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY3.PMT_DATE    
                                ))    
                            ELSE (A.DATE_START + (PY3.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                            ---END ADD YAHYA----    
                        END    
                    END    
                    WHEN PY4.COMPONENT_TYPE = ''4''    
                    THEN CASE     
                        WHEN PY4.FREQUENCY = ''N''    
                        THEN EOMONTH((A.DATE_START + (PY4.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH'')))    
                        ELSE CASE     
                            ---START ADD YAHYA ---    
                            WHEN PY4.DATE_END IS NOT NULL    
                                AND A.NEXT_PMTDATE = PY4.DATE_END    
                            THEN B.MIN_DATE    
                            WHEN ISDATE(CONCAT (    
                                (A.DATE_START + (PY4.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY4.PMT_DATE    
                                )) = 1    
                            THEN CONVERT(DATE, CONCAT (    
                                (A.DATE_START + (PY4.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY4.PMT_DATE    
                                ))    
                            ELSE (A.DATE_START + (PY4.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                            ---END ADD YAHYA----    
                        END    
                    END    
                    WHEN PY0.COMPONENT_TYPE = ''0''    
                    THEN CASE     
                        WHEN PY0.FREQUENCY = ''N''    
                        THEN EOMONTH((A.DATE_START + (PY0.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH'')))    
                        ELSE CASE     
                            ---START ADD YAHYA ---    
                            WHEN PY0.DATE_END IS NOT NULL    
                                AND A.NEXT_PMTDATE = PY0.DATE_END    
                            THEN B.MIN_DATE    
                            WHEN ISDATE(CONCAT (    
                                (A.DATE_START + (PY0.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY0.PMT_DATE    
                                )) = 1    
                            THEN CONVERT(DATE, CONCAT (    
                                (A.DATE_START + (PY0.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                                ,PY0.PMT_DATE    
                                ))    
                            ELSE (A.DATE_START + (PY0.INCREMENTS * A.NEXT_COUNTER_PAY * INTERVAL ''1 MONTH''))    
                            ---END ADD YAHYA----    
                        END    
                    END    
                    ELSE A.DATE_END ----ADD YAHYA    
                END    
                ,CASE     
                    WHEN PY1.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY1.DATE_END    
                    THEN 0    
                    WHEN PY2.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY2.DATE_END    
                    THEN 0    
                    WHEN PY3.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY3.DATE_END    
                    THEN 0    
                    WHEN PY4.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY4.DATE_END    
                    THEN 0    
                    WHEN PY5.DATE_END IS NOT NULL    
                        AND A.NEXT_PMTDATE = PY5.DATE_END    
                    THEN 0    
                    ELSE A.NEXT_COUNTER_PAY    
                END NEXT_COUNTER_PAY    
                ,A.SCH_FLAG    
                ,A.GRACE_DATE --BIBD FOR GRACE PERIOD    
                /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */    
            FROM ' || 'TMP_SCHEDULE_CURR' || ' A    
            LEFT JOIN (
                SELECT 
                    A.ACCOUNT_NUMBER    
                    ,MIN(A.DATE_START) AS MIN_DATE 
                FROM ' || V_TABLEINSERT5 || ' A    
                JOIN ' || 'TMP_SCHEDULE_CURR' || ' B 
                    ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER    
                    AND A.DATE_START > B.NEXT_PMTDATE    
                GROUP BY A.ACCOUNT_NUMBER
            ) B 
                ON A.MASTERID = B.ACCOUNT_NUMBER ---ADD YAHYA    
            LEFT JOIN ' || 'TMP_PY0' || ' PY0 
                ON A.MASTERID = PY0.MASTERID    
                AND A.NEXT_PMTDATE BETWEEN PY0.DATE_START    
                AND PY0.DATE_END    
                AND (EXTRACT(YEAR FROM AGE(A.NEXT_PMTDATE, PY0.DATE_START)) * 12 + EXTRACT(MONTH FROM AGE(A.NEXT_PMTDATE, PY0.DATE_START))) % PY0.INCREMENTS = 0    
            LEFT JOIN ' || 'TMP_PY1' || ' PY1 
                ON A.MASTERID = PY1.MASTERID    
                AND A.NEXT_PMTDATE BETWEEN PY1.DATE_START    
                AND PY1.DATE_END    
                AND (EXTRACT(YEAR FROM AGE(A.NEXT_PMTDATE, PY1.DATE_START)) * 12 + EXTRACT(MONTH FROM AGE(A.NEXT_PMTDATE, PY1.DATE_START))) % PY1.INCREMENTS = 0    
            LEFT JOIN ' || 'TMP_PY2' || ' PY2 
                ON A.MASTERID = PY2.MASTERID    
                AND A.NEXT_PMTDATE BETWEEN PY2.DATE_START    
                AND PY2.DATE_END    
                AND (EXTRACT(YEAR FROM AGE(A.NEXT_PMTDATE, PY2.DATE_START)) * 12 + EXTRACT(MONTH FROM AGE(A.NEXT_PMTDATE, PY2.DATE_START))) % PY2.INCREMENTS = 0    
            LEFT JOIN ' || 'TMP_PY3' || ' PY3 
                ON A.MASTERID = PY3.MASTERID    
                AND A.NEXT_PMTDATE BETWEEN PY3.DATE_START    
                AND PY3.DATE_END    
                AND (EXTRACT(YEAR FROM AGE(A.NEXT_PMTDATE, PY3.DATE_START)) * 12 + EXTRACT(MONTH FROM AGE(A.NEXT_PMTDATE, PY3.DATE_START))) % PY3.INCREMENTS = 0    
            LEFT JOIN ' || 'TMP_PY4' || ' PY4 
                ON A.MASTERID = PY4.MASTERID    
                AND A.NEXT_PMTDATE BETWEEN PY4.DATE_START    
                AND PY4.DATE_END    
                AND (EXTRACT(YEAR FROM AGE(A.NEXT_PMTDATE, PY4.DATE_START)) * 12 + EXTRACT(MONTH FROM AGE(A.NEXT_PMTDATE, PY4.DATE_START))) % PY4.INCREMENTS = 0    
            LEFT JOIN ' || 'TMP_PY5' || ' PY5 
                ON A.MASTERID = PY5.MASTERID    
                AND A.NEXT_PMTDATE BETWEEN PY5.DATE_START    
                AND PY5.DATE_END    
                AND (EXTRACT(YEAR FROM AGE(A.NEXT_PMTDATE, PY5.DATE_START)) * 12 + EXTRACT(MONTH FROM AGE(A.NEXT_PMTDATE, PY5.DATE_START))) % PY5.INCREMENTS = 0    
            WHERE A.TENOR >= ' || V_COUNTERPAY || ' 
                AND A.PMTDATE <= A.DATE_END    
                AND A.OSPRN > 0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_PREV_MTM_HIST' || ' 
            SELECT * 
            FROM ' || 'TMP_SCHEDULE_PREV' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHEDULE_CURR' || '';
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
                ,A.PMTDATE    
                ,A.OSPRN    
                ,A.PRINCIPAL    
                ,A.INTEREST    
                ,A.DISB_PERCENTAGE    
                ,A.DISB_AMOUNT    
                ,A.PLAFOND    
                ,A.I_DAYS    
                ,A.COUNTER    
                ,A.DATE_START    
                ,A.DATE_END    
                ,A.TENOR    
                ,A.PAYMENT_CODE    
                ,A.ICC    
                ,CASE     
                    WHEN A.NEXT_PMTDATE > A.DATE_END    
                    THEN A.DATE_END ------ADD YAHYA 20180312    
                    WHEN EOMONTH(A.NEXT_PMTDATE) = EOMONTH(A.DATE_END)    
                    THEN A.DATE_END    
                    ELSE A.NEXT_PMTDATE    
                END    
                ,A.NEXT_COUNTER_PAY + 1    
                ,A.SCH_FLAG    
                ,A.GRACE_DATE --BIBD FOR GRACE PERIOD    
                /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */    
            FROM ' || 'TMP_SCHEDULE_PREV' || ' A ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHEDULE_CURR_MTM_HIST' || ' 
            SELECT * 
            FROM ' || 'TMP_SCHEDULE_CURR' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
            (    
                MASTERID    
                --,ACCOUNT_NUMBER    
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
                ,GRACE_DATE --BIBD FOR GRACE PERIOD    
                /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */    
            ) SELECT 
                MASTERID    
                --,ACCOUNT_NUMBER    
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
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
                ,SCH_FLAG    
                ,GRACE_DATE --BIBD FOR GRACE PERIOD    
                /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */    
            FROM ' || 'TMP_SCHEDULE_CURR' || ' ';
        EXECUTE (V_STR_QUERY);
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCH_MAX' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHD' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCH_MAX' || ' 
        SELECT 
            MASTERID    
            ,MAX(PMTDATE) AS MAX_PMTDATE    
        FROM ' || V_TABLEINSERT7 || '
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHD' || ' 
        SELECT 
            A.MASTERID    
            ,A.OSPRN    
        FROM ' || V_TABLEINSERT7 || ' A    
        JOIN ' || 'TMP_SCH_MAX' || ' B ON A.MASTERID = B.MASTERID    
        AND A.PMTDATE = B.MAX_PMTDATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND EXCEPTION_CODE = ''V-2'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (    
            DOWNLOAD_DATE    
            ,DATA_SOURCE    
            ,PRD_CODE    
            ,ACCOUNT_NUMBER    
            ,MASTERID    
            ,PROCESS_ID    
            ,EXCEPTION_CODE    
            ,REMARKS    
        ) SELECT 
            PMA.DOWNLOAD_DATE    
            ,PMA.DATA_SOURCE    
            ,PMA.PRODUCT_CODE    
            ,PMA.ACCOUNT_NUMBER    
            ,PMA.MASTERID    
            ,''IFRS EXCEPTIONS'' AS PROCESS_ID    
            ,''V-2'' AS EXCEPTION_CODE    
            ,''SCHEDULE : LAST OSPRN SCHEDULE <> 0 '' AS REMARKS    
        FROM ' || V_TABLENAME || ' PMA    
        JOIN ' || 'TMP_SCHD' || ' SCH 
        ON PMA.MASTERID = SCH.MASTERID    
        AND PMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        --AND PMA.PMT_SCH_STATUS = ''Y''    
        AND COALESCE(SCH.OSPRN, 0) <> 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHD' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHD' || ' (MASTERID) 
        SELECT MASTERID 
        FROM ' || V_TABLEINSERT7 || ' 
        WHERE PMTDATE IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (    
            DOWNLOAD_DATE    
            ,DATA_SOURCE    
            ,PRD_CODE    
            ,ACCOUNT_NUMBER    
            ,MASTERID    
            ,PROCESS_ID    
            ,EXCEPTION_CODE    
            ,REMARKS    
        ) SELECT 
            PMA.DOWNLOAD_DATE    
            ,PMA.DATA_SOURCE    
            ,PMA.PRODUCT_CODE    
            ,PMA.ACCOUNT_NUMBER    
            ,PMA.MASTERID    
            ,''IFRS EXCEPTIONS'' AS PROCESS_ID    
            ,''V-2'' AS EXCEPTION_CODE    
            ,''PMTDATE : IS NULL'' AS REMARKS    
        FROM ' || V_TABLENAME || ' PMA    
        JOIN ' || 'TMP_SCHD' || ' SCH 
        ON PMA.MASTERID = SCH.MASTERID    
        AND PMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_SCHD' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SCHD' || ' (MASTERID) 
        SELECT DISTINCT MASTERID    
        FROM (    
            SELECT 
                MASTERID    
                ,PMTDATE    
            FROM IFRS_PAYM_SCHD_MTM    
            GROUP BY MASTERID, PMTDATE    
            HAVING COUNT(1) > 1    
        ) A ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (    
            DOWNLOAD_DATE    
            ,DATA_SOURCE    
            ,PRD_CODE    
            ,ACCOUNT_NUMBER    
            ,MASTERID    
            ,PROCESS_ID    
            ,EXCEPTION_CODE    
            ,REMARKS    
        ) SELECT 
            PMA.DOWNLOAD_DATE    
            ,PMA.DATA_SOURCE    
            ,PMA.PRODUCT_CODE    
            ,PMA.ACCOUNT_NUMBER    
            ,PMA.MASTERID    
            ,''IFRS EXCEPTIONS'' AS PROCESS_ID    
            ,''V-2'' AS EXCEPTION_CODE    
            ,''PMTDATE : DOUBLE '' AS REMARKS    
        FROM ' || V_TABLENAME || ' PMA    
        JOIN ' || 'TMP_SCHD' || ' SCH 
        ON PMA.MASTERID = SCH.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET PV_CF = (A.PRINCIPAL + A.INTEREST) / POWER((1 + Z.MARKET_RATE / 12 / 100), A.COUNTER)    
        FROM ' || V_TABLEINSERT3 || ' Z 
        WHERE A.MASTERID = Z.MASTERID    
        AND Z.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            DOWNLOAD_DATE    
            ,MASTERID    
            ,ACCOUNT_NUMBER    
            ,IFRS9_CLASS    
            ,LOAN_START_DATE    
            ,LOAN_DUE_DATE    
            ,OUTSTANDING    
            ,INTEREST_RATE    
            ,EIR    
            ,MARKET_RATE    
            ,FAIR_VALUE_AMT    
            ,TOTAL_PV_CF    
            ,--GANTI NAMA    
            TOT_ADJUST --- TOTAL_PV_CF - COALESCE(FAIRVALUEAMT, OUTSTANDING)    
        ) SELECT 
            A.DOWNLOAD_DATE    
            ,A.MASTERID    
            ,A.ACCOUNT_NUMBER    
            ,A.IFRS9_CLASS    
            ,A.LOAN_START_DATE    
            ,A.LOAN_DUE_DATE    
            ,A.OUTSTANDING    
            ,A.INTEREST_RATE    
            ,A.EIR    
            ,A.MARKET_RATE    
            ,A.FAIR_VALUE_AMOUNT    
            ,B.TOTAL_PV_CF AS TOTAL_PV_CF    
            ,(B.TOTAL_PV_CF - COALESCE(A.FAIR_VALUE_AMOUNT, A.OUTSTANDING)) AS TOT_ADJUST    
        FROM ' || V_TABLEINSERT3 || ' A    
        JOIN (    
            SELECT 
                X.MASTERID    
                ,MIN(X.PMTDATE) AS MIN_PMTDATE    
                ,MAX(X.PMTDATE) AS MAX_PMTDATE    
                ,SUM(X.PRINCIPAL + X.INTEREST) AS TOT_INSTALMENT    
                ,SUM((X.PRINCIPAL + X.INTEREST) / POWER((1 + Z.MARKET_RATE / 12 / 100), X.COUNTER)) AS TOTAL_PV_CF    
            FROM ' || V_TABLEINSERT7 || ' X    
            JOIN ' || V_TABLEINSERT3 || ' Z 
            ON X.MASTERID = Z.MASTERID    
            WHERE Z.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
            GROUP BY X.MASTERID    
        ) B 
        ON A.MASTERID = B.MASTERID    
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        AND A.IFRS9_CLASS IN (''FVTPL'',''FVOCI'') --IFRS_CLASS    
        AND A.ACCOUNT_STATUS = ''A'' ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_PAYM_SCHD_MTM', '');

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

$$
