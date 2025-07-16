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
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_PAYM_SCHD_SRC', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT6 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND MASTERID IN (
            SELECT DISTINCT MASTERID 
            FROM ' || 'IFRS_STG_PAYM_SCHD' || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT7 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET 
            STATUS = ''A'' 
            ,END_DATE = NULL 
        WHERE END_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET 
            STATUS = ''C'' 
            ,END_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,CREATED_DATE = CURRENT_TIMESTAMP 
        WHERE MASTERID IN (
            SELECT DISTINCT MASTERID 
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ORDER BY MASTERID ASC 
        ) 
        AND END_DATE IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET 
            STATUS = ''C'' 
            ,END_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,CREATED_DATE = CURRENT_TIMESTAMP 
        WHERE MASTERID IN (
            SELECT DISTINCT MASTERID 
            FROM ' || 'IFRS_STG_PAYM_SCHD' || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ORDER BY MASTERID ASC 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
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
            ,SOURCE_PROCESS 
            ,SCH_FLAG 
            ,GRACE_DATE 
            ,STATUS 
            ,CREATED_DATE 
        ) SELECT 
            DOWNLOAD_DATE 
            ,MASTERID 
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
            ,SOURCE_PROCESS 
            ,SCH_FLAG 
            ,GRACE_DATE 
            ,''A'' 
            ,CURRENT_TIMESTAMP 
        FROM ' || V_TABLEINSERT6 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
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
            ,SOURCE_PROCESS 
            ,SCH_FLAG 
            ,GRACE_DATE 
            ,STATUS 
            ,CREATED_DATE 
        ) SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.PMTDATE 
            ,B.INTEREST_RATE 
            ,A.OSPRN 
            ,A.PRINCIPAL 
            ,A.INTEREST 
            ,0.000000 
            ,0.000000 
            ,B.PLAFOND 
            ,CASE 
                WHEN B.INTEREST_CALCULATION_CODE = ''6'' 
                THEN CASE 
                    WHEN ' || V_CALC_IDAYS || ' = 0 
                    THEN CASE 
                        WHEN (
                            ROW_NUMBER() OVER (PARTITION BY A.MASTERID ORDER BY PMTDATE) - 1
                        ) = 1 
                        THEN CASE 
                            WHEN (
                                (EXTRACT(YEAR FROM A.PMTDATE) * 12 + EXTRACT(MONTH FROM A.PMTDATE)) - 
                                (
                                    EXTRACT(
                                        YEAR FROM COALESCE(
                                            LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                            ,A.PMTDATE
                                        )
                                    ) * 12 + EXTRACT(
                                        MONTH FROM COALESCE(
                                            LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                            ,A.PMTDATE
                                        )
                                    )
                                )
                            ) = 0 
                            THEN 30 
                            ELSE (
                                (EXTRACT(YEAR FROM A.PMTDATE) * 12 + EXTRACT(MONTH FROM A.PMTDATE)) - 
                                (
                                    EXTRACT(
                                        YEAR FROM COALESCE(
                                            LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                            ,A.PMTDATE
                                        )
                                    ) * 12 + EXTRACT(
                                        MONTH FROM COALESCE(
                                            LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                            ,A.PMTDATE
                                        )
                                    )
                                )
                            ) * 30
                        END
                        ELSE (
                            (EXTRACT(YEAR FROM A.PMTDATE) * 12 + EXTRACT(MONTH FROM A.PMTDATE)) - 
                            (
                                EXTRACT(
                                    YEAR FROM COALESCE(
                                        LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                        ,A.PMTDATE
                                    )
                                ) * 12 + EXTRACT(
                                    MONTH FROM COALESCE(
                                        LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                        ,A.PMTDATE
                                    )
                                )
                            )
                        ) * 30 
                    END 
                    WHEN ' || V_CALC_IDAYS || ' = 1 
                    THEN F_CNT_DAYS_30_360(
                        COALESCE(
                            LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                            ,A.PMTDATE
                        )
                        ,A.PMTDATE
                    )
                    ELSE F_CNT_DAYS_30_360(
                        COALESCE(
                            LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                            ,A.PMTDATE
                        )
                        ,A.PMTDATE
                    )
                END 
                ELSE COALESCE(
                    (EXTRACT(YEAR FROM A.PMTDATE) * 12 + EXTRACT(MONTH FROM A.PMTDATE)) - 
                    (
                        EXTRACT(
                            YEAR FROM COALESCE(
                                LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                ,A.PMTDATE
                            )
                        ) * 12 + EXTRACT(
                            MONTH FROM COALESCE(
                                LAG(A.PMTDATE) OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY A.PMTDATE)
                                ,A.PMTDATE
                            )
                        )
                    )
                    ,0
                )
            END AS I_DAYS 
            ,(ROW_NUMBER() OVER (PARTITION BY A.DOWNLOAD_DATE, A.MASTERID ORDER BY PMTDATE)) - 1 AS COUNTER 
            ,B.INTEREST_CALCULATION_CODE 
            ,B.OUTSTANDING 
            ,''DWH'' AS SOURCE_PROCESS 
            ,''Y'' AS SCH_FLAG 
            ,NULL AS GRACE_DATE 
            ,''A'' AS STATUS 
            ,CURRENT_TIMESTAMP AS CREATED_DATE 
        FROM ' || 'IFRS_STG_PAYM_SCHD' || ' A 
        JOIN ' || V_TABLEINSERT3 || ' B 
        ON A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
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
            ,SOURCE_PROCESS 
            ,SCH_FLAG 
            ,GRACE_DATE 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.PMTDATE 
            ,A.INTEREST_RATE 
            ,A.OSPRN 
            ,A.PRINCIPAL 
            ,A.INTEREST 
            ,A.DISB_PERCENTAGE 
            ,A.DISB_AMOUNT 
            ,A.PLAFOND 
            ,A.I_DAYS 
            ,A.COUNTER 
            ,A.ICC 
            ,A.OUTSTANDING 
            ,A.SOURCE_PROCESS 
            ,A.SCH_FLAG 
            ,A.GRACE_DATE 
        FROM ' || V_TABLEINSERT7 || ' A 
        JOIN ' || V_TABLEINSERT3 || ' B 
            ON A.MASTERID = B.MASTERID 
            AND A.STATUS = ''A'' 
        LEFT JOIN ' || V_TABLEINSERT6 || ' C 
            ON A.MASTERID = C.MASTERID 
            AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
        WHERE C.MASTERID IS NULL 
            AND B.ECF_STATUS = ''Y'' 
            AND A.PMTDATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_SCHD1' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_SCHD1' || ' AS 
        SELECT MASTERID, MAX(PMTDATE) AS LAST_PAYMENT_DATE_PYM 
        FROM ' || V_TABLEINSERT7 || ' 
        WHERE PMTDATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_SCHD2' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_SCHD2' || ' AS 
        SELECT MASTERID, MIN(PMTDATE) AS NEXT_PAYMENT_DATE_PYM 
        FROM ' || V_TABLEINSERT7 || ' 
        WHERE PMTDATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A 
        SET LAST_PAYMENT_DATE = B.LAST_PAYMENT_DATE_PYM 
        FROM ' || 'TMP_SCHD1' || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
        SET LAST_PAYMENT_DATE = B.LAST_PAYMENT_DATE_PYM 
        FROM ' || 'TMP_SCHD1' || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A 
        SET NEXT_PAYMENT_DATE = B.NEXT_PAYMENT_DATE_PYM 
        FROM ' || 'TMP_SCHD2' || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
        SET NEXT_PAYMENT_DATE = B.NEXT_PAYMENT_DATE_PYM 
        FROM ' || 'TMP_SCHD2' || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT6 || ' 
        WHERE PMTDATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT6 || ' A 
        USING (
            SELECT MASTERID, MIN(PMTDATE) AS MAX_PMTDATE 
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE OSPRN = 0 
            GROUP BY MASTERID 
        ) B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.PMTDATE > B.MAX_PMTDATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
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
            ,SOURCE_PROCESS 
            ,SCH_FLAG 
            ,GRACE_DATE 
        ) SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,C.START_AMORTIZATION_DATE 
            ,A.INTEREST_RATE 
            ,(A.OSPRN + A.PRINCIPAL) AS OSPRN 
            ,0.000000 AS PRINCIPAL 
            ,0.000000 AS INTEREST
            ,0.000000 AS DISB_PERCENTAGE 
            ,(A.OSPRN + A.PRINCIPAL) AS DISB_AMOUNT 
            ,A.PLAFOND 
            ,0 AS I_DAYS 
            ,0 AS COUNTER 
            ,A.ICC 
            ,A.OUTSTANDING 
            ,A.SOURCE_PROCESS 
            ,A.SCH_FLAG 
            ,A.GRACE_DATE 
        FROM ' || V_TABLEINSERT6 || ' A 
        JOIN ' || V_TABLEINSERT3 || ' B 
            ON A.MASTERID = B.MASTERID 
            AND B.ECF_STATUS = ''Y'' 
        JOIN (
            SELECT 
                PMA.DOWNLOAD_DATE 
                ,PMA.MASTERID 
                ,CASE 
                    WHEN ' || V_PARAM_CALC_TO_LAST_PAYMENT || ' = 0 
                    THEN ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                    ELSE CASE 
                        WHEN PYM.MASTERID IS NOT NULL 
                        THEN PYM.LAST_PAYMENT_DATE_PYM 
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
            FROM ' || V_TABLENAME || ' PMA 
            JOIN ' || V_TABLEINSERT3 || ' PMC 
                ON PMA.MASTERID = PMC.MASTERID 
                AND PMA.DOWNLOAD_DATE = PMC.DOWNLOAD_DATE 
            LEFT JOIN ' || V_TABLEINSERT3 || ' PMV 
                ON PMC.MASTERID = PMV.MASTERID 
                AND PMV.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            LEFT JOIN ' || 'TMP_SCHD1' || ' PYM 
                ON PYM.MASTERID = PMA.MASTERID 
            WHERE PMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND PMC.ECF_STATUS = ''Y'' 
                AND PMA.ACCOUNT_STATUS = ''A'' 
                AND PMA.IAS_CLASS IN (''A'', ''O'') 
                AND PMA.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND PMA.AMORT_TYPE = ''EIR'' 
        ) C 
        ON A.MASTERID = C.MASTERID 
        AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
        JOIN ( 
            SELECT MASTERID, MIN(COUNTER) COUNTER 
            FROM ' || V_TABLEINSERT6 || ' 
            GROUP BY MASTERID
        ) D 
        ON A.MASTERID = D.MASTERID 
        AND A.COUNTER = D.COUNTER ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT5 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (
            MASTERID 
            ,PREV_PMT_DATE 
            ,PMT_DATE 
            ,INTEREST_RATE 
            ,I_DAYS 
            ,PRN_AMT 
            ,INT_AMT 
            ,DISB_PERCENTAGE 
            ,DISB_AMOUNT 
            ,PLAFOND 
            ,OS_PRN_PREV 
            ,OS_PRN 
            ,COUNTER 
            ,ICC 
            ,GRACE_DATE 
        ) SELECT 
            SCH.MASTERID 
            ,COALESCE(LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.MASTERID ORDER BY SCH.PMTDATE), SCH.PMTDATE) 
            ,SCH.PMTDATE 
            ,SCH.INTEREST_RATE 
            ,CASE 
                WHEN SCH.ICC = ''6'' 
                THEN CASE 
                    WHEN ' || V_CALC_IDAYS || ' = 0 
                    THEN CASE 
                        WHEN (
                            ROW_NUMBER() OVER (PARTITION BY SCH.MASTERID ORDER BY PMTDATE) - 1
                        ) = 1 
                        THEN CASE 
                            WHEN (
                                (EXTRACT(YEAR FROM SCH.PMTDATE) * 12 + EXTRACT(MONTH FROM SCH.PMTDATE)) - 
                                (
                                    EXTRACT(
                                        YEAR FROM COALESCE(
                                            LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                            ,SCH.PMTDATE
                                        )
                                    ) * 12 + EXTRACT(
                                        MONTH FROM COALESCE(
                                            LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                            ,SCH.PMTDATE
                                        )
                                    )
                                )
                            ) = 0 
                            THEN 30 
                            ELSE (
                                (EXTRACT(YEAR FROM SCH.PMTDATE) * 12 + EXTRACT(MONTH FROM SCH.PMTDATE)) - 
                                (
                                    EXTRACT(
                                        YEAR FROM COALESCE(
                                            LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                            ,SCH.PMTDATE
                                        )
                                    ) * 12 + EXTRACT(
                                        MONTH FROM COALESCE(
                                            LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                            ,SCH.PMTDATE
                                        )
                                    )
                                )
                            ) * 30
                        END
                        ELSE (
                            (EXTRACT(YEAR FROM SCH.PMTDATE) * 12 + EXTRACT(MONTH FROM SCH.PMTDATE)) - 
                            (
                                EXTRACT(
                                    YEAR FROM COALESCE(
                                        LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                        ,SCH.PMTDATE
                                    )
                                ) * 12 + EXTRACT(
                                    MONTH FROM COALESCE(
                                        LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                        ,SCH.PMTDATE
                                    )
                                )
                            )
                        ) * 30 
                    END 
                    WHEN ' || V_CALC_IDAYS || ' = 1 
                    THEN F_CNT_DAYS_30_360(
                        COALESCE(
                            LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                            ,SCH.PMTDATE
                        )
                        ,SCH.PMTDATE
                    )
                    ELSE F_CNT_DAYS_30_360(
                        COALESCE(
                            LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                            ,SCH.PMTDATE
                        )
                        ,SCH.PMTDATE
                    )
                END 
                ELSE COALESCE(
                    (EXTRACT(YEAR FROM SCH.PMTDATE) * 12 + EXTRACT(MONTH FROM SCH.PMTDATE)) - 
                    (
                        EXTRACT(
                            YEAR FROM COALESCE(
                                LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                ,SCH.PMTDATE
                            )
                        ) * 12 + EXTRACT(
                            MONTH FROM COALESCE(
                                LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE)
                                ,SCH.PMTDATE
                            )
                        )
                    )
                    ,0
                )
            END AS I_DAYS 
            ,CASE 
                WHEN SCH.COUNTER = MAX(SCH.COUNTER) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID) 
                THEN LAG(SCH.OSPRN) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID ORDER BY SCH.PMTDATE) 
                ELSE SCH.PRINCIPAL 
            END AS NEW_PRINCIPAL 
            ,SCH.INTEREST 
            ,SCH.DISB_PERCENTAGE 
            ,SCH.DISB_AMOUNT 
            ,SCH.PLAFOND 
            ,COALESCE(LAG(SCH.OSPRN) OVER (PARTITION BY SCH.MASTERID ORDER BY SCH.PMTDATE), SCH.OSPRN) 
            ,CASE 
                WHEN SCH.COUNTER = MAX(SCH.COUNTER) OVER (PARTITION BY SCH.DOWNLOAD_DATE, SCH.MASTERID) 
                THEN 0 
                ELSE SCH.OSPRN 
            END AS OSPRN 
            ,(ROW_NUMBER () OVER (PARTITION BY SCH.MASTERID ORDER BY SCH.PMTDATE) - 1) COUNTER
            ,CAST(SCH.ICC AS INTEGER) 
            ,SCH.GRACE_DATE 
        FROM ' || V_TABLEINSERT6 || ' SCH 
        JOIN (
            SELECT 
                A.DOWNLOAD_DATE 
                ,A.MASTERID 
                ,CASE 
                    WHEN TENOR_TYPE = ''E'' 
                        AND A.LOAN_START_DATE + (B.EXPECTED_LIFE || '' MONTHS'')::INTERVAL > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                    THEN A.LOAN_START_DATE + (B.EXPECTED_LIFE || '' MONTHS'')::INTERVAL 
                    ELSE A.LOAN_DUE_DATE 
                END AS EXPECTED_DATE 
            FROM ' || V_TABLEINSERT3 || ' A 
            LEFT JOIN ' || V_TABLEINSERT8 || ' B 
            ON (A.PRODUCT_CODE = B.PRD_CODE OR B.PRD_CODE = ''ALL'') 
            AND (A.CURRENCY = B.CCY OR B.CCY = ''ALL'') 
            WHERE A.ECF_STATUS = ''Y'' 
        ) B 
        ON SCH.MASTERID = B.MASTERID 
        WHERE SCH.PMTDATE <= B.EXPECTED_DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    IF V_PARAM_CALC_TO_LAST_PAYMENT = 0 
    THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_LAST_PAYM_DATE' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_LAST_PAYM_DATE' || ' AS 
            SELECT 
                DOWNLOAD_DATE 
                ,MASTERID 
                ,CASE 
                    WHEN COALESCE(LAST_PAYMENT_DATE, ''1900-01-01'') = ''1900-01-01'' 
                    THEN LOAN_START_DATE 
                    ELSE LAST_PAYMENT_DATE 
                END AS LAST_PAYM_DATE 
                ,NEXT_PAYMENT_DATE 
            FROM ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
            SET INT_AMT = CASE 
                WHEN A.ICC IN (''1'', ''2'') 
                THEN OS_PRN_PREV * CAST(A.I_DAYS AS FLOAT) * INTEREST_RATE / CASE 
                    WHEN A.ICC = 1 
                    THEN 36000 
                    ELSE 36500 
                END 
                ELSE (CAST(A.I_DAYS AS FLOAT) / CAST(F_CNT_DAYS_30_360(LAST_PAYM_DATE, NEXT_PAYMENT_DATE) AS FLOAT)) * INT_AMT 
            END 
            FROM ' || 'TMP_LAST_PAYM_DATE' || ' B 
            WHERE A.MASTERID = B.MASTERID 
            AND A.COUNTER = 1 
            AND CAST(RIGHT(A.MASTERID, 3) AS INT) NOT IN (SELECT DISTINCT  CAST(VALUE1 AS INT) FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''SCM010'') ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' 
        SET 
            I_DAYS = CASE 
                WHEN (
                    (EXTRACT(YEAR FROM AGE(NEXT_PAYMENT_DATE, ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)) * 12) + 
                    EXTRACT(MONTH FROM AGE(NEXT_PAYMENT_DATE, ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE))
                ) - 1 >= CAST(VALUE2 AS INT) 
                THEN CASE 
                    WHEN A.ICC IN (''1'', ''2'') 
                    THEN EXTRACT(DAY FROM A.PMT_DATE - (A.PMT_DATE + ((CAST(B.VALUE2 AS INT) * -1) || '' MONTHS'')::INTERVAL)) 
                    ELSE 30 * CAST(VALUE2 AS INT) 
                END 
                ELSE A.I_DAYS 
            END 
            ,INT_AMT = CASE 
                WHEN (
                    (EXTRACT(YEAR FROM AGE(NEXT_PAYMENT_DATE, ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)) * 12) + 
                    EXTRACT(MONTH FROM AGE(NEXT_PAYMENT_DATE, ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE))
                ) - 1 < CAST(VALUE2 AS INT) 
                THEN CASE 
                    WHEN A.ICC IN (''1'', ''2'') 
                    THEN (CAST(A.I_DAYS AS FLOAT) / CAST(CASE 
                        WHEN (NEXT_PAYMENT_DATE - LAST_PAYM_DATE) = 0 
                        THEN 1 
                        ELSE (NEXT_PAYMENT_DATE - LAST_PAYM_DATE) 
                    END AS FLOAT)) * A.INT_AMT 
                    ELSE (CAST(A.I_DAYS AS FLOAT) / CAST(
                        (((F_CNT_DAYS_30_360(LAST_PAYM_DATE, NEXT_PAYMENT_DATE) - 1) / 30) + 1) * 30
                        AS FLOAT
                    )) * A.INT_AMT 
                END 
                ELSE A.INT_AMT 
            END 
        FROM ' || V_TABLEINSERT5 || ' A 
        JOIN (SELECT * FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''SCM010'') B 
        ON CAST(RIGHT(A.MASTERID, 3) AS INT) =  CAST(VALUE1 AS INT) 
        LEFT JOIN ' || 'TMP_LAST_PAYM_DATE' || ' C 
        ON A.MASTERID = C.MASTERID 
        WHERE A.COUNTER = 1 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' 
        SET I_DAYS = CASE 
            WHEN A.ICC IN (''1'', ''2'') 
            THEN EXTRACT(DAY FROM A.PMT_DATE - (A.PMT_DATE + ((CAST(VALUE2 AS INT) * -1) || '' MONTHS'')::INTERVAL)) 
            ELSE 30 * CAST(VALUE2 AS INT) 
        END 
        FROM ' || V_TABLEINSERT5 || ' A 
        JOIN (SELECT * FROM TBLM_COMMONCODEDETAIL WHERE COMMONCODE = ''SCM010'') B 
        ON CAST(RIGHT(A.MASTERID, 3) AS INT) =  CAST(VALUE1 AS INT) 
        LEFT JOIN ' || 'TMP_LAST_PAYM_DATE' || ' C 
        ON A.MASTERID = C.MASTERID 
        WHERE A.COUNTER >= 2 ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''FRZPYM'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND STATUS = ''ACT'' 
        AND METHOD = ''EIR'' 
        AND FLAG_AL = ''A'' 
        AND MASTERID NOT IN (
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY MASTERID 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE MASTERID IN (
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND STATUS IN (''FRZPYM'', ''FRZNF'') 
            GROUP BY MASTERID 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_TRANS' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_TRANS' || ' AS 
        SELECT 
            DOWNLOAD_DATE 
            ,MASTERID 
            ,SUM(AMOUNT) FILTER (WHERE FLAG_CF = ''C'') AS C
            ,SUM(AMOUNT) FILTER (WHERE FLAG_CF = ''F'') AS F 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND FLAG_REVERSE = ''N'' 
        GROUP BY DOWNLOAD_DATE, MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET 
            STATUS = ''PNL'' 
            ,CREATEDBY = ''ABS_EIR_THRESHOLD'' 
        FROM ' || V_TABLEINSERT1 || ' FEE 
        JOIN ' || V_TABLEINSERT6 || ' SCHD 
            ON FEE.DOWNLOAD_DATE = SCHD.PMTDATE 
            AND FEE.MASTERID = SCHD.MASTERID 
        JOIN (
            SELECT 
                A.DOWNLOAD_DATE 
                ,A.MASTERID 
                ,ABS(C * C.RATE_AMOUNT - F * C.RATE_AMOUNT) AS AMOUNT 
                ,B.CURRENCY 
                ,C.RATE_AMOUNT 
            FROM (
                SELECT DOWNLOAD_DATE, MASTERID, COALESCE(C, 0) AS C, COALESCE(F, 0) AS F 
                FROM ' || 'TMP_TRANS' || ' 
            ) A 
            JOIN ' || V_TABLEINSERT3 || ' B 
                ON A.MASTERID = B.MASTERID 
                AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
            JOIN ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' C 
                ON B.CURRENCY = C.CURRENCY 
                AND B.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) TRANS 
            ON SCHD.PMTDATE = TRANS.DOWNLOAD_DATE 
            AND SCHD.MASTERID = TRANS.MASTERID 
        WHERE SCHD.PMTDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND (TRANS.AMOUNT / (SCHD.OSPRN * TRANS.RATE_AMOUNT)) * 100 >= ' || V_PARAM_EIR_THRESHOLD || ' 
        AND FEE.STATUS = ''ACT'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET 
            STATUS = ''PNL'' 
            ,CREATEDBY = ''ABS_INT_THRESHOLD'' 
        FROM ' || V_TABLEINSERT1 || ' FEE 
        JOIN (
            SELECT 
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
                ,MASTERID 
                ,SUM(INT_AMT) AS INT_AMT 
                ,SUM(INT_AMT) * ' || V_PARAM_INT_THRESHOLD || ' AS INT_AMT_TRSHLD 
            FROM ' || V_TABLEINSERT5 || ' 
            GROUP BY MASTERID 
        ) SCHD 
        ON FEE.MASTERID = SCHD.MASTERID 
        AND FEE.DOWNLOAD_DATE = SCHD.DOWNLOAD_DATE 
        JOIN (
            SELECT 
                A.DOWNLOAD_DATE 
                ,A.MASTERID 
                ,ABS(C * C.RATE_AMOUNT - F * C.RATE_AMOUNT) AS AMOUNT 
                ,B.CURRENCY 
                ,C.RATE_AMOUNT 
            FROM (
                SELECT DOWNLOAD_DATE, MASTERID, COALESCE(C, 0) AS C, COALESCE(F, 0) AS F 
                FROM ' || 'TMP_TRANS' || ' 
            ) A 
            JOIN ' || V_TABLEINSERT3 || ' B 
                ON A.MASTERID = B.MASTERID 
                AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
            JOIN ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' C 
                ON B.CURRENCY = C.CURRENCY 
                AND B.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) TRANS 
        ON FEE.DOWNLOAD_DATE = TRANS.DOWNLOAD_DATE 
        AND FEE.MASTERID = TRANS.MASTERID 
        WHERE FEE.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND TRANS.AMOUNT >= (SCHD.INT_AMT_TRSHLD * TRANS.RATE_AMOUNT) 
        AND FEE.STATUS = ''ACT'' ';
    EXECUTE (V_STR_QUERY);

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