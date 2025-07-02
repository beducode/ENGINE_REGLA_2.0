---- DROP PROCEDURE SP_IFRS_ACCT_EIR_GS_INSERT4;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_GS_INSERT4(
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
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    V_I INT;
    
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
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_ECF_NOCF_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_EIR_ECF_T2_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_ECF1_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_ECF2_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_ACCT_EIR_PAYM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_ECF_NOCF';
        V_TABLEINSERT3 := 'IFRS_ACCT_EIR_ECF_T2';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_ECF1';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_ECF2';
        V_TABLEINSERT7 := 'IFRS_ACCT_EIR_PAYM';
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

    V_I := 0;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF_NOCF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF_T2 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF1 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF2 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_EIR_GS_INSERT4', '');
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT5 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (
            MASTERID
            ,DOWNLOAD_DATE
            ,N_LOAN_AMT
            ,N_INT_RATE
            ,N_EFF_INT_RATE
            ,STARTAMORTDATE
            ,ENDAMORTDATE
            ,GRACEDATE
            ,DISB_PERCENTAGE
            ,DISB_AMOUNT
            ,PLAFOND
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
            ,N_FEE_AMT
            ,N_COST_AMT
        ) SELECT 
            A.MASTERID
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.N_LOAN_AMT
            ,A.N_INT_RATE
            ,C.EIR
            ,A.STARTAMORTDATE
            ,A.ENDAMORTDATE
            ,A.GRACEDATE
            ,A.DISB_PERCENTAGE
            ,A.DISB_AMOUNT
            ,A.PLAFOND
            ,A.PAYMENTCODE
            ,A.INTCALCCODE
            ,A.PAYMENTTERM
            ,A.ISGRACE
            ,A.PREV_PMT_DATE
            ,A.PMT_DATE
            ,A.I_DAYS
            ,A.I_DAYS
            ,A.N_OSPRN_PREV
            ,A.N_INSTALLMENT
            ,A.N_PRN_PAYMENT
            ,A.N_INT_PAYMENT
            ,A.N_OSPRN
            ,0 + A.N_OSPRN N_FAIRVALUE_PREV --ZERO UNAMORT
            ,0 N_EFF_INT_AMT
            ,0 + A.N_OSPRN N_FAIRVALUE --ZERO UNAMORT
            ,0 N_UNAMORT_AMT_PREV --ZERO UNAMORT
            ,0 N_AMORT_AMT
            ,0 N_UNAMORT_AMT
            ,0 N_COST_UNAMORT_AMT_PREV
            ,0 N_COST_AMORT_AMT
            ,0 N_COST_UNAMORT_AMT
            ,0 N_FEE_UNAMORT_AMT_PREV
            ,0 N_FEE_AMORT_AMT
            ,0 N_FEE_UNAMORT_AMT
            ,0 N_FEE_AMT
            ,0 N_COST_AMT_PREV
        FROM ' || V_TABLEINSERT7 || ' A
        JOIN ' || 'IFRS_ACCT_EIR_CF_ECF' || ' B 
        ON B.MASTERID = A.MASTERID
        JOIN ' || 'IFRS_ACCT_EIR_GS_RESULT4' || ' C 
        ON C.MASTERID = A.MASTERID
        AND C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        WHERE A.PMT_DATE = A.PREV_PMT_DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            MASTERID
            ,DOWNLOAD_DATE
            ,N_LOAN_AMT
            ,N_INT_RATE
            ,N_EFF_INT_RATE
            ,STARTAMORTDATE
            ,ENDAMORTDATE
            ,GRACEDATE
            ,DISB_PERCENTAGE
            ,DISB_AMOUNT
            ,PLAFOND
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
        ) SELECT 
            MASTERID
            ,DOWNLOAD_DATE
            ,N_LOAN_AMT
            ,N_INT_RATE
            ,N_EFF_INT_RATE
            ,STARTAMORTDATE
            ,ENDAMORTDATE
            ,GRACEDATE
            ,DISB_PERCENTAGE
            ,DISB_AMOUNT
            ,PLAFOND
            ,PAYMENTCODE
            ,INTCALCCODE
            ,PAYMENTTERM
            ,ISGRACE
            ,PREV_PMT_DATE
            ,PMT_DATE
            ,I_DAYS
            ,(PMT_DATE - PREV_PMT_DATE) AS I_DAYS2
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
        FROM ' || V_TABLEINSERT5 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T9' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T9' || ' 
        (
            MASTERID
            ,PMTDATE
        ) SELECT 
            MASTERID
            ,PMT_DATE
        FROM ' || V_TABLEINSERT7 || ' 
        WHERE PMT_DATE = PREV_PMT_DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT3 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (
            MASTERID
            ,PMTDATE
        ) SELECT 
            A.MASTERID
            ,MIN(A.PMT_DATE) AS PMTDATE
        FROM ' || V_TABLEINSERT7 || ' A
        JOIN ' || 'TMP_T9' || ' B 
        ON B.MASTERID = A.MASTERID
        AND A.PMT_DATE > B.PMTDATE
        GROUP BY A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM ' || V_TABLEINSERT3 || '';
    EXECUTE (V_STR_QUERY) INTO V_I;

    WHILE V_I > 0
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
            (
                MASTERID
                ,DOWNLOAD_DATE
                ,N_LOAN_AMT
                ,N_INT_RATE
                ,N_EFF_INT_RATE
                ,STARTAMORTDATE
                ,ENDAMORTDATE
                ,GRACEDATE
                ,DISB_PERCENTAGE
                ,DISB_AMOUNT
                ,PLAFOND
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
                ,N_FEE_AMT
                ,N_COST_AMT
            ) SELECT 
                A.MASTERID
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                ,A.N_LOAN_AMT
                ,A.N_INT_RATE
                ,C.N_EFF_INT_RATE
                ,A.STARTAMORTDATE
                ,A.ENDAMORTDATE
                ,A.GRACEDATE
                ,A.DISB_PERCENTAGE
                ,A.DISB_AMOUNT
                ,A.PLAFOND
                ,A.PAYMENTCODE
                ,A.INTCALCCODE
                ,A.PAYMENTTERM
                ,A.ISGRACE
                ,A.PREV_PMT_DATE
                ,A.PMT_DATE
                ,A.I_DAYS
                ,A.I_DAYS
                ,A.N_OSPRN_PREV
                ,A.N_INSTALLMENT
                ,A.N_PRN_PAYMENT
                ,A.N_INT_PAYMENT
                ,A.N_OSPRN
                ,C.N_FAIRVALUE N_FAIRVALUE_PREV
                ,ROUND(CASE 
                    --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428 
                    WHEN A.INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                    --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                    WHEN A.INTCALCCODE = ''4''
                    THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                    ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                END, ' || V_ROUND || ') AS N_EFF_INT_AMT
                ,C.N_FAIRVALUE - A.N_PRN_PAYMENT + ROUND(CASE 
                        --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                        WHEN A.INTCALCCODE IN (''1'', ''6'')
                        THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                        --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                        WHEN A.INTCALCCODE IN (''2'', ''3'')
                        THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                        WHEN A.INTCALCCODE = ''4''
                        THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                        ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, ' || V_ROUND || ') - A.N_INT_PAYMENT + A.DISB_AMOUNT AS N_FAIRVALUE
                ,C.N_UNAMORT_AMT N_UNAMORT_AMT_PREV
                ,ROUND(CASE 
                        --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                        WHEN A.INTCALCCODE IN (''1'', ''6'')
                        THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                        --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                        WHEN A.INTCALCCODE IN (''2'', ''3'')
                        THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                        WHEN A.INTCALCCODE = ''4''
                        THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                        ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, ' || V_ROUND || ') - A.N_INT_PAYMENT AS N_AMORT_AMT
                ,C.N_UNAMORT_AMT + ROUND(CASE 
                        --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                        WHEN A.INTCALCCODE IN (''1'', ''6'')
                        THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                        --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                        WHEN A.INTCALCCODE IN (''2'', ''3'')
                        THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                        WHEN A.INTCALCCODE = ''4''
                        THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                        ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, ' || V_ROUND || ') - A.N_INT_PAYMENT AS N_UNAMORT_AMT
                ,C.N_COST_UNAMORT_AMT N_COST_UNAMORT_AMT_PREV
                ,CASE 
                    WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                    ELSE (
                        ROUND(CASE 
                            --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''1'', ''6'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''2'', ''3'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            WHEN A.INTCALCCODE = ''4''
                            THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                            ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                        END, ' || V_ROUND || ') - A.N_INT_PAYMENT
                    ) * C.N_COST_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                    END AS N_COST_AMORT_AMT
                ,C.N_COST_UNAMORT_AMT + CASE 
                    WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                    ELSE (
                        ROUND(CASE 
                            --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''1'', ''6'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''2'', ''3'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            WHEN A.INTCALCCODE = ''4''
                            THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                            ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                        END, ' || V_ROUND || ') - A.N_INT_PAYMENT
                    ) * C.N_COST_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                    END AS N_COST_UNAMORT_AMT
                ,C.N_FEE_UNAMORT_AMT N_FEE_UNAMORT_AMT_PREV
                ,CASE 
                    WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                    ELSE (
                        ROUND(CASE 
                            --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''1'', ''6'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''2'', ''3'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            WHEN A.INTCALCCODE = ''4''
                            THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                            ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                        END, ' || V_ROUND || ') - A.N_INT_PAYMENT
                    ) * C.N_FEE_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                    END AS N_FEE_AMORT_AMT
                ,C.N_FEE_UNAMORT_AMT + CASE 
                    WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                    ELSE (
                        ROUND(CASE 
                            --WHEN A.INTCALCCODE IN(''2'',''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''1'', ''6'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE=''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                            WHEN A.INTCALCCODE IN (''2'', ''3'')
                            THEN CAST(A.I_DAYS AS FLOAT) / CAST(365 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            WHEN A.INTCALCCODE = ''4''
                            THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                            ELSE (CAST(30 AS FLOAT) * A.M / CAST(360 AS FLOAT) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                        END, ' || V_ROUND || ') - A.N_INT_PAYMENT
                    ) * C.N_FEE_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                    END AS N_FEE_UNAMORT_AMT
                ,C.N_FEE_AMT
                ,C.N_COST_AMT
            FROM ' || V_TABLEINSERT7 || ' A
            JOIN ' || V_TABLEINSERT3 || ' B 
            ON B.MASTERID = A.MASTERID
            AND B.PMTDATE = A.PMT_DATE
            JOIN ' || V_TABLEINSERT5 || ' C 
            ON C.MASTERID = B.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
            (
                MASTERID
                ,DOWNLOAD_DATE
                ,N_LOAN_AMT
                ,N_INT_RATE
                ,N_EFF_INT_RATE
                ,STARTAMORTDATE
                ,ENDAMORTDATE
                ,GRACEDATE
                ,DISB_PERCENTAGE
                ,DISB_AMOUNT
                ,PLAFOND
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
            ) SELECT 
                MASTERID
                ,DOWNLOAD_DATE
                ,N_LOAN_AMT
                ,N_INT_RATE
                ,N_EFF_INT_RATE
                ,STARTAMORTDATE
                ,ENDAMORTDATE
                ,GRACEDATE
                ,DISB_PERCENTAGE
                ,DISB_AMOUNT
                ,PLAFOND
                ,PAYMENTCODE
                ,INTCALCCODE
                ,PAYMENTTERM
                ,ISGRACE
                ,PREV_PMT_DATE
                ,PMT_DATE
                ,I_DAYS
                ,(PMT_DATE - PREV_PMT_DATE) AS I_DAYS2
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
            FROM ' || V_TABLEINSERT4 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT5 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
            (
                MASTERID
                ,DOWNLOAD_DATE
                ,N_LOAN_AMT
                ,N_INT_RATE
                ,N_EFF_INT_RATE
                ,STARTAMORTDATE
                ,ENDAMORTDATE
                ,GRACEDATE
                ,DISB_PERCENTAGE
                ,DISB_AMOUNT
                ,PLAFOND
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
                ,N_FEE_AMT
                ,N_COST_AMT
            ) SELECT 
                MASTERID
                ,DOWNLOAD_DATE
                ,N_LOAN_AMT
                ,N_INT_RATE
                ,N_EFF_INT_RATE
                ,STARTAMORTDATE
                ,ENDAMORTDATE
                ,GRACEDATE
                ,DISB_PERCENTAGE
                ,DISB_AMOUNT
                ,PLAFOND
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
                ,N_FEE_AMT
                ,N_COST_AMT
            FROM ' || V_TABLEINSERT4 || '';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT3 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
            (
                MASTERID
                ,PMTDATE
            ) SELECT 
                A.MASTERID
                ,MIN(A.PMT_DATE) AS PMTDATE
            FROM ' || V_TABLEINSERT7 || ' A
            JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.MASTERID = A.MASTERID
            AND A.PMT_DATE > B.PMT_DATE
            GROUP BY A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM ' || V_TABLEINSERT3 || '';
        EXECUTE (V_STR_QUERY) INTO V_I;
    END LOOP;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_EIR_GS_INSERT4', '');

    RAISE NOTICE 'SP_IFRS_ACCT_EIR_GS_INSERT4 | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT5;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_EIR_GS_INSERT4';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT5 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;