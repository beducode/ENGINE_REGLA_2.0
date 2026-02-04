CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_GS_INSERT
AS
 V_CURRDATE DATE;
 V_PREVDATE DATE;
 V_VI NUMBER(19);
 V_ROUND NUMBER(10);
 V_FUNCROUND NUMBER(10);
  BEGIN

    SELECT MAX(CURRDATE)
        , MAX(PREVDATE) INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES( V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','');

    BEGIN
      SELECT CAST(VALUE1 AS NUMBER(10))
          , CAST(VALUE2 AS NUMBER(10)) INTO V_ROUND, V_FUNCROUND
      FROM TBLM_COMMONCODEDETAIL
      WHERE COMMONCODE = 'SCM003';
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_ROUND:=6;
      V_FUNCROUND:=1;
    END;


    -- INSERT INITIAL ROW PREVDATE=PMTDATE
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_ECF1';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_ECF2';


    COMMIT;

    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF2 (
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
        )
    SELECT /*+ PARALLEL(8) */ A.MASTERID
        ,V_CURRDATE
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
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN B.BENEFIT
            ELSE B.COST_AMT + B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417
                - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417
            END + A.N_OSPRN N_FAIRVALUE_PREV
        ,0 N_EFF_INT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN B.BENEFIT
            ELSE B.COST_AMT + B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417
                - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417
            END + A.N_OSPRN N_FAIRVALUE
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN B.BENEFIT
            ELSE B.COST_AMT + B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417
                - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417
            END N_UNAMORT_AMT_PREV
        ,0 N_AMORT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN B.BENEFIT
            ELSE B.COST_AMT + B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417
                - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417
            END N_UNAMORT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN CASE
                        WHEN B.BENEFIT > 0
                            THEN B.BENEFIT
                        ELSE 0
                        END
            ELSE B.COST_AMT - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417 C
            END N_COST_UNAMORT_AMT_PREV
        ,0 N_COST_AMORT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN CASE
                        WHEN B.BENEFIT > 0
                            THEN B.BENEFIT
                        ELSE 0
                        END
            ELSE B.COST_AMT - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417 C
            END N_COST_UNAMORT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN CASE
                        WHEN B.BENEFIT < 0
                            THEN B.BENEFIT
                        ELSE 0
                        END
            ELSE B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417 F
            END N_FEE_UNAMORT_AMT_PREV
        ,0 N_FEE_AMORT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN CASE
                        WHEN B.BENEFIT < 0
                            THEN B.BENEFIT
                        ELSE 0
                        END
            ELSE B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417 F
            END N_FEE_UNAMORT_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN CASE
                        WHEN B.BENEFIT < 0
                            THEN B.BENEFIT
                        ELSE 0
                        END
            ELSE B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT, 0) --201801417 F
            END N_FEE_AMT
        ,CASE
            WHEN B.STAFFLOAN = 1
                --AND B.PREV_EIR IS NULL
                AND B.BENEFIT IS NOT NULL
                THEN CASE
                        WHEN B.BENEFIT > 0
                            THEN B.BENEFIT
                        ELSE 0
                        END
            ELSE B.COST_AMT - NVL(B.GAIN_LOSS_COST_AMT, 0) --201801417
            END N_COST_AMT_PREV
    FROM IFRS_LBM_ACCT_EIR_PAYM A
    JOIN IFRS_LBM_ACCT_EIR_CF_ECF B ON B.MASTERID = A.MASTERID
    JOIN IFRS_LBM_ACCT_EIR_GS_RESULT C ON C.MASTERID = A.MASTERID
        AND C.DOWNLOAD_DATE = V_CURRDATE
    WHERE A.PMT_DATE = A.PREV_PMT_DATE;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','1');

    COMMIT;

    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF (
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
        )
    SELECT /*+ PARALLEL(8) */ MASTERID
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
        ,PMT_DATE - PREV_PMT_DATE AS I_DAYS2
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
    FROM IFRS_LBM_ACCT_EIR_ECF2;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','2');

    -- PREPARE TEMP TABLE FOR LOOPING
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T9';

    INSERT /*+ PARALLEL(8) */ INTO TMP_T9 (
        MASTERID
        ,PMTDATE
        )
    SELECT /*+ PARALLEL(8) */ MASTERID
        ,PMT_DATE
    FROM IFRS_LBM_ACCT_EIR_PAYM
    WHERE PMT_DATE = PREV_PMT_DATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_ECF_T2';

    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF_T2 (
        MASTERID
        ,PMTDATE
        )
    SELECT /*+ PARALLEL(8) */ A.MASTERID
        ,MIN(A.PMT_DATE) AS PMTDATE
    FROM IFRS_LBM_ACCT_EIR_PAYM A
    JOIN TMP_T9 B ON B.MASTERID = A.MASTERID
        AND A.PMT_DATE > B.PMTDATE
    GROUP BY A.MASTERID;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','3');

    COMMIT;

    --EXECUTE IMMEDIATE 'DROP INDEX PSAK_EIR_PAYM_IDX2';
    --EXECUTE IMMEDIATE 'CREATE INDEX PSAK_EIR_PAYM_IDX2 ON IFRS_LBM_ACCT_EIR_PAYM(MASTERID,PMT_DATE)';
    BEGIN
      SELECT COUNT(*) INTO V_VI
      FROM IFRS_LBM_ACCT_EIR_ECF_T2;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_VI:=0;
    END;

    WHILE V_VI > 0
    LOOP
        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','4');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_ECF1';

        -- PREPARE INDEX
        --EXECUTE IMMEDIATE 'DROP INDEX PSAK_EIR_ECF_T2_IDX1';
        --EXECUTE IMMEDIATE 'CREATE INDEX PSAK_EIR_ECF_T2_IDX1 ON IFRS_LBM_ACCT_EIR_ECF_T2(MASTERID,PMTDATE)';
        --EXECUTE IMMEDIATE 'DROP INDEX PSAK_EIR_ECF2_IDX1';
        --EXECUTE IMMEDIATE 'CREATE INDEX PSAK_EIR_ECF2_IDX1 ON IFRS_LBM_ACCT_EIR_ECF2(MASTERID)';
        INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF1 (
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
            )
        SELECT /*+ PARALLEL(8) */ A.MASTERID
            ,V_CURRDATE
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
            ,
            /*  BCA DISABLE BPI
                                        CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                        THEN
                                                C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                                C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                            CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                            THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                                A.PMT_DATE))
                                        ELSE
                                        */
            ROUND(CASE
                    --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                    WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                    ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, V_ROUND)
            /*  BCA DISABLE BPI END */
            AS N_EFF_INT_AMT
            ,C.N_FAIRVALUE - A.N_PRN_PAYMENT +
            /*  BCA DISABLE BPI
                                CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                ELSE
                                */
            ROUND(CASE
                    --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                    WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                    ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, V_ROUND)
            /*  BCA DISABLE BPI END */
            - A.N_INT_PAYMENT + COALESCE(A.DISB_AMOUNT, 0) AS N_FAIRVALUE
            ,C.N_UNAMORT_AMT N_UNAMORT_AMT_PREV
            ,
            /*  BCA DISABLE BPI
                                CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                ELSE
                                */
            ROUND(CASE
                    --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                    WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                    ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, V_ROUND)
            /*  BCA DISABLE BPI END */
            - A.N_INT_PAYMENT AS N_AMORT_AMT
            ,C.N_UNAMORT_AMT +
            /*  BCA DISABLE BPI
                                CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                ELSE
                                */
            ROUND(CASE
                    --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                            --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                    WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                    WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                    ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                    END, V_ROUND)
            /*  BCA DISABLE BPI END */
            - A.N_INT_PAYMENT AS N_UNAMORT_AMT
            ,C.N_COST_UNAMORT_AMT N_COST_UNAMORT_AMT_PREV
            ,CASE
                WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                ELSE (
                        /*  BCA DISABLE BPI
                                     CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                     THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                      ELSE
                                      */
                        ROUND(CASE
                                --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                        --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                                ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                                END, V_ROUND)
                        /*  BCA DISABLE BPI END */
                        - A.N_INT_PAYMENT
                        ) * C.N_COST_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                END AS N_COST_AMORT_AMT
            ,C.N_COST_UNAMORT_AMT + CASE
                WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                ELSE (
                        /*  BCA DISABLE BPI
                                       CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                        THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                        ELSE
                                        */
                        ROUND(CASE
                                --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                        --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                                ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                                END, V_ROUND)
                        /*  BCA DISABLE BPI END */
                        - A.N_INT_PAYMENT
                        ) * C.N_COST_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                END AS N_COST_UNAMORT_AMT
            ,C.N_FEE_UNAMORT_AMT N_FEE_UNAMORT_AMT_PREV
            ,CASE
                WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                ELSE (
                        /*  BCA DISABLE BPI
                                     CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                      THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                        ELSE
                                        */
                        ROUND(CASE
                                --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                        --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                                ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                                END, V_ROUND)
                        /*  BCA DISABLE BPI END */
                        - A.N_INT_PAYMENT
                        ) * C.N_FEE_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                END AS N_FEE_AMORT_AMT
            ,C.N_FEE_UNAMORT_AMT + CASE
                WHEN C.N_FEE_AMT + C.N_COST_AMT = 0
                    THEN 0
                ELSE (
                        /*  BCA DISABLE BPI
                                       CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                        THEN
                                        C.N_EFF_INT_RATE/100*(DATEDIFF(DAY,A.STARTAMORTDATE,A.PMT_DATE))*
                                        C.N_FAIRVALUE/12/(DATEDIFF(DAY,
                                                    CASE WHEN A.PMT_DATE = EOMONTH(A.PMT_DATE)
                                                    THEN EOMONTH(DATEADD(MONTH,-1,A.PMT_DATE)) ELSE DATEADD(MONTH,-1,A.PMT_DATE) END,
                                        A.PMT_DATE))
                                        ELSE
                                        */
                        ROUND(CASE
                                --WHEN A.INTCALCCODE IN('2','6') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('1','6') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                        --WHEN A.INTCALCCODE='3' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428
                                WHEN A.INTCALCCODE IN ('2','3') THEN CAST(A.I_DAYS AS BINARY_DOUBLE) / CAST(365 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                                ELSE (CAST(30 AS BINARY_DOUBLE) * A.M / CAST(360 AS BINARY_DOUBLE) * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE)
                                END, V_ROUND)
                        /*  BCA DISABLE BPI END */
                        - A.N_INT_PAYMENT
                        ) * C.N_FEE_AMT / (C.N_FEE_AMT + C.N_COST_AMT)
                END AS N_FEE_UNAMORT_AMT
            ,C.N_FEE_AMT
            ,C.N_COST_AMT
        FROM IFRS_LBM_ACCT_EIR_PAYM A
        JOIN IFRS_LBM_ACCT_EIR_ECF_T2 B ON B.MASTERID = A.MASTERID
            AND B.PMTDATE = A.PMT_DATE
        JOIN IFRS_LBM_ACCT_EIR_ECF2 C ON C.MASTERID = B.MASTERID;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','5');

        -- INSERT TO ECF
        INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF (
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
            )
        SELECT /*+ PARALLEL(8) */ MASTERID
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
            ,PMT_DATE - PREV_PMT_DATE AS I_DAYS2
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
        FROM IFRS_LBM_ACCT_EIR_ECF1;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','6');

        COMMIT;

        -- INSERT TO ECF2
        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_ECF2';

        INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF2 (
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
            )
        SELECT /*+ PARALLEL(8) */ MASTERID
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
        FROM IFRS_LBM_ACCT_EIR_ECF1;COMMIT;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','7');

        -- NEXT CYCLE PREPARE #T2
        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_ECF_T2';

        INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_ACCT_EIR_ECF_T2 (
            MASTERID
            ,PMTDATE
            )
        SELECT /*+ PARALLEL(8) */ A.MASTERID
            ,MIN(A.PMT_DATE) AS PMTDATE
        FROM IFRS_LBM_ACCT_EIR_PAYM A
        JOIN IFRS_LBM_ACCT_EIR_ECF1 B ON B.MASTERID = A.MASTERID
            AND A.PMT_DATE > B.PMT_DATE
        GROUP BY A.MASTERID;COMMIT;

        -- ASSIGN VAR @I
        BEGIN
          SELECT COUNT(*) INTO V_VI
          FROM IFRS_LBM_ACCT_EIR_ECF_T2;
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_VI:=0;
        END;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES( V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','8');
    END LOOP;
    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES( V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_LBM_ACCT_EIR_GS_ECF_INSERT','');
END;