CREATE OR REPLACE PROCEDURE SP_IFRS_LI_PAYM_SCHD_MTM
AS
  /*
    - RUNNING ONLY ENDOFMONTH
    - MARKET RATE > 0
    - DATA SOURCE LOAN
    - PAYMENT SCHEDULE TYPE CUTOFF

  COMPONENT TYPE :
  0 : FIX PRINCIPAL AMOUNT
  1 : FIX INTEREST AMOUNT
  2 : FIX INSTALMENT AMOUNT
  3 : FIX INTEREST PERCENTAGE
  4 : FIX INSTALMENT AMOUNT FOR COMPONENT TYPE 3 AND 5
  5 : STEP UP DISBURSMENT
  */
  --VARIABLE
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  V_COUNTER_PAY NUMBER(10);
  V_MAX_COUNTERPAY NUMBER(10);
  V_NEXT_COUNTER_PAY NUMBER(10);
  V_ENDOFMONTH DATE;
	--CONSTANT
  V_CUT_OFF_DATE DATE;
  V_ROUND NUMBER(10);
  V_FUNCROUND NUMBER(10);
  V_LOG_ID NUMBER(10);
  V_PARAM_CALC_TO_LASTPAYMENT NUMBER(10);
BEGIN
    ---ADD YAHYA IF 0 CURRDATE 1 LAST CYCLEDATE

    V_CUT_OFF_DATE := '1 JAN 2016';
    V_MAX_COUNTERPAY := 0;
    V_COUNTER_PAY := 0;
    V_NEXT_COUNTER_PAY := 1;
    V_ROUND := 6; --DEFAULT
    V_FUNCROUND := 1; --DEFAULT
    V_LOG_ID := 912;
    V_PARAM_CALC_TO_LASTPAYMENT := 0; ---ADD YAHYA

    SELECT CURRDATE, PREVDATE, LAST_DAY(CURRDATE) INTO V_CURRDATE, V_PREVDATE, V_ENDOFMONTH
    FROM IFRS_LI_PRC_DATE_AMORT;

    IF (V_CURRDATE = V_ENDOFMONTH)
    THEN
      BEGIN
        SELECT CAST(VALUE1 AS NUMBER(10))
          , CAST(VALUE2 AS NUMBER(10))
        INTO V_ROUND, V_FUNCROUND
        FROM TBLM_COMMONCODEDETAIL
        WHERE COMMONCODE = 'SCM003';
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_ROUND:=6;
        V_FUNCROUND:=1;
      END;
    END IF;

    DELETE FROM IFRS_LI_BATCH_LOG_DETAILS
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND BATCH_ID_HEADER = V_LOG_ID
      AND BATCH_NAME = 'PMTSCHD';

    --TRACKING--
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,0,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',0,'JUST STARTED');

    COMMIT;

    ----COMMIT;
    --IF V_CURRDATE < @CUT_OFF_DATE RETURN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_MAIN_MTM';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LI_PAYM_SCHD_MTM';

    COMMIT;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',1,'INSERT TMP_LI_SCHEDULE_MAIN_MTM');

    ----COMMIT;
    INSERT INTO TMP_LI_SCHEDULE_MAIN_MTM (
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
      ) --BIBD GRACE PERIOD
    /*  BCA DISABLE BPI ,SPECIAL_FLAG */
    SELECT PMA.DOWNLOAD_DATE ,
              PMA.MASTERID ,
              PMA.ACCOUNT_NUMBER ,
              PMA.BRANCH_CODE ,
              PMA.PRODUCT_CODE ,
              PMA.LOAN_START_DATE ,
              PMA.LOAN_DUE_DATE ,
              CASE WHEN V_PARAM_CALC_TO_LASTPAYMENT = 0 THEN V_CURRDATE
                  ELSE CASE WHEN NVL(PMA.LAST_PAYMENT_DATE,PMA.LOAN_START_DATE) <= PMA.LOAN_START_DATE THEN PMA.LOAN_START_DATE
                            ELSE CASE WHEN PMA.LAST_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE) THEN FN_PMTDATE(NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),-1) ELSE PMA.LAST_PAYMENT_DATE END
                       END
              END START_AMORTIZATION_DATE,
              PMA.LOAN_DUE_DATE ,
              CASE WHEN PMA.NEXT_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE) OR TO_CHAR(PMA.NEXT_PAYMENT_DATE,'YYYYMM') = TO_CHAR(NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),'YYYYMM')
                      THEN NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)
                   ELSE PMA.NEXT_PAYMENT_DATE
              END AS FIRST_PMT_DATE ,
              PMA.CURRENCY ,
              PMA.OUTSTANDING ,
              PMA.PLAFOND ,
              PMA.INTEREST_RATE ,
              CASE WHEN NVL(PMA.TENOR, 0) > MONTHS_BETWEEN(PMA.LOAN_DUE_DATE,PMA.LOAN_START_DATE) THEN NVL(PMA.TENOR, 0)
                   ELSE MONTHS_BETWEEN(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE)
              END AS TENOR ,
              PMA.PAYMENT_TERM ,
              PMA.PAYMENT_CODE ,
              PMA.INTEREST_CALCULATION_CODE ,
              CASE WHEN PMA.NEXT_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE) OR TO_CHAR(PMA.NEXT_PAYMENT_DATE,'YYYYMM') = TO_CHAR(NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),'YYYYMM')
                      THEN NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)
                   ELSE PMA.NEXT_PAYMENT_DATE
              END,
              0 ,
              'N' ,
              PMA.INSTALLMENT_GRACE_PERIOD AS GRACE_DATE
      /*  BCA DISABLE BPI  ,CASE WHEN PMA.NEXT_PAYMENT_DATE = PMA.FIRST_INSTALLMENT_DATE AND PMA.SPECIAL_FLAG = 1 THEN 1 ELSE 0 END --- BPI FLAG ONLY CTBC */
    FROM IFRS_LI_MASTER_ACCOUNT PMA
    INNER JOIN IFRS_LI_IMA_AMORT_CURR PMC ON PMA.MASTERID = PMC.MASTERID
      AND PMA.DOWNLOAD_DATE = PMC.DOWNLOAD_DATE
    WHERE PMA.DOWNLOAD_DATE = V_CURRDATE
      AND PMA.IFRS9_CLASS IN ('FVTPL','FVOCI')
      AND PMA.DATA_SOURCE = 'LOAN'
      AND PMA.MARKET_RATE > 0
      AND PMA.ACCOUNT_STATUS = 'A'
      AND PMA.IAS_CLASS = 'A' -----ADD YAHYA
      AND PMA.LOAN_DUE_DATE > V_CURRDATE;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',3,'INITIAL PROCESS');

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_PY0';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_PY1';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_PY2';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_PY3';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_PY4';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_PY5';

    COMMIT;

    INSERT INTO TMP_LI_PY0
    SELECT * FROM IFRS_LI_MASTER_PAYMENT_SETTING PY0
    WHERE PY0.COMPONENT_TYPE = '0'
      AND PY0.DOWNLOAD_DATE = V_CURRDATE
      AND PY0.FREQUENCY IN ('M','N','D');

    INSERT INTO TMP_LI_PY1
    SELECT *
    FROM IFRS_LI_MASTER_PAYMENT_SETTING PY1
    WHERE PY1.COMPONENT_TYPE = '1'
      AND PY1.DOWNLOAD_DATE = V_CURRDATE
      AND PY1.FREQUENCY IN ('M','N','D');

    INSERT INTO TMP_LI_PY2
    SELECT * FROM IFRS_LI_MASTER_PAYMENT_SETTING PY2
    WHERE PY2.COMPONENT_TYPE = '2'
      AND PY2.DOWNLOAD_DATE = V_CURRDATE
      AND PY2.FREQUENCY IN ('M','N','D');

    INSERT INTO TMP_LI_PY3
    SELECT * FROM IFRS_LI_MASTER_PAYMENT_SETTING PY3
    WHERE PY3.COMPONENT_TYPE = '3'
      AND PY3.DOWNLOAD_DATE = V_CURRDATE
      AND PY3.FREQUENCY IN ('M','N','D');

    INSERT INTO TMP_LI_PY4
    SELECT * FROM IFRS_LI_MASTER_PAYMENT_SETTING PY4
    WHERE PY4.COMPONENT_TYPE = '4'
      AND PY4.DOWNLOAD_DATE = V_CURRDATE
      AND PY4.FREQUENCY IN ('M','N','D');

    INSERT INTO TMP_LI_PY5
    SELECT * FROM IFRS_LI_MASTER_PAYMENT_SETTING PY5
    WHERE PY5.COMPONENT_TYPE = '5'
      AND PY5.DOWNLOAD_DATE = V_CURRDATE
      AND PY5.FREQUENCY IN ('M','N','D');

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_CURR_MTM_HIST';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_PREV_MTM_HIST';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_CURR';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_PREV';

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',4,'INSERT TMP_LI_SCHEDULE_CURR');

    COMMIT;


    INSERT INTO TMP_LI_SCHEDULE_CURR (
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
      ) --BIBD FOR GRACE PERIOD
    /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    SELECT A.MASTERID
      ,A.ACCOUNT_NUMBER
      ,A.INTEREST_RATE
      ,A.START_AMORTIZATION_DATE
      ,A.OUTSTANDING
      ,0 AS PRINCIPAL
      ,0 AS INTEREST
      ,NVL(PY5.AMOUNT, 0) AS DISB_PERCENTAGE
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
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    FROM TMP_LI_SCHEDULE_MAIN_MTM A
    LEFT JOIN TMP_LI_PY5 PY5 ON A.MASTERID = PY5.MASTERID
      AND A.DOWNLOAD_DATE BETWEEN PY5.DATE_START
      AND PY5.DATE_END
      AND MOD(MONTHS_BETWEEN(PY5.DATE_START,A.DOWNLOAD_DATE), PY5.INCREMENTS) = 0;

    COMMIT;
    INSERT INTO TMP_LI_SCHEDULE_CURR_MTM_HIST
    SELECT * FROM TMP_LI_SCHEDULE_CURR;

    COMMIT;
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',5,'INSERT IFRS_LI_PAYM_SCHD');

    COMMIT;
    INSERT INTO IFRS_LI_PAYM_SCHD_MTM (
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
      ,ICC
      ,COUNTER
      ,DOWNLOAD_DATE
      ,SCH_FLAG
      ,GRACE_DATE
      ) --BIBD FOR GRACE
    /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    SELECT MASTERID
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
      ,ICC
      ,COUNTER
      ,V_CURRDATE
      ,SCH_FLAG
      ,GRACE_DATE --BIBD FOR GRACE
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    FROM TMP_LI_SCHEDULE_CURR;

    SELECT MAX(TENOR) INTO V_MAX_COUNTERPAY
    FROM TMP_LI_SCHEDULE_MAIN_MTM;

    WHILE (V_COUNTER_PAY <= V_MAX_COUNTERPAY)
    LOOP
      --- START ADD YAHYA--
      DELETE FROM TMP_LI_MIN_MAX_DATE;

      INSERT INTO TMP_LI_MIN_MAX_DATE
      SELECT A.ACCOUNT_NUMBER
      ,MIN(A.DATE_START) AS MIN_DATE -- INTO #TMP_LI_MIN_MAX_DATE
      FROM IFRS_LI_MASTER_PAYMENT_SETTING A
      INNER JOIN TMP_LI_SCHEDULE_CURR B ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
      AND A.DATE_START > B.NEXT_PMTDATE
      GROUP BY A.ACCOUNT_NUMBER;

      --- END ADD YAHYA--
      V_COUNTER_PAY := V_COUNTER_PAY + 1;
      V_NEXT_COUNTER_PAY := V_NEXT_COUNTER_PAY + 1;

      INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
      VALUES (V_CURRDATE,2,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',V_COUNTER_PAY,'PAYMENT SCHEDULE LOOPING');

      COMMIT;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_PREV';

      INSERT INTO TMP_LI_SCHEDULE_PREV (
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
      ) --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      SELECT  A.MASTERID ,
                A.ACCOUNT_NUMBER ,
                NVL(PY3.AMOUNT, A.INTEREST_RATE) AS INTEREST_RATE ,
                A.NEXT_PMTDATE AS NEW_PMTDATE ,
                ROUND(( CASE WHEN PY5.COMPONENT_TYPE = '5' THEN A.OSPRN + ( PY5.AMOUNT / 100 * A.PLAFOND )
                             ELSE A.OSPRN
                        END
                        - ( ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL
                                          THEN 0--bibd for grace period
                                         ELSE CASE WHEN A.NEXT_PMTDATE >= A.DATE_END THEN A.OSPRN
                                                   ELSE CASE WHEN PY0.COMPONENT_TYPE = 0 THEN CASE WHEN A.OSPRN <= PY0.AMOUNT THEN A.OSPRN ELSE PY0.AMOUNT END--FIX PRINCIPAL
                                                             WHEN PY2.COMPONENT_TYPE = 2 THEN --INSTALMENT
                                                                                         CASE WHEN A.OSPRN <= PY2.AMOUNT THEN A.OSPRN ELSE PY2.AMOUNT
                                                                                                   - ( ROUND(( CASE WHEN PY1.COMPONENT_TYPE = '1' THEN PY1.AMOUNT--FIX INTEREST
                                                                                                                    ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                                                                              WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                                                                              WHEN A.ICC = '6' THEN A.INTEREST_RATE/ 100 * A.OSPRN* NVL(PY1.INCREMENTS,PY2.INCREMENTS)* 30 / 360 --30 / 360
                                                                                                                              ELSE 0
                                                                                                                          END
                                                                                                               END ), V_ROUND) )
                                                                                         END
                                                              WHEN PY4.COMPONENT_TYPE = 4 THEN --INSTALMENT
                                                                                          CASE WHEN A.OSPRN <= PY4.AMOUNT THEN A.OSPRN
                                                                                               ELSE PY4.AMOUNT - ( ROUND(( CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                                                                                WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365 --ACTUAL/365
                                                                                                                                WHEN A.ICC = '6' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* NVL(PY3.INCREMENTS,PY4.INCREMENTS)* 30 / 360 --ACTUAL/365
                                                                                                                                ELSE 0
                                                                                                                           END ), V_ROUND) )
                                                                                          END
                                                              ELSE 0
                                                          END
                                              END
                                    END ), V_ROUND) ) ),
                V_ROUND) AS NEW_OSPRN ,
                ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0--bibd for grace period
                             ELSE CASE WHEN A.NEXT_PMTDATE >= A.DATE_END THEN A.OSPRN
                                       ELSE CASE WHEN PY0.COMPONENT_TYPE = 0 THEN --FIX PRINCIPAL
                                                    CASE WHEN A.OSPRN <= PY0.AMOUNT THEN A.OSPRN ELSE PY0.AMOUNT END
                                                         WHEN PY2.COMPONENT_TYPE = 2 THEN --INSTALMENT
                                                            CASE WHEN A.OSPRN <= PY2.AMOUNT THEN A.OSPRN ELSE PY2.AMOUNT
                                                            - ( ROUND(( CASE WHEN PY1.COMPONENT_TYPE = '1' THEN PY1.AMOUNT --FIX INTEREST
                                                                             ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360--ACTUAL/360
                                                                                       WHEN A.ICC = '2' THEN A.INTEREST_RATE / 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                                       WHEN A.ICC = '6' THEN A.INTEREST_RATE/ 100 * A.OSPRN* NVL(PY1.INCREMENTS,PY2.INCREMENTS)* 30 / 360--30/360
                                                                                       ELSE 0
                                                                                  END
                                                                        END ), V_ROUND))
                                                            END
                                                         WHEN PY4.COMPONENT_TYPE = 4 THEN --INSTALMENT
                                                              CASE WHEN A.OSPRN <= PY4.AMOUNT THEN A.OSPRN ELSE PY4.AMOUNT
                                                              - ( ROUND(( CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                               WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                               WHEN A.ICC = '6' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* NVL(PY3.INCREMENTS,PY4.INCREMENTS)* 30 / 360--30/360
                                                                               ELSE 0
                                                                          END ), V_ROUND) )
                                                              END
                                                         ELSE 0
                                                    END
                                             END
                                  END ), V_ROUND) AS NEW_PRINCIPAL ,
                ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0 --bibd for grace period
                             ELSE-- add yahya to calculate BPI Flag only ctbc
                                  CASE WHEN PY1.COMPONENT_TYPE = '1'
                                       THEN --FIX INTEREST
                                        CASE WHEN PY1.AMOUNT = 0 THEN CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                           WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                           WHEN A.ICC = '6'
                                                                            THEN --30/360
                                                                            -- add yahya to calculate interest if migration in cutoff
                                                                            CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1 ) THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360
                                                                                 ELSE A.INTEREST_RATE/ 100 * A.OSPRN* PY1.INCREMENTS* 30 / 360
                                                                            END
                                                                            ----end add yahya
                                                                          ELSE 0
                                                                     END
                                             ELSE PY1.AMOUNT
                                        END
												          WHEN PY3.COMPONENT_TYPE = '3' THEN CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                          WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                          WHEN A.ICC = '6'
                                                                            THEN --30/360
                                                                            -- add yahya to calculate interest if migration in cutoff
                                                                            CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1 )THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360
                                                                                 ELSE NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* PY3.INCREMENTS* 30 / 360
                                                                            END
                                                                            ----end add yahya
                                                                          ELSE 0
                                                                     END
												          ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                            WHEN A.ICC = '2'THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                            WHEN A.ICC = '6'
                                              THEN --30/360
                                              -- add yahya to calculate interest if migration in cutoff
                                              CASE WHEN V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1  THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360
                                                   ELSE A.INTEREST_RATE/ 100 * A.OSPRN* NVL(PY2.INCREMENTS,1) * 30 / 360
                                              END
                                              ----end add yahya
                                            ELSE 0
                                       END
                          END
                END ), V_ROUND) AS NEW_INTEREST ,
                NVL(PY5.AMOUNT, 0) AS DISB_PERCENTAGE ,
                NVL(PY5.AMOUNT, 0) / 100 * A.PLAFOND AS DISB_AMOUNT ,
                A.PLAFOND ,
                CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0
                     ELSE CASE WHEN A.ICC IN ('1','2') THEN CASE WHEN PY1.COMPONENT_TYPE = '1' THEN A.NEXT_PMTDATE -A.PMTDATE
                                                                 WHEN PY2.COMPONENT_TYPE = '2' THEN A.NEXT_PMTDATE -A.PMTDATE
                                                                 WHEN PY3.COMPONENT_TYPE = '3' THEN A.NEXT_PMTDATE -A.PMTDATE
                                                                 ELSE 0
                                                            END
                               WHEN A.ICC = '6'
                                 THEN
                                 ---- add yahya to calculate I_DAYS if migration in cutoff
                                 CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1 )
                                      THEN CASE WHEN PY1.COMPONENT_TYPE = '1' THEN A.NEXT_PMTDATE -A.PMTDATE
                                                WHEN PY2.COMPONENT_TYPE = '2' THEN A.NEXT_PMTDATE -A.PMTDATE
                                                WHEN PY3.COMPONENT_TYPE = '3' THEN A.NEXT_PMTDATE -A.PMTDATE
                                                ELSE 0
                                           END
                                      ELSE CASE WHEN PY1.COMPONENT_TYPE = '1' THEN NVL(PY1.INCREMENTS,1) * 30
                                                WHEN PY2.COMPONENT_TYPE = '2' THEN NVL(PY2.INCREMENTS,1) * 30
                                                WHEN PY3.COMPONENT_TYPE = '3' THEN NVL(PY3.INCREMENTS,1) * 30
                                                ELSE 0
                                           END
                                 END
                                --------- end add yahya
                               ELSE 0 -- NOT IN 1,2,6
                          END
                END AS I_DAYS ,
                V_COUNTER_PAY AS COUNTER ,
                CASE WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END THEN B.MIN_DATE
                     WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END THEN B.MIN_DATE
                     WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END THEN B.MIN_DATE
                     WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END THEN B.MIN_DATE
                     WHEN PY5.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY5.DATE_END THEN B.MIN_DATE
                     ELSE A.DATE_START
                END DATE_START ,
                A.DATE_END ,
                A.TENOR ,
                A.PAYMENT_CODE ,
                A.ICC ,
                CASE WHEN PY1.COMPONENT_TYPE = '1' THEN CASE WHEN PY1.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY1.INCREMENTS* A.NEXT_COUNTER_PAY)))
                                                             ELSE CASE
                                                                  ---START ADD YAHYA ---
                                                                  WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END THEN B.MIN_DATE --- add yahya
                                                                  WHEN FN_ISDATE(TO_CHAR (FN_PMTDATE(A.DATE_START, (PY1.INCREMENTS * A.NEXT_COUNTER_PAY )),'YYYYMM') || PY1.PMT_DATE,'YYYYMMDD') = 1
                                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY1.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY1.PMT_DATE,'YYYYMMDD')
                                                                  ELSE FN_PMTDATE (A.DATE_START,(PY1.INCREMENTS * A.NEXT_COUNTER_PAY))
                                                                  ---END ADD YAHYA----
                                                                  END
                                                        END
                     WHEN PY2.COMPONENT_TYPE = '2' THEN CASE WHEN PY2.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY2.INCREMENTS* A.NEXT_COUNTER_PAY)))
                                                             ELSE CASE
                                                                  ---START ADD YAHYA ---
                                                                  WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END THEN B.MIN_DATE
                                                                  WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY2.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY2.PMT_DATE,'YYYYMMDD') = 1
                                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY2.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY2.PMT_DATE,'YYYYMMDD')
                                                                  ELSE FN_PMTDATE(A.DATE_START,(PY2.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                                  ---END ADD YAHYA----
                                                                  END
                                                        END
                     WHEN PY3.COMPONENT_TYPE = '3' THEN CASE WHEN PY3.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY3.INCREMENTS* A.NEXT_COUNTER_PAY)))
                                                             ELSE CASE
                                                                   ---START ADD YAHYA ---
                                                                   WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END THEN B.MIN_DATE
                                                                   WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY3.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY3.PMT_DATE,'YYYYMMDD') = 1
                                                                     THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY3.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY3.PMT_DATE,'YYYYMMDD')
                                                                   ELSE FN_PMTDATE(A.DATE_START,(PY3.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                                   ---END ADD YAHYA----
                                                                  END
                                                        END
                     WHEN PY4.COMPONENT_TYPE = '4' THEN CASE WHEN PY4.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY4.INCREMENTS* A.NEXT_COUNTER_PAY)))
                                                             ELSE CASE
                                                                  ---START ADD YAHYA ---
                                                                  WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END THEN B.MIN_DATE
                                                                  WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY4.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY4.PMT_DATE,'YYYYMMDD') = 1
                                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY4.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY4.PMT_DATE,'YYYYMMDD')
                                                                  ELSE FN_PMTDATE(A.DATE_START,( PY4.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                                  ---END ADD YAHYA----
                                                                  END
                                                        END
                     WHEN PY0.COMPONENT_TYPE = '0' THEN CASE WHEN PY0.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY0.INCREMENTS* A.NEXT_COUNTER_PAY)))
                                                             ELSE CASE
                                                                  ---START ADD YAHYA ---
                                                                  WHEN PY0.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY0.DATE_END THEN B.MIN_DATE
                                                                  WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY0.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY0.PMT_DATE,'YYYYMMDD') = 1
                                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY0.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY0.PMT_DATE,'YYYYMMDD')
                                                                  ELSE FN_PMTDATE(A.DATE_START,(PY0.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                                  ---END ADD YAHYA----
                                                                  END
                                                       END
                     ELSE A.DATE_END ----ADD YAHYA
                END ,
                CASE WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END THEN 0
                     WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END THEN 0
                     WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END THEN 0
                     WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END THEN 0
                     WHEN PY5.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY5.DATE_END THEN 0
                     ELSE A.NEXT_COUNTER_PAY
                END NEXT_COUNTER_PAY,
                A.SCH_FLAG ,
                A.GRACE_DATE --bibd for grace period
      /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      FROM TMP_LI_SCHEDULE_CURR A
      LEFT JOIN TMP_LI_MIN_MAX_DATE B ON A.MASTERID = B.ACCOUNT_NUMBER ---ADD YAHYA
      LEFT JOIN TMP_LI_PY0 PY0 ON A.MASTERID = PY0.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY0.DATE_START
        AND PY0.DATE_END
      AND MOD(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY0.DATE_START), PY0.INCREMENTS) = 0
      LEFT JOIN TMP_LI_PY1 PY1 ON A.MASTERID = PY1.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY1.DATE_START
        AND PY1.DATE_END
      AND MOD(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY1.DATE_START), PY1.INCREMENTS) = 0
      LEFT JOIN TMP_LI_PY2 PY2 ON A.MASTERID = PY2.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY2.DATE_START
        AND PY2.DATE_END
      AND MOD(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY2.DATE_START), PY2.INCREMENTS) = 0
      LEFT JOIN TMP_LI_PY3 PY3 ON A.MASTERID = PY3.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY3.DATE_START
        AND PY3.DATE_END
      AND MOD(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY3.DATE_START), PY3.INCREMENTS) = 0
      LEFT JOIN TMP_LI_PY4 PY4 ON A.MASTERID = PY4.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY4.DATE_START
        AND PY4.DATE_END
      AND MOD(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY4.DATE_START), PY4.INCREMENTS) = 0
      LEFT JOIN TMP_LI_PY5 PY5 ON A.MASTERID = PY5.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY5.DATE_START
        AND PY5.DATE_END
      AND MOD(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY5.DATE_START), PY5.INCREMENTS) = 0
      WHERE A.TENOR >= V_COUNTER_PAY
      AND A.PMTDATE <= A.DATE_END
      AND A.OSPRN > 0;

      COMMIT;

      INSERT INTO TMP_LI_SCHEDULE_PREV_MTM_HIST
      SELECT *  FROM TMP_LI_SCHEDULE_PREV;

      COMMIT;
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_CURR';

      INSERT INTO TMP_LI_SCHEDULE_CURR (
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
      ) --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      SELECT A.MASTERID
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
      ,CASE WHEN A.NEXT_PMTDATE > A.DATE_END THEN A.DATE_END ------ADD YAHYA 20180312
            WHEN LAST_DAY(A.NEXT_PMTDATE) = LAST_DAY(A.DATE_END) THEN A.DATE_END
            ELSE A.NEXT_PMTDATE
        END
      ,A.NEXT_COUNTER_PAY + 1
      ,A.SCH_FLAG
      ,A.GRACE_DATE --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      FROM TMP_LI_SCHEDULE_PREV A;

      COMMIT;

      INSERT INTO TMP_LI_SCHEDULE_CURR_MTM_HIST
      SELECT * FROM TMP_LI_SCHEDULE_CURR;

      COMMIT;

      --COMMIT;
      INSERT INTO IFRS_LI_PAYM_SCHD_MTM (
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
      ,ICC
      ,COUNTER
      ,DOWNLOAD_DATE
      ,SCH_FLAG
      ,GRACE_DATE --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      )
      SELECT MASTERID
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
      ,ICC
      ,COUNTER
      ,V_CURRDATE
      ,SCH_FLAG
      ,GRACE_DATE --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      FROM TMP_LI_SCHEDULE_CURR;
    END LOOP;

    COMMIT;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,3,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',1,'PAYMENT SCHEDULE EXCEPTIONS');

    COMMIT;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCH_MAX';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHD';

    INSERT INTO TMP_LI_SCH_MAX
    SELECT MASTERID
      ,MAX(PMTDATE) AS MAX_PMTDATE
    FROM IFRS_LI_PAYM_SCHD_MTM
    GROUP BY MASTERID;

    INSERT INTO TMP_LI_SCHD
    SELECT A.MASTERID
      ,A.OSPRN
    FROM IFRS_LI_PAYM_SCHD_MTM A
    INNER JOIN TMP_LI_SCH_MAX B ON A.MASTERID = B.MASTERID
      AND A.PMTDATE = B.MAX_PMTDATE;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,3,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',2,'INSERT IFRS_LI_EXCEPTION_DETAILS');

    COMMIT;
    DELETE IFRS_LI_EXCEPTION_DETAILS
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND EXCEPTION_CODE = 'V-2';

    COMMIT;
    INSERT INTO IFRS_LI_EXCEPTION_DETAILS (
      DOWNLOAD_DATE
      ,DATA_SOURCE
      ,PRD_CODE
      ,ACCOUNT_NUMBER
      ,MASTERID
      ,PROCESS_ID
      ,EXCEPTION_CODE
      ,REMARKS
      )
    SELECT PMA.DOWNLOAD_DATE
      ,PMA.DATA_SOURCE
      ,PMA.PRODUCT_CODE
      ,PMA.ACCOUNT_NUMBER
      ,PMA.MASTERID
      ,'IFRS EXCEPTIONS' AS PROCESS_ID
      ,'V-2' AS EXCEPTION_CODE
      ,'SCHEDULE : LAST OSPRN SCHEDULE <> 0 ' AS REMARKS
    FROM IFRS_LI_MASTER_ACCOUNT PMA
    INNER JOIN TMP_LI_SCHD SCH ON PMA.MASTERID = SCH.MASTERID
      AND PMA.DOWNLOAD_DATE = V_CURRDATE
      --AND PMA.PMT_SCH_STATUS = 'Y'
      AND NVL(SCH.OSPRN, 0) <> 0;

    COMMIT;
    -- EXCEPTIONS IF THERE IS PMTDATE IS NULL
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHD';

    INSERT INTO TMP_LI_SCHD (MASTERID)
    SELECT MASTERID FROM IFRS_LI_PAYM_SCHD_MTM
    WHERE PMTDATE IS NULL;

    INSERT INTO IFRS_LI_EXCEPTION_DETAILS (
      DOWNLOAD_DATE
      ,DATA_SOURCE
      ,PRD_CODE
      ,ACCOUNT_NUMBER
      ,MASTERID
      ,PROCESS_ID
      ,EXCEPTION_CODE
      ,REMARKS
      )
    SELECT PMA.DOWNLOAD_DATE
      ,PMA.DATA_SOURCE
      ,PMA.PRODUCT_CODE
      ,PMA.ACCOUNT_NUMBER
      ,PMA.MASTERID
      ,'IFRS EXCEPTIONS' AS PROCESS_ID
      ,'V-2' AS EXCEPTION_CODE
      ,'PMTDATE : IS NULL' AS REMARKS
    FROM IFRS_LI_MASTER_ACCOUNT PMA
    INNER JOIN TMP_LI_SCHD SCH ON PMA.MASTERID = SCH.MASTERID
      AND PMA.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHD';

    INSERT INTO TMP_LI_SCHD (MASTERID)
    SELECT DISTINCT MASTERID
    FROM (SELECT MASTERID,PMTDATE
          FROM IFRS_LI_PAYM_SCHD_MTM
          GROUP BY MASTERID,PMTDATE
          HAVING COUNT(1) > 1
          ) A;

    INSERT INTO IFRS_LI_EXCEPTION_DETAILS (
      DOWNLOAD_DATE
      ,DATA_SOURCE
      ,PRD_CODE
      ,ACCOUNT_NUMBER
      ,MASTERID
      ,PROCESS_ID
      ,EXCEPTION_CODE
      ,REMARKS
      )
    SELECT PMA.DOWNLOAD_DATE
      ,PMA.DATA_SOURCE
      ,PMA.PRODUCT_CODE
      ,PMA.ACCOUNT_NUMBER
      ,PMA.MASTERID
      ,'IFRS EXCEPTIONS' AS PROCESS_ID
      ,'V-2' AS EXCEPTION_CODE
      ,'PMTDATE : DOUBLE ' AS REMARKS
    FROM IFRS_LI_MASTER_ACCOUNT PMA
    INNER JOIN TMP_LI_SCHD SCH ON PMA.MASTERID = SCH.MASTERID;

    --START 20160331 PRE-PAYMENT, LATE-PAYMENT, RESTRUCTURE WITH NO CHANGE EIR
    MERGE INTO IFRS_LI_PAYM_SCHD_MTM X
    USING IFRS_LI_IMA_AMORT_CURR Z
    ON (X.MASTERID = Z.MASTERID
        AND Z.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET X.PV_CF = (X.PRINCIPAL + X.INTEREST) / POWER((1 + Z.MARKET_RATE / 12 / 100), X.COUNTER) ;



    DELETE IFRS_LI_EIR_ADJUSTMENT
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    INSERT INTO IFRS_LI_EIR_ADJUSTMENT (
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
      TOT_ADJUST --- TOTAL_PV_CF - ISNULL(FAIRVALUEAMT, OUTSTANDING)
      )
    SELECT A.DOWNLOAD_DATE
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
    FROM IFRS_LI_IMA_AMORT_CURR A
    INNER JOIN (SELECT X.MASTERID
                      ,MIN(X.PMTDATE) AS MIN_PMTDATE
                      ,MAX(X.PMTDATE) AS MAX_PMTDATE
                      ,SUM(X.PRINCIPAL + X.INTEREST) AS TOT_INSTALMENT
                      ,SUM((X.PRINCIPAL + X.INTEREST) / POWER((1 + Z.MARKET_RATE / 12 / 100), X.COUNTER)) AS TOTAL_PV_CF
                FROM IFRS_LI_PAYM_SCHD_MTM X
                INNER JOIN IFRS_LI_IMA_AMORT_CURR Z ON X.MASTERID = Z.MASTERID
                WHERE Z.DOWNLOAD_DATE = V_CURRDATE
                GROUP BY X.MASTERID
                ) B ON A.MASTERID = B.MASTERID
    WHERE A.DOWNLOAD_DATE = V_CURRDATE
      AND A.IFRS9_CLASS IN ('FVTPL','FVOCI') --IFRS_LI_CLASS
      AND A.ACCOUNT_STATUS = 'A';

    COMMIT;

    --END 20160331 PRE-PAYMENT, LATE-PAYMENT, RESTRUCTURE
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,4,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',1,'MARKET TO MARKET');

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY,COUNTER,REMARKS)
    VALUES (V_CURRDATE,99,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE_MTM',SYSTIMESTAMP,'IFRS ENGINE',99,'JUST ENDED');
    COMMIT; --=========

END;