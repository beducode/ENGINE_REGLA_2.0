CREATE OR REPLACE PROCEDURE SP_IFRS_LI_PAYM_SCHD
AS
  /*
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
  V_COUNTER_PAY NUMBER(10) ;
  V_MAX_COUNTERPAY NUMBER(10)  ;
  V_NEXT_COUNTER_PAY NUMBER(10)  ;
  V_PMT_DATE DATE  ;
  ---ADD YAHYA
  V_NEXT_START_DATE DATE;
  ---ADD YAHYA
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
    V_LOG_ID := 911;

    --SET V_PARAM_CALC_TO_LASTPAYMENT = 1 ---ADD YAHYA
    SELECT CURRDATE, PREVDATE
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_LI_PRC_DATE_AMORT;

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

    --ADD YAHYA
    BEGIN
      SELECT CASE WHEN COMMONUSAGE = 'Y'THEN 1 ELSE 0 END
      INTO V_PARAM_CALC_TO_LASTPAYMENT
      FROM TBLM_COMMONCODEHEADER
      WHERE COMMONCODE = 'SCM005';
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      V_PARAM_CALC_TO_LASTPAYMENT:=0;
    END;

    DELETE IFRS_LI_BATCH_LOG_DETAILS
    WHERE DOWNLOAD_DATE = V_CURRDATE
     AND BATCH_ID_HEADER = V_LOG_ID
     AND BATCH_NAME = 'PMTSCHD'  ;

    --TRACKING--
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,0,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',0,'JUST STARTED');

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_MAIN';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LI_PAYM_SCHD';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LI_PAYM_CORE_SRC';

    COMMIT;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',1,'INSERT TMP_LI_SCHEDULE_MAIN');

    COMMIT;

    INSERT INTO TMP_LI_SCHEDULE_MAIN (
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
     ,HOLD_AMOUNT
     ,INTEREST_RATE
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
    SELECT PMA.DOWNLOAD_DATE
     ,PMA.MASTERID
     ,PMA.ACCOUNT_NUMBER
     ,PMA.BRANCH_CODE
     ,PMA.PRODUCT_CODE
     ,PMA.LOAN_START_DATE
     ,PMA.LOAN_DUE_DATE
     ,
     --PMA.LOAN_START_AMORTIZATION,
     CASE  WHEN V_PARAM_CALC_TO_LASTPAYMENT = 0  THEN V_CURRDATE
           ELSE CASE  WHEN ECF.MASTERID IS NOT NULL  THEN ECF.LAST_PAYMENT_DATE_ECF
                      ELSE CASE  WHEN NVL(PMA.LAST_PAYMENT_DATE, PMA.LOAN_START_DATE) <=  PMA.LOAN_START_DATE  THEN  PMA.LOAN_START_DATE
                                 WHEN PMV.MASTERID IS NULL  THEN PMA.LOAN_START_DATE  ELSE CASE  WHEN PMA.LAST_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) THEN FN_PMTDATE (NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),-1) ELSE PMA.LAST_PAYMENT_DATE END
                                 END
                      END
      END START_AMORTIZATION_DATE
     ,
     /*
                       CASE WHEN ISNULL(PMA.LAST_PAYMENT_DATE,PMA.LOAN_START_DATE) <= PMA.LOAN_START_DATE THEN
            PMA.LOAN_START_DATE
          ELSE
          CASE WHEN PMA.LAST_PAYMENT_DATE >= ISNULL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE) THEN DATEADD(MONTH,-1,ISNULL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)) ELSE PMA.LAST_PAYMENT_DATE END
        END START_AMORTIZATION_DATE ,
        */
     /*
        CASE WHEN C.MASTERID IS NULL THEN
         V_CURRDATE
        ELSE
        PMA.LOAN_START_DATE
        END AS START_AMORTIZATION_DATE,
        */
     PMA.LOAN_DUE_DATE
     ,CASE WHEN PMA.NEXT_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) OR TO_CHAR (PMA.NEXT_PAYMENT_DATE, 'YYYYMMDD') = TO_CHAR (NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE), 'YYYYMMDD')
        THEN NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)
        ELSE PMA.NEXT_PAYMENT_DATE
      END AS FIRST_PMT_DATE
     ,PMA.CURRENCY
     ,PMA.OUTSTANDING
     ,PMA.PLAFOND
     ,PMA.OUTSTANDING
     ,PMA.INTEREST_RATE
     ,
     --@YY 20150622 FOR ANOMALY TENOR PMA
     CASE  WHEN NVL(PMA.TENOR, 0) > MONTHS_BETWEEN(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE) THEN NVL(PMA.TENOR, 0)
            ELSE (MONTHS_BETWEEN(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE) + 2)
     END AS TENOR
     ,
     --PMA.TENOR,
     PMA.PAYMENT_TERM
     ,PMA.PAYMENT_CODE
     ,PMA.INTEREST_CALCULATION_CODE
     ,
     --CASE
     --WHEN PMA.NEXT_PAYMENT_DATE > PMA.DOWNLOAD_DATE THEN
     CASE  WHEN PMA.NEXT_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) OR TO_CHAR (PMA.NEXT_PAYMENT_DATE, 'YYYYMMDD') = TO_CHAR (NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE), 'YYYYMMDD')
       THEN NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)
      ELSE PMA.NEXT_PAYMENT_DATE
      END
     ,0
     ,'N'
     ,PMA.INSTALLMENT_GRACE_PERIOD AS GRACE_DATE -- BIBD GRACE PERIOD
     /*  BCA DISABLE BPI  CASE WHEN PMA.NEXT_PAYMENT_DATE = PMA.FIRST_INSTALLMENT_DATE AND PMA.SPECIAL_FLAG = 1 THEN 1 ELSE 0 END --- BPI FLAG ONLY CTBC */
    FROM IFRS_LI_MASTER_ACCOUNT PMA
    INNER JOIN IFRS_LI_IMA_AMORT_CURR PMC
      ON PMA.MASTERID = PMC.MASTERID
      AND PMA.DOWNLOAD_DATE = PMC.DOWNLOAD_DATE
    LEFT JOIN IFRS_LI_IMA_AMORT_PREV PMV
      ON PMC.MASTERID = PMV.MASTERID
      AND PMV.DOWNLOAD_DATE = V_PREVDATE
    LEFT JOIN (SELECT MASTERID ,MAX(PMT_DATE) LAST_PAYMENT_DATE_ECF
               FROM IFRS_LI_ACCT_EIR_ECF
               WHERE AMORTSTOPDATE IS NULL
                AND AMORTSTOPMSG IS NULL
                AND PMT_DATE <= V_CURRDATE
               GROUP BY MASTERID
     ) ECF ON ECF.MASTERID = PMA.MASTERID

    WHERE PMA.DOWNLOAD_DATE = V_CURRDATE
     AND PMC.ECF_STATUS = 'Y'
     AND PMA.ACCOUNT_STATUS = 'A'
     --AND DATEDIFF(MONTH,V_CURRDATE,PMA.LOAN_DUE_DATE) >= 1
     AND PMA.IAS_CLASS = 'L' -----ADD YAHYA
     AND PMA.LOAN_DUE_DATE > V_CURRDATE
     AND PMA.AMORT_TYPE = 'EIR';

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LI_MASTER_PAYMENT_SETTING';

    INSERT INTO IFRS_LI_MASTER_PAYMENT_SETTING
(
        DOWNLOAD_DATE,
        MASTERID,
        ACCOUNT_NUMBER,
        COMPONENT_TYPE,
        COMPONENT_STATUS,
        FREQUENCY,
        INCREMENTS,
        AMOUNT,
        TIMES_ORG,
        TIMES_USED,
        DATE_START,
        DATE_END,
	PMT_DATE,
        IS_INSERTED
)
        SELECT
        A.DOWNLOAD_DATE,
        A.MASTERID,
        A.ACCOUNT_NUMBER,
        1, -- COMPONENT_TYPE (FIX INTEREST)
        1 AS COMPONENT_STATUS,
        'M' AS FREQUENCY, -- D=DAILY, M=MONTHLY, N=EOM DATE
        1 AS INCREMENTS_NEW, -- INCREMENTS
        0, -- AMOUNT
        A.TENOR / 1, --INCREMENTS_NEW, -- TIMES_ORG
        0 AS TIMES_USED,
        A.NEXT_PMTDATE AS DATE_START_NEW,
        A.END_AMORTIZATION_DATE AS DATE_END_NEW,
        CASE WHEN LENGTH(TO_CHAR(EXTRACT (DAY FROM A.END_AMORTIZATION_DATE))) = 1 THEN
                '0' || TO_CHAR(EXTRACT (DAY FROM A.END_AMORTIZATION_DATE))
             ELSE
                TO_CHAR(EXTRACT (DAY FROM A.END_AMORTIZATION_DATE))
        END,
        'Y'
FROM TMP_LI_SCHEDULE_MAIN A
WHERE A.DOWNLOAD_DATE = V_CURRDATE
UNION ALL
SELECT
        A.DOWNLOAD_DATE,
        A.MASTERID,
        A.ACCOUNT_NUMBER,
        0 AS LXTYPE_PMT_SCHD_TYPE,
        1 AS COMPONENT_STATUS,
        'M' AS FREQUENCY,
        1 AS LXPFRQ_SCHD_FRQ,
        A.HOLD_AMOUNT,
        1,
        0 AS TIMES_USED,
        A.END_AMORTIZATION_DATE,
        A.END_AMORTIZATION_DATE AS DATE_END,
        CASE WHEN LENGTH(TO_CHAR(EXTRACT (DAY FROM A.END_AMORTIZATION_DATE))) = 1 THEN
                '0' || TO_CHAR(EXTRACT (DAY FROM A.END_AMORTIZATION_DATE))
             ELSE
                TO_CHAR(EXTRACT (DAY FROM A.END_AMORTIZATION_DATE))
        END,
        'Y'
FROM TMP_LI_SCHEDULE_MAIN   A
WHERE A.DOWNLOAD_DATE = V_CURRDATE;

COMMIT;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1  ,V_LOG_ID  ,'PMTSCHD'  ,'SP_IFRS_LI_PAYMENT_SCHEDULE'  ,SYSTIMESTAMP  ,'IFRS ENGINE'  ,3  ,'INITIAL PROCESS' );

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
    SELECT * FROM IFRS_LI_MASTER_PAYMENT_SETTING PY1
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

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_CURR_HIST';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_PREV_HIST';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_CURR';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHEDULE_PREV';

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',4,'INSERT TMP_LI_SCHEDULE_CURR');

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
     /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    FROM TMP_LI_SCHEDULE_MAIN A
    LEFT JOIN TMP_LI_PY5 PY5 ON A.MASTERID = PY5.MASTERID
     AND A.DOWNLOAD_DATE BETWEEN PY5.DATE_START
      AND PY5.DATE_END
       --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
     AND MOD(MONTHS_BETWEEN(TRUNC(PY5.DATE_START,'MM'),TRUNC(A.DOWNLOAD_DATE,'MM')), PY5.INCREMENTS) = 0;


    COMMIT;

    INSERT INTO TMP_LI_SCHEDULE_CURR_HIST
    SELECT *
    FROM TMP_LI_SCHEDULE_CURR;

    COMMIT;


    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,1  ,V_LOG_ID  ,'PMTSCHD'  ,'SP_IFRS_LI_PAYMENT_SCHEDULE'  ,SYSTIMESTAMP  ,'IFRS ENGINE'  ,5  ,'INSERT IFRS_LI_PAYM_SCHD'  );

    COMMIT;


    INSERT INTO IFRS_LI_PAYM_SCHD (
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

    --COMMIT;
    /* REMOVE OUTSIDE LOOP 20160524
          INSERT  INTO IFRS_LI_PAYM_CORE_SRC
                ( MASTERID ,
                  ACCTNO ,
                  PMT_DATE ,
        INTEREST_RATE ,
        I_DAYS,
                  PRN_AMT ,
                INT_AMT ,
        DISB_PERCENTAGE ,
        DISB_AMOUNT ,
                  PLAFOND ,
                  OS_PRN ,
        COUNTER ,
                  ICC ,
        GRACE_DATE
                )
                SELECT  SCH.MASTERID ,
                       MA.ACCOUNT_NUMBER ,
                       SCH.PMTDATE ,
        SCH.INTEREST_RATE ,
        SCH.I_DAYS ,
                       SCH.PRINCIPAL ,
                       SCH.INTEREST ,
        SCH.DISB_PERCENTAGE ,
        SCH.DISB_AMOUNT ,
        SCH.PLAFOND ,
                       SCH.OSPRN ,
        SCH.COUNTER ,
                       SCH.INTEREST_CALCULATION_CODE ,
        SCH.GRACE_DATE
                FROM    TMP_LI_SCHEDULE_CURR SCH
                       INNER JOIN TMP_LI_SCHEDULE_MAIN MA ON SCH.MASTERID = MA.MASTERID ;
    REMOVE OUTSIDE LOOP 20160524 */
    COMMIT;


    SELECT MAX(TENOR) INTO V_MAX_COUNTERPAY
    FROM TMP_LI_SCHEDULE_MAIN;

    WHILE (V_COUNTER_PAY <= V_MAX_COUNTERPAY)
    LOOP
     --- START ADD YAHYA--

     EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_MIN_MAX_DATE';

     INSERT INTO TMP_LI_MIN_MAX_DATE  (ACCOUNT_NUMBER,MIN_DATE)
     SELECT A.ACCOUNT_NUMBER
      ,MIN(A.DATE_START) AS MIN_DATE
     FROM IFRS_LI_MASTER_PAYMENT_SETTING A
     INNER JOIN TMP_LI_SCHEDULE_CURR B ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
      AND A.DATE_START > B.NEXT_PMTDATE
     GROUP BY A.ACCOUNT_NUMBER;

     --- END ADD YAHYA--
     V_COUNTER_PAY := V_COUNTER_PAY + 1;
     V_NEXT_COUNTER_PAY := V_NEXT_COUNTER_PAY + 1;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,2  ,V_LOG_ID  ,'PMTSCHD'  ,'SP_IFRS_LI_PAYMENT_SCHEDULE'  ,SYSTIMESTAMP  ,'IFRS ENGINE'  ,V_COUNTER_PAY,'PAYMENT SCHEDULE LOOPING');

     --COMMIT;
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
      ,GRACE_DATE --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
      )
     SELECT A.MASTERID
      ,A.ACCOUNT_NUMBER
      ,NVL(PY3.AMOUNT, A.INTEREST_RATE) AS INTEREST_RATE
      ,A.NEXT_PMTDATE AS NEW_PMTDATE
      ,ROUND((
        CASE WHEN PY5.COMPONENT_TYPE = '5' THEN A.OSPRN + (PY5.AMOUNT / 100 * A.PLAFOND)
        ELSE A.OSPRN
        END - (
        ROUND((CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0
               ELSE CASE WHEN A.NEXT_PMTDATE >= A.DATE_END THEN A.OSPRN
                    ELSE CASE WHEN PY0.COMPONENT_TYPE = 0 THEN --FIX PRINCIPAL
                                                          CASE WHEN A.OSPRN <= PY0.AMOUNT THEN A.OSPRN ELSE PY0.AMOUNT END
                              WHEN PY2.COMPONENT_TYPE = 2 THEN --INSTALMENT
                                                          CASE WHEN A.OSPRN <= PY2.AMOUNT THEN A.OSPRN ELSE PY2.AMOUNT - (ROUND((CASE WHEN PY1.COMPONENT_TYPE = '1' THEN --FIX INTEREST
                                                                                                                                                                    PY1.AMOUNT
                                                                                                                                      ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 360
                                                                                                                                                WHEN A.ICC = '2' THEN A.INTEREST_RATE / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 365
                                                                                                                                                WHEN A.ICC = '6' THEN A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360
                                                                                                                                                ELSE 0
                                                                                                                                            END
                                                                                                                                       END), V_ROUND))END

                              WHEN PY4.COMPONENT_TYPE = 4 THEN --INSTALMENT
                                                          CASE WHEN A.OSPRN <= PY4.AMOUNT THEN A.OSPRN ELSE PY4.AMOUNT - (ROUND((CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 360
                                                                                                                                      WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 365
                                                                                                                                      WHEN A.ICC = '6' THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * NVL(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360
                                                                                                                                      ELSE 0
                                                                                                                                  END), V_ROUND))
                                                          END
                              ELSE 0
                              END
                    END

               END
          ), V_ROUND))
        ), V_ROUND) AS NEW_OSPRN
      ,ROUND((
        CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0 --BIBD FOR GRACE PERIOD
        ELSE CASE WHEN A.NEXT_PMTDATE >= A.DATE_END THEN A.OSPRN
              ELSE CASE  WHEN PY0.COMPONENT_TYPE = 0 THEN --FIX PRINCIPAL
                                                     CASE WHEN A.OSPRN <= PY0.AMOUNT THEN A.OSPRN ELSE PY0.AMOUNT END
                         WHEN PY2.COMPONENT_TYPE = 2 THEN --INSTALMENT
                                                          CASE WHEN A.OSPRN <= PY2.AMOUNT THEN A.OSPRN
                                                          ELSE PY2.AMOUNT - (ROUND((CASE WHEN PY1.COMPONENT_TYPE = '1' THEN --FIX INTEREST
                                                                                                                       PY1.AMOUNT
                                                                                         ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 360
                                                                                                   WHEN A.ICC = '2' THEN A.INTEREST_RATE / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 365
                                                                                                   WHEN A.ICC = '6' THEN A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360
                                                                                                   ELSE 0
                                                                                              END
                                                                                         END ), V_ROUND))
                                                          END
                         WHEN PY4.COMPONENT_TYPE = 4  THEN --INSTALMENT
                                                      CASE WHEN A.OSPRN <= PY4.AMOUNT THEN A.OSPRN
                                                      ELSE PY4.AMOUNT - (ROUND((CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 360
                                                                                     WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN *( A.NEXT_PMTDATE - A.PMTDATE )/ 365
                                                                                     WHEN A.ICC = '6' THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * NVL(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360
                                                                                     ELSE 0
                                                                                END), V_ROUND))
                                                      END
                         ELSE 0
                         END
              END
        END
        ), V_ROUND) AS NEW_PRINCIPAL
      ,ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0--bibd for grace period
                             ELSE -- add yahya to calculate BPI Flag only ctbc

                                       CASE WHEN PY1.COMPONENT_TYPE = '1' THEN --FIX INTEREST
                                                                               CASE WHEN PY1.AMOUNT = 0 THEN CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                                                                  WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                                                                  WHEN A.ICC = '6' THEN --30/360
                                                                                                                                    -- add yahya to calculate interest if migration in cutoff
                                                                                                                                    CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1 )
                                                                                                                                          THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360
                                                                                                                                         ELSE A.INTEREST_RATE/ 100 * A.OSPRN* PY1.INCREMENTS* 30 / 360
                                                                                                                                    END
                                                                                                                                    ----end add yahya
                                                                                                                  ELSE 0
                                                                                                              END

                                                                                    ELSE PY1.AMOUNT
                                                                               END
            WHEN PY3.COMPONENT_TYPE = '3' THEN CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                                         WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                                         WHEN A.ICC = '6' THEN --30/360
                                                                                                          -- add yahya to calculate interest if migration in cutoff
                                                                                                          CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1 )
                                                                                                               THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360
                                                                                                               ELSE NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* PY3.INCREMENTS* 30 / 360
                                                                                                          END
                                                                                                          ----end add yahya
                                                                                         ELSE 0
                                                                                    END
             ELSE CASE  WHEN A.ICC = '1' THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360
                        WHEN A.ICC = '2' THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 365
                        WHEN A.ICC = '6' THEN --30/360
                                         -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF
                                         CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1) THEN A.INTEREST_RATE / 100 * A.OSPRN * (A.NEXT_PMTDATE - A.PMTDATE) / 360
                                         ELSE A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY2.INCREMENTS, 1) * 30 / 360
                                         END
                                         ----END ADD YAHYA
                        ELSE 0
                        END
             END
         /*  BCA DISABLE BPI  END */
         END), V_ROUND) AS NEW_INTEREST
      ,NVL(PY5.AMOUNT, 0) AS DISB_PERCENTAGE
      ,NVL(PY5.AMOUNT, 0) / 100 * A.PLAFOND AS DISB_AMOUNT
      ,A.PLAFOND
      ,CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0
       ELSE CASE WHEN A.ICC IN ('1','2')  THEN CASE WHEN PY1.COMPONENT_TYPE = '1' THEN( A.NEXT_PMTDATE - A.PMTDATE )
                                                    WHEN PY2.COMPONENT_TYPE = '2' THEN( A.NEXT_PMTDATE - A.PMTDATE )
                                                    WHEN PY3.COMPONENT_TYPE = '3' THEN( A.NEXT_PMTDATE - A.PMTDATE )
                                                    ELSE( A.NEXT_PMTDATE - A.PMTDATE )
                                                    END
                  WHEN A.ICC = '6' THEN ---- ADD YAHYA TO CALCULATE I_DAYS IF MIGRATION IN CUTOFF
                                   CASE  WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1) THEN CASE WHEN PY1.COMPONENT_TYPE = '1' THEN( A.NEXT_PMTDATE - A.PMTDATE )
                                                                                                                                WHEN PY2.COMPONENT_TYPE = '2' THEN( A.NEXT_PMTDATE - A.PMTDATE )
                                                                                                                                WHEN PY3.COMPONENT_TYPE = '3' THEN( A.NEXT_PMTDATE - A.PMTDATE )
                                                                                                                                ELSE( A.NEXT_PMTDATE - A.PMTDATE )
                                                                                                                                END
                                    ELSE CASE  WHEN PY1.COMPONENT_TYPE = '1' THEN NVL(PY1.INCREMENTS, 1) * 30
                                               WHEN PY2.COMPONENT_TYPE = '2' THEN NVL(PY2.INCREMENTS, 1) * 30
                                               WHEN PY3.COMPONENT_TYPE = '3' THEN NVL(PY3.INCREMENTS, 1) * 30
                                               ELSE( A.NEXT_PMTDATE - A.PMTDATE )
                                               END
                                    END
                                          --------- END ADD YAHYA
                  ELSE 0 -- NOT IN 1,2,6
                  END
       END AS I_DAYS
      ,V_COUNTER_PAY AS COUNTER
      ,CASE WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END THEN B.MIN_DATE
            WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END THEN B.MIN_DATE
            WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END THEN B.MIN_DATE
            WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END THEN B.MIN_DATE
            WHEN PY5.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY5.DATE_END THEN B.MIN_DATE
            ELSE A.DATE_START
       END DATE_START
      ,A.DATE_END
      ,A.TENOR
      ,A.PAYMENT_CODE
      ,A.ICC
      ,CASE WHEN PY1.COMPONENT_TYPE = '1' THEN CASE  WHEN PY1.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY1.INCREMENTS* A.NEXT_COUNTER_PAY )))
                                               ELSE CASE ---START ADD YAHYA ---
                                                    WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END THEN B.MIN_DATE --- add yahya
                                                    WHEN FN_ISDATE(TO_CHAR (FN_PMTDATE(A.DATE_START, (PY1.INCREMENTS * A.NEXT_COUNTER_PAY )),'YYYYMM') || PY1.PMT_DATE,'YYYYMMDD') = 1
                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY1.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY1.PMT_DATE,'YYYYMMDD')
                                                    ELSE FN_PMTDATE (A.DATE_START,(PY1.INCREMENTS * A.NEXT_COUNTER_PAY))
                                                        ---END ADD YAHYA----
               /*
               WHEN DATEPART(DAY,DATEADD(MONTH,( PY1.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)) > PY1.PMT_DATE
               THEN DATEADD(MONTH,( PY1.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               ELSE DATEADD(MONTH,( PY1.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
                 */
                                                        END
                                               END
            WHEN PY2.COMPONENT_TYPE = '2' THEN CASE WHEN PY2.FREQUENCY = 'N' THEN LAST_DAY (FN_PMTDATE (A.DATE_START,(PY2.INCREMENTS * A.NEXT_COUNTER_PAY)))
                                               ELSE CASE---START ADD YAHYA ---
                                                    WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END THEN B.MIN_DATE
                                                    WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY2.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY2.PMT_DATE,'YYYYMMDD') = 1
                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY2.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY2.PMT_DATE,'YYYYMMDD')
                                                    ELSE FN_PMTDATE(A.DATE_START,(PY2.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                        ---END ADD YAHYA----
               /*
               WHEN DATEPART(DAY,DATEADD(MONTH,( PY2.INCREMENTS* A.NEXT_COUNTER_PAY ),A.DATE_START)) > PY2.PMT_DATE
               THEN DATEADD(MONTH,( PY2.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               ELSE DATEADD(MONTH,( PY2.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               */
                                                        END
                                               END
            WHEN PY3.COMPONENT_TYPE = '3' THEN CASE WHEN PY3.FREQUENCY = 'N' THEN  LAST_DAY (FN_PMTDATE (A.DATE_START,(PY3.INCREMENTS * A.NEXT_COUNTER_PAY)))
                                               ELSE CASE---START ADD YAHYA ---
                                                    WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END THEN B.MIN_DATE
                                                    WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY3.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY3.PMT_DATE,'YYYYMMDD') = 1
                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY3.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY3.PMT_DATE,'YYYYMMDD')
                                                    ELSE FN_PMTDATE(A.DATE_START,(PY3.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                        ---END ADD YAHYA----
               /*
               DATEPART(DAY,DATEADD(MONTH,( PY3.INCREMENTS * A.NEXT_COUNTER_PAY ),A.DATE_START)) > PY3.PMT_DATE
               THEN DATEADD(MONTH, ( PY3.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               ELSE DATEADD(MONTH, ( PY3.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               */
                                                        END
                                               END
            WHEN PY4.COMPONENT_TYPE = '4' THEN CASE WHEN PY4.FREQUENCY = 'N' THEN  LAST_DAY (FN_PMTDATE (A.DATE_START,(PY4.INCREMENTS * A.NEXT_COUNTER_PAY)))
                                               ELSE CASE---START ADD YAHYA ---
                                                    WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END THEN B.MIN_DATE
                                                    WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY4.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY4.PMT_DATE,'YYYYMMDD') = 1
                                                    THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY4.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY4.PMT_DATE,'YYYYMMDD')
                                                    ELSE FN_PMTDATE(A.DATE_START,( PY4.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                    ---END ADD YAHYA----
               /*
               DATEPART(DAY,DATEADD(MONTH,( PY4.INCREMENTS* A.NEXT_COUNTER_PAY ),A.DATE_START)) > PY4.PMT_DATE
               THEN DATEADD(MONTH, ( PY4.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               ELSE DATEADD(MONTH, ( PY4.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)
               */
                                                        END
                                               END
            WHEN PY0.COMPONENT_TYPE = '0' THEN CASE WHEN PY0.FREQUENCY = 'N' THEN LAST_DAY (FN_PMTDATE (A.DATE_START,(PY0.INCREMENTS * A.NEXT_COUNTER_PAY)))
                                               ELSE CASE ---START ADD YAHYA ---
                                                        WHEN PY0.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY0.DATE_END THEN B.MIN_DATE
                                                        WHEN FN_ISDATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY0.INCREMENTS * A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY0.PMT_DATE,'YYYYMMDD') = 1
                                                        THEN TO_DATE (TO_CHAR (FN_PMTDATE (A.DATE_START,(PY0.INCREMENTS* A.NEXT_COUNTER_PAY)),'YYYYMM')|| PY0.PMT_DATE,'YYYYMMDD')
                                                        ELSE FN_PMTDATE(A.DATE_START,(PY0.INCREMENTS * A.NEXT_COUNTER_PAY ))
                                                        ---END ADD YAHYA----
               /*
               WHEN DATEPART(DAY,DATEADD(MONTH,( PY0.INCREMENTS * A.NEXT_COUNTER_PAY ), A.DATE_START)) > PY0.PMT_DATE
               THEN DATEADD(MONTH,( PY0.INCREMENTS * A.NEXT_COUNTER_PAY ),A.DATE_START)
               ELSE DATEADD(MONTH,( PY0.INCREMENTS * A.NEXT_COUNTER_PAY ),A.DATE_START)
               */
                                                        END
                                               END
            ELSE A.DATE_END ----ADD YAHYA
       END
      ,CASE WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END THEN 0
            WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END THEN 0
            WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END THEN 0
            WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END THEN 0
            WHEN PY5.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY5.DATE_END THEN 0
            ELSE A.NEXT_COUNTER_PAY
       END NEXT_COUNTER_PAY
      ,A.SCH_FLAG
      ,A.GRACE_DATE --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
     FROM TMP_LI_SCHEDULE_CURR A
     LEFT JOIN TMP_LI_MIN_MAX_DATE B ON A.MASTERID = B.ACCOUNT_NUMBER ---ADD YAHYA
     LEFT JOIN TMP_LI_PY0 PY0
      ON A.MASTERID = PY0.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY0.DATE_START
      AND PY0.DATE_END
      AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY0.DATE_START),0), PY0.INCREMENTS)= 0

     LEFT JOIN TMP_LI_PY1 PY1 ON A.MASTERID = PY1.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY1.DATE_START
      AND PY1.DATE_END
      AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY1.DATE_START),0), PY1.INCREMENTS) = 0

     LEFT JOIN TMP_LI_PY2 PY2 ON A.MASTERID = PY2.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY2.DATE_START
      AND PY2.DATE_END
      --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
      AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY2.DATE_START),0), PY2.INCREMENTS) = 0

     LEFT JOIN TMP_LI_PY3 PY3 ON A.MASTERID = PY3.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY3.DATE_START
      AND PY3.DATE_END
      AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY3.DATE_START),0), PY3.INCREMENTS)= 0

     LEFT JOIN TMP_LI_PY4 PY4 ON A.MASTERID = PY4.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY4.DATE_START
      AND PY4.DATE_END
      AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY4.DATE_START),0), PY4.INCREMENTS) = 0

     LEFT JOIN TMP_LI_PY5 PY5 ON A.MASTERID = PY5.MASTERID
      AND A.NEXT_PMTDATE BETWEEN PY5.DATE_START
      AND PY5.DATE_END
      --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
      AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY5.DATE_START),0), PY5.INCREMENTS) = 0
     WHERE A.TENOR >= V_COUNTER_PAY
      AND A.PMTDATE <= A.DATE_END
      AND A.OSPRN > 0;

     COMMIT;

     INSERT INTO TMP_LI_SCHEDULE_PREV_HIST
     SELECT *
     FROM TMP_LI_SCHEDULE_PREV;

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
            WHEN LAST_DAY(A.NEXT_PMTDATE) = LAST_DAY(A.DATE_END)
            THEN A.DATE_END
       ELSE A.NEXT_PMTDATE
       END
      ,A.NEXT_COUNTER_PAY + 1
      ,A.SCH_FLAG
      ,A.GRACE_DATE --BIBD FOR GRACE PERIOD
      /*  BCA DISABLE BPI ,A.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
     FROM TMP_LI_SCHEDULE_PREV A;

     COMMIT;

     INSERT INTO TMP_LI_SCHEDULE_CURR_HIST
     SELECT *
     FROM TMP_LI_SCHEDULE_CURR;

     COMMIT;


     INSERT INTO IFRS_LI_PAYM_SCHD (
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
     COMMIT;
      /* REMOVE OUTSIDE LOOP 20160524
                INSERT  INTO IFRS_LI_PAYM_CORE_SRC
                       ( MASTERID ,
          ACCTNO ,
          PMT_DATE ,
          INTEREST_RATE ,
          I_DAYS,
          PRN_AMT ,
          INT_AMT ,
          DISB_PERCENTAGE ,
          DISB_AMOUNT ,
          PLAFOND ,
          OS_PRN ,
          COUNTER ,
          ICC ,
          GRACE_DATE
                       )
                       SELECT  SCH.MASTERID ,
                             MA.ACCOUNT_NUMBER ,
                             SCH.PMTDATE ,
          SCH.INTEREST_RATE ,
          SCH.I_DAYS ,
                             SCH.PRINCIPAL ,
                             SCH.INTEREST ,
          SCH.DISB_PERCENTAGE ,
          SCH.DISB_AMOUNT ,
          SCH.PLAFOND ,
                             SCH.OSPRN ,
          SCH.COUNTER ,
                             SCH.INTEREST_CALCULATION_CODE ,
          SCH.GRACE_DATE
                       FROM    TMP_LI_SCHEDULE_CURR SCH
                             INNER JOIN TMP_LI_SCHEDULE_MAIN MA ON SCH.MASTERID = MA.MASTERID ;
    REMOVE OUTSIDE LOOP 20160524 */
      --COMMIT;
    END  LOOP;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,3,V_LOG_ID ,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',1 ,'PAYMENT SCHEDULE EXCEPTIONS');

    COMMIT;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCH_MAX';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHD';

    INSERT INTO TMP_LI_SCH_MAX
    SELECT MASTERID
     ,MAX(PMTDATE) AS MAX_PMTDATE
    FROM IFRS_LI_PAYM_SCHD
    GROUP BY MASTERID;

    INSERT INTO TMP_LI_SCHD
    SELECT A.MASTERID
     ,A.OSPRN
    FROM IFRS_LI_PAYM_SCHD A
    INNER JOIN TMP_LI_SCH_MAX B ON A.MASTERID = B.MASTERID
     AND A.PMTDATE = B.MAX_PMTDATE;

    COMMIT;

    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,3,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',2,'INSERT IFRS_LI_EXCEPTION_DETAILS');

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
    SELECT MASTERID
    FROM IFRS_LI_PAYM_SCHD
    WHERE PMTDATE IS NULL  ;

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
     ,'PMTDATE : IS NULL' AS REMARKS
    FROM IFRS_LI_MASTER_ACCOUNT PMA
    INNER JOIN TMP_LI_SCHD SCH ON PMA.MASTERID = SCH.MASTERID
     AND PMA.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LI_SCHD';

    INSERT INTO TMP_LI_SCHD (MASTERID)
    SELECT DISTINCT MASTERID
    FROM (SELECT MASTERID ,PMTDATE FROM IFRS_LI_PAYM_SCHD GROUP BY MASTERID,PMTDATE HAVING COUNT(1) > 1) A  ;

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
    INNER JOIN TMP_LI_SCHD SCH ON PMA.MASTERID = SCH.MASTERID
     AND PMA.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    /*
    DELETE IFRS_LI_PAYM_SCHD A
    WHERE A.MASTERID IN (SELECT DISTINCT (MASTERID)
              FROM IFRS_LI_EXCEPTION_DETAILS
             WHERE DOWNLOAD_DATE = V_CURRDATE
               AND EXCEPTION_CODE = 'V-2');
    --COMMIT;
    */
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,3,V_LOG_ID,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',3,'UPDATE IFRS_LI_MASTER_ACCOUNT');

    COMMIT;
    /*
          MERGE INTO IFRS_LI_MASTER_ACCOUNT PMA
             USING TMP_LI_SCHD SCH
             ON ( PMA.MASTERID = SCH.MASTERID
                 AND PMA.DOWNLOAD_DATE = V_CURRDATE
                )
             WHEN MATCHED
                THEN
    UPDATE            SET
          PMA.IFRS_LI_ACCT_STATUS = CASE WHEN SCH.OSPRN = 0
                                THEN PMA.IFRS_LI_ACCT_STATUS
                                ELSE 'CLS'
                            END ,
          PMA.PMT_SCH_STATUS = CASE WHEN SCH.OSPRN = 0 THEN 'C'
                               ELSE 'N'
                          END ;  */
    --COMMIT;
    --======================== END EXCEPTION FOR OS  LAST PMT DATE > 0 ===============================--
    --TRACKING--
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,4,V_LOG_ID ,'PMTSCHD','SP_IFRS_LI_PAYMENT_SCHEDULE',SYSTIMESTAMP,'IFRS ENGINE',1 ,'INSERT IFRS_LI_PAYM_CORE_SRC');

    -- INSERT INTO IFRS_LI_PAYM_CORE_SRC 20160524
    INSERT INTO IFRS_LI_PAYM_CORE_SRC (
     MASTERID
     ,ACCTNO
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
     )
    /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    SELECT SCH.MASTERID
     ,SCH.ACCOUNT_NUMBER
     ,NVL(LAG(SCH.PMTDATE) OVER (PARTITION BY SCH.MASTERID ORDER BY SCH.PMTDATE), SCH.PMTDATE)
     ,SCH.PMTDATE
     ,SCH.INTEREST_RATE
     ,SCH.I_DAYS
     ,SCH.PRINCIPAL
     ,SCH.INTEREST
     ,SCH.DISB_PERCENTAGE
     ,SCH.DISB_AMOUNT
     ,SCH.PLAFOND
     ,NVL(LAG(SCH.OSPRN) OVER (PARTITION BY SCH.MASTERID ORDER BY SCH.PMTDATE), SCH.OSPRN)
     ,SCH.OSPRN
     ,SCH.COUNTER
     ,SCH.ICC
     ,SCH.GRACE_DATE
    /*  BCA DISABLE BPI ,SCH.SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
    FROM IFRS_LI_PAYM_SCHD SCH
    LEFT JOIN (SELECT DISTINCT MASTERID
               FROM IFRS_LI_EXCEPTION_DETAILS
               WHERE EXCEPTION_CODE = 'V-2'
                AND DOWNLOAD_DATE = V_CURRDATE
     ) EX ON SCH.MASTERID = EX.MASTERID
    WHERE EX.MASTERID IS NULL ;

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_LI_PAYM_SCHD_FUNDING');---TO EXEC  FUNDING YAHYA

    /* HISTORY PAYMENT SCHEDULE
       DELETE FROM IFRS_LI_PAYM_SCHD_HISTORY
       WHERE DOWNLOAD_DATE = V_CURRDATE

       INSERT INTO IFRS_LI_PAYM_SCHD_HISTORY
       SELECT * FROM IFRS_LI_PAYM_SCHD
       WHERE DOWNLOAD_DATE = V_CURRDATE
       */
    INSERT INTO IFRS_LI_BATCH_LOG_DETAILS (DOWNLOAD_DATE,BATCH_ID,BATCH_ID_HEADER,BATCH_NAME,PROCESS_NAME,START_DATE,CREATEDBY ,COUNTER,REMARKS)
    VALUES (V_CURRDATE,99 ,V_LOG_ID  ,'PMTSCHD'  ,'SP_IFRS_LI_PAYMENT_SCHEDULE'  ,SYSTIMESTAMP   ,'IFRS ENGINE'  ,99  ,'JUST ENDED'  );

    COMMIT;
END;