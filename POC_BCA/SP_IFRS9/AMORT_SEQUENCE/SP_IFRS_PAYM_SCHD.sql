CREATE OR REPLACE PROCEDURE SP_IFRS_PAYMENT_SCHEDULE
AS
  /*
  COMPONENT TYPE :
  0 : FIX PRINCIPAL AMOUNT
  1 : FIX INTEREST AMOUNT
  2 : FIX INSTALMENT AMOUNT
  3 : FIX INTEREST PERCENTAGE
  4 : FIX INSTALMENT AMOUNT FOR COMPONENT TYPE 3
  5 : STEP UP DISBURSMENT
  */
/*---------------------------
Changes : COmment ECF_STATUS - 26 DEC 2018
----------------------------*/
  --VARIABLE
  V_CURRDATE DATE ;
  V_PREVDATE DATE ;
  V_SESSIONID   VARCHAR2(36);
  V_COUNTER_PAY NUMBER(10) ;
  V_MAX_COUNTERPAY NUMBER(10) ;
  V_NEXT_COUNTER_PAY NUMBER(10) ;
  --CONSTANT
  V_CUT_OFF_DATE DATE ;
  V_ROUND NUMBER(10) ;
  V_FUNCROUND NUMBER(10);
  V_LOG_ID NUMBER(10);
  V_PARAM_CALC_TO_LASTPAYMENT NUMBER(10);
  V_CALC_IDAYS NUMBER(10);
  V_ACTIVE  NUMBER;
  V_ACTION  NUMBER;
  V_ECODE   VARCHAR2(10);
BEGIN
    ---ADD YAHYA IF 0 CURRDATE 1 LAST CYCLEDATE


    --V_CUT_OFF_DATE := '01-SEP-2018';
    V_MAX_COUNTERPAY := 0;
    V_COUNTER_PAY := 0;
    V_NEXT_COUNTER_PAY := 1;
    V_ROUND := 6; --default
    V_FUNCROUND := 1; --default
    V_LOG_ID := 911;
    V_CALC_IDAYS := 0;
    --SET @param_calc_to_lastpayment = 1	---ADD YAHYA

    SELECT  CURRDATE, PREVDATE, SESSIONID
       INTO V_CURRDATE, V_PREVDATE, V_SESSIONID
    FROM    IFRS_PRC_DATE_AMORT ;



    BEGIN
      SELECT CAST(VALUE1 AS NUMBER(10))
           , CAST(VALUE2 AS NUMBER(10))
      INTO V_ROUND, V_FUNCROUND
      FROM TBLM_COMMONCODEDETAIL
      WHERE COMMONCODE = 'SCM003';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_ROUND := 2;
        V_FUNCROUND:=0;
    END;

    BEGIN
      SELECT COMMONUSAGE INTO V_CALC_IDAYS
	 FROM TBLM_COMMONCODEHEADER
	 WHERE COMMONCODE = 'SCM002';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_CALC_IDAYS:=0;
    END;

    BEGIN
      SELECT  CASE WHEN COMMONUSAGE = 'Y' THEN 1  ELSE 0  END
    INTO V_PARAM_CALC_TO_LASTPAYMENT
		FROM    TBLM_COMMONCODEHEADER
		WHERE   COMMONCODE = 'SCM005';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_PARAM_CALC_TO_LASTPAYMENT:=0;
    END;

	DELETE IFRS_BATCH_LOG_DETAILS
    WHERE DOWNLOAD_DATE = V_CURRDATE
    AND BATCH_ID_HEADER = V_LOG_ID
    AND BATCH_NAME = 'PMTSCHD';

    COMMIT;

		--TRACKING--
    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,1 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,0 ,'JUST STARTED') ;

    COMMIT;

    --IF @V_CURRDATE < @CUT_OFF_DATE RETURN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_SCHEDULE_MAIN' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PAYM_SCHD' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PAYM_CORE_SRC' ;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,1 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,1 ,'INSERT TMP_IFRS_SCHEDULE_MAIN') ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_IFRS_SCHEDULE_MAIN
    ( DOWNLOAD_DATE ,
      MASTERID ,
      ACCOUNT_NUMBER ,
      BRANCH_CODE ,
      PRODUCT_CODE ,
      START_DATE ,
      DUE_DATE ,
      START_AMORTIZATION_DATE ,
      END_AMORTIZATION_DATE ,
      FIRST_PMT_DATE ,
      CURRENCY ,
      OUTSTANDING ,
      PLAFOND ,
      --HOLD_AMOUNT,
      INTEREST_RATE ,
      TENOR ,
      PAYMENT_TERM ,
      PAYMENT_CODE ,
      INTEREST_CALCULATION_CODE ,
      NEXT_PMTDATE ,
      NEXT_COUNTER_PAY ,
      SCH_FLAG ,
      GRACE_DATE ,
      FIRST_CYCLE_DATE
    ) --bibd grace period
    SELECT  /*+ PARALLEL(12) */ PMA.DOWNLOAD_DATE ,
            PMA.MASTERID ,
            PMA.ACCOUNT_NUMBER ,
            PMA.BRANCH_CODE ,
            PMA.PRODUCT_CODE ,
            PMA.LOAN_START_DATE ,
            PMA.LOAN_DUE_DATE ,
            --PMA.LOAN_START_AMORTIZATION,
            CASE
            WHEN V_PARAM_CALC_TO_LASTPAYMENT = 0 THEN V_CURRDATE
            ELSE CASE WHEN ECF.MASTERID IS NOT NULL THEN ECF.LAST_PAYMENT_DATE_ECF
                ELSE CASE  WHEN NVL(PMA.LAST_PAYMENT_DATE, PMA.LOAN_START_DATE) <=  PMA.LOAN_START_DATE THEN  PMA.LOAN_START_DATE
                           --WHEN PMV.MASTERID IS NULL THEN PMA.LOAN_START_DATE
                           ELSE CASE  WHEN PMA.LAST_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE) THEN FN_PMTDATE (NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),-1)
                                ELSE PMA.LAST_PAYMENT_DATE
                                END
                      END
                END
            END START_AMORTIZATION_DATE,
            /*
            case when NVL(pma.last_payment_date,pma.loan_start_date) <= pma.loan_start_date then
            pma.loan_start_date
            else
            case when PMA.LAST_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE) then dateadd(month,-1,NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)) else PMA.LAST_PAYMENT_DATE end
            end START_AMORTIZATION_DATE ,
            */
            /*
            CASE WHEN C.MASTERID IS NULL THEN
            @V_CURRDATE
            ELSE
            PMA.LOAN_START_DATE
            END AS START_AMORTIZATION_DATE,
            */
            CASE WHEN PYM.MAX_DATE_END < PMA.LOAN_DUE_DATE THEN
                 MAX_DATE_END
            ELSE PMA.LOAN_DUE_DATE
            END AS END_AMORTIZATION_DATE,
            CASE WHEN PMA.NEXT_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)
                      OR TO_CHAR(PMA.NEXT_PAYMENT_DATE,'YYYYMM') = TO_CHAR(NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),'YYYYMM')
                      THEN NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)
                 ELSE PMA.NEXT_PAYMENT_DATE
            END AS FIRST_PMT_DATE ,
            PMA.CURRENCY ,
            PMA.OUTSTANDING ,
            PMA.PLAFOND ,
            --PMA.OUTSTANDING,
            PMA.INTEREST_RATE ,
            --@YY 20150622 FOR ANOMALY TENOR PMA
            CASE WHEN NVL(PMA.TENOR, 0) > MONTHS_BETWEEN(PMA.LOAN_DUE_DATE,PMA.LOAN_START_DATE) THEN NVL(PMA.TENOR, 0)
                 ELSE (MONTHS_BETWEEN(PMA.LOAN_DUE_DATE, PMA.LOAN_START_DATE) + 2 )
            END AS TENOR ,
            --PMA.TENOR,
            PMA.PAYMENT_TERM ,
            PMA.PAYMENT_CODE ,
            PMA.INTEREST_CALCULATION_CODE ,
            --CASE
            --WHEN PMA.NEXT_PAYMENT_DATE > PMA.DOWNLOAD_DATE THEN
            CASE WHEN PMA.NEXT_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)
                      OR TO_CHAR(PMA.NEXT_PAYMENT_DATE,'YYYYMM') = TO_CHAR(NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE),'YYYYMM')
                      THEN NVL(PMA.LOAN_END_AMORTIZATION,PMA.LOAN_DUE_DATE)
                 ELSE PMA.NEXT_PAYMENT_DATE
            END
            ,
            0 ,
            'N' ,
            PMA.INSTALLMENT_GRACE_PERIOD AS GRACE_DATE,
            CASE
             WHEN ECF.MASTERID IS NOT NULL
              THEN ECF.LAST_PAYMENT_DATE_ECF
             ELSE CASE
               WHEN NVL(PMA.LAST_PAYMENT_DATE, PMA.LOAN_START_DATE) <=  PMA.LOAN_START_DATE
                THEN  PMA.LOAN_START_DATE
--               WHEN PMV.MASTERID IS NULL -- REMARKS DURING DAY 1 MIGRATION
--                THEN PMA.LOAN_START_DATE -- REMARKS DURING DAY 1 MIGRATION
               ELSE CASE
                 WHEN PMA.LAST_PAYMENT_DATE >= NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE)
                  THEN FN_PMTDATE(NVL(PMA.LOAN_END_AMORTIZATION, PMA.LOAN_DUE_DATE), - 1)
                 ELSE PMA.LAST_PAYMENT_DATE
                 END
               END
             END   AS  FIRST_CYCLE_DATE
    FROM    IFRS_MASTER_ACCOUNT PMA
    INNER JOIN IFRS_IMA_AMORT_CURR PMC
      ON PMA.MASTERID = PMC.MASTERID
      AND PMA.DOWNLOAD_DATE = PMC.DOWNLOAD_DATE
    LEFT JOIN IFRS_IMA_AMORT_PREV PMV
      ON PMC.MASTERID = PMV.MASTERID
      AND PMV.DOWNLOAD_DATE = V_PREVDATE
    LEFT JOIN (SELECT MASTERID , MAX(PMT_DATE) LAST_PAYMENT_DATE_ECF
               FROM IFRS_ACCT_EIR_ECF
               WHERE  AMORTSTOPDATE  IS NULL
               AND AMORTSTOPMSG IS NULL
               AND PMT_DATE <= V_CURRDATE
               GROUP BY MASTERID
              ) ECF
      ON ECF.MASTERID = PMA.MASTERID
    INNER JOIN (SELECT DOWNLOAD_DATE, MASTERID, MAX(DATE_END) MAX_DATE_END
                FROM IFRS_MASTER_PAYMENT_SETTING
                WHERE DOWNLOAD_DATE = V_CURRDATE
                GROUP BY DOWNLOAD_DATE, MASTERID
                ) PYM
      ON PYM.MASTERID = PMA.MASTERID AND PYM.DOWNLOAD_DATE = PMA.DOWNLOAD_DATE
    WHERE   PMA.DOWNLOAD_DATE = V_CURRDATE
   -- AND ((V_CURRDATE=V_CUT_OFF_DATE) OR PMC.ECF_STATUS = 'Y')
    AND PMA.ACCOUNT_STATUS = 'A'
    --AND datediff(month,@V_CURRDATE,PMA.LOAN_DUE_DATE) >= 1
    AND PMA.IAS_CLASS = 'A'-----ADD YAHYA
    AND PMA.LOAN_DUE_DATE > V_CURRDATE
    AND PMA.AMORT_TYPE = 'EIR';

    COMMIT;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,1 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,3 ,'INITIAL PROCESS') ;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PY0' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PY1' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PY2' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PY3' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PY4' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PY5' ;

    INSERT /*+ PARALLEL(12) */ INTO TMP_PY0
    SELECT /*+ PARALLEL(12) */ *
    FROM    IFRS_MASTER_PAYMENT_SETTING PY0
    WHERE   PY0.COMPONENT_TYPE = '0'
    AND PY0.DOWNLOAD_DATE = V_CURRDATE
    AND PY0.FREQUENCY IN ( 'M', 'N', 'D' ) ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_PY1
    SELECT /*+ PARALLEL(12) */ *
    FROM    IFRS_MASTER_PAYMENT_SETTING PY1
    WHERE   PY1.COMPONENT_TYPE = '1'
    AND PY1.DOWNLOAD_DATE = V_CURRDATE
    AND PY1.FREQUENCY IN ( 'M', 'N', 'D' ) ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_PY2
    SELECT /*+ PARALLEL(12) */ *
    FROM    IFRS_MASTER_PAYMENT_SETTING PY2
    WHERE   PY2.COMPONENT_TYPE = '2'
    AND PY2.DOWNLOAD_DATE = V_CURRDATE
    AND PY2.FREQUENCY IN ( 'M', 'N', 'D' ) ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_PY3
    SELECT /*+ PARALLEL(12) */ *
    FROM    IFRS_MASTER_PAYMENT_SETTING PY3
    WHERE   PY3.COMPONENT_TYPE = '3'
    AND PY3.DOWNLOAD_DATE = V_CURRDATE
    AND PY3.FREQUENCY IN ( 'M', 'N', 'D' ) ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_PY4
    SELECT /*+ PARALLEL(12) */ *
    FROM    IFRS_MASTER_PAYMENT_SETTING PY4
    WHERE   PY4.COMPONENT_TYPE = '4'
    AND PY4.DOWNLOAD_DATE = V_CURRDATE
    AND PY4.FREQUENCY IN ( 'M', 'N', 'D' ) ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_PY5
    SELECT /*+ PARALLEL(12) */ *
    FROM    IFRS_MASTER_PAYMENT_SETTING PY5
    WHERE   PY5.COMPONENT_TYPE = '5'
    AND PY5.DOWNLOAD_DATE = V_CURRDATE
    AND PY5.FREQUENCY IN ( 'M', 'N', 'D' ) ;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_SCHEDULE_CURR_TEMP' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_SCHEDULE_PREV_TEMP' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_SCHEDULE_CURR' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_SCHEDULE_PREV' ;


    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,1 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,4 ,'INSERT TT_SCHEDULE_CURR') ;
    COMMIT;


    INSERT /*+ PARALLEL(12) */ INTO TMP_SCHEDULE_CURR
    ( MASTERID ,
      ACCOUNT_NUMBER ,
      INTEREST_RATE ,
      PMTDATE ,
      OSPRN ,
      PRINCIPAL ,
      INTEREST ,
      DISB_PERCENTAGE ,
      DISB_AMOUNT ,
      PLAFOND ,
      I_DAYS ,
      COUNTER ,
      DATE_START ,
      DATE_END ,
      TENOR ,
      PAYMENT_CODE ,
      ICC ,
      NEXT_PMTDATE ,
      NEXT_COUNTER_PAY ,
      SCH_FLAG ,
      GRACE_DATE ,
      FIRST_CYCLE_DATE
    ) --bibd for grace period
    SELECT  /*+ PARALLEL(12) */ A.MASTERID ,
            A.ACCOUNT_NUMBER ,
            A.INTEREST_RATE ,
            A.START_AMORTIZATION_DATE ,
            A.OUTSTANDING ,
            0 AS PRINCIPAL ,
            0 AS INTEREST ,
            NVL(PY5.AMOUNT, 0) AS DISB_PERCENTAGE ,
            A.OUTSTANDING AS DISB_AMOUNT ,
            A.PLAFOND AS PLAFOND ,
						0 AS I_DAYS ,
            0 COUNTER ,
            A.FIRST_PMT_DATE AS DATE_START ,
            A.END_AMORTIZATION_DATE ,
            A.TENOR ,
            A.PAYMENT_CODE ,
            A.INTEREST_CALCULATION_CODE ,
            A.NEXT_PMTDATE AS NEXT_PMTDATE ,
            A.NEXT_COUNTER_PAY + 1 ,
            A.SCH_FLAG ,
            A.GRACE_DATE, --bibd for grace period
            A.FIRST_CYCLE_DATE
    FROM TMP_IFRS_SCHEDULE_MAIN A
    LEFT JOIN TMP_PY5 PY5
      ON A.MASTERID = PY5.MASTERID
      AND A.DOWNLOAD_DATE BETWEEN PY5.DATE_START AND PY5.DATE_END
      --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
      AND MOD(MONTHS_BETWEEN(TRUNC(PY5.DATE_START,'MM'),TRUNC(A.DOWNLOAD_DATE,'MM')), PY5.INCREMENTS) = 0 ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_SCHEDULE_CURR_TEMP
    SELECT /*+ PARALLEL(12) */ *
    FROM    TMP_SCHEDULE_CURR ;
    COMMIT;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,1 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,5 ,'INSERT IFRS_PAYM_SCHD') ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS_PAYM_SCHD
    ( MASTERID ,
      --ACCOUNT_NUMBER ,
      PMTDATE ,
      INTEREST_RATE ,
      OSPRN ,
      PRINCIPAL ,
      INTEREST ,
      DISB_PERCENTAGE ,
      DISB_AMOUNT ,
      PLAFOND ,
      I_DAYS ,
      ICC ,
      COUNTER ,
      DOWNLOAD_DATE ,
      SCH_FLAG ,
      GRACE_DATE
    ) --bibd for grace
    SELECT  /*+ PARALLEL(12) */ MASTERID ,
            --ACCOUNT_NUMBER ,
            PMTDATE ,
            INTEREST_RATE ,
            OSPRN ,
            PRINCIPAL ,
            INTEREST ,
            DISB_PERCENTAGE ,
            DISB_AMOUNT ,
            PLAFOND ,
						I_DAYS ,
						ICC ,
						COUNTER ,
						V_CURRDATE ,
						SCH_FLAG ,
						GRACE_DATE --bibd for grace
    FROM    TMP_SCHEDULE_CURR ;

    COMMIT;
    /* remove outside loop 20160524
        INSERT  INTO IFRS_PAYM_CORE_SRC
                ( MASTERID ,
                  ACCTNO ,
                  PMT_DATE ,
				  interest_rate ,
				  I_DAYS,
                  PRN_AMT ,
                  INT_AMT ,
				  disb_percentage ,
				  disb_amount ,
                  plafond ,
                  OS_PRN ,
				  COUNTER ,
                  ICC ,
				  grace_date
                )
                SELECT  SCH.ACC_MSTR_ID ,
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
                FROM    TT_SCHEDULE_CURR SCH
                        INNER JOIN TT_IFRS_SCHEDULE_MAIN MA ON SCH.ACC_MSTR_ID = MA.ACC_MSTR_ID ;
    remove outside loop 20160524 */

    --COMMIT;

    BEGIN
      SELECT  MAX(TENOR) INTO V_MAX_COUNTERPAY
      FROM    TMP_IFRS_SCHEDULE_MAIN ;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_MAX_COUNTERPAY := -1;
    END;

    WHILE ( V_COUNTER_PAY <= V_MAX_COUNTERPAY )
    LOOP

				EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_MIN_MAX_DATE' ;
                COMMIT;


				INSERT /*+ PARALLEL(12) */ INTO TMP_MIN_MAX_DATE
				SELECT /*+ PARALLEL(12) */ A.MASTERID,MIN(A.DATE_START) AS MIN_DATE -- INTO #TT_MIN_MAX_DATE
				FROM IFRS_MASTER_PAYMENT_SETTING A
				INNER JOIN TMP_SCHEDULE_CURR B
                ON A.MASTERID = B.MASTERID
                AND A.DATE_START > B.NEXT_PMTDATE
				GROUP BY A.MASTERID ;
                COMMIT;
				--- END ADD YAHYA--

        V_COUNTER_PAY := V_COUNTER_PAY + 1 ;
        V_NEXT_COUNTER_PAY := V_NEXT_COUNTER_PAY + 1 ;


        INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
        VALUES  ( V_CURRDATE ,2 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,V_COUNTER_PAY ,'PAYMENT SCHEDULE LOOPING') ;

        COMMIT;
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_SCHEDULE_PREV' ;

        INSERT /*+ PARALLEL(12) */ INTO TMP_SCHEDULE_PREV
        ( MASTERID ,
          ACCOUNT_NUMBER ,
          INTEREST_RATE ,
          PMTDATE ,
          OSPRN ,
          PRINCIPAL ,
          INTEREST ,
          DISB_PERCENTAGE ,
          DISB_AMOUNT ,
          PLAFOND ,
          I_DAYS ,
          COUNTER ,
          DATE_START ,
          DATE_END ,
          TENOR ,
          PAYMENT_CODE ,
          ICC ,
          NEXT_PMTDATE ,
          NEXT_COUNTER_PAY ,
          SCH_FLAG ,
          GRACE_DATE
        ) --bibd for grace period
        SELECT  /*+ PARALLEL(12) */ A.MASTERID ,
                A.ACCOUNT_NUMBER ,
                NVL(PY3.AMOUNT, A.INTEREST_RATE) AS INTEREST_RATE ,
                A.NEXT_PMTDATE AS NEW_PMTDATE ,


                ROUND(( CASE WHEN PY5.COMPONENT_TYPE = '5' THEN A.OSPRN + ( PY5.AMOUNT / 100* A.PLAFOND )
                             ELSE A.OSPRN
                        END
                - ( ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0 --bibd for grace period
                                 ELSE CASE WHEN A.NEXT_PMTDATE >= A.DATE_END THEN A.OSPRN
                                           ELSE CASE WHEN PY0.COMPONENT_TYPE = 0
                                                     THEN --FIX PRINCIPAL
                                                          CASE WHEN A.OSPRN <= PY0.AMOUNT
                                                               THEN A.OSPRN
                                                               ELSE PY0.AMOUNT
                                                          END
                                                     WHEN PY2.COMPONENT_TYPE = 2
                                                     THEN --INSTALMENT
                                                          CASE WHEN A.OSPRN <= PY2.AMOUNT THEN A.OSPRN
                                                          ELSE
                                                                CASE WHEN V_COUNTER_PAY = 1 AND V_PARAM_CALC_TO_LASTPAYMENT = 0 THEN
                                                                    PY2.AMOUNT- ( ROUND((
                                                                    CASE WHEN PY1.COMPONENT_TYPE = '1' THEN PY1.AMOUNT --FIX INTEREST PY1.AMOUNT
                                                                         ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE - A.FIRST_CYCLE_DATE)/ 360--ACTUAL/360
                                                                                   WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE - A.FIRST_CYCLE_DATE)/ 365--ACTUAL/365
                                                                                   WHEN A.ICC = '6'
                                                                                   THEN/* A.INTEREST_RATE/ 100 * A.OSPRN* NVL(PY1.INCREMENTS,PY2.INCREMENTS)* 30 / 360--30 / 360*/
                                                                                        CASE WHEN V_CALC_IDAYS = 0 THEN A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360
                                                                                             WHEN V_CALC_IDAYS = 1 THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.FIRST_CYCLE_DATE, A.NEXT_PMTDATE) / 360
                                                                                             ELSE A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.FIRST_CYCLE_DATE, A.NEXT_PMTDATE) / 360
                                                                                        END
                                                                                   /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                                   THEN FN_INT_ACT_ACT(A.OSPRN,A.INTEREST_RATE,A.PMTDATE,A.NEXT_PMTDATE)
                                                                                   WHEN A.ICC = '9' --30/ACTUAL
                                                                                   THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, A.INTEREST_RATE,A.PMTDATE, A.NEXT_PMTDATE,NVL(PY1.INCREMENTS, PY2.INCREMENTS))
                                                                                    /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   ELSE 0
                                                                              END
                                                                    END ), V_ROUND))
                                                               ELSE PY2.AMOUNT- ( ROUND((
                                                                    CASE WHEN PY1.COMPONENT_TYPE = '1' THEN PY1.AMOUNT --FIX INTEREST PY1.AMOUNT
                                                                         ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                                   WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                                   WHEN A.ICC = '6'
                                                                                   THEN/* A.INTEREST_RATE/ 100 * A.OSPRN* NVL(PY1.INCREMENTS,PY2.INCREMENTS)* 30 / 360--30 / 360*/
                                                                                        CASE WHEN V_CALC_IDAYS = 0 THEN A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360
                                                                                             WHEN V_CALC_IDAYS = 1 THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                                             ELSE A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                                        END
                                                                                   /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                                   THEN FN_INT_ACT_ACT(A.OSPRN,A.INTEREST_RATE,A.PMTDATE,A.NEXT_PMTDATE)
                                                                                   WHEN A.ICC = '9' --30/ACTUAL
                                                                                   THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, A.INTEREST_RATE,A.PMTDATE, A.NEXT_PMTDATE,NVL(PY1.INCREMENTS, PY2.INCREMENTS))
                                                                                    /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   ELSE 0
                                                                              END
                                                                    END ), V_ROUND))
                                                          END
                                                        END
                                                     WHEN PY4.COMPONENT_TYPE = 4
                                                     THEN --INSTALMENT
                                                          CASE WHEN A.OSPRN <= PY4.AMOUNT THEN A.OSPRN
                                                               ELSE
                                                                CASE WHEN V_COUNTER_PAY = 1  AND V_PARAM_CALC_TO_LASTPAYMENT = 0 THEN
                                                                   PY4.AMOUNT- ( ROUND((
                                                                        CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE - A.FIRST_CYCLE_DATE)/ 360--ACTUAL/360
                                                                             WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE - A.FIRST_CYCLE_DATE)/ 365--ACTUAL/365
                                                                             WHEN A.ICC = '6'
                                                                             THEN CASE WHEN V_CALC_IDAYS = 0 THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * NVL(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360
                                                                                       WHEN V_CALC_IDAYS = 1 THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.FIRST_CYCLE_DATE, A.NEXT_PMTDATE) / 360
                                                                                       ELSE NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.FIRST_CYCLE_DATE, A.NEXT_PMTDATE) / 360
                                                                                  END

                                                                             /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                             WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                             THEN FN_INT_ACT_ACT(A.OSPRN,NVL(PY3.AMOUNT,A.INTEREST_RATE),A.PMTDATE,A.NEXT_PMTDATE)
                                                                             WHEN A.ICC = '9' --30/ACTUAL
                                                                             THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, NVL(PY3.AMOUNT, A.INTEREST_RATE),A.PMTDATE, A.NEXT_PMTDATE,NVL(PY3.INCREMENTS, PY4.INCREMENTS))
                                                                             /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                             ELSE 0
                                                                        END ), V_ROUND))
                                                                ELSE
                                                                   PY4.AMOUNT- ( ROUND((
                                                                        CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                             WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                             WHEN A.ICC = '6'
                                                                             THEN CASE WHEN V_CALC_IDAYS = 0 THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * NVL(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360
                                                                                       WHEN V_CALC_IDAYS = 1 THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                                       ELSE NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                                  END

                                                                             /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                             WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                             THEN FN_INT_ACT_ACT(A.OSPRN,NVL(PY3.AMOUNT,A.INTEREST_RATE),A.PMTDATE,A.NEXT_PMTDATE)
                                                                             WHEN A.ICC = '9' --30/ACTUAL
                                                                             THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, NVL(PY3.AMOUNT, A.INTEREST_RATE),A.PMTDATE, A.NEXT_PMTDATE,NVL(PY3.INCREMENTS, PY4.INCREMENTS))
                                                                             /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                             ELSE 0
                                                                        END ), V_ROUND))
                                                                END
                                                          END
                                                     ELSE 0
                                                END
                                      END
                            END ), V_ROUND))),V_ROUND) AS NEW_OSPRN ,


                ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0--bibd for grace period
                             ELSE CASE WHEN A.NEXT_PMTDATE >= A.DATE_END THEN A.OSPRN
                                       ELSE CASE WHEN PY0.COMPONENT_TYPE = 0
                                                 THEN --FIX PRINCIPAL
                                                      CASE WHEN A.OSPRN <= PY0.AMOUNT THEN A.OSPRN
                                                           ELSE PY0.AMOUNT
                                                      END
                                                 WHEN PY2.COMPONENT_TYPE = 2
                                                 THEN --INSTALMENT
                                                      CASE WHEN A.OSPRN <= PY2.AMOUNT THEN A.OSPRN
                                                           ELSE
                                                                CASE WHEN V_COUNTER_PAY = 1  AND V_PARAM_CALC_TO_LASTPAYMENT = 0 THEN
                                                                    PY2.AMOUNT- ( ROUND((
                                                                    CASE WHEN PY1.COMPONENT_TYPE = '1' THEN PY1.AMOUNT--FIX INTEREST
                                                                         ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE - A.FIRST_CYCLE_DATE)/ 360--ACTUAL/360
                                                                                   WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE - A.FIRST_CYCLE_DATE)/ 365--ACTUAL/365
                                                                                   WHEN A.ICC = '6'
                                                                                   THEN CASE WHEN V_CALC_IDAYS = 0 THEN A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360
                                                                                             WHEN V_CALC_IDAYS = 1 THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.FIRST_CYCLE_DATE, A.NEXT_PMTDATE) / 360
                                                                                             ELSE A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.FIRST_CYCLE_DATE, A.NEXT_PMTDATE) / 360
                                                                                        END
                                                                                   /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                                   THEN FN_INT_ACT_ACT(A.OSPRN,A.INTEREST_RATE,A.PMTDATE,A.NEXT_PMTDATE)
                                                                                   WHEN A.ICC = '9' --30/ACTUAL
                                                                                   THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, A.INTEREST_RATE,A.PMTDATE, A.NEXT_PMTDATE,NVL(PY1.INCREMENTS, PY2.INCREMENTS))
                                                                                   /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   ELSE 0
                                                                              END
                                                                    END ), V_ROUND) )
                                                                ELSE
                                                                                                                                        PY2.AMOUNT- ( ROUND((
                                                                    CASE WHEN PY1.COMPONENT_TYPE = '1' THEN PY1.AMOUNT--FIX INTEREST
                                                                         ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                                   WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                                   WHEN A.ICC = '6'
                                                                                   THEN CASE WHEN V_CALC_IDAYS = 0 THEN A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY1.INCREMENTS, PY2.INCREMENTS) * 30 / 360
                                                                                             WHEN V_CALC_IDAYS = 1 THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                                             ELSE A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                                        END
                                                                                   /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                                   THEN FN_INT_ACT_ACT(A.OSPRN,A.INTEREST_RATE,A.PMTDATE,A.NEXT_PMTDATE)
                                                                                   WHEN A.ICC = '9' --30/ACTUAL
                                                                                   THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, A.INTEREST_RATE,A.PMTDATE, A.NEXT_PMTDATE,NVL(PY1.INCREMENTS, PY2.INCREMENTS))
                                                                                   /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                                   ELSE 0
                                                                              END
                                                                    END ), V_ROUND) )
                                                            END
                                                      END
                                                 WHEN PY4.COMPONENT_TYPE = 4
                                                 THEN --INSTALMENT
                                                      CASE WHEN A.OSPRN <= PY4.AMOUNT THEN A.OSPRN
                                                           ELSE PY4.AMOUNT - ( ROUND((
                                                                CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                                     WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                                     WHEN A.ICC = '6'
                                                                     THEN CASE WHEN V_CALC_IDAYS = 0
                                                                               THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * NVL(PY3.INCREMENTS, PY4.INCREMENTS) * 30 / 360
                                                                               WHEN V_CALC_IDAYS = 1 THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                               ELSE NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                          END
                                                                     /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                     WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                                     THEN FN_INT_ACT_ACT(A.OSPRN,NVL(PY3.AMOUNT,A.INTEREST_RATE),A.PMTDATE,A.NEXT_PMTDATE)
                                                                     WHEN A.ICC = '9' --30/ACTUAL
                                                                     THEN FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, NVL(PY3.AMOUNT, A.INTEREST_RATE),A.PMTDATE, A.NEXT_PMTDATE,NVL(PY3.INCREMENTS, PY4.INCREMENTS))
                                                                     /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                     ELSE 0
                                                                END ), V_ROUND) )
                                                      END
                                                 ELSE 0
                                            END
                                  END
                        END ), V_ROUND) AS NEW_PRINCIPAL ,


                ROUND(( CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0--bibd for grace period
                             ELSE -- add yahya to calculate BPI Flag only ctbc
                                  CASE WHEN PY1.COMPONENT_TYPE = '1'
                                       THEN --FIX INTEREST
                                            CASE WHEN PY1.AMOUNT = 0
                                                 THEN CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                           WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                           WHEN A.ICC = '6'
                                                           THEN --30/360
                                                                CASE WHEN V_CALC_IDAYS = 0
                                                                     THEN -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF
                                                                          CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1)
                                                                               THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360( A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                               ELSE A.INTEREST_RATE / 100 * A.OSPRN * PY1.INCREMENTS * 30 / 360
                                                                          END
                                                                          ----END ADD YAHYA
                                                                     WHEN V_CALC_IDAYS = 1 THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                     ELSE	A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                END
                                                           /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                           WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                           THEN FN_INT_ACT_ACT(A.OSPRN,A.INTEREST_RATE,A.PMTDATE,A.NEXT_PMTDATE)
                                                           WHEN A.ICC = '9' --30/ACTUAL
                                                           THEN CASE WHEN V_COUNTER_PAY=1 AND V_PARAM_CALC_TO_LASTPAYMENT = 0
                                                                THEN FN_INT_30_ACT(1,A.OSPRN, A.INTEREST_RATE,A.PMTDATE, A.NEXT_PMTDATE,PY1.INCREMENTS)
                                                                ELSE FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN, A.INTEREST_RATE,A.PMTDATE, A.NEXT_PMTDATE,PY1.INCREMENTS)
                                                                END
                                                           /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                           ELSE 0
                                                      END
                                                 ELSE PY1.AMOUNT
                                            END
                                       WHEN PY3.COMPONENT_TYPE = '3'
                                       THEN CASE WHEN A.ICC = '1' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                 WHEN A.ICC = '2' THEN NVL(PY3.AMOUNT,A.INTEREST_RATE)/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                 WHEN A.ICC = '6'
                                                 THEN --30/360
                                                      CASE WHEN V_CALC_IDAYS = 0
                                                           THEN -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF
                                                                CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1)
                                                                     THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * FN_LI_CNT_DAYS_30_360( A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                     ELSE NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * A.OSPRN * PY3.INCREMENTS * 30 / 360
                                                                END
                                                                ----END ADD YAHYA
                                                           WHEN V_CALC_IDAYS = 1 THEN NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                           ELSE NVL(PY3.AMOUNT, A.INTEREST_RATE) / 100 * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                      END
                                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 WHEN A.ICC = '7' --ACTUAL/ACTUAL
                                                 THEN FN_INT_ACT_ACT(A.OSPRN,NVL(PY3.AMOUNT,A.INTEREST_RATE),A.PMTDATE,A.NEXT_PMTDATE)
                                                 WHEN A.ICC = '9' --30/ACTUAL
                                                 THEN CASE WHEN V_COUNTER_PAY=1 AND V_PARAM_CALC_TO_LASTPAYMENT = 0
                                                      THEN FN_INT_30_ACT(1,A.OSPRN,NVL(PY3.AMOUNT, A.INTEREST_RATE) ,A.PMTDATE, A.NEXT_PMTDATE,PY3.INCREMENTS)
                                                      ELSE FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN,NVL(PY3.AMOUNT, A.INTEREST_RATE) ,A.PMTDATE, A.NEXT_PMTDATE,PY3.INCREMENTS)
                                                       END
                                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 ELSE 0
                                            END
                                       ELSE CASE WHEN A.ICC = '1' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 360--ACTUAL/360
                                                 WHEN A.ICC = '2' THEN A.INTEREST_RATE/ 100 * A.OSPRN* (A.NEXT_PMTDATE -A.PMTDATE)/ 365--ACTUAL/365
                                                 WHEN A.ICC = '6'
                                                 THEN --30/360
                                                      CASE WHEN V_CALC_IDAYS = 0
                                                           THEN -- ADD YAHYA TO CALCULATE INTEREST IF MIGRATION IN CUTOFF
                                                               CASE WHEN V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC = '6' AND V_COUNTER_PAY = 1
                                                                    THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_LI_CNT_DAYS_30_360( A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                                    ELSE A.INTEREST_RATE / 100 * A.OSPRN * NVL(PY2.INCREMENTS, 1) * 30 / 360
                                                               END
                                                               ----END ADD YAHYA
                                                           WHEN V_CALC_IDAYS = 1 THEN A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                           ELSE A.INTEREST_RATE / 100 * A.OSPRN * FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE) / 360
                                                      END
                                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 WHEN A.ICC = '7' --ACTUAL/ACTUAL
												 THEN FN_INT_ACT_ACT(A.OSPRN,A.INTEREST_RATE,A.PMTDATE,A.NEXT_PMTDATE)
												 WHEN A.ICC = '9' --30/ACTUAL
												 THEN CASE WHEN V_COUNTER_PAY=1 AND V_PARAM_CALC_TO_LASTPAYMENT = 0
                                   THEN FN_INT_30_ACT(1,A.OSPRN,A.INTEREST_RATE ,A.PMTDATE, A.NEXT_PMTDATE,PY2.INCREMENTS)
                                   ELSE FN_INT_30_ACT(V_CALC_IDAYS,A.OSPRN,A.INTEREST_RATE ,A.PMTDATE, A.NEXT_PMTDATE,PY2.INCREMENTS)
                                   END
                                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 ELSE 0
                                            END
                                  END
                        END ), V_ROUND) AS NEW_INTEREST ,


                NVL(PY5.AMOUNT, 0) AS DISB_PERCENTAGE ,
                NVL(PY5.AMOUNT, 0) / 100 * A.PLAFOND AS DISB_AMOUNT ,
                A.PLAFOND ,
                CASE WHEN A.GRACE_DATE >= A.NEXT_PMTDATE AND A.GRACE_DATE IS NOT NULL THEN 0
                     ELSE CASE WHEN A.ICC IN ('1','2','7')
                               THEN CASE WHEN PY1.COMPONENT_TYPE = '1' THEN A.NEXT_PMTDATE -A.PMTDATE
                                         WHEN PY2.COMPONENT_TYPE = '2' THEN A.NEXT_PMTDATE -A.PMTDATE
                                         WHEN PY3.COMPONENT_TYPE = '3' THEN A.NEXT_PMTDATE -A.PMTDATE
                                         ELSE A.NEXT_PMTDATE -A.PMTDATE
                                    END
                               WHEN A.ICC IN('6','9')
                               THEN CASE WHEN V_CALC_IDAYS = 0
                                         THEN ---- ADD YAHYA TO CALCULATE I_DAYS IF MIGRATION IN CUTOFF
                                              CASE WHEN (V_PARAM_CALC_TO_LASTPAYMENT = 0 AND A.ICC IN( '6','9') AND V_COUNTER_PAY = 1) THEN FN_CNT_DAYS_30_360( A.PMTDATE, A.NEXT_PMTDATE)
                                                   ELSE CASE WHEN PY1.COMPONENT_TYPE = '1' THEN NVL(PY1.INCREMENTS, 1) * 30
                                                             WHEN PY2.COMPONENT_TYPE = '2' THEN NVL(PY2.INCREMENTS, 1) * 30
                                                             WHEN PY3.COMPONENT_TYPE = '3' THEN NVL(PY3.INCREMENTS, 1) * 30
                                                             ELSE FN_CNT_DAYS_30_360( A.PMTDATE, A.NEXT_PMTDATE)
                                                        END
                                              END
                                              --------- END ADD YAHYA
                                         WHEN V_CALC_IDAYS = 1 THEN FN_CNT_DAYS_30_360(A.PMTDATE, A.NEXT_PMTDATE)
                                    END
                               ELSE 0 -- NOT IN 1,2,6,7,9
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
                CASE WHEN PY1.COMPONENT_TYPE = '1'
                     THEN CASE WHEN PY1.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY1.INCREMENTS* A.NEXT_COUNTER_PAY )))
                               ELSE ---START ADD YAHYA ---
                                    CASE WHEN PY1.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY1.DATE_END  THEN B.MIN_DATE --- ADD YAHYA
                                         WHEN FN_ISDATE(CASE WHEN PY1.FREQUENCY = 'D' THEN TO_CHAR(A.DATE_START + PY1.INCREMENTS * A.NEXT_COUNTER_PAY,'dd-MON-yyyy')
                                                             ELSE PY1.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE(A.DATE_START, PY1.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy')
                                                        END, 'dd-MON-yyyy') = 1
                                         THEN CASE WHEN PY1.FREQUENCY = 'D' THEN A.DATE_START + PY1.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE TO_DATE(PY1.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE (A.DATE_START, PY1.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy'), 'dd-MON-yyyy')
                                              END
                                         ELSE CASE WHEN PY1.FREQUENCY = 'D' THEN A.DATE_START + PY1.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE FN_PMTDATE(A.DATE_START, PY1.INCREMENTS * A.NEXT_COUNTER_PAY)
                                              END
                                    END
                                    ---END ADD YAHYA----
                          END
                     WHEN PY2.COMPONENT_TYPE = '2'
                     THEN CASE WHEN PY2.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY2.INCREMENTS * A.NEXT_COUNTER_PAY)))
                               ELSE ---START ADD YAHYA ---
                                    CASE WHEN PY2.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY2.DATE_END  THEN B.MIN_DATE --- ADD YAHYA
                                         WHEN FN_ISDATE(CASE WHEN PY2.FREQUENCY = 'D' THEN TO_CHAR(A.DATE_START + PY2.INCREMENTS * A.NEXT_COUNTER_PAY,'dd-MON-yyyy')
                                                             ELSE PY2.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE(A.DATE_START, PY2.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy')
                                                        END, 'dd-MON-yyyy') = 1
                                         THEN CASE WHEN PY2.FREQUENCY = 'D' THEN A.DATE_START + PY2.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE TO_DATE(PY2.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE (A.DATE_START, PY2.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy'), 'dd-MON-yyyy')
                                              END
                                         ELSE CASE WHEN PY2.FREQUENCY = 'D' THEN A.DATE_START + PY2.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE FN_PMTDATE(A.DATE_START, PY2.INCREMENTS * A.NEXT_COUNTER_PAY)
                                              END
                                    END
                                    ---END ADD YAHYA----
                          END
                     WHEN PY3.COMPONENT_TYPE = '3'
                     THEN CASE WHEN PY3.FREQUENCY = 'N' THEN LAST_DAY(FN_PMTDATE (A.DATE_START,(PY3.INCREMENTS * A.NEXT_COUNTER_PAY)))
                                 ELSE ---START ADD YAHYA ---
                                      CASE WHEN PY3.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY3.DATE_END  THEN B.MIN_DATE --- ADD YAHYA
                                           WHEN FN_ISDATE(CASE WHEN PY3.FREQUENCY = 'D' THEN TO_CHAR(A.DATE_START + PY3.INCREMENTS * A.NEXT_COUNTER_PAY,'dd-MON-yyyy')
                                                               ELSE PY3.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE(A.DATE_START, PY3.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy')
                                                          END, 'dd-MON-yyyy') = 1
                                           THEN CASE WHEN PY3.FREQUENCY = 'D' THEN A.DATE_START + PY3.INCREMENTS * A.NEXT_COUNTER_PAY
                                                     ELSE TO_DATE(PY3.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE (A.DATE_START, PY3.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy'), 'dd-MON-yyyy')
                                                END
                                           ELSE CASE WHEN PY3.FREQUENCY = 'D' THEN A.DATE_START + PY3.INCREMENTS * A.NEXT_COUNTER_PAY
                                                     ELSE FN_PMTDATE(A.DATE_START, PY3.INCREMENTS * A.NEXT_COUNTER_PAY)
                                                END
                                      END
                                      ---END ADD YAHYA----
                          END
                     WHEN PY4.COMPONENT_TYPE = '4'
                     THEN CASE WHEN PY4.FREQUENCY = 'N' THEN  LAST_DAY(FN_PMTDATE(A.DATE_START,(PY4.INCREMENTS * A.NEXT_COUNTER_PAY)))
                               ELSE ---START ADD YAHYA ---
                                    CASE WHEN PY4.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY4.DATE_END  THEN B.MIN_DATE --- ADD YAHYA
                                         WHEN FN_ISDATE(CASE WHEN PY4.FREQUENCY = 'D' THEN TO_CHAR(A.DATE_START + PY4.INCREMENTS * A.NEXT_COUNTER_PAY,'dd-MON-yyyy')
                                                             ELSE PY4.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE(A.DATE_START, PY4.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy')
                                                        END, 'dd-MON-yyyy') = 1
                                         THEN CASE WHEN PY4.FREQUENCY = 'D' THEN A.DATE_START + PY4.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE TO_DATE(PY4.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE (A.DATE_START, PY4.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy'), 'dd-MON-yyyy')
                                              END
                                         ELSE CASE WHEN PY4.FREQUENCY = 'D' THEN A.DATE_START + PY4.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE FN_PMTDATE(A.DATE_START, PY4.INCREMENTS * A.NEXT_COUNTER_PAY)
                                              END
                                    END
                                    ---END ADD YAHYA----
                          END
                     WHEN PY0.COMPONENT_TYPE = '0'
                     THEN CASE WHEN PY0.FREQUENCY = 'N'THEN LAST_DAY(FN_PMTDATE(A.DATE_START,(PY0.INCREMENTS * A.NEXT_COUNTER_PAY)))
                               ELSE ---START ADD YAHYA ---
                                    CASE WHEN PY0.DATE_END IS NOT NULL AND A.NEXT_PMTDATE = PY0.DATE_END  THEN B.MIN_DATE --- ADD YAHYA
                                         WHEN FN_ISDATE(CASE WHEN PY0.FREQUENCY = 'D' THEN TO_CHAR(A.DATE_START + PY0.INCREMENTS * A.NEXT_COUNTER_PAY,'dd-MON-yyyy')
                                                             ELSE PY0.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE(A.DATE_START, PY0.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy')
                                                        END, 'dd-MON-yyyy') = 1
                                         THEN CASE WHEN PY0.FREQUENCY = 'D' THEN A.DATE_START + PY0.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE TO_DATE(PY0.PMT_DATE || '-' || TO_CHAR(FN_PMTDATE (A.DATE_START, PY0.INCREMENTS * A.NEXT_COUNTER_PAY), 'MON-yyyy'), 'dd-MON-yyyy')
                                              END
                                         ELSE CASE WHEN PY0.FREQUENCY = 'D' THEN A.DATE_START + PY0.INCREMENTS * A.NEXT_COUNTER_PAY
                                                   ELSE FN_PMTDATE(A.DATE_START, PY0.INCREMENTS * A.NEXT_COUNTER_PAY)
                                              END
                                    END
                                    ---END ADD YAHYA----
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
        FROM    TMP_SCHEDULE_CURR A
        LEFT JOIN TMP_MIN_MAX_DATE B
          ON A.MASTERID = B.MASTERID ---ADD YAHYA
        LEFT JOIN TMP_PY0 PY0
          ON A.MASTERID = PY0.MASTERID
          AND A.NEXT_PMTDATE BETWEEN PY0.DATE_START AND PY0.DATE_END
          --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY0.DATE_START),PY0.INCREMENTS) = 0
          --AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY0.DATE_START),0), PY0.INCREMENTS) = 0
	  AND (
	  --(PY0.FREQUENCY = 'D' AND MOD(FN_CNT_DAYS_30_360(PY0.DATE_START, A.NEXT_PMTDATE), PY0.INCREMENTS) = 0)
	  --OR
	  (MOD(MONTHS_BETWEEN(LAST_DAY(A.NEXT_PMTDATE),LAST_DAY(PY0.DATE_START)), PY0.INCREMENTS) = 0))
        LEFT JOIN TMP_PY1 PY1
          ON A.MASTERID = PY1.MASTERID
          AND A.NEXT_PMTDATE BETWEEN PY1.DATE_START AND PY1.DATE_END
          --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY1.DATE_START),PY1.INCREMENTS) = 0
          --AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY1.DATE_START),0), PY1.INCREMENTS) = 0
	  AND (
	  --(PY1.FREQUENCY = 'D' AND MOD(FN_CNT_DAYS_30_360(PY1.DATE_START, A.NEXT_PMTDATE), PY1.INCREMENTS) = 0)
	  --OR
	  (MOD(MONTHS_BETWEEN(LAST_DAY(A.NEXT_PMTDATE),LAST_DAY(PY1.DATE_START)), PY1.INCREMENTS) = 0))
        LEFT JOIN TMP_PY2 PY2
          ON A.MASTERID = PY2.MASTERID
          AND A.NEXT_PMTDATE BETWEEN PY2.DATE_START AND PY2.DATE_END
          --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
          --AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY2.DATE_START),0), PY2.INCREMENTS) = 0
	  AND (
        --(PY2.FREQUENCY = 'D' AND MOD(FN_CNT_DAYS_30_360(PY2.DATE_START, A.NEXT_PMTDATE), PY2.INCREMENTS) = 0)
        --OR
        (MOD(MONTHS_BETWEEN(LAST_DAY(A.NEXT_PMTDATE),LAST_DAY(PY2.DATE_START)), PY2.INCREMENTS) = 0))
        LEFT JOIN TMP_PY3 PY3
          ON A.MASTERID = PY3.MASTERID
          AND A.NEXT_PMTDATE BETWEEN PY3.DATE_START AND PY3.DATE_END
          --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
          --AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY3.DATE_START),0),PY3.INCREMENTS) = 0
	  AND (
	  --(PY3.FREQUENCY = 'D' AND MOD(FN_CNT_DAYS_30_360(PY3.DATE_START, A.NEXT_PMTDATE), PY3.INCREMENTS) = 0)
	  --OR
	  (MOD(MONTHS_BETWEEN(LAST_DAY(A.NEXT_PMTDATE),LAST_DAY(PY3.DATE_START)), PY3.INCREMENTS) = 0))
        LEFT JOIN TMP_PY4 PY4
          ON A.MASTERID = PY4.MASTERID
          AND A.NEXT_PMTDATE BETWEEN PY4.DATE_START AND PY4.DATE_END
          --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
          --AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY4.DATE_START),0), PY4.INCREMENTS) = 0
	  AND (
	  --(PY4.FREQUENCY = 'D' AND MOD(FN_CNT_DAYS_30_360(PY4.DATE_START, A.NEXT_PMTDATE), PY4.INCREMENTS) = 0)
	  --OR
	  (MOD(MONTHS_BETWEEN(LAST_DAY(A.NEXT_PMTDATE),LAST_DAY(PY4.DATE_START)), PY4.INCREMENTS) = 0))
        LEFT JOIN TMP_PY5 PY5
          ON A.MASTERID = PY5.MASTERID
          AND A.NEXT_PMTDATE BETWEEN PY5.DATE_START AND PY5.DATE_END
          --AND MOD(DATEDIFF(A.NEXT_PMTDATE, PY2.DATE_START),PY2.INCREMENTS) = 0
          --AND MOD(ROUND(MONTHS_BETWEEN(A.NEXT_PMTDATE,PY5.DATE_START),0), PY5.INCREMENTS) = 0
	  AND (
	  --(PY5.FREQUENCY = 'D' AND MOD(FN_CNT_DAYS_30_360(PY5.DATE_START, A.NEXT_PMTDATE), PY5.INCREMENTS) = 0)
	  --OR
	  (MOD(MONTHS_BETWEEN(LAST_DAY(A.NEXT_PMTDATE),LAST_DAY(PY5.DATE_START)), PY5.INCREMENTS) = 0))
        WHERE   A.TENOR >= V_COUNTER_PAY
        AND A.PMTDATE <= A.DATE_END
        AND A.OSPRN > 0 ;

        COMMIT;

        INSERT /*+ PARALLEL(12) */ INTO TMP_SCHEDULE_PREV_TEMP
        SELECT /*+ PARALLEL(12) */ * FROM TMP_SCHEDULE_PREV ;
        COMMIT;

        EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_SCHEDULE_CURR');

       INSERT /*+ PARALLEL(12) */ INTO TMP_SCHEDULE_CURR
       ( MASTERID ,
         ACCOUNT_NUMBER ,
         INTEREST_RATE ,
         PMTDATE ,
         OSPRN ,
         PRINCIPAL ,
         INTEREST ,
         DISB_PERCENTAGE ,
         DISB_AMOUNT ,
         PLAFOND ,
         I_DAYS ,
         COUNTER ,
         DATE_START ,
         DATE_END ,
         TENOR ,
         PAYMENT_CODE ,
         ICC ,
         NEXT_PMTDATE ,
         NEXT_COUNTER_PAY ,
         SCH_FLAG ,
         GRACE_DATE
        ) --bibd for grace period
        SELECT  /*+ PARALLEL(12) */ A.MASTERID ,
                A.ACCOUNT_NUMBER ,
                A.INTEREST_RATE ,
                A.PMTDATE ,
                A.OSPRN ,
                A.PRINCIPAL ,
                A.INTEREST ,
                A.DISB_PERCENTAGE ,
                A.DISB_AMOUNT ,
                A.PLAFOND ,
								A.I_DAYS ,
                A.COUNTER ,
                A.DATE_START ,
                A.DATE_END ,
                A.TENOR ,
                A.PAYMENT_CODE ,
                A.ICC ,
                CASE WHEN A.NEXT_PMTDATE > A.DATE_END THEN A.DATE_END ------ADD YAHYA 20180312
                     WHEN LAST_DAY(A.NEXT_PMTDATE) = LAST_DAY(A.DATE_END) THEN A.DATE_END
                     ELSE A.NEXT_PMTDATE
                END ,
                A.NEXT_COUNTER_PAY + 1 ,
                A.SCH_FLAG ,
                A.GRACE_DATE --bibd for grace period
                FROM    TMP_SCHEDULE_PREV A ;

        COMMIT;

        INSERT /*+ PARALLEL(12) */ INTO TMP_SCHEDULE_CURR_TEMP
        SELECT /*+ PARALLEL(12) */ * FROM    TMP_SCHEDULE_CURR ;
        COMMIT;

        INSERT /*+ PARALLEL(12) */ INTO IFRS_PAYM_SCHD
        ( MASTERID ,
          --ACCOUNT_NUMBER ,
          PMTDATE ,
          INTEREST_RATE ,
          OSPRN ,
          PRINCIPAL ,
          INTEREST ,
          DISB_PERCENTAGE ,
          DISB_AMOUNT ,
          PLAFOND ,
          I_DAYS ,
          ICC ,
          COUNTER ,
          DOWNLOAD_DATE ,
          SCH_FLAG ,
          GRACE_DATE --bibd for grace period
        )
        SELECT  /*+ PARALLEL(12) */ MASTERID ,
                --ACCOUNT_NUMBER ,
                PMTDATE ,
                INTEREST_RATE ,
                OSPRN ,
                PRINCIPAL ,
                INTEREST ,
                DISB_PERCENTAGE ,
                DISB_AMOUNT ,
                PLAFOND ,
								I_DAYS ,
								ICC ,
                COUNTER ,
                V_CURRDATE ,
                SCH_FLAG ,
                GRACE_DATE --bibd for grace period
        FROM    TMP_SCHEDULE_CURR ;

      COMMIT;
      /* remove outside loop 20160524
                INSERT  INTO IFRS_PAYM_CORE_SRC
                        ( MASTERID ,
						  ACCTNO ,
						  PMT_DATE ,
						  interest_rate ,
						  I_DAYS,
						  PRN_AMT ,
						  INT_AMT ,
						  disb_percentage ,
						  disb_amount ,
						  plafond ,
						  OS_PRN ,
						  COUNTER ,
						  ICC ,
						  grace_date
                        )
                        SELECT  SCH.ACC_MSTR_ID ,
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
                        FROM    TT_SCHEDULE_CURR SCH
                                INNER JOIN TT_IFRS_SCHEDULE_MAIN MA ON SCH.ACC_MSTR_ID = MA.ACC_MSTR_ID ;
        remove outside loop 20160524 */

        --COMMIT;
    END LOOP;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,3 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,1 ,'PAYMENT SCHEDULE EXCEPTIONS') ;

    COMMIT;
    EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_SCH_MAX');
    EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_SCHD') ;

    INSERT /*+ PARALLEL(12) */ INTO TMP_SCH_MAX
    SELECT /*+ PARALLEL(12) */ MASTERID , MAX(PMTDATE) AS MAX_PMTDATE
    FROM    IFRS_PAYM_SCHD
    GROUP BY MASTERID ;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_SCHD
    SELECT /*+ PARALLEL(12) */ A.MASTERID ,A.OSPRN
    FROM    IFRS_PAYM_SCHD A
    INNER JOIN TMP_SCH_MAX B
      ON A.MASTERID = B.MASTERID
      AND A.PMTDATE = B.MAX_PMTDATE ;

    COMMIT;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,3 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,2 ,'INSERT IFRS_EXCEPTION_DETAILS') ;

    COMMIT;

    DELETE /*+ PARALLEL(12) */ IFRS_EXCEPTION_DETAILS
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND EXCEPTION_CODE LIKE 'PAY-%' ;

    COMMIT;

    /*PAY-1001 - Last Payment Outstanding Principal should be equal to zero.*/
    V_ACTIVE := 0;
    V_ACTION := 0;
    V_ECODE := 'PAY-1001';
    SELECT IS_ACTIVE, DEFAULT_ACTION INTO V_ACTIVE, V_ACTION FROM IFRS_EXCEPTION_CODE WHERE EXCEPTION_CODE = V_ECODE;
    IF V_ACTIVE = 1 THEN
        INSERT /*+ PARALLEL(12) */ INTO IFRS_EXCEPTION_DETAILS
          (SESSION_ID, DOWNLOAD_DATE, UNIQUE_ID, EXCEPTION_CODE, VALUE, ACTION, ACTION_DATE)
        SELECT    /*+ PARALLEL(12) */ V_SESSIONID,
                  V_CURRDATE,
                  PMA.ACCOUNT_NUMBER,
                  V_ECODE AS EXCEPTION_CODE,
                  CAST(SCH.OSPRN AS VARCHAR2(50)) AS VALUE,
                  V_ACTION AS ACTION,
                  SYSDATE AS ACTION_DATE
        FROM    IFRS_MASTER_ACCOUNT PMA
        INNER JOIN TMP_SCHD SCH
          ON PMA.MASTERID = SCH.MASTERID
          AND PMA.DOWNLOAD_DATE = V_CURRDATE
          --AND PMA.PMT_SCH_STATUS = 'Y'
          AND NVL(SCH.OSPRN, 0) <> 0 ;
    END IF;

    COMMIT;

    -- EXCEPTIONS IF THERE IS PMTDATE IS NULL
    EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_SCHD') ;

	INSERT /*+ PARALLEL(12) */ INTO TMP_SCHD(MASTERID)
		SELECT /*+ PARALLEL(12) */ MASTERID
    FROM IFRS_PAYM_SCHD
		WHERE PMTDATE IS NULL;

    COMMIT;

    /*PAY-1002 - Payment Date should not be blank.*/
    V_ACTIVE := 0;
    V_ACTION := 0;
    V_ECODE := 'PAY-1002';
    SELECT IS_ACTIVE, DEFAULT_ACTION INTO V_ACTIVE, V_ACTION FROM IFRS_EXCEPTION_CODE WHERE EXCEPTION_CODE = V_ECODE;
    IF V_ACTIVE = 1 THEN
        INSERT /*+ PARALLEL(12) */ INTO IFRS_EXCEPTION_DETAILS
          (SESSION_ID, DOWNLOAD_DATE, UNIQUE_ID, EXCEPTION_CODE, VALUE, ACTION, ACTION_DATE)
        SELECT    /*+ PARALLEL(12) */ V_SESSIONID,
                  V_CURRDATE,
                  PMA.ACCOUNT_NUMBER,
                  V_ECODE AS EXCEPTION_CODE,
                  ' ' AS VALUE,
                  V_ACTION AS ACTION,
                  SYSDATE AS ACTION_DATE
        FROM    IFRS_MASTER_ACCOUNT PMA
        INNER JOIN TMP_SCHD SCH
          ON PMA.MASTERID = SCH.MASTERID
          AND PMA.DOWNLOAD_DATE = V_CURRDATE;
    END IF;
    COMMIT;

    EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_SCHD');

	INSERT /*+ PARALLEL(12) */ INTO TMP_SCHD (MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
    FROM    ( SELECT    MASTERID ,PMTDATE
              FROM      IFRS_PAYM_SCHD
							GROUP BY  MASTERID ,PMTDATE
							HAVING    COUNT(1) > 1
            ) A;

    COMMIT;

    /*PAY-1003 - Payment Date should not be duplicate.*/
    V_ACTIVE := 0;
    V_ACTION := 0;
    V_ECODE := 'PAY-1003';
    SELECT IS_ACTIVE, DEFAULT_ACTION INTO V_ACTIVE, V_ACTION FROM IFRS_EXCEPTION_CODE WHERE EXCEPTION_CODE = V_ECODE;
    IF V_ACTIVE = 1 THEN
        INSERT /*+ PARALLEL(12) */ INTO IFRS_EXCEPTION_DETAILS
          (SESSION_ID, DOWNLOAD_DATE, UNIQUE_ID, EXCEPTION_CODE, VALUE, ACTION, ACTION_DATE)
        SELECT    /*+ PARALLEL(12) */ V_SESSIONID,
                  V_CURRDATE,
                  PMA.ACCOUNT_NUMBER,
                  V_ECODE AS EXCEPTION_CODE,
                  ' ' AS VALUE,
                  V_ACTION AS ACTION,
                  SYSDATE AS ACTION_DATE
        FROM    IFRS_MASTER_ACCOUNT PMA
        INNER JOIN TMP_SCHD SCH
          ON PMA.MASTERID = SCH.MASTERID
          AND PMA.DOWNLOAD_DATE = V_CURRDATE;
    END IF;

    COMMIT;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,3 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,3 ,'UPDATE IFRS_MASTER_ACCOUNT') ;

    COMMIT;

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,4 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP ,'IFRS ENGINE' ,1 ,'INSERT IFRS_PAYM_CORE_SRC') ;

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_PAYM_SCHD_SRC','AMT','Y');

    INSERT  INTO IFRS_BATCH_LOG_DETAILS( DOWNLOAD_DATE ,BATCH_ID ,BATCH_ID_HEADER ,BATCH_NAME ,PROCESS_NAME ,START_DATE,END_DATE ,CREATEDBY ,COUNTER ,REMARKS)
    VALUES  ( V_CURRDATE ,99 ,V_LOG_ID ,'PMTSCHD' ,'SP_IFRS_PAYMENT_SCHEDULE' ,SYSTIMESTAMP,SYSTIMESTAMP ,'IFRS ENGINE' ,99 ,'JUST ENDED') ;

    COMMIT;
    END ;