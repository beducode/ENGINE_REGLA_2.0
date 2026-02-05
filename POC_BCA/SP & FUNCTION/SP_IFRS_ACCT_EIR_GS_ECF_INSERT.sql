CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_GS_ECF_INSERT
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  V_VI NUMBER(19);
  V_ROUND NUMBER(10);
  V_FUNCROUND NUMBER(10);
BEGIN


    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM    IFRS_PRC_DATE_AMORT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'');

    COMMIT;


    BEGIN
      SELECT CAST(VALUE1 AS NUMBER(10))
           , CAST(VALUE2 AS NUMBER(10))
      INTO V_ROUND, V_FUNCROUND
      FROM TBLM_COMMONCODEDETAIL
      WHERE COMMONCODE = 'SCM003';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_ROUND := 6;
        V_FUNCROUND:=1;
    END;


    --20171016 set default value
    IF V_ROUND IS NULL THEN V_ROUND:=6; END IF;
    IF V_FUNCROUND IS NULL THEN V_FUNCROUND:=1; END IF;

    -- insert initial row prevdate=pmtdate
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_ECF1';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_ECF2';


    -- prepare index
    --execute immediate 'drop index psak_eir_paym_idx1';
    --execute immediate 'create index psak_eir_paym_idx1 on IFRS_ACCT_EIR_PAYM(masterid,pmt_date,prev_pmt_date)';
    --execute immediate 'drop index psak_eir_cf_ecf_idx1';
    --execute immediate 'create index psak_eir_cf_ecf_idx1 on IFRS_ACCT_EIR_CF_ECF(masterid)';
    --execute immediate 'drop index psak_goal_seek_result_idx1';
    --execute immediate 'create index psak_goal_seek_result_idx1 on IFRS_ACCT_EIR_GOAL_SEEK_RESULT(masterid,DOWNLOAD_DATE)';

    INSERT  INTO IFRS_ACCT_EIR_ECF2
    ( MASTERID ,
      DOWNLOAD_DATE ,
      N_LOAN_AMT ,
      N_INT_RATE ,
      N_EFF_INT_RATE ,
      STARTAMORTDATE ,
      ENDAMORTDATE ,
      GRACEDATE ,
      DISB_PERCENTAGE ,
      DISB_AMOUNT ,
      PLAFOND ,
      PAYMENTCODE ,
      INTCALCCODE ,
      PAYMENTTERM ,
      ISGRACE ,
      PREV_PMT_DATE ,
      PMT_DATE ,
      I_DAYS ,
      I_DAYS2 ,
      N_OSPRN_PREV ,
      N_INSTALLMENT ,
      N_PRN_PAYMENT ,
      N_INT_PAYMENT ,
      N_OSPRN ,
      N_FAIRVALUE_PREV ,
      N_EFF_INT_AMT ,
      N_FAIRVALUE ,
      N_UNAMORT_AMT_PREV ,
      N_AMORT_AMT ,
      N_UNAMORT_AMT ,
      N_COST_UNAMORT_AMT_PREV ,
      N_COST_AMORT_AMT ,
      N_COST_UNAMORT_AMT ,
      N_FEE_UNAMORT_AMT_PREV ,
      N_FEE_AMORT_AMT ,
      N_FEE_UNAMORT_AMT ,
      N_FEE_AMT ,
      N_COST_AMT
    )
    SELECT  A.MASTERID ,
            V_CURRDATE ,
            A.N_LOAN_AMT ,
            A.N_INT_RATE ,
            C.EIR ,
            A.STARTAMORTDATE ,
            A.ENDAMORTDATE ,
            A.GRACEDATE ,
            A.DISB_PERCENTAGE ,
            A.DISB_AMOUNT ,
            A.PLAFOND ,
            A.PAYMENTCODE ,
            A.INTCALCCODE ,
            A.PAYMENTTERM ,
            A.ISGRACE ,
            A.PREV_PMT_DATE ,
            A.PMT_DATE ,
            A.I_DAYS ,
            A.I_DAYS ,
            A.N_OSPRN_PREV ,
            A.N_INSTALLMENT ,
            A.N_PRN_PAYMENT ,
            A.N_INT_PAYMENT ,
            A.N_OSPRN ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN B.BENEFIT
                 ELSE B.COST_AMT + B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT,0) - NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417
                 END + A.N_OSPRN N_FAIRVALUE_PREV ,
            0 N_EFF_INT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN B.BENEFIT
                 ELSE B.COST_AMT + B.FEE_AMT- NVL(B.GAIN_LOSS_FEE_AMT,0)- NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417
                 END + A.N_OSPRN N_FAIRVALUE ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN B.BENEFIT
                 ELSE B.COST_AMT + B.FEE_AMT- NVL(B.GAIN_LOSS_FEE_AMT,0)- NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417
                 END N_UNAMORT_AMT_PREV ,
            0 N_AMORT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN B.BENEFIT
                 ELSE B.COST_AMT + B.FEE_AMT- NVL(B.GAIN_LOSS_FEE_AMT,0)- NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417
                 END N_UNAMORT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN CASE WHEN B.BENEFIT > 0 THEN B.BENEFIT ELSE 0 END
                 ELSE B.COST_AMT - NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417 c
                 END N_COST_UNAMORT_AMT_PREV ,
            0 N_COST_AMORT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN CASE WHEN B.BENEFIT > 0 THEN B.BENEFIT ELSE 0 END
                 ELSE B.COST_AMT - NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417 c
                 END N_COST_UNAMORT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN CASE WHEN B.BENEFIT < 0 THEN B.BENEFIT ELSE 0 END
                 ELSE B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT,0)	--201801417 f
                 END N_FEE_UNAMORT_AMT_PREV ,
            0 N_FEE_AMORT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN CASE WHEN B.BENEFIT < 0 THEN B.BENEFIT ELSE 0 END
                 ELSE B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT,0)	--201801417 f
                 END N_FEE_UNAMORT_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN CASE WHEN B.BENEFIT < 0 THEN B.BENEFIT ELSE 0 END
                 ELSE B.FEE_AMT - NVL(B.GAIN_LOSS_FEE_AMT,0)	--201801417 f
                 END N_FEE_AMT ,
            CASE WHEN B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL AND B.BENEFIT IS NOT NULL THEN CASE WHEN B.BENEFIT > 0 THEN B.BENEFIT ELSE 0 END
                 ELSE B.COST_AMT - NVL(B.GAIN_LOSS_COST_AMT,0)	--201801417
                 END N_COST_AMT_PREV
    FROM    IFRS_ACCT_EIR_PAYM A
    JOIN IFRS_ACCT_EIR_CF_ECF B ON B.MASTERID = A.MASTERID
    JOIN IFRS_ACCT_EIR_GOAL_SEEK_RESULT C ON C.MASTERID = A.MASTERID AND C.DOWNLOAD_DATE = V_CURRDATE
    WHERE   A.PMT_DATE = A.PREV_PMT_DATE;
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'1');

    INSERT  INTO IFRS_ACCT_EIR_ECF
    ( MASTERID ,
      DOWNLOAD_DATE ,
      N_LOAN_AMT ,
      N_INT_RATE ,
      N_EFF_INT_RATE ,
      STARTAMORTDATE ,
      ENDAMORTDATE ,
      GRACEDATE ,
      DISB_PERCENTAGE ,
      DISB_AMOUNT ,
      PLAFOND ,
      PAYMENTCODE ,
      INTCALCCODE ,
      PAYMENTTERM ,
      ISGRACE ,
      PREV_PMT_DATE ,
      PMT_DATE ,
      I_DAYS ,
      I_DAYS2 ,
      N_OSPRN_PREV ,
      N_INSTALLMENT ,
      N_PRN_PAYMENT ,
      N_INT_PAYMENT ,
      N_OSPRN ,
      N_FAIRVALUE_PREV ,
      N_EFF_INT_AMT ,
      N_FAIRVALUE ,
      N_UNAMORT_AMT_PREV ,
      N_AMORT_AMT ,
      N_UNAMORT_AMT ,
      N_COST_UNAMORT_AMT_PREV ,
      N_COST_AMORT_AMT ,
      N_COST_UNAMORT_AMT ,
      N_FEE_UNAMORT_AMT_PREV ,
      N_FEE_AMORT_AMT ,
      N_FEE_UNAMORT_AMT
    )
    SELECT  MASTERID ,
            DOWNLOAD_DATE ,
            N_LOAN_AMT ,
            N_INT_RATE ,
            N_EFF_INT_RATE ,
            STARTAMORTDATE ,
            ENDAMORTDATE ,
            GRACEDATE ,
            DISB_PERCENTAGE ,
            DISB_AMOUNT ,
            PLAFOND ,
            PAYMENTCODE ,
            INTCALCCODE ,
            PAYMENTTERM ,
            ISGRACE ,
            PREV_PMT_DATE ,
            PMT_DATE ,
            I_DAYS ,
            PMT_DATE - PREV_PMT_DATE AS I_DAYS2 ,
            N_OSPRN_PREV ,
            N_INSTALLMENT ,
            N_PRN_PAYMENT ,
            N_INT_PAYMENT ,
            N_OSPRN ,
            N_FAIRVALUE_PREV ,
            N_EFF_INT_AMT ,
            N_FAIRVALUE ,
            N_UNAMORT_AMT_PREV ,
            N_AMORT_AMT ,
            N_UNAMORT_AMT ,
            N_COST_UNAMORT_AMT_PREV ,
            N_COST_AMORT_AMT ,
            N_COST_UNAMORT_AMT ,
            N_FEE_UNAMORT_AMT_PREV ,
            N_FEE_AMORT_AMT ,
            N_FEE_UNAMORT_AMT
    FROM    IFRS_ACCT_EIR_ECF2;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'2');
    COMMIT;

    -- prepare temp table for looping
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T9';
    COMMIT;

    INSERT  INTO TMP_T9
    ( MASTERID ,PMTDATE
    )
    SELECT  MASTERID ,PMT_DATE
    FROM    IFRS_ACCT_EIR_PAYM
    WHERE   PMT_DATE = PREV_PMT_DATE;

    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_ECF_T2';

    INSERT  INTO IFRS_ACCT_EIR_ECF_T2
    ( MASTERID ,PMTDATE
    )
    SELECT  A.MASTERID ,MIN(A.PMT_DATE) AS PMTDATE
    FROM    IFRS_ACCT_EIR_PAYM A
    JOIN TMP_T9 B ON B.MASTERID = A.MASTERID AND A.PMT_DATE > B.PMTDATE
    GROUP BY A.MASTERID;

    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'3');

    COMMIT;

    --execute immediate 'drop index psak_eir_paym_idx2';
    --execute immediate 'create index psak_eir_paym_idx2 on IFRS_ACCT_EIR_PAYM(masterid,pmt_date)';


    SELECT  COUNT(*) INTO V_VI
    FROM    IFRS_ACCT_EIR_ECF_T2;

    WHILE V_VI > 0
    LOOP --loop

        INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
        VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'4');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_ECF1';

        COMMIT;

        -- prepare index
        --execute immediate 'drop index PSAK_EIR_ECF_T2_IDX1';
        --execute immediate 'create index PSAK_EIR_ECF_T2_IDX1 on IFRS_ACCT_EIR_ECF_T2(masterid,pmtdate)';
        --execute immediate 'drop index psak_eir_ecf2_IDX1';
        --execute immediate 'create index psak_eir_ecf2_IDX1 on IFRS_ACCT_EIR_ECF2(masterid)';

        INSERT  INTO IFRS_ACCT_EIR_ECF1
        ( MASTERID ,
          DOWNLOAD_DATE ,
          N_LOAN_AMT ,
          N_INT_RATE ,
          N_EFF_INT_RATE ,
          STARTAMORTDATE ,
          ENDAMORTDATE ,
          GRACEDATE ,
          DISB_PERCENTAGE ,
          DISB_AMOUNT ,
          PLAFOND ,
          PAYMENTCODE ,
          INTCALCCODE ,
          PAYMENTTERM ,
          ISGRACE ,
          PREV_PMT_DATE ,
          PMT_DATE ,
          I_DAYS ,
          I_DAYS2 ,
          N_OSPRN_PREV ,
          N_INSTALLMENT ,
          N_PRN_PAYMENT ,
          N_INT_PAYMENT ,
          N_OSPRN ,
          N_FAIRVALUE_PREV ,
          N_EFF_INT_AMT ,
          N_FAIRVALUE ,
          N_UNAMORT_AMT_PREV ,
          N_AMORT_AMT ,
          N_UNAMORT_AMT ,
          N_COST_UNAMORT_AMT_PREV ,
          N_COST_AMORT_AMT ,
          N_COST_UNAMORT_AMT ,
          N_FEE_UNAMORT_AMT_PREV ,
          N_FEE_AMORT_AMT ,
          N_FEE_UNAMORT_AMT ,
          N_FEE_AMT ,
          N_COST_AMT
        )
        SELECT  A.MASTERID ,
                V_CURRDATE ,
                A.N_LOAN_AMT ,
                A.N_INT_RATE ,
                C.N_EFF_INT_RATE ,
                A.STARTAMORTDATE ,
                A.ENDAMORTDATE ,
                A.GRACEDATE ,
                A.DISB_PERCENTAGE ,
                A.DISB_AMOUNT ,
                A.PLAFOND ,
                A.PAYMENTCODE ,
                A.INTCALCCODE ,
                A.PAYMENTTERM ,
                A.ISGRACE ,
                A.PREV_PMT_DATE ,
                A.PMT_DATE ,
                A.I_DAYS ,
                A.I_DAYS ,
                A.N_OSPRN_PREV ,
                A.N_INSTALLMENT ,
                A.N_PRN_PAYMENT ,
                A.N_INT_PAYMENT ,
                A.N_OSPRN ,
                C.N_FAIRVALUE N_FAIRVALUE_PREV ,
              /*  CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                        THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE - CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                      ELSE
                      */ROUND(CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS/360* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                  WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS / 365 * C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                  WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE / C.N_INT_RATE * A.N_INT_PAYMENT
                                                  /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                  WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                       THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                       ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                           +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                  END
                                                  /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                  ELSE ( 30 * A.M / 360 * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE )
                                                  END, V_ROUND)
                                 /* END */
                                  AS N_EFF_INT_AMT ,
                C.N_FAIRVALUE - A.N_PRN_PAYMENT +
							/*	CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                      THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE -
													CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                               ELSE */ROUND(CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS/ 360* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                               WHEN A.INTCALCCODE IN ( '2', '3' )THEN A.I_DAYS/ 365 * C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                               /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                               WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                            THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                            ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                        END
                                               /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                               WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT
                                               ELSE ( 30 * A.M/ 360* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE )
                                               END, V_ROUND) /*END*/ - A.N_INT_PAYMENT + COALESCE(A.DISB_AMOUNT,0) AS N_FAIRVALUE ,

                C.N_UNAMORT_AMT N_UNAMORT_AMT_PREV ,

                /*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                      THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE -
                           CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                ELSE*/ ROUND(CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS/ 360* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS/ 365 * C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                             THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                             ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                 +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                         END
                                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT
                                                ELSE ( 30 * A.M/ 360* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE )
                                                END, V_ROUND)/* END*/- A.N_INT_PAYMENT AS N_AMORT_AMT ,
                C.N_UNAMORT_AMT +
							/*	CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                      THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE -
													 CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                ELSE*/ ROUND(CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS / 360 * C.N_EFF_INT_RATE / 100 * C.N_FAIRVALUE
                                                WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS/ 365 * C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                             THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                             ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                 +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                         END
                                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                WHEN A.INTCALCCODE = '4'THEN C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT
                                                ELSE ( 30 * A.M/ 360* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE )
                                                END, V_ROUND) /*END*/ - A.N_INT_PAYMENT AS N_UNAMORT_AMT ,

                C.N_COST_UNAMORT_AMT N_COST_UNAMORT_AMT_PREV ,

                CASE WHEN C.N_FEE_AMT + C.N_COST_AMT = 0 THEN 0
                     ELSE (/*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                  THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE - CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                            ELSE*/ ROUND(CASE WHEN A.INTCALCCODE IN ('1', '6' )THEN A.I_DAYS/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                              WHEN A.INTCALCCODE IN ('2', '3' )THEN A.I_DAYS/ 365* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                              /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                              WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                                           THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                                           ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                               +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                       END
                                                              /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                              WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT
                                                              ELSE ( 30* A.M/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE )
                                                        END, V_ROUND) /*END*/ - A.N_INT_PAYMENT
                            ) * C.N_COST_AMT / ( C.N_FEE_AMT+ C.N_COST_AMT ) END AS N_COST_AMORT_AMT ,

                C.N_COST_UNAMORT_AMT
                + CASE WHEN C.N_FEE_AMT + C.N_COST_AMT = 0 THEN 0
                       ELSE(/* CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                          THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE
                                               - CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                 ELSE*/ ROUND(CASE WHEN A.INTCALCCODE IN ('1', '6' )THEN A.I_DAYS/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                 WHEN A.INTCALCCODE IN ('2', '3' )THEN A.I_DAYS/ 365* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                              THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                              ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                  +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                         END
                                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 WHEN A.INTCALCCODE = '4' THEN (C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT)
                                                 ELSE (30* A.M/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE)
                                                 END, V_ROUND
                                            )
                                /* END*/ - A.N_INT_PAYMENT )
                    * C.N_COST_AMT / ( C.N_FEE_AMT + C.N_COST_AMT ) END AS N_COST_UNAMORT_AMT ,

                C.N_FEE_UNAMORT_AMT N_FEE_UNAMORT_AMT_PREV ,

                CASE WHEN C.N_FEE_AMT + C.N_COST_AMT = 0 THEN 0
                     ELSE (/*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                  THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)* C.N_FAIRVALUE/12/(A.PMT_DATE -
                                       CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                ELSE*/
                                ROUND(CASE WHEN A.INTCALCCODE IN ('1', '6' )THEN A.I_DAYS / 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                WHEN A.INTCALCCODE IN ('2', '3' )THEN A.I_DAYS/ 365* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                 WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                              THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                              ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                  +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                         END
                                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT
                                                ELSE ( 30* A.M/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE )
                                                END, V_ROUND)/* END*/ - A.N_INT_PAYMENT
                           ) * C.N_FEE_AMT / ( C.N_FEE_AMT+ C.N_COST_AMT ) END AS N_FEE_AMORT_AMT ,

                C.N_FEE_UNAMORT_AMT
                + CASE WHEN C.N_FEE_AMT + C.N_COST_AMT = 0 THEN 0
                       ELSE (/*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                    THEN C.N_EFF_INT_RATE/100*(A.PMT_DATE -A.STARTAMORTDATE)*C.N_FAIRVALUE/12/(A.PMT_DATE -
                                         CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE)THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                              ELSE */ROUND(CASE WHEN A.INTCALCCODE IN ('1', '6' ) THEN A.I_DAYS/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                                WHEN A.INTCALCCODE IN ('2', '3' )THEN A.I_DAYS/ 365* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE
                                                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                                WHEN A.INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM A.PREV_PMT_DATE)= EXTRACT(YEAR FROM A.PMT_DATE)
                                                                                                             THEN A.I_DAYS/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE
                                                                                                             ELSE ((FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)/ (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                                 +((A.I_DAYS - (FN_LASTDAY_OF_YEAR(A.PREV_PMT_DATE)-A.PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(A.PMT_DATE)* C.N_EFF_INT_RATE / 100* C.N_FAIRVALUE)
                                                                                                        END
                                                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                                              WHEN A.INTCALCCODE = '4' THEN C.N_EFF_INT_RATE/ C.N_INT_RATE* A.N_INT_PAYMENT
                                                              ELSE ( 30* A.M/ 360* C.N_EFF_INT_RATE/ 100* C.N_FAIRVALUE )
                                                              END, V_ROUND) /*END*/ - A.N_INT_PAYMENT
                             )* C.N_FEE_AMT / ( C.N_FEE_AMT+ C.N_COST_AMT )END AS N_FEE_UNAMORT_AMT ,
                C.N_FEE_AMT ,
                C.N_COST_AMT
        FROM    IFRS_ACCT_EIR_PAYM A
        JOIN IFRS_ACCT_EIR_ECF_T2 B ON B.MASTERID = A.MASTERID AND B.PMTDATE = A.PMT_DATE
        JOIN IFRS_ACCT_EIR_ECF2 C ON C.MASTERID = B.MASTERID;

        COMMIT;



        INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
        VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'5');

        -- insert to ecf
        INSERT  INTO IFRS_ACCT_EIR_ECF
        ( MASTERID ,
          DOWNLOAD_DATE ,
          N_LOAN_AMT ,
          N_INT_RATE ,
          N_EFF_INT_RATE ,
          STARTAMORTDATE ,
          ENDAMORTDATE ,
          GRACEDATE ,
          DISB_PERCENTAGE ,
          DISB_AMOUNT ,
          PLAFOND ,
          PAYMENTCODE ,
          INTCALCCODE ,
          PAYMENTTERM ,
          ISGRACE ,
          PREV_PMT_DATE ,
          PMT_DATE ,
          I_DAYS ,
          I_DAYS2 ,
          N_OSPRN_PREV ,
          N_INSTALLMENT ,
          N_PRN_PAYMENT ,
          N_INT_PAYMENT ,
          N_OSPRN ,
          N_FAIRVALUE_PREV ,
          N_EFF_INT_AMT ,
          N_FAIRVALUE ,
          N_UNAMORT_AMT_PREV ,
          N_AMORT_AMT ,
          N_UNAMORT_AMT ,
          N_COST_UNAMORT_AMT_PREV ,
          N_COST_AMORT_AMT ,
          N_COST_UNAMORT_AMT ,
          N_FEE_UNAMORT_AMT_PREV ,
          N_FEE_AMORT_AMT ,
          N_FEE_UNAMORT_AMT
        )
        SELECT  MASTERID ,
                DOWNLOAD_DATE ,
                N_LOAN_AMT ,
                N_INT_RATE ,
                N_EFF_INT_RATE ,
                STARTAMORTDATE ,
                ENDAMORTDATE ,
                GRACEDATE ,
                DISB_PERCENTAGE ,
                DISB_AMOUNT ,
                PLAFOND ,
                PAYMENTCODE ,
                INTCALCCODE ,
                PAYMENTTERM ,
                ISGRACE ,
                PREV_PMT_DATE ,
                PMT_DATE ,
                I_DAYS ,
                PMT_DATE - PREV_PMT_DATE AS I_DAYS2 ,
                N_OSPRN_PREV ,
                N_INSTALLMENT ,
                N_PRN_PAYMENT ,
                N_INT_PAYMENT ,
                N_OSPRN ,
                N_FAIRVALUE_PREV ,
                N_EFF_INT_AMT ,
                N_FAIRVALUE ,
                N_UNAMORT_AMT_PREV ,
                N_AMORT_AMT ,
                N_UNAMORT_AMT ,
                N_COST_UNAMORT_AMT_PREV ,
                N_COST_AMORT_AMT ,
                N_COST_UNAMORT_AMT ,
                N_FEE_UNAMORT_AMT_PREV ,
                N_FEE_AMORT_AMT ,
                N_FEE_UNAMORT_AMT
        FROM    IFRS_ACCT_EIR_ECF1;

        COMMIT;


        INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
        VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'6');

        -- insert to ecf2
        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_ECF2';

        COMMIT;

        INSERT  INTO IFRS_ACCT_EIR_ECF2
        ( MASTERID ,
          DOWNLOAD_DATE ,
          N_LOAN_AMT ,
          N_INT_RATE ,
          N_EFF_INT_RATE ,
          STARTAMORTDATE ,
          ENDAMORTDATE ,
          GRACEDATE ,
          DISB_PERCENTAGE ,
          DISB_AMOUNT ,
          PLAFOND ,
          PAYMENTCODE ,
          INTCALCCODE ,
          PAYMENTTERM ,
          ISGRACE ,
          PREV_PMT_DATE ,
          PMT_DATE ,
          I_DAYS ,
          I_DAYS2 ,
          N_OSPRN_PREV ,
          N_INSTALLMENT ,
          N_PRN_PAYMENT ,
          N_INT_PAYMENT ,
          N_OSPRN ,
          N_FAIRVALUE_PREV ,
          N_EFF_INT_AMT ,
          N_FAIRVALUE ,
          N_UNAMORT_AMT_PREV ,
          N_AMORT_AMT ,
          N_UNAMORT_AMT ,
          N_COST_UNAMORT_AMT_PREV ,
          N_COST_AMORT_AMT ,
          N_COST_UNAMORT_AMT ,
          N_FEE_UNAMORT_AMT_PREV ,
          N_FEE_AMORT_AMT ,
          N_FEE_UNAMORT_AMT ,
          N_FEE_AMT ,
          N_COST_AMT
        )
        SELECT  MASTERID ,
                DOWNLOAD_DATE ,
                N_LOAN_AMT ,
                N_INT_RATE ,
                N_EFF_INT_RATE ,
                STARTAMORTDATE ,
                ENDAMORTDATE ,
                GRACEDATE ,
                DISB_PERCENTAGE ,
                DISB_AMOUNT ,
                PLAFOND ,
                PAYMENTCODE ,
                INTCALCCODE ,
                PAYMENTTERM ,
                ISGRACE ,
                PREV_PMT_DATE ,
                PMT_DATE ,
                I_DAYS ,
                I_DAYS2 ,
                N_OSPRN_PREV ,
                N_INSTALLMENT ,
                N_PRN_PAYMENT ,
                N_INT_PAYMENT ,
                N_OSPRN ,
                N_FAIRVALUE_PREV ,
                N_EFF_INT_AMT ,
                N_FAIRVALUE ,
                N_UNAMORT_AMT_PREV ,
                N_AMORT_AMT ,
                N_UNAMORT_AMT ,
                N_COST_UNAMORT_AMT_PREV ,
                N_COST_AMORT_AMT ,
                N_COST_UNAMORT_AMT ,
                N_FEE_UNAMORT_AMT_PREV ,
                N_FEE_AMORT_AMT ,
                N_FEE_UNAMORT_AMT ,
                N_FEE_AMT ,
                N_COST_AMT
        FROM    IFRS_ACCT_EIR_ECF1;

        COMMIT;


        INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
        VALUES( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'7');

        COMMIT;


        -- next cycle prepare #t2
        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_ECF_T2';

        INSERT  INTO IFRS_ACCT_EIR_ECF_T2( MASTERID ,PMTDATE)
        SELECT  A.MASTERID ,MIN(A.PMT_DATE) AS PMTDATE
        FROM    IFRS_ACCT_EIR_PAYM A
        JOIN IFRS_ACCT_EIR_ECF1 B ON B.MASTERID = A.MASTERID AND A.PMT_DATE > B.PMT_DATE
        GROUP BY A.MASTERID;

        COMMIT;


        -- assign var @i
        SELECT  COUNT(*) INTO V_VI
        FROM    IFRS_ACCT_EIR_ECF_T2;

        INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
        VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'8');

        COMMIT;

    END LOOP;--loop;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_ACCT_EIR_GS_ECF_INSERT' ,'');

    COMMIT;



END;