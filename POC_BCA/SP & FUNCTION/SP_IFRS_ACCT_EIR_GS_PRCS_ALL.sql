CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_GS_PRCS_ALL
AS
  V_CURRDATE   DATE;
  V_PREVDATE   DATE;
  V_VLOOP2       NUMBER(19);
  V_VCNT2        NUMBER(19);
  V_COUNTER    NUMBER(19);
  V_MAXCOUNTER NUMBER(19);


BEGIN

    SELECT MAX (CURRDATE),MAX (PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT  ;

   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
   VALUES(V_CURRDATE, SYSTIMESTAMP, 'START', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '');

   COMMIT;

   UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
   SET EIR = NULL
     , NEXT_EIR = NULL
     , FINAL_EIR = NULL
     , EIR_NOCF = NULL
     , NEXT_EIR_NOCF = NULL
     , FINAL_EIR_NOCF = NULL;
   COMMIT;

   EXECUTE IMMEDIATE 'truncate table TMP_GS2'  ;

   COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO TMP_GS2 (
   MASTERID,
   DTMIN,
   CNTMIN , --adding for min counter
   BENEFIT,
   STAFFLOAN,
   COST_AMT,
   FEE_AMT,
   GAIN_LOSS_FEE_AMT,
   GAIN_LOSS_COST_AMT
   )
   SELECT /*+ PARALLEL(12) */ B.MASTERID,
          C.DTMIN,
          C.CNTMIN , --adding for min counter
          B.BENEFIT,
          B.STAFFLOAN,
          B.COST_AMT,
          B.FEE_AMT
          ,COALESCE(B.GAIN_LOSS_FEE_AMT,0) --20180226 gain loss adj
          ,COALESCE(B.GAIN_LOSS_COST_AMT,0)
   FROM    IFRS_ACCT_EIR_CF_ECF1 B
   JOIN IFRS_ACCT_EIR_PAYM_GS_DATE C
   ON C.MASTERID = B.MASTERID;

   COMMIT;



    MERGE INTO IFRS_ACCT_EIR_PAYM_GS A
    USING TMP_GS2 B
    ON (B.MASTERID = A.MASTERID
       --AND A.pmt_date = b.dtmin
       AND A.COUNTER = B.CNTMIN
       )
    WHEN MATCHED THEN
    UPDATE
    SET PREV_UNAMORT1 = CASE WHEN B.STAFFLOAN = 1 AND B.BENEFIT < 0 THEN B.BENEFIT
                             WHEN B.STAFFLOAN = 1 AND B.BENEFIT >= 0 THEN 0
                             ELSE B.FEE_AMT - B.GAIN_LOSS_FEE_AMT --20180417
                             END
                             + CASE WHEN B.STAFFLOAN = 1 AND B.BENEFIT <= 0 THEN 0
                                    WHEN B.STAFFLOAN = 1 AND B.BENEFIT > 0 THEN B.BENEFIT
                                    ELSE B.COST_AMT  - B.GAIN_LOSS_COST_AMT --20180417
                                    END,
         PREV_UNAMORT_NOCF1 = 0 , --for no cf calculation
         EIR1 = CASE WHEN B.STAFFLOAN = 1 THEN 10.5 WHEN N_INT_RATE > 1 THEN N_INT_RATE ELSE 1 END,
         EIR2 = CASE WHEN B.STAFFLOAN = 1 THEN 11 WHEN N_INT_RATE > 1 THEN N_INT_RATE ELSE 1 END
                + (0.01 * CASE WHEN N_INT_RATE > 1 THEN N_INT_RATE ELSE 1 END),
         EIR_NOCF1 = CASE WHEN B.STAFFLOAN = 1 THEN 10.5 WHEN N_INT_RATE > 1 THEN N_INT_RATE ELSE 1 END,
         EIR_NOCF2 = CASE WHEN B.STAFFLOAN = 1 THEN 11 WHEN N_INT_RATE > 1 THEN N_INT_RATE ELSE 1 END
                     + (0.01 * CASE WHEN N_INT_RATE > 1 THEN N_INT_RATE ELSE 1 END)    ;

   COMMIT;


   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE, DTM,OPS,PROCNAME,REMARK)
   VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','1');


   COMMIT;


   -- 20131106 daniel s : note unamort amount for each masterid
    MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
    USING IFRS_ACCT_EIR_PAYM_GS B
    ON (B.MASTERID = A.MASTERID
        AND B.PMT_DATE = A.DTMIN
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.UNAMORT = B.PREV_UNAMORT1,
        A.UNAMORT_NOCF = B.PREV_UNAMORT_NOCF1  ;

   COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE, DTM, OPS, PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','2');

   COMMIT;


    --select * from IFRS_ACCT_EIR_PAYM_GS order by pmt_date
    MERGE INTO IFRS_ACCT_EIR_PAYM_GS A
    USING IFRS_ACCT_EIR_PAYM_GS_DATE C
    ON( C.MASTERID = A.MASTERID
        AND A.COUNTER = C.CNTMIN
      )
    WHEN MATCHED THEN
    UPDATE
    SET PREV_UNAMORT_NOCF2 = PREV_UNAMORT_NOCF1 ,
            PREV_CRYAMT_NOCF1 = N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ,
            PREV_CRYAMT_NOCF2 = N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ,
            EIRAMT_NOCF1 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS/ 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS/ 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                ELSE ( M/ 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                           END ,
            EIRAMT_NOCF2 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                           END ,
            AMORT_NOCF1 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                               ELSE ( M / 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                          END - N_INT_PAYMENT ,
            AMORT_NOCF2 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                               ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                          END - N_INT_PAYMENT ,
            UNAMORT_NOCF1 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 ELSE ( M / 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                            END - N_INT_PAYMENT + PREV_UNAMORT_NOCF1 ,
            UNAMORT_NOCF2 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT1 )
                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                            END - N_INT_PAYMENT + PREV_UNAMORT_NOCF1 ,
            CRYAMT_NOCF1 = (N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) - N_PRN_PAYMENT
            + ( CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                  --WHEN intcalccode = '3' remarks for align with icc payment schedule
                     WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                     /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                     WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                   +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                END
                     /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                     ELSE ( M/ 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                END - N_INT_PAYMENT ) + DISB_AMOUNT,
            CRYAMT_NOCF2 = ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) - N_PRN_PAYMENT
                           + ( CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                    WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                    /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                    WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                               THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                               ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                                   +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                          END
                                    /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                    ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                               END - N_INT_PAYMENT ) + DISB_AMOUNT,
         --with cf part
         PREV_UNAMORT2 = PREV_UNAMORT1,
         PREV_CRYAMT1 = N_OSPRN_PREV + PREV_UNAMORT1,
         PREV_CRYAMT2 = N_OSPRN_PREV + PREV_UNAMORT1,
         EIRAMT1 =/*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                          THEN EIR1 /100*(A.PMT_DATE -A.PREV_PMT_DATE)* (N_OSPRN_PREV + PREV_UNAMORT1)/12/(A.PMT_DATE
                               -  CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1) )
                                       ELSE FN_PMTDATE(A.PMT_DATE,-1) END  )
                       ELSE */
                       CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                  END
                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                       END
               /*   END*/,
         EIRAMT2 = /*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                          THEN EIR2 /100*(A.PMT_DATE -A.PREV_PMT_DATE)* (N_OSPRN_PREV + PREV_UNAMORT1)/12/(A.PMT_DATE
                               - CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1))
                                      ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                        ELSE */
                       CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                  END
                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                       END
               /*   END*/,
         AMORT1 = CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                       WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                       /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                       WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                  THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                  ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                      +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                             END
                       /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                       ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                  END - N_INT_PAYMENT,
         AMORT2 = CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                       WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                       /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                       WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                  THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                  ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                      +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                             END
                       /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                       ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                  END - N_INT_PAYMENT,
         UNAMORT1 =CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                        WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                        /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                        WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                   THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                   ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                       +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                              END
                        /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                        ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                   END - N_INT_PAYMENT + PREV_UNAMORT1,
         UNAMORT2 =CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                        WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                        /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                        WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                   THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                   ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                       +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                              END
                        /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                        ELSE (  M / 1200* EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                   END - N_INT_PAYMENT + PREV_UNAMORT1,
         CRYAMT1 = (N_OSPRN_PREV + PREV_UNAMORT1) - N_PRN_PAYMENT
                    + (CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                  END
                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                       END - N_INT_PAYMENT) + DISB_AMOUNT,
         CRYAMT2 = (N_OSPRN_PREV + PREV_UNAMORT1) - N_PRN_PAYMENT
                    + (CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                  END
                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                       END - N_INT_PAYMENT) + DISB_AMOUNT;

   COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','3');

    EXECUTE IMMEDIATE 'truncate table IFRS_GS_DATE1' ;

    INSERT /*+ PARALLEL(12) */ INTO IFRS_GS_DATE1 (MASTERID, PMT_DATE,PERIOD)
    SELECT /*+ PARALLEL(12) */ MASTERID,DTMIN,PERIOD
    FROM IFRS_ACCT_EIR_PAYM_GS_DATE  ;


   COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','4');

    V_VLOOP2 := 0;
    -- outer loop
    WHILE 1 = 1
    LOOP --LOOP
        V_VLOOP2 := V_VLOOP2 + 1;
        EXIT WHEN V_VLOOP2 > 50; -- max count for outer loop

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE, DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','loop ' || TO_CHAR (V_VLOOP2));

        V_COUNTER := 1;
        SELECT NVL(MAX(PERIOD),0) INTO V_MAXCOUNTER FROM IFRS_GS_DATE1;

        EXIT WHEN V_MAXCOUNTER <= 0 ;


        --inner loop
        --WHILE 1 = 1
        WHILE V_COUNTER <= V_MAXCOUNTER
        LOOP --LOOP

            MERGE INTO IFRS_ACCT_EIR_PAYM_GS A
            USING (SELECT B.MASTERID,B.PMT_DATE,B.PERIOD,B.PMT_OS
                         ,C.UNAMORT_NOCF1
                         ,C.UNAMORT_NOCF2
                         ,C.CRYAMT_NOCF1
                         ,C.CRYAMT_NOCF2
                         ,C.EIR_NOCF1
                         ,C.EIR_NOCF2
                         ,C.UNAMORT1
                         ,C.UNAMORT2
                         ,C.CRYAMT1
                         ,C.CRYAMT2
                         ,C.EIR1
                         ,C.EIR2
                   FROM IFRS_GS_DATE1 B
                   JOIN IFRS_ACCT_EIR_PAYM_GS C ON B.MASTERID = C.MASTERID
                                                AND C.COUNTER = V_COUNTER - 1
                  )B
            ON (A.MASTERID = B.MASTERID
                AND A.COUNTER = V_COUNTER
               )
            WHEN MATCHED THEN
            UPDATE
            SET A.PREV_UNAMORT_NOCF1 = B.UNAMORT_NOCF1 ,
                A.PREV_UNAMORT_NOCF2 = B.UNAMORT_NOCF2 ,
                A.PREV_CRYAMT_NOCF1 = B.CRYAMT_NOCF1 ,
                A.PREV_CRYAMT_NOCF2 = B.CRYAMT_NOCF2 ,
                A.EIR_NOCF1 = B.EIR_NOCF1 ,
                A.EIR_NOCF2 = B.EIR_NOCF2 ,
                A.EIRAMT_NOCF1 = CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS / 360 * B.EIR_NOCF1 / 100 * B.CRYAMT_NOCF1
                                      WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS / 365 * B.EIR_NOCF1 / 100 * B.CRYAMT_NOCF1
                                      /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                      WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                                 THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR_NOCF1 / 100 * B.CRYAMT_NOCF1
                                                                                 ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE)* B.EIR_NOCF1 / 100 * B.CRYAMT_NOCF1)
                                                                                     +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE)* B.EIR_NOCF1 / 100 * B.CRYAMT_NOCF1)
                                                                            END
                                      /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                      ELSE ( A.M / 1200 * B.EIR_NOCF1 * B.CRYAMT_NOCF1 )
                                 END ,
                A.EIRAMT_NOCF2 = CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS / 360 * B.EIR_NOCF2 / 100 * B.CRYAMT_NOCF2
                                      WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS / 365 * B.EIR_NOCF2 / 100 * B.CRYAMT_NOCF2
                                      /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                      WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                                 THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR_NOCF2 / 100 * B.CRYAMT_NOCF2
                                                                                 ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * B.EIR_NOCF2 / 100 * B.CRYAMT_NOCF2)
                                                                                     +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR_NOCF2 / 100 * B.CRYAMT_NOCF2)
                                                                            END
                                      /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                      ELSE ( A.M / 1200 * B.EIR_NOCF2 * B.CRYAMT_NOCF2 )
                                 END ,
                A.PREV_UNAMORT1 = B.UNAMORT1 ,
                A.PREV_UNAMORT2 = B.UNAMORT2 ,
                A.PREV_CRYAMT1 = B.CRYAMT1 ,
                A.PREV_CRYAMT2 = B.CRYAMT2 ,
                A.EIR1 = B.EIR1 ,
                A.EIR2 = B.EIR2 ,
                A.EIRAMT1 =  /*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                    THEN B.EIR1 /100*(A.PMT_DATE -A.PREV_PMT_DATE)* B.CRYAMT1/12/(A.PMT_DATE
                                    - CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE ) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1)  END)
                                  ELSE */
                                  CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS / 360 * B.EIR1 / 100 * B.CRYAMT1
                                            WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS / 365 * B.EIR1 / 100 * B.CRYAMT1
                                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR1 / 100 * B.CRYAMT1
                                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * B.EIR1 / 100 * B.CRYAMT1)
                                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR1 / 100 * B.CRYAMT1)
                                                                            END
                                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                            ELSE ( A.M / 1200 * B.EIR1 * B.CRYAMT1 )
                           /*  END*/ END,
                EIRAMT2 =  /*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                                  THEN B.EIR2 /100*(A.PMT_DATE -A.PREV_PMT_DATE)* B.CRYAMT2/12/(A.PMT_DATE
                                  - CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE ) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                                ELSE */CASE WHEN A.INTCALCCODE IN ( '1', '6' ) THEN A.I_DAYS/ 360 * B.EIR2 / 100 * B.CRYAMT2
                                            WHEN A.INTCALCCODE IN ( '2', '3' ) THEN A.I_DAYS/ 365 * B.EIR2 / 100  * B.CRYAMT2
                                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR2 / 100 * B.CRYAMT2
                                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * B.EIR2 / 100 * B.CRYAMT2)
                                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * B.EIR2 / 100 * B.CRYAMT2)
                                                                            END
                                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                          ELSE ( A.M / 1200 * B.EIR2 * B.CRYAMT2 )
                         /* END*/ END;

            INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS, PROCNAME, REMARK)
            VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '5');

            COMMIT;


            MERGE INTO IFRS_ACCT_EIR_PAYM_GS A
            USING IFRS_GS_DATE1 B
            ON (A.MASTERID = B.MASTERID
            AND A.COUNTER = V_COUNTER
               )
            WHEN MATCHED THEN
            UPDATE
            SET
            --without cf part
               AMORT_NOCF1 = EIRAMT_NOCF1 - N_INT_PAYMENT,
               AMORT_NOCF2 = EIRAMT_NOCF2 - N_INT_PAYMENT,
               UNAMORT_NOCF1 = (EIRAMT_NOCF1 - N_INT_PAYMENT) + PREV_UNAMORT_NOCF1,
               UNAMORT_NOCF2 = (EIRAMT_NOCF2 - N_INT_PAYMENT) + PREV_UNAMORT_NOCF2,
               CRYAMT_NOCF1 = PREV_CRYAMT_NOCF1 + (EIRAMT_NOCF1 - N_INT_PAYMENT) - N_PRN_PAYMENT,
               CRYAMT_NOCF2 = PREV_CRYAMT_NOCF2 + (EIRAMT_NOCF2 - N_INT_PAYMENT) - N_PRN_PAYMENT,
            --with cf part
               AMORT1 = EIRAMT1 - N_INT_PAYMENT ,
               AMORT2 = EIRAMT2 - N_INT_PAYMENT ,
               UNAMORT1 = ( EIRAMT1 - N_INT_PAYMENT ) + PREV_UNAMORT1 ,
               UNAMORT2 = ( EIRAMT2 - N_INT_PAYMENT ) + PREV_UNAMORT2 ,
               CRYAMT1 = PREV_CRYAMT1 + ( EIRAMT1 - N_INT_PAYMENT ) - N_PRN_PAYMENT ,
               CRYAMT2 = PREV_CRYAMT2 + ( EIRAMT2 - N_INT_PAYMENT ) - N_PRN_PAYMENT         ;
            COMMIT;

            INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE, DTM, OPS, PROCNAME, REMARK)
            VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG','SP_IFRS_ACCT_EIR_GS_PROCESS', '6');

            COMMIT;

            --prepare next
            V_COUNTER := V_COUNTER + 1;

        END LOOP; --LOOP;

        --inner loop
        -- get success eir1
        --with cf part
        EXECUTE IMMEDIATE 'truncate table TMP_T14';

        INSERT /*+ PARALLEL(12) */ INTO TMP_T14 (MASTERID, E1)
        SELECT /*+ PARALLEL(12) */ B.MASTERID, C.EIR1
        FROM IFRS_ACCT_EIR_PAYM_GS_DATE B
        JOIN IFRS_GS_DATE1 D ON B.MASTERID = D.MASTERID --adding join 20160525
        JOIN IFRS_ACCT_EIR_PAYM_GS C
          ON C.COUNTER = B.CNTMAX--c.pmt_date = b.dtmax
          AND B.MASTERID = C.MASTERID
          AND ABS(C.UNAMORT1) < 0.01;
        COMMIT;

        MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
        USING TMP_T14 C
        ON (A.MASTERID = C.MASTERID
            AND A.EIR IS NULL
           )
        WHEN MATCHED THEN
        UPDATE
        SET FINAL_EIR = C.E1;
        COMMIT;

        UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
        SET EIR = FINAL_EIR
        WHERE FINAL_EIR IS NOT NULL
        AND EIR IS NULL;

        COMMIT;

        --without cf part
        EXECUTE IMMEDIATE 'truncate table TMP_T14';

        INSERT /*+ PARALLEL(12) */ INTO TMP_T14 (MASTERID, E1)
        SELECT /*+ PARALLEL(12) */ B.MASTERID, C.EIR_NOCF1
        FROM IFRS_ACCT_EIR_PAYM_GS_DATE B
        JOIN IFRS_GS_DATE1 D ON B.MASTERID = D.MASTERID --adding join 20160525
        JOIN IFRS_ACCT_EIR_PAYM_GS C
          ON C.COUNTER = B.CNTMAX--c.pmt_date = b.dtmax
          AND B.MASTERID = C.MASTERID
          AND ABS(C.UNAMORT_NOCF1) < 0.01;
        COMMIT;

        MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
        USING TMP_T14 C
        ON (A.MASTERID = C.MASTERID
            AND A.EIR_NOCF IS NULL
           )
        WHEN MATCHED THEN
        UPDATE
        SET FINAL_EIR_NOCF = C.E1 ;
        COMMIT;

        UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
        SET EIR_NOCF = FINAL_EIR_NOCF
        WHERE FINAL_EIR_NOCF IS NOT NULL
        AND EIR_NOCF IS NULL;
        COMMIT;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG','SP_IFRS_ACCT_EIR_GS_PROCESS','7b');

        -- set next eir
        EXECUTE IMMEDIATE 'truncate table TMP_GS1' ;
        COMMIT;
        INSERT /*+ PARALLEL(12) */ INTO TMP_GS1 (
        MASTERID,
        UNAMORT_NOCF1,
        UNAMORT_NOCF2,
        EIR_NOCF1,
        EIR_NOCF2,
        UNAMORT1,
        UNAMORT2,
        EIR1,
        EIR2
        )
        SELECT /*+ PARALLEL(12) */ B.MASTERID,
               C.UNAMORT_NOCF1,
               C.UNAMORT_NOCF2,
               C.EIR_NOCF1,
               C.EIR_NOCF2,
               C.UNAMORT1,
               C.UNAMORT2,
               C.EIR1,
               C.EIR2
        FROM IFRS_ACCT_EIR_PAYM_GS_DATE B
        JOIN IFRS_GS_DATE1 D ON B.MASTERID = D.MASTERID --adding join 20160525
        JOIN IFRS_ACCT_EIR_PAYM_GS C
          ON B.MASTERID = C.MASTERID
          AND (B.EIR IS NULL OR B.EIR_NOCF IS NULL)
          AND C.COUNTER = B.CNTMAX; --c.pmt_date = b.dtmax;
        COMMIT;


        MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
        USING TMP_GS1 C
        ON ( A.MASTERID = C.MASTERID
            AND (A.EIR IS NULL OR A.EIR_NOCF IS NULL)
           )
        WHEN MATCHED THEN
        UPDATE
        SET
            --without cf part
            NEXT_EIR_NOCF = NVL(EIR_NOCF,
               CASE WHEN ABS (( ( (C.UNAMORT_NOCF2 - C.UNAMORT_NOCF1) / (C.EIR_NOCF2 - C.EIR_NOCF1)) * C.EIR_NOCF1 - C.UNAMORT_NOCF1)/ ( (C.UNAMORT_NOCF2 - C.UNAMORT_NOCF1) / (C.EIR_NOCF2 - C.EIR_NOCF1))) > 2000
                    THEN 15
                    ELSE ( ( (C.UNAMORT_NOCF2 - C.UNAMORT_NOCF1) / (C.EIR_NOCF2 - C.EIR_NOCF1))
                          * C.EIR_NOCF1 - C.UNAMORT_NOCF1)
                         / ( (C.UNAMORT_NOCF2 - C.UNAMORT_NOCF1) / (C.EIR_NOCF2 - C.EIR_NOCF1))
                    END),
            --with cf part
            NEXT_EIR = NVL(EIR,
               CASE WHEN ABS (( ( (C.UNAMORT2 - C.UNAMORT1) / (C.EIR2 - C.EIR1))* C.EIR1 - C.UNAMORT1)/ ( (C.UNAMORT2 - C.UNAMORT1) / (C.EIR2 - C.EIR1))) >2000
                    THEN 15
                    ELSE ( ( (C.UNAMORT2 - C.UNAMORT1) / (C.EIR2 - C.EIR1))
                         * C.EIR1 - C.UNAMORT1)
                         / ( (C.UNAMORT2 - C.UNAMORT1) / (C.EIR2 - C.EIR1))
                    END) ;

        COMMIT;
        -- if next_eir=eir1 then probably gs has reach its limit so terminate as soon as possible
        --with cf part
        MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
        USING TMP_GS1 C
        ON (A.MASTERID = C.MASTERID
          AND A.EIR IS NULL
          AND C.EIR1 = A.NEXT_EIR
          AND ABS (C.UNAMORT1) < 1
           )
        WHEN MATCHED THEN
        UPDATE
        SET FINAL_EIR = NEXT_EIR ;
        COMMIT;

        UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
        SET EIR = FINAL_EIR
        WHERE FINAL_EIR IS NOT NULL
        AND EIR IS NULL;
        COMMIT;

        --without cf part
        MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
        USING TMP_GS1 C
        ON (A.MASTERID = C.MASTERID
            AND A.EIR_NOCF IS NULL
            AND C.EIR_NOCF1 = A.NEXT_EIR_NOCF
            AND ABS (C.UNAMORT_NOCF1) < 1
           )
        WHEN MATCHED THEN
        UPDATE
        SET FINAL_EIR_NOCF = NEXT_EIR_NOCF;
        COMMIT;

        UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
        SET EIR_NOCF = FINAL_EIR_NOCF
        WHERE FINAL_EIR_NOCF IS NOT NULL
        AND EIR_NOCF IS NULL;
        COMMIT;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE, DTM,OPS, PROCNAME, REMARK)
        VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '8');
        COMMIT;
        --init loop 1 --select * from TMP_GS2
        EXECUTE IMMEDIATE 'truncate table TMP_GS2'  ;

        INSERT /*+ PARALLEL(12) */ INTO TMP_GS2 (
        MASTERID,
        DTMIN,
        CNTMIN,
        NEXT_EIR,
        NEXT_EIR_NOCF,
        STAFFLOAN,
        BENEFIT,
        FEE_AMT,
        COST_AMT,
        GAIN_LOSS_FEE_AMT, --20180417
        GAIN_LOSS_COST_AMT --20180417
        )
        SELECT /*+ PARALLEL(12) */ B.MASTERID,
               C.DTMIN,
               C.CNTMIN,
               C.NEXT_EIR,
               C.NEXT_EIR_NOCF,
               B.STAFFLOAN,
               B.BENEFIT,
               B.FEE_AMT,
               B.COST_AMT
               ,COALESCE(B.GAIN_LOSS_FEE_AMT,0) --20180417 gain loss adj
               ,COALESCE(B.GAIN_LOSS_COST_AMT,0)
        FROM IFRS_ACCT_EIR_CF_ECF1 B
        JOIN IFRS_ACCT_EIR_PAYM_GS_DATE C
        ON C.MASTERID = B.MASTERID AND (C.EIR IS NULL OR C.EIR_NOCF IS NULL);
        COMMIT;

        SELECT COUNT (*) INTO V_VCNT2 FROM TMP_GS2;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES(V_CURRDATE, SYSTIMESTAMP,'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','NOA GS : ' || TO_CHAR (V_VCNT2));


        MERGE INTO IFRS_ACCT_EIR_PAYM_GS A
        USING TMP_GS2 B
        ON (B.MASTERID = A.MASTERID
            --AND A.pmt_date = b.dtmin -- remarks 20160525
            AND A.COUNTER = B.CNTMIN
           )
        WHEN MATCHED THEN
        UPDATE
        SET
          --without cf part
          PREV_UNAMORT_NOCF1 = 0 ,    --zero unamort
          EIR_NOCF1 = B.NEXT_EIR_NOCF ,
          EIR_NOCF2 = B.NEXT_EIR_NOCF + ( 0.001 * B.NEXT_EIR_NOCF ),
          --with cf part
          PREV_UNAMORT1 = CASE WHEN B.STAFFLOAN = 1 AND B.BENEFIT < 0 THEN B.BENEFIT
                               WHEN B.STAFFLOAN = 1 AND B.BENEFIT >= 0 THEN 0
                               ELSE B.FEE_AMT - B.GAIN_LOSS_FEE_AMT --201801417
                          END + CASE WHEN B.STAFFLOAN = 1 AND B.BENEFIT <= 0 THEN 0
                                     WHEN B.STAFFLOAN = 1 AND B.BENEFIT > 0 THEN B.BENEFIT
                                     ELSE B.COST_AMT - B.GAIN_LOSS_COST_AMT --201801417
                                     END,
          EIR1 = B.NEXT_EIR,
          EIR2 = B.NEXT_EIR + (0.001 * B.NEXT_EIR)  ;

        COMMIT;
        INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE, DTM, OPS, PROCNAME, REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '9');

        COMMIT;

        MERGE INTO IFRS_ACCT_EIR_PAYM_GS A
        USING IFRS_ACCT_EIR_PAYM_GS_DATE C
        ON (C.MASTERID = A.MASTERID
            --AND dbo.IFRS_ACCT_EIR_PAYM_GS.pmt_date = c.dtmin --remarks 20160525
            AND A.COUNTER = C.CNTMIN --adding 20160525
            AND (C.EIR IS NULL OR C.EIR_NOCF IS NULL)
           )
        WHEN MATCHED THEN
        UPDATE
        SET -- without cf part
            PREV_UNAMORT_NOCF2 = PREV_UNAMORT_NOCF1 ,
            PREV_CRYAMT_NOCF1 = N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ,
            PREV_CRYAMT_NOCF2 = N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ,
            EIRAMT_NOCF1 = CASE  WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                            THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                            ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                                +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 ELSE ( M / 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                                 END ,
            EIRAMT_NOCF2 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                                END ,
            AMORT_NOCF1 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                               ELSE ( M / 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                               END - N_INT_PAYMENT ,
            AMORT_NOCF2 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                               /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                      THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                      ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                          +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                 END
                                /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                               ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                               END - N_INT_PAYMENT ,
            UNAMORT_NOCF1 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                            THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                            ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                              +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                       END
                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 ELSE ( M / 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                                 END - N_INT_PAYMENT + PREV_UNAMORT_NOCF1 ,
            UNAMORT_NOCF2 = CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                 WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT1 )
                                 /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                            THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                            ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                                +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                       END
                                 /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                 ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                                 END - N_INT_PAYMENT + PREV_UNAMORT_NOCF1 ,
            CRYAMT_NOCF1 =(N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) - N_PRN_PAYMENT
                          + ( CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                   WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                   /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                   WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                              THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                              ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ))
                                                                                  +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF1 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                         END
                                   /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                   ELSE ( M / 1200 * EIR_NOCF1 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) )
                                   END - N_INT_PAYMENT ) + DISB_AMOUNT,
            CRYAMT_NOCF2 =( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) - N_PRN_PAYMENT
                          + ( CASE WHEN INTCALCCODE IN ( '1', '6' ) THEN I_DAYS / 360 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                   WHEN INTCALCCODE IN ( '2', '3' ) THEN I_DAYS / 365 * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                   /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                   WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                              THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 )
                                                                              ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                                  +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR_NOCF2 / 100 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1))
                                                                         END
                                   /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                   ELSE ( M / 1200 * EIR_NOCF2 * ( N_OSPRN_PREV + PREV_UNAMORT_NOCF1 ) ) END - N_INT_PAYMENT )+ DISB_AMOUNT,


            -- with cf part
            PREV_UNAMORT2 = PREV_UNAMORT1,
            PREV_CRYAMT1 = N_OSPRN_PREV + PREV_UNAMORT1,
            PREV_CRYAMT2 = N_OSPRN_PREV + PREV_UNAMORT1,
            EIRAMT1 = /*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                            THEN EIR1/100*(A.PMT_DATE -A.PREV_PMT_DATE)
                                 * (N_OSPRN_PREV + PREV_UNAMORT1)/12/(A.PMT_DATE -
                                 CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                           ELSE */CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                       WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                       /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                       WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                                  THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                                  ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                                      +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                             END
                                       /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                     ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                        /*   END*/ END,
            EIRAMT2 = /*CASE WHEN (A.COUNTER = 1 AND A.SPECIAL_FLAG = 1)
                            THEN EIR2/100*(A.PMT_DATE -A.PREV_PMT_DATE)
                                 * (N_OSPRN_PREV + PREV_UNAMORT2)/12/(A.PMT_DATE -
                                 CASE WHEN A.PMT_DATE = LAST_DAY(A.PMT_DATE) THEN LAST_DAY(FN_PMTDATE(A.PMT_DATE,-1)) ELSE FN_PMTDATE(A.PMT_DATE,-1) END)
                           ELSE */
                           CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                     WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                     /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                     WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                                THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                                ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                                    +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                           END
                                     /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                                     ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                     /*END */END,
            AMORT1 = CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                          WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                          /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                          WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                     THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                     ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                         +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                END
                          /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                          ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                          END - N_INT_PAYMENT,
            AMORT2 = CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                          WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                          /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                          WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                     THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                     ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                         +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                END
                          /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                          ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                          END - N_INT_PAYMENT,
            UNAMORT1 = CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                              WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                         THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                         ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                             +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                    END
                              /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                            END - N_INT_PAYMENT + PREV_UNAMORT1,
            UNAMORT2 = CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                            /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                       THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                       ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                           +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                  END
                            /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                            ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                            END - N_INT_PAYMENT + PREV_UNAMORT1,
            CRYAMT1 = (N_OSPRN_PREV + PREV_UNAMORT1) - N_PRN_PAYMENT
                      + (CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                              WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                              /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                              WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                         THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                         ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                             +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                    END
                              /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                              ELSE (  M / 1200 * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                              END - N_INT_PAYMENT) + DISB_AMOUNT,
            CRYAMT2 = (N_OSPRN_PREV + PREV_UNAMORT1) - N_PRN_PAYMENT
                      + (CASE WHEN INTCALCCODE IN ('1','6') THEN I_DAYS / 360 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                              WHEN INTCALCCODE IN ('2','3') THEN I_DAYS / 365 * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                              /******* 24 JAN 2019 - START ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                              WHEN INTCALCCODE IN ( '7', '9' ) THEN CASE WHEN EXTRACT(YEAR FROM PREV_PMT_DATE)= EXTRACT(YEAR FROM PMT_DATE)
                                                                         THEN I_DAYS/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                                                                         ELSE ((FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE)/ FN_IDAYS_CURR_YEAR(PREV_PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                             +((I_DAYS - (FN_LASTDAY_OF_YEAR(PREV_PMT_DATE)-PREV_PMT_DATE))/ FN_IDAYS_CURR_YEAR(PMT_DATE) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1))
                                                                    END
                              /******* 24 JAN 2019 - END ADDED BY VIVI, FOR ICC 7 AND 9 *******/
                              ELSE (  M / 1200 * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT1))
                              END - N_INT_PAYMENT) + DISB_AMOUNT
        ;

        COMMIT;

        INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE, DTM, OPS, PROCNAME, REMARK)
        VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '9b');

        EXECUTE IMMEDIATE 'truncate table IFRS_GS_DATE1'   ;

        INSERT /*+ PARALLEL(12) */ INTO IFRS_GS_DATE1 (MASTERID, PMT_DATE, PERIOD)
        SELECT /*+ PARALLEL(12) */ MASTERID,DTMIN,PERIOD
        FROM IFRS_ACCT_EIR_PAYM_GS_DATE
        WHERE (EIR IS NULL OR EIR_NOCF IS NULL);

        COMMIT;

        /*remarks 20160525
        SELECT a.masterid, MIN (a.pmt_date) dt
        FROM    IFRS_ACCT_EIR_PAYM_GS a
        JOIN IFRS_ACCT_EIR_PAYM_GS_DATE b
          ON     a.pmt_date > b.dtmin
          AND a.masterid = b.masterid
          AND b.eir IS NULL
        GROUP BY a.masterid
        end remarks 20160525*/

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM, OPS, PROCNAME, REMARK)
        VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '10');

    END LOOP; --LOOP;


    --outer loop
    -- get success eir1 last loop
    --with cf part
    MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
    USING (SELECT B.MASTERID, C.EIR1
          FROM    IFRS_ACCT_EIR_PAYM_GS_DATE B
          JOIN IFRS_ACCT_EIR_PAYM_GS C
            ON     C.COUNTER = B.CNTMAX--c.pmt_date = b.dtmax
            AND B.MASTERID = C.MASTERID
            AND ABS (C.UNAMORT1) < 1
          ) B
    ON (A.MASTERID = B.MASTERID
    AND A.EIR IS NULL
       )
    WHEN MATCHED THEN
    UPDATE
    SET FINAL_EIR = B.EIR1  ;

    COMMIT;


    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
    SET EIR = FINAL_EIR
    WHERE FINAL_EIR IS NOT NULL
    AND EIR IS NULL;

    COMMIT;

    --without cf part
    MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
    USING (SELECT B.MASTERID, C.EIR_NOCF1
            FROM IFRS_ACCT_EIR_PAYM_GS_DATE B
            JOIN IFRS_ACCT_EIR_PAYM_GS C
            ON C.COUNTER = B.CNTMAX--c.pmt_date = b.dtmax
            AND B.MASTERID = C.MASTERID
            AND  ABS (C.UNAMORT_NOCF1) < 1
          ) B
    ON (A.MASTERID = B.MASTERID
        AND A.EIR_NOCF IS NULL
       )
    WHEN MATCHED THEN
    UPDATE SET FINAL_EIR_NOCF = B.EIR_NOCF1;

    COMMIT;


    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
    SET EIR_NOCF = FINAL_EIR_NOCF
    WHERE FINAL_EIR_NOCF IS NOT NULL
    AND EIR_NOCF IS NULL;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM, OPS, PROCNAME, REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS','11');

    COMMIT;

    -- 20131106 daniel s : get success eir1 last loop with big initial unamort
    -- with cf part
    MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
    USING (SELECT B.MASTERID, C.EIR1, C.EIR_NOCF1
           FROM IFRS_ACCT_EIR_PAYM_GS_DATE B
           JOIN IFRS_ACCT_EIR_PAYM_GS C
           ON C.COUNTER = B.CNTMAX--c.pmt_date = b.dtmax
           AND B.MASTERID = C.MASTERID
           AND ABS (C.UNAMORT1) < 10
           ) B
    ON (A.MASTERID = B.MASTERID
           AND A.EIR IS NULL
           AND ABS (A.UNAMORT) > 1000000000
        )
    WHEN MATCHED THEN
    UPDATE
    SET FINAL_EIR = B.EIR1  ;

    COMMIT;


    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
    SET EIR = FINAL_EIR
    WHERE FINAL_EIR IS NOT NULL
    AND EIR IS NULL;

    COMMIT;

    -- without cf part
    MERGE INTO IFRS_ACCT_EIR_PAYM_GS_DATE A
    USING (SELECT B.MASTERID, C.EIR_NOCF1
           FROM IFRS_ACCT_EIR_PAYM_GS_DATE B
           JOIN IFRS_ACCT_EIR_PAYM_GS C
           ON C.COUNTER = B.CNTMAX--c.pmt_date = b.dtmax
           AND B.MASTERID = C.MASTERID
           AND ABS (C.UNAMORT_NOCF1) < 10
           ) B
    ON (A.MASTERID = B.MASTERID
        AND A.EIR_NOCF IS NULL
        AND ABS (A.UNAMORT_NOCF) > 1000000000
       )
    WHEN MATCHED THEN
    UPDATE
    SET FINAL_EIR_NOCF = B.EIR_NOCF1 ;

    COMMIT;


    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_PAYM_GS_DATE
    SET EIR_NOCF = FINAL_EIR_NOCF
    WHERE FINAL_EIR_NOCF IS NOT NULL
    AND EIR_NOCF IS NULL;
    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME, REMARK)
    VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '12');


    -- failed goal seek with cf part
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_FAILED_GOAL_SEEK (
    DOWNLOAD_DATE,
    MASTERID,
    CREATEDBY,
    CREATEDDATE
    )
    SELECT /*+ PARALLEL(12) */ V_CURRDATE,
           MASTERID,
           'EIR_GS',
           SYSTIMESTAMP
    FROM IFRS_ACCT_EIR_PAYM_GS_DATE
    WHERE EIR IS NULL;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM, OPS, PROCNAME,REMARK)
    VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '13');

    COMMIT;


    -- success goal seek with cf part
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_GOAL_SEEK_RESULT (
    DOWNLOAD_DATE,
    MASTERID,
    CREATEDBY,
    CREATEDDATE,
    EIR
    )
    SELECT /*+ PARALLEL(12) */ V_CURRDATE,
           MASTERID,
           'EIR_GS',
           SYSTIMESTAMP,
           EIR
    FROM IFRS_ACCT_EIR_PAYM_GS_DATE
    WHERE EIR IS NOT NULL;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES ( V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '14');


    -- failed goal seek without cf part
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_FAILED_GOALSEEK4 (
    DOWNLOAD_DATE,
    MASTERID,
    CREATEDBY,
    CREATEDDATE
    )
    SELECT /*+ PARALLEL(12) */ V_CURRDATE, MASTERID, 'EIR_GS', SYSTIMESTAMP
    FROM IFRS_ACCT_EIR_PAYM_GS_DATE
    WHERE EIR_NOCF IS NULL;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE, DTM, OPS, PROCNAME, REMARK)
    VALUES (V_CURRDATE, SYSTIMESTAMP, 'DEBUG', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '15');


    -- success goal seek without cf part
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_GOALSEEK_RESULT4 (
    DOWNLOAD_DATE,
    MASTERID,
    CREATEDBY,
    CREATEDDATE,
    EIR
    )
    SELECT /*+ PARALLEL(12) */ V_CURRDATE,
             MASTERID,
             'EIR_GS',
             SYSTIMESTAMP,
             EIR_NOCF
    FROM IFRS_ACCT_EIR_PAYM_GS_DATE
    WHERE EIR_NOCF IS NOT NULL;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE, DTM, OPS, PROCNAME, REMARK)
    VALUES (V_CURRDATE, SYSTIMESTAMP, 'END', 'SP_IFRS_ACCT_EIR_GS_PROCESS', '');

    COMMIT;

END;