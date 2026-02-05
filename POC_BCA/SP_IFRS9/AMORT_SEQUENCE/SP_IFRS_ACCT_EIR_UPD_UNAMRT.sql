CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_UPD_UNAMRT
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
BEGIN
   SELECT MAX (CURRDATE), MAX (PREVDATE)
     INTO V_CURRDATE, V_PREVDATE
     FROM IFRS_PRC_DATE_AMORT;


   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,
                               DTM,
                               OPS,
                               PROCNAME,
                               REMARK)
        VALUES (V_CURRDATE,
                SYSTIMESTAMP,
                'START',
                'SP_IFRS_ACCT_EIR_UPD_UNAMORT',
                '');

   COMMIT;

   --get active ecf
   EXECUTE IMMEDIATE 'truncate table TMP_B1';

   INSERT                                                   /*+ PARALLEL(8) */
         INTO                     TMP_B1 (MASTERID)
      SELECT                                                /*+ PARALLEL(8) */
            DISTINCT A.MASTERID
        FROM    IFRS_ACCT_EIR_ECF a
             LEFT JOIN
                IFRS_IMA_AMORT_CURR b
             ON A.MASTERID = B.MASTERID
       WHERE AMORTSTOPDATE IS NULL AND b.amort_type = 'EIR';

   COMMIT;


   UPDATE                                                   /*+ PARALLEL(8) */
         IFRS_MASTER_ACCOUNT A
      SET A.UNAMORT_FEE_AMT = 0, A.UNAMORT_COST_AMT = 0
    WHERE A.DOWNLOAD_DATE = V_CURRDATE AND A.AMORT_TYPE = 'EIR';

   COMMIT;

   UPDATE                                                   /*+ PARALLEL(8) */
         IFRS_IMA_AMORT_CURR A
      SET A.UNAMORT_FEE_AMT = 0, A.UNAMORT_COST_AMT = 0
    WHERE                                     --A.DOWNLOAD_DATE=V_CURRDATE AND
         A.AMORT_TYPE = 'EIR';

   COMMIT;

   --UPDATE BY WILLY 22 JUN 2023
   UPDATE                                                   /*+ PARALLEL(8) */
         IFRS_MASTER_ACCOUNT A
      SET EIR = NULL,
          FAIR_VALUE_AMOUNT = OUTSTANDING,
          A.RESERVED_AMOUNT_5 = 0,
          A.RESERVED_AMOUNT_6 = 0,
          A.INITIAL_UNAMORT_ORG_FEE = 0,
          A.UNAMORT_BENEFIT = NULL,
          A.UNAMORT_FEE_AMT = 0
    WHERE A.DOWNLOAD_DATE = V_CURRDATE AND A.AMORT_TYPE = 'SL';

   COMMIT;

   -- clean up already done on SP_PSAK_ACCT_SL_UPD_UNAMORT
   --get last acf id
   EXECUTE IMMEDIATE 'truncate table TMP_P1';

   INSERT                                                   /*+ PARALLEL(8) */
         INTO                     TMP_P1 (ID)
        SELECT                                              /*+ PARALLEL(8) */
              MAX (ID) ID
          FROM IFRS_ACCT_EIR_ACF
         WHERE DOWNLOAD_DATE = V_CURRDATE
               AND MASTERID IN (SELECT MASTERID FROM TMP_B1)
      GROUP BY MASTERID;

   COMMIT;



   EXECUTE IMMEDIATE 'truncate table TMP_U1';

   INSERT                                                   /*+ PARALLEL(8) */
         INTO                     TMP_U1 (DOWNLOAD_DATE,
                                          MASTERID,
                                          N_UNAMORT_FEE,
                                          N_UNAMORT_COST,
                                          ECFDATE,
                                          ACCTNO,
                                          ENDAMORTDATE)
      SELECT                                                /*+ PARALLEL(8) */
            B.DOWNLOAD_DATE,
             B.MASTERID,
             B.N_UNAMORT_FEE,
             B.N_UNAMORT_COST,
             B.ECFDATE,
             B.ACCTNO,
             E.ENDAMORTDATE
        FROM IFRS_ACCT_EIR_ACF B
             JOIN TMP_P1 C
                ON C.ID = B.ID
             LEFT JOIN IFRS_ACCT_EIR_ECF E
                ON     E.MASTERID = B.MASTERID
                   AND E.PREV_PMT_DATE = E.PMT_DATE
                   AND E.DOWNLOAD_DATE = B.ECFDATE
                   AND E.AMORTSTOPDATE IS NULL;

   COMMIT;

   -- update to master acct
   --select * from IFRS_ACCT_EIR_ACF
   MERGE INTO IFRS_IMA_AMORT_CURR A
        USING TMP_U1 X
           ON (X.MASTERID = A.MASTERID AND X.ACCTNO = A.ACCOUNT_NUMBER)
   WHEN MATCHED
   THEN
      UPDATE SET
         A.UNAMORT_FEE_AMT = X.N_UNAMORT_FEE,
         A.UNAMORT_COST_AMT = X.N_UNAMORT_COST,
         A.FAIR_VALUE_AMOUNT =
            A.OUTSTANDING + X.N_UNAMORT_FEE + X.N_UNAMORT_COST,
         --FAIR_VALUE_AMOUNT = dbo.IMA_AMORT_CURR.OUTSTANDING_JF + x.n_unamort_fee + x.n_unamort_cost, --20160510
         A.LOAN_START_AMORTIZATION = X.ECFDATE,
         A.LOAN_END_AMORTIZATION = X.ENDAMORTDATE,
         A.AMORT_TYPE = 'EIR';

   COMMIT;


   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING IFRS_IMA_AMORT_CURR X
           ON (    A.DOWNLOAD_DATE = V_CURRDATE
               AND A.MASTERID = X.MASTERID
               AND A.ACCOUNT_NUMBER = X.ACCOUNT_NUMBER
               AND X.AMORT_TYPE = 'EIR')
   WHEN MATCHED
   THEN
      UPDATE SET                    --UNAMORT_AMT_TOTAL = b.UNAMORT_AMT_TOTAL,
                A.UNAMORT_FEE_AMT = X.UNAMORT_FEE_AMT,
                 A.UNAMORT_COST_AMT = X.UNAMORT_COST_AMT,
                 A.FAIR_VALUE_AMOUNT = X.FAIR_VALUE_AMOUNT,
                 A.LOAN_START_AMORTIZATION = X.LOAN_START_AMORTIZATION,
                 A.LOAN_END_AMORTIZATION = X.LOAN_END_AMORTIZATION,
                 A.AMORT_TYPE = X.AMORT_TYPE;

   COMMIT;


   -- Update EIR to PMA 20151124 by Ris
   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (  SELECT MASTERID, MAX (N_EFF_INT_RATE) N_EFF_INT_RATE
                   FROM IFRS_ACCT_EIR_ECF
                  WHERE AMORTSTOPDATE IS NULL AND PREV_PMT_DATE = PMT_DATE
               GROUP BY MASTERID) B
           ON (A.DOWNLOAD_DATE = V_CURRDATE AND A.MASTERID = B.MASTERID)
   WHEN MATCHED
   THEN
      UPDATE SET A.EIR = B.N_EFF_INT_RATE
              WHERE A.AMORT_TYPE = 'EIR';

   COMMIT;


   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,
                               DTM,
                               OPS,
                               PROCNAME,
                               REMARK)
        VALUES (V_CURRDATE,
                SYSTIMESTAMP,
                'END',
                'SP_IFRS_ACCT_EIR_UPD_UNAMORT',
                '');
END;