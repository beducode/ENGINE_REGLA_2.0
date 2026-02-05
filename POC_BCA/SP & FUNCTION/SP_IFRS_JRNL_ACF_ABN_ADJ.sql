CREATE OR REPLACE PROCEDURE SP_IFRS_JRNL_ACF_ABN_ADJ
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
   V_VI         NUMBER (10);
   V_VI2        NUMBER (10);
BEGIN
   /******************************************************************************
   01. DECLARE VARIABLE
   *******************************************************************************/
   SELECT MAX (CURRDATE) INTO V_CURRDATE FROM IFRS_PRC_DATE_AMORT;

   SELECT MAX (PREVDATE) INTO V_PREVDATE FROM IFRS_PRC_DATE_AMORT;

   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,
                               DTM,
                               OPS,
                               PROCNAME,
                               REMARK)
        VALUES (V_CURRDATE,
                SYSTIMESTAMP,
                'START',
                'SP_IFRS_JRNL_ACF_ABN_ADJ',
                '');

   COMMIT;

   /******************************************************************************
   02. CLEAN UP
   *******************************************************************************/
   DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_INTM_SUMM
         WHERE DOWNLOAD_DATE = V_CURRDATE
               AND SOURCEPROCESS IN ('ACFABN_ADJ', 'REV_ADJ', 'REV_ADJ2');

   COMMIT;

   DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_INTM
         WHERE DOWNLOAD_DATE = V_CURRDATE
               AND SOURCEPROCESS IN ('ACFABN_ADJ', 'REV_ADJ', 'REV_ADJ2');

   COMMIT;

   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,
                               DTM,
                               OPS,
                               PROCNAME,
                               REMARK)
        VALUES (V_CURRDATE,
                SYSTIMESTAMP,
                'DEBUG',
                'SP_IFRS_JRNL_ACF_ABN_ADJ',
                'CLEAN UP DONE');

   COMMIT;

   /******************************************************************************
   03. GET ACCOUNT LAST ACF ABN
   *******************************************************************************/
   EXECUTE IMMEDIATE 'truncate table TMP_T1';

   INSERT /*+ PARALLEL(12) */ INTO TMP_T1 (MASTERID)
      SELECT /*+ PARALLEL(12) */ MASTERID
        FROM IFRS_ACCT_EIR_ACF
       WHERE DOWNLOAD_DATE = V_CURRDATE AND CREATEDBY = 'SP_EIR_LAST_ACF_ABN'
      UNION
      SELECT MASTERID
        FROM IFRS_ACCT_SL_ACF
       WHERE DOWNLOAD_DATE = V_CURRDATE AND CREATEDBY = 'SP_SL_LAST_ACF_ABN';

   COMMIT;

   /******************************************************************************
   04.--20180108 reversal resona will use similar logic for simplicity
      --20180108 cf reversal will exclude cf and its cf rev pair from ecf generation
   *******************************************************************************/
   EXECUTE IMMEDIATE 'truncate table TMP_T2';

   INSERT /*+ PARALLEL(12) */ INTO TMP_T2 (MASTERID)
      SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
        FROM IFRS_ACCT_COST_FEE
       WHERE     FLAG_REVERSE = 'Y'
             AND CF_ID_REV IS NOT NULL
             AND DOWNLOAD_DATE = V_CURRDATE
             AND MASTERID NOT IN (SELECT MASTERID FROM TMP_T1) --prevent double processing
      --20180310 also process today new EIR ECF having amort amt <= currdate
      UNION
        SELECT /*+ PARALLEL(12) */ MASTERID
          FROM IFRS_ACCT_EIR_ECF
         WHERE     DOWNLOAD_DATE = V_CURRDATE
               AND AMORTSTOPDATE IS NULL
               AND PMT_DATE <= V_CURRDATE
               AND MASTERID NOT IN (SELECT MASTERID FROM TMP_T1) --prevent double processing
      GROUP BY MASTERID
        HAVING SUM (N_AMORT_AMT) <> 0
      --20180328 also process today new SL ECF having amort amt <= currdate
      UNION
        SELECT /*+ PARALLEL(12) */ MASTERID
          FROM IFRS_ACCT_SL_ECF
         WHERE     DOWNLOAD_DATE = V_CURRDATE
               AND AMORTSTOPDATE IS NULL
               AND PMTDATE <= V_CURRDATE
               AND MASTERID NOT IN (SELECT MASTERID FROM TMP_T1) --prevent double processing
      GROUP BY MASTERID
        HAVING SUM (N_AMORT_FEE) <> 0 OR SUM (N_AMORT_COST) <> 0;

   COMMIT;

   /******************************************************************************
   05. CHECK
   *******************************************************************************/

   SELECT COUNT (*) INTO V_VI FROM TMP_T1;

   SELECT COUNT (*) INTO V_VI2 FROM TMP_T2;

   IF V_VI <= 0 AND V_VI2 <= 0
   THEN
      INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,
                                  DTM,
                                  OPS,
                                  PROCNAME,
                                  REMARK)
           VALUES (V_CURRDATE,
                   SYSTIMESTAMP,
                   'END',
                   'SP_IFRS_JRNL_ACF_ABN_ADJ',
                   '');

      RETURN;
   END IF;

   /******************************************************************************
   06. INSERT JOURNAL INTM
   *******************************************************************************/
   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_JOURNAL_INTM');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_JOURNAL_INTM',
                                  DEGREE    => 2);
   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_CFID_JOURNAL_INTM_SUMM');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_CFID_JOURNAL_INTM_SUMM',
                                  DEGREE    => 2);
   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_COST_FEE');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_COST_FEE',
                                  DEGREE    => 2);

   INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (DOWNLOAD_DATE,
                                       MASTERID,
                                       FACNO,
                                       CIFNO,
                                       ACCTNO,
                                       DATASOURCE,
                                       PRDCODE,
                                       TRXCODE,
                                       CCY,
                                       JOURNALCODE,
                                       JOURNALCODE2,
                                       STATUS,
                                       REVERSE,
                                       FLAG_CF,
                                       N_AMOUNT,
                                       SOURCEPROCESS,
                                       CREATEDDATE,
                                       CREATEDBY,
                                       BRANCH,
                                       PRDTYPE,
                                       IS_PNL,
                                       CF_ID,
                                       METHOD)
      SELECT /*+ PARALLEL(12) */ V_CURRDATE,
             A.MASTERID,
             B.FACNO,
             B.CIFNO,
             B.ACCTNO,
             B.DATASOURCE,
             B.PRD_CODE,
             B.TRX_CODE,
             B.CCY,
             'AMORT',
             CASE WHEN B.METHOD = 'SL' THEN 'ACCRU_SL' ELSE 'ACCRU' END,
             'ACT',
             'N',
             B.FLAG_CF,
             -1 * A.UNAMORT_AMT,
             'ACFABN_ADJ',
             SYSTIMESTAMP,
             'ACFABN_ADJ',
             B.BRCODE,
             B.PRD_TYPE,
             '',
             B.CF_ID,
             B.METHOD
        FROM    IFRS_CFID_JOURNAL_INTM_SUMM A
             JOIN
                IFRS_ACCT_COST_FEE B
             ON B.CF_ID = A.CF_ID
       WHERE     A.DOWNLOAD_DATE = V_CURRDATE
             AND A.MASTERID IN (SELECT MASTERID FROM TMP_T1)
             AND A.UNAMORT_AMT <> 0;

   COMMIT;

   /******************************************************************************
   07. REV ADJ
   *******************************************************************************/

   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_CFID_JOURNAL_INTM_SUMM');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_CFID_JOURNAL_INTM_SUMM',
                                  DEGREE    => 2);

   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_COST_FEE');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_COST_FEE',
                                  DEGREE    => 2);

   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME => 'IFRS', TABNAME => 'TMP_T2');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'TMP_T2',
                                  DEGREE    => 2);

   INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (DOWNLOAD_DATE,
                                       MASTERID,
                                       FACNO,
                                       CIFNO,
                                       ACCTNO,
                                       DATASOURCE,
                                       PRDCODE,
                                       TRXCODE,
                                       CCY,
                                       JOURNALCODE,
                                       JOURNALCODE2,
                                       STATUS,
                                       REVERSE,
                                       FLAG_CF,
                                       N_AMOUNT,
                                       SOURCEPROCESS,
                                       CREATEDDATE,
                                       CREATEDBY,
                                       BRANCH,
                                       PRDTYPE,
                                       IS_PNL,
                                       CF_ID,
                                       METHOD)
      SELECT /*+ PARALLEL(12) */ V_CURRDATE,
             A.MASTERID,
             B.FACNO,
             B.CIFNO,
             B.ACCTNO,
             B.DATASOURCE,
             B.PRD_CODE,
             B.TRX_CODE,
             B.CCY,
             'AMORT',
             CASE WHEN B.METHOD = 'SL' THEN 'ACCRU_SL' ELSE 'ACCRU' END,
             'ACT',
             C.FLAG_REVERSE           --20180305 follow flag rev from cost fee
                           ,
             B.FLAG_CF,
             CASE
                WHEN C.FLAG_REVERSE = 'N' THEN -1 * A.UNAMORT_AMT
                ELSE A.UNAMORT_AMT
             END                      --20180305 follow flag rev from cost fee
                ,
             'REV_ADJ',
             SYSTIMESTAMP,
             'ACFABN_ADJ',
             B.BRCODE,
             B.PRD_TYPE,
             '',
             B.CF_ID,
             B.METHOD
        FROM IFRS_CFID_JOURNAL_INTM_SUMM A
             JOIN IFRS_ACCT_COST_FEE B
                ON     (B.CF_ID = A.CF_ID OR B.CF_ID_REV = A.CF_ID) --only process today cf_id rev and its pair
                   AND B.DOWNLOAD_DATE = V_CURRDATE
                   AND B.FLAG_REVERSE = 'Y'
                   AND B.CF_ID_REV IS NOT NULL
             JOIN IFRS_ACCT_COST_FEE C
                ON C.CF_ID = A.CF_ID
       WHERE     A.DOWNLOAD_DATE = V_CURRDATE
             AND A.MASTERID IN (SELECT MASTERID FROM TMP_T2)
             AND A.UNAMORT_AMT <> 0;

   COMMIT;

   /******************************************************************************
   08. 20180109 adj diff between already amortized vs current for resona rev cf recalc start from loan_start_date
   *******************************************************************************/
   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_SL_COST_FEE_PREV');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_SL_COST_FEE_PREV',
                                  DEGREE    => 2);

   INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (DOWNLOAD_DATE,
                                       MASTERID,
                                       FACNO,
                                       CIFNO,
                                       ACCTNO,
                                       DATASOURCE,
                                       PRDCODE,
                                       TRXCODE,
                                       CCY,
                                       JOURNALCODE,
                                       JOURNALCODE2,
                                       STATUS,
                                       REVERSE,
                                       FLAG_CF,
                                       N_AMOUNT,
                                       SOURCEPROCESS,
                                       CREATEDDATE,
                                       CREATEDBY,
                                       BRANCH,
                                       PRDTYPE,
                                       IS_PNL,
                                       CF_ID,
                                       METHOD)
      SELECT /*+ PARALLEL(12) */ V_CURRDATE,
             A.MASTERID,
             B.FACNO,
             B.CIFNO,
             B.ACCTNO,
             B.DATASOURCE,
             B.PRDCODE,
             B.TRXCODE,
             B.CCY,
             'AMORT',
             CASE WHEN B.METHOD = 'SL' THEN 'ACCRU_SL' ELSE 'ACCRU' END,
             'ACT',
             'N',
             B.FLAG_CF,
             (CASE
                 WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT
                 ELSE B.AMOUNT
              END
              - A.UNAMORT_AMT),
             'REV_ADJ2',
             SYSTIMESTAMP,
             'ACFABN_ADJ',
             B.BRCODE,
             B.PRDTYPE,
             '',
             B.CF_ID,
             B.METHOD
        FROM IFRS_CFID_JOURNAL_INTM_SUMM A
             JOIN VW_LAST_SL_COST_FEE_PREV C
                ON C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
                   AND C.MASTERID = A.MASTERID
             JOIN IFRS_ACCT_SL_COST_FEE_PREV B
                ON     B.CF_ID = A.CF_ID
                   AND B.DOWNLOAD_DATE = C.DOWNLOAD_DATE
                   AND B.MASTERID = C.MASTERID
                   AND B.SEQ = C.SEQ
       WHERE A.DOWNLOAD_DATE = V_CURRDATE
             AND A.MASTERID IN (SELECT MASTERID FROM TMP_T2);

   COMMIT;

   /******************************************************************************
   09. 20180305 EIR adj diff between already amortized vs current for resona rev cf recalc start from loan_start_date
   *******************************************************************************/

   DBMS_STATS.UNLOCK_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_EIR_COST_FEE_PREV');
   DBMS_STATS.GATHER_TABLE_STATS (OWNNAME   => 'IFRS',
                                  TABNAME   => 'IFRS_ACCT_EIR_COST_FEE_PREV',
                                  DEGREE    => 2);

   INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (DOWNLOAD_DATE,
                                       MASTERID,
                                       FACNO,
                                       CIFNO,
                                       ACCTNO,
                                       DATASOURCE,
                                       PRDCODE,
                                       TRXCODE,
                                       CCY,
                                       JOURNALCODE,
                                       JOURNALCODE2,
                                       STATUS,
                                       REVERSE,
                                       FLAG_CF,
                                       N_AMOUNT,
                                       SOURCEPROCESS,
                                       CREATEDDATE,
                                       CREATEDBY,
                                       BRANCH,
                                       PRDTYPE,
                                       IS_PNL,
                                       CF_ID,
                                       METHOD)
      SELECT /*+ PARALLEL(12) */ V_CURRDATE,
             A.MASTERID,
             B.FACNO,
             B.CIFNO,
             B.ACCTNO,
             B.DATASOURCE,
             B.PRDCODE,
             B.TRXCODE,
             B.CCY,
             'AMORT',
             CASE WHEN B.METHOD = 'SL' THEN 'ACCRU_SL' ELSE 'ACCRU' END,
             'ACT',
             'N',
             B.FLAG_CF,
             (CASE
                 WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT
                 ELSE B.AMOUNT
              END
              - A.UNAMORT_AMT),
             'REV_ADJ2',
             SYSTIMESTAMP,
             'ACFABN_ADJ',
             B.BRCODE,
             B.PRDTYPE,
             '',
             B.CF_ID,
             B.METHOD
        FROM    IFRS_CFID_JOURNAL_INTM_SUMM A
             --JOIN VW_LAST_EIR_COST_FEE_PREV C
             --ON C.DOWNLOAD_DATE = A.DOWNLOAD_DATE AND C.MASTERID = A.MASTERID
             JOIN
                (SELECT A.*
                   FROM    IFRS_ACCT_EIR_COST_FEE_PREV A
                        JOIN
                           VW_LAST_EIR_COST_FEE_PREV E
                        ON A.MASTERID = E.MASTERID AND A.SEQ = E.SEQ) B
             ON     B.CF_ID = A.CF_ID
                AND B.DOWNLOAD_DATE = A.DOWNLOAD_DATE
                AND B.MASTERID = A.MASTERID
       WHERE A.DOWNLOAD_DATE = V_CURRDATE
             AND A.MASTERID IN (SELECT MASTERID FROM TMP_T2);

   --TUNING SP 17 DEC 2019
   /*FROM IFRS_CFID_JOURNAL_INTM_SUMM A
   JOIN VW_LAST_EIR_COST_FEE_PREV C
     ON C.DOWNLOAD_DATE=A.DOWNLOAD_DATE
     AND C.MASTERID=A.MASTERID
   JOIN IFRS_ACCT_EIR_COST_FEE_PREV B
     ON B.CF_ID=A.CF_ID
     AND B.DOWNLOAD_DATE=C.DOWNLOAD_DATE
     AND B.MASTERID=C.MASTERID
     AND B.SEQ=C.SEQ
   WHERE A.DOWNLOAD_DATE=V_CURRDATE
    AND A.MASTERID IN (SELECT MASTERID FROM TMP_T2);*/

   COMMIT;

   /******************************************************************************
   10.  update by RISWANTO 03-12-2015
       -- not reverse
   *******************************************************************************/

   MERGE INTO IFRS_ACCT_JOURNAL_INTM A
        USING IFRS_MASTER_EXCHANGE_RATE B
           ON (    A.CCY = B.CURRENCY
               AND B.DOWNLOAD_DATE = V_CURRDATE
               AND A.DOWNLOAD_DATE = V_CURRDATE
               AND A.REVERSE = 'N'
               AND A.SOURCEPROCESS IN ('ACFABN_ADJ', 'REV_ADJ', 'REV_ADJ2'))
   WHEN MATCHED
   THEN
      UPDATE SET A.N_AMOUNT_IDR = A.N_AMOUNT * COALESCE (B.RATE_AMOUNT, 1);

   COMMIT;

   /******************************************************************************
   11. intm reverse data
   *******************************************************************************/
   MERGE INTO IFRS_ACCT_JOURNAL_INTM A
        USING IFRS_MASTER_EXCHANGE_RATE B
           ON (    A.CCY = B.CURRENCY
               AND B.DOWNLOAD_DATE = V_CURRDATE
               AND A.DOWNLOAD_DATE = V_CURRDATE
               AND A.REVERSE = 'Y'
               AND A.SOURCEPROCESS IN ('ACFABN_ADJ', 'REV_ADJ', 'REV_ADJ2'))
   WHEN MATCHED
   THEN
      UPDATE SET A.N_AMOUNT_IDR = A.N_AMOUNT * COALESCE (B.RATE_AMOUNT, 1);

   COMMIT;

   /******************************************************************************
   12. INSERT INTO INTM SUMM
   *******************************************************************************/
   INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM_SUMM (DOWNLOAD_DATE,
                                            MASTERID,
                                            FACNO,
                                            CIFNO,
                                            ACCTNO,
                                            DATASOURCE,
                                            PRDCODE,
                                            TRXCODE,
                                            CCY,
                                            JOURNALCODE,
                                            STATUS,
                                            REVERSE,
                                            FLAG_CF,
                                            N_AMOUNT,
                                            SOURCEPROCESS,
                                            CREATEDDATE,
                                            CREATEDBY,
                                            BRANCH,
                                            IS_PNL,
                                            JOURNALCODE2,
                                            PRDTYPE)
      SELECT /*+ PARALLEL(12) */ DOWNLOAD_DATE,
             MASTERID,
             FACNO,
             CIFNO,
             ACCTNO,
             DATASOURCE,
             PRDCODE,
             TRXCODE,
             CCY,
             JOURNALCODE,
             STATUS,
             REVERSE,
             FLAG_CF,
             N_AMOUNT,
             SOURCEPROCESS,
             CREATEDDATE,
             CREATEDBY,
             BRANCH,
             IS_PNL,
             JOURNALCODE2,
             PRDTYPE
        FROM IFRS_ACCT_JOURNAL_INTM
       WHERE DOWNLOAD_DATE = V_CURRDATE
             AND SOURCEPROCESS IN ('ACFABN_ADJ', 'REV_ADJ', 'REV_ADJ2');

   COMMIT;

   /******************************************************************************
   13. added 03122015 update amort amount on cost fee summ
   *******************************************************************************/
   EXECUTE IMMEDIATE 'truncate table TMP_AP';

   INSERT /*+ PARALLEL(12) */ INTO TMP_AP (MASTERID, FLAG_CF, AMOUNT)
        SELECT /*+ PARALLEL(12) */ MASTERID,
               FLAG_CF,
               SUM (
                  CASE WHEN REVERSE = 'Y' THEN -1 * N_AMOUNT ELSE N_AMOUNT END)
                  AS AMORT_AMOUNT
          FROM IFRS_ACCT_JOURNAL_INTM_SUMM
         WHERE     DOWNLOAD_DATE = V_CURRDATE
               AND JOURNALCODE IN ('ACCRU', 'ACCRU_SL', 'AMORT')
               AND TRXCODE <> 'BENEFIT'
               AND MASTERID IN (SELECT MASTERID FROM TMP_T1
                                UNION ALL
                                SELECT MASTERID FROM TMP_T2)
      GROUP BY MASTERID, FLAG_CF;

   COMMIT;

   /******************************************************************************
   14. UPDATE FEE INTO TEMP
   *******************************************************************************/
   MERGE INTO IFRS_ACCT_COST_FEE_SUMM A
        USING TMP_AP B
           ON (    A.MASTERID = B.MASTERID
               AND A.DOWNLOAD_DATE = V_CURRDATE
               AND B.FLAG_CF = 'F'
               AND A.MASTERID IN (SELECT MASTERID FROM TMP_T1
                                  UNION ALL
                                  SELECT MASTERID FROM TMP_T2))
   WHEN MATCHED
   THEN
      UPDATE SET A.AMORT_FEE = B.AMOUNT;

   COMMIT;

   /******************************************************************************
   15. UPDATE COST INTO TEMP
   *******************************************************************************/

   MERGE INTO IFRS_ACCT_COST_FEE_SUMM A
        USING TMP_AP B
           ON (    A.MASTERID = B.MASTERID
               AND A.DOWNLOAD_DATE = V_CURRDATE
               AND B.FLAG_CF = 'C'
               AND A.MASTERID IN (SELECT MASTERID FROM TMP_T1
                                  UNION ALL
                                  SELECT MASTERID FROM TMP_T2))
   WHEN MATCHED
   THEN
      UPDATE SET A.AMORT_COST = B.AMOUNT;

   COMMIT;

   /******************************************************************************
   16. Update IFRS_CFID_JOURNAL_INTM_SUMM
   *******************************************************************************/
   UPDATE /*+ PARALLEL(12) */ IFRS_CFID_JOURNAL_INTM_SUMM
      SET AMORT_AMT = ITRCG_AMT * -1, UNAMORT_AMT = 0
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T1)
          AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   /******************************************************************************
   17. 20180108 Update IFRS_CFID_JOURNAL_INTM_SUMM for REV_ADJ
   *******************************************************************************/
   UPDATE /*+ PARALLEL(12) */ IFRS_CFID_JOURNAL_INTM_SUMM
      SET AMORT_AMT = ITRCG_AMT * -1, UNAMORT_AMT = 0
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T2)
          AND CF_ID IN
                 (SELECT CF_ID
                    FROM IFRS_ACCT_COST_FEE
                   WHERE     DOWNLOAD_DATE = V_CURRDATE
                         AND FLAG_REVERSE = 'Y'
                         AND CF_ID_REV IS NOT NULL
                  UNION ALL
                  SELECT CF_ID_REV
                    FROM IFRS_ACCT_COST_FEE
                   WHERE     DOWNLOAD_DATE = V_CURRDATE
                         AND FLAG_REVERSE = 'Y'
                         AND CF_ID_REV IS NOT NULL)
          AND DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   /******************************************************************************
   18. 20180109 update IFRS_CFID_JOURNAL_INTM_SUMM for REV_ADJ2
   *******************************************************************************/
   MERGE INTO IFRS_CFID_JOURNAL_INTM_SUMM Z
        USING IFRS_ACCT_JOURNAL_INTM A
           ON (    A.CF_ID = Z.CF_ID
               AND Z.MASTERID IN (SELECT MASTERID FROM TMP_T2)
               AND Z.DOWNLOAD_DATE = V_CURRDATE
               AND A.DOWNLOAD_DATE = V_CURRDATE
               AND A.SOURCEPROCESS = 'REV_ADJ2')
   WHEN MATCHED
   THEN
      UPDATE SET
         Z.AMORT_AMT = Z.AMORT_AMT + A.N_AMOUNT,
         Z.UNAMORT_AMT = Z.UNAMORT_AMT + A.N_AMOUNT;

   COMMIT;

   --- 20180827 DELETE JOURNAL WHEN AMOUNT = 0
   DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_INTM
         WHERE DOWNLOAD_DATE = V_CURRDATE AND N_AMOUNT = 0;

   COMMIT;

   INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,
                               DTM,
                               OPS,
                               PROCNAME,
                               REMARK)
        VALUES (V_CURRDATE,
                SYSTIMESTAMP,
                'END',
                'SP_IFRS_JRNL_ACF_ABN_ADJ',
                '');

   COMMIT;
END;