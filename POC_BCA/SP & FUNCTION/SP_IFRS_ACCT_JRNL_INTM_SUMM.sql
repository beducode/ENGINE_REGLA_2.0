CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_JRNL_INTM_SUMM
AS
  V_CURRDATE    DATE;
  V_PREVDATE    DATE;

BEGIN

    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_ACCT_JRNL_INTM_SUMM','');
    COMMIT;

    /******************************************************************************
    02. DELETE
    *******************************************************************************/
    DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_INTM_SUMM WHERE DOWNLOAD_DATE>=V_CURRDATE;

    COMMIT;

    /******************************************************************************
    03. INSERT INTO TEMP
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_JRNL1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_JRNL1
    (FACNO
    ,CIFNO
    ,DATASOURCE
    ,PRDCODE
    ,TRXCODE
    ,CCY
    ,JOURNALCODE
    ,STATUS
    ,REVERSE
    ,N_AMOUNT
    ,MASTERID
    ,ACCTNO
    ,FLAG_CF
    ,BRANCH
    ,IS_PNL
    ,JOURNALCODE2
    ,PRDTYPE
    )
    SELECT /*+ PARALLEL(12) */ FACNO
          ,CIFNO
          ,DATASOURCE
          ,PRDCODE
          ,TRXCODE
          ,CCY
          ,JOURNALCODE
          ,STATUS
          ,REVERSE
          ,N_AMOUNT
          ,MASTERID
          ,ACCTNO
          ,FLAG_CF
          ,BRANCH
          ,IS_PNL
          ,JOURNALCODE2
          ,PRDTYPE
    FROM IFRS_ACCT_JOURNAL_INTM_SUMM
    WHERE DOWNLOAD_DATE=V_PREVDATE
    UNION ALL
    SELECT /*+ PARALLEL(12) */ FACNO
          ,CIFNO
          ,DATASOURCE
          ,PRDCODE
          ,TRXCODE
          ,CCY
          ,JOURNALCODE
          ,STATUS
          ,REVERSE
          ,N_AMOUNT
          ,MASTERID
          ,ACCTNO
          ,FLAG_CF
          ,BRANCH
          ,IS_PNL
          ,JOURNALCODE2
          ,PRDTYPE
    FROM IFRS_ACCT_JOURNAL_INTM WHERE DOWNLOAD_DATE=V_CURRDATE;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','TMP INSERT','');

    COMMIT;

    /******************************************************************************
    04. INSERT INTO ACCT JOURNAL SUMM
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM_SUMM
    (DOWNLOAD_DATE
    ,FACNO
    ,CIFNO
    ,DATASOURCE
    ,PRDCODE
    ,TRXCODE
    ,CCY
    ,JOURNALCODE
    ,STATUS
    ,REVERSE
    ,N_AMOUNT
    ,MASTERID
    ,CREATEDDATE
    ,ACCTNO
    ,FLAG_CF
    ,BRANCH
    ,IS_PNL
    ,JOURNALCODE2
    ,PRDTYPE
    )
    SELECT /*+ PARALLEL(12) */ DOWNLOAD_DATE
          ,FACNO
          ,CIFNO
          ,DATASOURCE
          ,PRDCODE
          ,TRXCODE
          ,CCY
          ,JOURNALCODE
          ,STATUS
          ,REVERSE
          ,AMOUNT
          ,MASTERID
          ,CREATEDDATE
          ,ACCTNO
          ,FLAG_CF
          ,BRANCH
          ,IS_PNL
          ,JOURNALCODE2
          ,PRDTYPE
    FROM (SELECT V_CURRDATE AS DOWNLOAD_DATE
                ,FACNO
                ,CIFNO
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,SUM(COALESCE(N_AMOUNT,0)) AS AMOUNT
                ,MASTERID
                ,SYSTIMESTAMP AS CREATEDDATE
                ,ACCTNO
                ,FLAG_CF
                ,BRANCH
                ,IS_PNL
                ,JOURNALCODE2
                ,PRDTYPE
          FROM TMP_JRNL1
          GROUP BY FACNO
                  ,CIFNO
                  ,DATASOURCE
                  ,PRDCODE
                  ,TRXCODE
                  ,CCY
                  ,JOURNALCODE
                  ,STATUS
                  ,REVERSE
                  ,MASTERID
                  ,ACCTNO
                  ,FLAG_CF
                  ,BRANCH
                  ,IS_PNL
                  ,JOURNALCODE2
                  ,PRDTYPE
    ) B;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SUMM INSERT','');

    COMMIT;

    /******************************************************************************
    05. update amort amount on cost fee summ IN TEMP
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_AP';

    INSERT /*+ PARALLEL(12) */ INTO TMP_AP(MASTERID,FLAG_CF,AMOUNT)
    SELECT /*+ PARALLEL(12) */ MASTERID
          ,FLAG_CF
          ,SUM(CASE WHEN REVERSE='Y' THEN -1 * N_AMOUNT ELSE N_AMOUNT END) AS AMORT_AMOUNT
    FROM IFRS_ACCT_JOURNAL_INTM_SUMM
    WHERE DOWNLOAD_DATE=V_CURRDATE
    AND JOURNALCODE IN ('ACCRU','ACCRU_SL','AMORT')
    AND TRXCODE<>'BENEFIT'
    GROUP BY MASTERID,FLAG_CF;

    COMMIT;

    /******************************************************************************
    06. UPDATE AMORT FEE
    *******************************************************************************/
    MERGE INTO IFRS_ACCT_COST_FEE_SUMM A
    USING TMP_AP B
    ON (A.MASTERID=B.MASTERID
        AND A.DOWNLOAD_DATE=V_CURRDATE
        AND B.FLAG_CF='F'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.AMORT_FEE = B.AMOUNT;

    COMMIT;
    /******************************************************************************
    07. UPDATE AMORT COST
    *******************************************************************************/
    MERGE INTO IFRS_ACCT_COST_FEE_SUMM A
    USING TMP_AP B
    ON (A.MASTERID=B.MASTERID
        AND A.DOWNLOAD_DATE=V_CURRDATE
        AND B.FLAG_CF='C'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.AMORT_COST = B.AMOUNT;


    COMMIT;
    /******************************************************************************
    08. GET SUMM JOURNAL
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_AP';

    INSERT /*+ PARALLEL(12) */ INTO TMP_AP(MASTERID,FLAG_CF,AMOUNT)
    SELECT /*+ PARALLEL(12) */ MASTERID
          ,FLAG_CF
          ,SUM(CASE WHEN REVERSE='Y' THEN -1 * N_AMOUNT ELSE N_AMOUNT END) AS AMORT_AMOUNT
    FROM IFRS_ACCT_JOURNAL_INTM_SUMM
    WHERE DOWNLOAD_DATE=V_CURRDATE
    AND JOURNALCODE IN ('ACCRU','AMORT')
    AND TRXCODE='BENEFIT'
    GROUP BY MASTERID,FLAG_CF;

    COMMIT;
    /******************************************************************************
    09. UPDATE AMORT FEE STAFF
    *******************************************************************************/
    MERGE INTO IFRS_STAFF_BENEFIT_SUMM A
    USING TMP_AP B
    ON (A.MASTERID=B.MASTERID
        AND A.DOWNLOAD_DATE=V_CURRDATE
        AND B.FLAG_CF='F'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.AMORT_FEE=B.AMOUNT;

    COMMIT;

    /******************************************************************************
    10. UPDATE AMORT COST STAFF
    *******************************************************************************/
    MERGE INTO IFRS_STAFF_BENEFIT_SUMM A
    USING TMP_AP B
    ON (A.MASTERID=B.MASTERID
        AND A.DOWNLOAD_DATE=V_CURRDATE
        AND B.FLAG_CF='C'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.AMORT_COST=B.AMOUNT;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','UPDATE AMORT AMT','');
    COMMIT;

    /******************************************************************************
    11. update initial cost fee
    *******************************************************************************/
    MERGE INTO IFRS_IMA_AMORT_CURR A
    USING (SELECT DOWNLOAD_DATE,MASTERID,SUM(AMOUNT_COST)AMOUNT_COST, SUM(AMOUNT_FEE)AMOUNT_FEE FROM IFRS_ACCT_COST_FEE_SUMM
            WHERE DOWNLOAD_DATE = V_CURRDATE GROUP BY DOWNLOAD_DATE,MASTERID)B
    ON ( A.MASTERID=B.MASTERID
         AND B.DOWNLOAD_DATE=V_CURRDATE
         --AND NVL(A.FACILITY_NUMBER, ' ') = NVL(B.FACNO, ' ')
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.INITIAL_UNAMORT_TXN_COST=B.AMOUNT_COST
       ,A.INITIAL_UNAMORT_ORG_FEE=B.AMOUNT_FEE
       WHERE A.AMORT_TYPE = 'EIR';

    COMMIT;

    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING (SELECT DOWNLOAD_DATE,MASTERID,SUM(AMOUNT_COST)AMOUNT_COST, SUM(AMOUNT_FEE)AMOUNT_FEE FROM IFRS_ACCT_COST_FEE_SUMM
            WHERE DOWNLOAD_DATE = V_CURRDATE GROUP BY DOWNLOAD_DATE,MASTERID)B
    ON ( A.MASTERID=B.MASTERID
         AND B.DOWNLOAD_DATE=V_CURRDATE
         AND A.DOWNLOAD_DATE=V_CURRDATE
         --AND NVL(A.FACILITY_NUMBER, ' ') = NVL(B.FACNO, ' ')
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.INITIAL_UNAMORT_TXN_COST=B.AMOUNT_COST
       ,A.INITIAL_UNAMORT_ORG_FEE=B.AMOUNT_FEE
       WHERE A.AMORT_TYPE = 'EIR'; --ADD BY WILLY 22 JUN 2023


    /******************************************************************************
    12. update LBM initial cost fee
    *******************************************************************************/
    MERGE INTO IFRS_IMA_AMORT_CURR A
    USING (SELECT DOWNLOAD_DATE,MASTERID,SUM(AMOUNT_COST)AMOUNT_COST, SUM(AMOUNT_FEE)AMOUNT_FEE FROM IFRS_LBM_STAFF_BENEFIT_SUMM
            WHERE DOWNLOAD_DATE = V_CURRDATE GROUP BY DOWNLOAD_DATE,MASTERID)B
    ON ( A.MASTERID=B.MASTERID
         AND B.DOWNLOAD_DATE=V_CURRDATE
        -- AND NVL(A.FACILITY_NUMBER, ' ') = NVL(B.FACNO, ' ')
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.INITIAL_UNAMORT_TXN_COST=B.AMOUNT_COST
       ,A.INITIAL_UNAMORT_ORG_FEE=B.AMOUNT_FEE;

    COMMIT;

    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING (SELECT DOWNLOAD_DATE,MASTERID,SUM(AMOUNT_COST)AMOUNT_COST, SUM(AMOUNT_FEE)AMOUNT_FEE FROM IFRS_LBM_STAFF_BENEFIT_SUMM
            WHERE DOWNLOAD_DATE = V_CURRDATE GROUP BY DOWNLOAD_DATE,MASTERID)B
    ON ( A.MASTERID=B.MASTERID
         AND B.DOWNLOAD_DATE=V_CURRDATE
         AND A.DOWNLOAD_DATE=V_CURRDATE
         --AND NVL(A.FACILITY_NUMBER, ' ') = NVL(B.FACNO, ' ')
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.INITIAL_UNAMORT_TXN_COST=B.AMOUNT_COST
       ,A.INITIAL_UNAMORT_ORG_FEE=B.AMOUNT_FEE;


    COMMIT;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','UPDATE INITIAL AMT','');

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_ACCT_JRNL_INTM_SUMM','');

    COMMIT;

END;