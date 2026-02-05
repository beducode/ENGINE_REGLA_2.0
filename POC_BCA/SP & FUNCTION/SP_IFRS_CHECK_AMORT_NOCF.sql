CREATE OR REPLACE PROCEDURE SP_IFRS_CHECK_AMORT_NOCF
AS
  V_CURRDATE    DATE;
  V_PREVDATE    DATE;


BEGIN

    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT MAX(CURRDATE),MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_CHECK_AMORT_NOCF','');

    COMMIT;


    EXECUTE IMMEDIATE 'truncate table IFRS_CHECK_AMORT_NOCF';
    EXECUTE IMMEDIATE 'truncate table TMP_X';

    COMMIT;

    /******************************************************************************
    02. INSERT INTO TEMP
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO TMP_X(DOWNLOAD_DATE,CCY,MASTERID,JOURNALCODE,AMOUNT)
    SELECT /*+ PARALLEL(12) */ DOWNLOAD_DATE,CCY,MASTERID
          , CASE WHEN JOURNALCODE='ACRU4' THEN 'AMRT4' ELSE  JOURNALCODE END AS JOURNALCODE
          , SUM(CASE WHEN REVERSE='Y' THEN -1 * N_AMOUNT ELSE N_AMOUNT END) AS AMOUNT
    FROM IFRS_ACCT_JOURNAL_INTM_SUMM
    WHERE DOWNLOAD_DATE=V_CURRDATE AND JOURNALCODE IN ('ACRU4','AMRT4')
    GROUP BY DOWNLOAD_DATE,CCY
    , CASE WHEN JOURNALCODE='ACRU4' THEN 'AMRT4' ELSE  JOURNALCODE END,MASTERID;

    COMMIT;

    /******************************************************************************
    03. INSERT INTO CHECK AMORT NO CF
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_CHECK_AMORT_NOCF(DOWNLOAD_DATE,MASTERID,CCY,JURNAL_AMORT_AMT)
    SELECT /*+ PARALLEL(12) */ DOWNLOAD_DATE,MASTERID,CCY,AMOUNT FROM TMP_X;

    COMMIT;
    /******************************************************************************
    04, UPDATE
    *******************************************************************************/
    MERGE INTO IFRS_CHECK_AMORT_NOCF A
    USING (SELECT A.*
           FROM IFRS_ACCT_EIR_ACF A
           WHERE A.ID IN (SELECT MAX(ID) FROM IFRS_ACCT_EIR_ACF
                                  WHERE DOWNLOAD_DATE>=V_PREVDATE AND DOWNLOAD_DATE<=V_CURRDATE
                                  AND MASTERID IN (SELECT MASTERID FROM TMP_X)
                                  GROUP BY MASTERID
                                  )
           ) B
    ON (A.MASTERID=B.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.ACF_UNAMORT_AMT = B.N_UNAMORT_PREV_NOCF + B.N_ACCRU_NOCF;
    COMMIT;


    UPDATE /*+ PARALLEL(12) */ IFRS_CHECK_AMORT_NOCF
    SET CONTROL_AMT = JURNAL_AMORT_AMT - ACF_UNAMORT_AMT;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_CHECK_AMORT_NOCF','');

    COMMIT;
END;