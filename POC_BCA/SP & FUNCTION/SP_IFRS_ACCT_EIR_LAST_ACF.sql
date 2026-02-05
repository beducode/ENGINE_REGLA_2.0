CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_LAST_ACF
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
    VALUES( V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_ACCT_EIR_LAST_ACF','');

    -- do this after accrual process to fully amortization of specific account
    -- closed accounts subject to this condition
    -- get closed masterid

    COMMIT;

    /******************************************************************************
    02. INSERT INTO TEMP FOR CLOSED ACCOUNT
    *******************************************************************************/

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1(MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT A.MASTERID
    FROM IFRS_ACCT_CLOSED A
    JOIN IFRS_ACCT_EIR_ACF B
      ON B.DOWNLOAD_DATE=V_PREVDATE
      AND B.MASTERID=A.MASTERID
    JOIN IFRS_ACCT_EIR_ECF C
      ON C.AMORTSTOPDATE IS NULL
      AND C.MASTERID=A.MASTERID
      AND C.PREV_PMT_DATE=C.PMT_DATE
    WHERE A.DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;

    /******************************************************************************
    03. GET MAX ID of ACF
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_P1(ID)
    SELECT /*+ PARALLEL(12) */ MAX(ID) ID
    FROM IFRS_ACCT_EIR_ACF
    WHERE DOWNLOAD_DATE=V_PREVDATE AND MASTERID IN (SELECT MASTERID FROM TMP_T1)
    GROUP BY MASTERID;

    COMMIT;

    /******************************************************************************
    04. AMORT ACCRUED
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ACCRU_PREV
    SET STATUS=TO_CHAR(V_CURRDATE,'YYYYMMDD') ,CREATEDBY='EIR_LAST_ACF'
    WHERE STATUS='ACT' AND MASTERID IN (SELECT MASTERID FROM TMP_T1);

    COMMIT;

    /******************************************************************************
    05. INSERT LAST ACF FOR FULL AMORT
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_ACF(
     DOWNLOAD_DATE
    ,FACNO
    ,CIFNO
    ,ACCTNO
    ,DATASOURCE
    ,ECFDATE
    ,MASTERID
    ,BRANCH
    ,N_UNAMORT_FEE
    ,N_AMORT_FEE
    ,N_ACCRU_FEE
    ,N_ACCRUFULL_FEE
    ,N_UNAMORT_COST
    ,N_AMORT_COST
    ,N_ACCRU_COST
    ,N_ACCRUFULL_COST
    ,N_ACCRU_PREV_FEE
    ,N_ACCRU_PREV_COST
    ,DO_AMORT
    ,CREATEDDATE
    ,CREATEDBY
    )
    SELECT /*+ PARALLEL(12) */ V_CURRDATE
          ,FACNO
          ,CIFNO
          ,ACCTNO
          ,DATASOURCE
          ,ECFDATE
          ,MASTERID
          ,BRANCH
          ,0
          , N_AMORT_FEE + N_UNAMORT_FEE
          , CASE WHEN DO_AMORT='Y' THEN N_AMORT_FEE - N_UNAMORT_FEE ELSE - N_UNAMORT_FEE + N_ACCRU_FEE END
          , CASE WHEN DO_AMORT='Y' THEN N_AMORT_FEE - N_UNAMORT_FEE ELSE - N_UNAMORT_FEE + N_ACCRU_FEE END
          , 0 AS N_UNAMORT_COST2
          , N_AMORT_COST + N_UNAMORT_COST
          , CASE WHEN DO_AMORT='Y' THEN N_AMORT_COST - N_UNAMORT_COST ELSE - N_UNAMORT_COST + N_ACCRU_COST END
          , CASE WHEN DO_AMORT='Y' THEN N_AMORT_COST - N_UNAMORT_COST ELSE - N_UNAMORT_COST + N_ACCRU_COST END
          , 0
          , 0
          , 'Y' AS DO_AMORT2
          , SYSTIMESTAMP CREATEDDATE2
          ,'SP_EIR_LAST_ACF' CREATEDBY2
    FROM IFRS_ACCT_EIR_ACF
    WHERE DOWNLOAD_DATE=V_PREVDATE AND ID IN (SELECT ID FROM TMP_P1);

    COMMIT;

    /******************************************************************************
    06. STOP EIR ECF
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ECF
    SET AMORTSTOPDATE=V_CURRDATE,AMORTSTOPMSG='CLOSED'
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T1)
    AND AMORTSTOPDATE IS NULL;

    COMMIT;

    /******************************************************************************
    07. FULLY AMORT ABNORMAL FEE ON ACF
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2';

    /*  ridwan  start abnormal unamortized */
    INSERT /*+ PARALLEL(12) */ INTO TMP_T2(MASTERID)
    SELECT /*+ PARALLEL(12) */ MASTERID
    FROM IFRS_ACCT_EIR_ACF
    WHERE CASE
              WHEN (N_UNAMORT_FEE>0 OR N_UNAMORT_COST<0) AND COALESCE(FLAG_AL,'A')='A' THEN 1
              WHEN (N_UNAMORT_FEE<0 OR N_UNAMORT_COST>0) AND COALESCE(FLAG_AL,'A')<>'A' THEN 1
              ELSE 0
          END=1
    AND ACF_CODE='2'
    AND DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ACF
    SET
        N_UNAMORT_FEE = 0
      , N_AMORT_FEE = N_AMORT_FEE + N_UNAMORT_FEE
      , N_ACCRU_FEE = CASE WHEN DO_AMORT='Y' THEN N_AMORT_FEE - N_UNAMORT_FEE ELSE - N_UNAMORT_FEE + N_ACCRU_FEE END
      , N_ACCRUFULL_FEE = CASE WHEN DO_AMORT='Y' THEN N_AMORT_FEE - N_UNAMORT_FEE ELSE - N_UNAMORT_FEE + N_ACCRU_FEE END
      , N_UNAMORT_COST = 0
      , N_AMORT_COST = N_AMORT_COST + N_UNAMORT_COST
      , N_ACCRU_COST = CASE WHEN DO_AMORT='Y' THEN N_AMORT_COST - N_UNAMORT_COST ELSE - N_UNAMORT_COST + N_ACCRU_COST END
      , N_ACCRUFULL_COST = CASE WHEN DO_AMORT='Y' THEN N_AMORT_COST - N_UNAMORT_COST ELSE - N_UNAMORT_COST + N_ACCRU_COST END
      , N_ACCRU_PREV_FEE = 0
      , N_ACCRU_PREV_COST = 0
      , DO_AMORT = 'Y'
      , CREATEDDATE = SYSTIMESTAMP
      , CREATEDBY = 'SP_EIR_LAST_ACF_ABN'
    WHERE CASE
              WHEN (N_UNAMORT_FEE>0 OR N_UNAMORT_COST<0) AND COALESCE(FLAG_AL,'A')='A' THEN 1
              WHEN (N_UNAMORT_FEE<0 OR N_UNAMORT_COST>0) AND COALESCE(FLAG_AL,'A')<>'A' THEN 1
              ELSE 0
          END=1
    AND ACF_CODE='2'
    AND DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;
    /*ridwan  end abnormal unamortized*/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ECF
    SET AMORTSTOPDATE=V_CURRDATE,AMORTSTOPMSG='ABN'
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T2)
    AND AMORTSTOPDATE IS NULL;

    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ACCRU_PREV
    SET STATUS=TO_CHAR(V_CURRDATE,'YYYYMMDD') ,CREATEDBY='EIR_LAST_ACF_ABN'
    WHERE STATUS='ACT' AND MASTERID IN (SELECT MASTERID FROM TMP_T2);

    COMMIT;

    /******************************************************************************
    08. STOP EIR ECF END TODAY
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ECF
    SET AMORTSTOPDATE=V_CURRDATE,AMORTSTOPMSG='END'
    WHERE ENDAMORTDATE=V_CURRDATE AND AMORTSTOPDATE IS NULL;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_ACCT_EIR_LAST_ACF','');

    COMMIT;
END;