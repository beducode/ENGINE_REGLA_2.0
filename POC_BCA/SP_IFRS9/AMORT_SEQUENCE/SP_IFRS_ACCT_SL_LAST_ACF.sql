CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SL_LAST_ACF
AS
    V_CURRDATE	DATE;
    V_PREVDATE	DATE;

BEGIN
    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/

    SELECT MAX(CURRDATE) INTO V_CURRDATE FROM IFRS_PRC_DATE_AMORT;
    SELECT MAX(PREVDATE) INTO V_PREVDATE FROM IFRS_PRC_DATE_AMORT;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_ACCT_SL_LAST_ACF','');

    COMMIT;

    /******************************************************************************
    02. -- do this after accrual process to fully amortization of specific account
        -- closed accounts subject to this condition
        -- get closed masterid with active ecf
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_T1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1(MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT A.MASTERID
    FROM IFRS_ACCT_CLOSED A
    JOIN IFRS_ACCT_SL_ACF B
      ON B.DOWNLOAD_DATE=V_PREVDATE
      AND B.MASTERID=A.MASTERID
    JOIN IFRS_ACCT_SL_ECF C
      ON C.AMORTSTOPDATE IS NULL
      AND C.MASTERID=A.MASTERID
      AND C.PREVDATE=C.PMTDATE
    WHERE A.DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;
    /******************************************************************************
    03. GET MAX ID OF ACF
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_P1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_P1(ID)
    SELECT /*+ PARALLEL(12) */ MAX(ID) ID
    FROM IFRS_ACCT_SL_ACF
    WHERE DOWNLOAD_DATE=V_PREVDATE AND MASTERID IN (SELECT MASTERID FROM TMP_T1)
    GROUP BY MASTERID;

    COMMIT;
    /******************************************************************************
    04. AMORT ACCRUED
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_SL_ACCRU_PREV
    SET STATUS=TO_CHAR(V_CURRDATE,'YYYYMMDD') ,CREATEDBY='SL_LAST_ACF'
    WHERE STATUS='ACT' AND MASTERID IN (SELECT MASTERID FROM TMP_T1);

    COMMIT;
    /******************************************************************************
    05.insert last acf for full amort
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_SL_ACF
    (DOWNLOAD_DATE
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
    ,ACF_CODE
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
          ,'SP_SL_LAST_ACF' CREATEDBY2
          ,'9'
    FROM IFRS_ACCT_SL_ACF
    WHERE DOWNLOAD_DATE=V_PREVDATE
    AND ID IN (SELECT ID FROM TMP_P1);

    COMMIT;

    /******************************************************************************
    06. STOP SL ECF
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_SL_ECF
    SET AMORTSTOPDATE=V_CURRDATE,AMORTSTOPREASON='CLOSED'
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T1)
    AND AMORTSTOPDATE IS NULL;

    COMMIT;

    /******************************************************************************
    07. fully amort abnormal fee on acf
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_T2';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T2(MASTERID)
    SELECT /*+ PARALLEL(12) */ MASTERID
    FROM IFRS_ACCT_SL_ACF
    WHERE
      CASE
        WHEN (N_UNAMORT_FEE>0 OR N_UNAMORT_COST<0) AND COALESCE(FLAG_AL,'A')='A' THEN 1
        WHEN (N_UNAMORT_FEE<0 OR N_UNAMORT_COST>0) AND COALESCE(FLAG_AL,'A')<>'A' THEN 1
        ELSE 0
      END=1
    AND ACF_CODE='2' AND DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_SL_ACF
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
      , CREATEDDATE=SYSTIMESTAMP
      ,CREATEDBY = 'SP_SL_LAST_ACF_ABN'
    WHERE
      CASE
        WHEN (N_UNAMORT_FEE>0 OR N_UNAMORT_COST<0) AND COALESCE(FLAG_AL,'A')='A' THEN 1
        WHEN (N_UNAMORT_FEE<0 OR N_UNAMORT_COST>0) AND COALESCE(FLAG_AL,'A')<>'A' THEN 1
        ELSE 0
      END=1
    AND ACF_CODE='2' AND DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;

    /******************************************************************************
    08. UPDATE STATUS ABN IN IFRS_ACCT_SL_ECF
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_SL_ECF
    SET AMORTSTOPDATE=V_CURRDATE,AMORTSTOPREASON='ABN'
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T2)
    AND AMORTSTOPDATE IS NULL;

    COMMIT;
    /******************************************************************************
    09. UPDATE STATUS ABN IN IFRS_ACCT_SL_ACCRU_PREV
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_SL_ACCRU_PREV
    SET STATUS=TO_CHAR(V_CURRDATE,'YYYYMMDD') ,CREATEDBY='SL_LAST_ACF_ABN'
    WHERE STATUS='ACT'
    AND MASTERID IN (SELECT MASTERID FROM TMP_T2);
    COMMIT;
    /******************************************************************************
    10.stop sl ecf end today
    *******************************************************************************/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_SL_ECF
    SET AMORTSTOPDATE=V_CURRDATE,AMORTSTOPREASON='END'
    WHERE AMORTENDDATE=V_CURRDATE AND AMORTSTOPDATE IS NULL;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_ACCT_SL_LAST_ACF','');
    COMMIT;

END;