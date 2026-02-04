CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SL_UPD_UNAMRT
AS

  V_CURRDATE	DATE;
	V_PREVDATE	DATE;

BEGIN
    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT MAX(CURRDATE),MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_ACCT_SL_UPD_UNAMRT','');

    COMMIT;

    /******************************************************************************
    02. CLEAN UP
    *******************************************************************************/
    UPDATE /*+ PARALLEL(8) */ IFRS_IMA_AMORT_CURR
    SET UNAMORT_FEE_AMT = 0
        ,UNAMORT_COST_AMT = 0
        ,FAIR_VALUE_AMOUNT = NULL
        ,LOAN_START_AMORTIZATION = NULL
        ,LOAN_END_AMORTIZATION = NULL
    WHERE DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;


    UPDATE /*+ PARALLEL(8) */ IFRS_MASTER_ACCOUNT
    SET  UNAMORT_FEE_AMT = 0
        ,UNAMORT_COST_AMT = 0
        ,FAIR_VALUE_AMOUNT = NULL
        ,LOAN_START_AMORTIZATION = NULL
        ,LOAN_END_AMORTIZATION = NULL
    WHERE DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_ACCT_SL_UPD_UNAMRT','');

    COMMIT;
    /******************************************************************************
    03. GET ACTIVE ECF
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_B1';

    INSERT /*+ PARALLEL(8) */ INTO TMP_B1(MASTERID)
    SELECT /*+ PARALLEL(8) */ DISTINCT MASTERID
    FROM IFRS_ACCT_SL_ECF
    WHERE AMORTSTOPDATE IS NULL;

    COMMIT;

    /******************************************************************************
    04. GET LAST ACF ID
    *******************************************************************************/
    EXECUTE IMMEDIATE 'truncate table TMP_P1';

    INSERT /*+ PARALLEL(8) */ INTO TMP_P1(ID)
    SELECT /*+ PARALLEL(8) */ MAX(ID) ID
    FROM IFRS_ACCT_SL_ACF
    WHERE DOWNLOAD_DATE=V_CURRDATE AND MASTERID IN (SELECT MASTERID FROM TMP_B1)
    GROUP BY MASTERID;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG1','SP_IFRS_ACCT_SL_UPD_UNAMRT','');

    COMMIT;

    /******************************************************************************
    05. UPDATE TO IMA
    *******************************************************************************/
    MERGE INTO IFRS_IMA_AMORT_CURR A
    USING (	SELECT B.DOWNLOAD_DATE,B.MASTERID,B.N_UNAMORT_FEE,B.N_UNAMORT_COST,B.ECFDATE,E.AMORTENDDATE
            FROM IFRS_ACCT_SL_ACF B
            JOIN TMP_P1 C ON C.ID=B.ID
            LEFT JOIN IFRS_ACCT_SL_ECF E ON E.MASTERID=B.MASTERID AND E.PREVDATE=E.PMTDATE AND E.DOWNLOAD_DATE=B.ECFDATE
          ) X
    ON (X.MASTERID=A.MASTERID
        AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.UNAMORT_FEE_AMT = X.N_UNAMORT_FEE
      , A.UNAMORT_COST_AMT = X.N_UNAMORT_COST
      , A.FAIR_VALUE_AMOUNT = A.OUTSTANDING + X.N_UNAMORT_FEE + X.N_UNAMORT_COST
      --,FAIR_VALUE_AMOUNT = dbo.IMA_AMORT_CURR.OUTSTANDING_JF + x.n_unamort_fee + x.n_unamort_cost
      , A.LOAN_START_AMORTIZATION=X.ECFDATE
      , A.LOAN_END_AMORTIZATION=X.AMORTENDDATE
      , A.AMORT_TYPE='SL';

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG2','SP_IFRS_ACCT_SL_UPD_UNAMRT','');

    COMMIT;

    /******************************************************************************
    06. UPDATE IMA PART 2
    *******************************************************************************/
    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING IFRS_IMA_AMORT_CURR X
    ON (A.MASTERID=X.MASTERID
        AND A.DOWNLOAD_DATE=X.DOWNLOAD_DATE
        AND X.AMORT_TYPE='SL'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.UNAMORT_FEE_AMT = X.UNAMORT_FEE_AMT
      , A.UNAMORT_COST_AMT = X.UNAMORT_COST_AMT
      , A.FAIR_VALUE_AMOUNT = X.FAIR_VALUE_AMOUNT
      , A.LOAN_START_AMORTIZATION = X.LOAN_START_AMORTIZATION
      , A.LOAN_END_AMORTIZATION = X.LOAN_END_AMORTIZATION
      , A.AMORT_TYPE = X.AMORT_TYPE
      --20160407 update to sl unamort fields
      --,unamortizedamount_sl = b.unamortizedamount
      --,unamortized_fee_amount_sl = b.unamortized_fee_amount
        --,unamortized_cost_amount_sl = b.unamortized_cost_amount
      ;

    COMMIT;


    --20160407 update to sl unamort fields
    /* pindah ke atas dijadikan single update
    update dbo.PMA
    set  unamortizedamount_sl = b.unamortizedamount
        ,unamortized_fee_amount_sl = b.unamortized_fee_amount
        ,unamortized_cost_amount_sl = b.unamortized_cost_amount
    from IMA_AMORT_CURR b
    where dbo.PMA.masterid=b.masterid and dbo.PMA.DOWNLOAD_DATE=b.DOWNLOAD_DATE and b.amort_type='SL'
    */

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_ACCT_SL_UPD_UNAMRT','');

    COMMIT;

END;