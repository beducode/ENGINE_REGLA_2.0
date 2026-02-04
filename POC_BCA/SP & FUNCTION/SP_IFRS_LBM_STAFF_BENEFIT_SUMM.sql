CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_STAFF_BENEFIT_SUMM
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;


BEGIN


    SELECT MAX(CURRDATE)
      , MAX(PREVDATE) INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(  V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_STAFF_BENEFIT_SUMM','');

    --DELETE FIRST
    DELETE /*+ PARALLEL(12) */ FROM IFRS_LBM_STAFF_BENEFIT_SUMM
    WHERE DOWNLOAD_DATE >= V_CURRDATE;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_STAFF_BENEFIT_SUMM (
      DOWNLOAD_DATE
      ,MASTERID
      ,BRCODE
      ,CIFNO
      ,FACNO
      ,ACCTNO
      ,DATASOURCE
      ,CCY
      ,AMOUNT_FEE
      ,AMOUNT_COST
      ,CREATEDDATE
      ,CREATEDBY
      ,AMORT_FEE
      ,AMORT_COST
      )
    SELECT /*+ PARALLEL(12) */ DOWNLOAD_DATE
      ,MASTERID
      ,BRCODE
      ,CIFNO
      ,FACNO
      ,ACCTNO
      ,DATASOURCE
      ,CCY
      ,AMOUNT_FEE
      ,AMOUNT_COST
      ,CREATEDDATE
      ,CREATEDBY
      ,AMORT_FEE
      ,AMORT_COST
    FROM (SELECT V_CURRDATE AS DOWNLOAD_DATE
            ,A.MASTERID
            ,A.BRCODE
            ,A.CIFNO
            ,A.FACNO
            ,A.ACCTNO
            ,A.DATASOURCE
            ,A.CCY
            ,SUM(COALESCE(A.AMOUNT_FEE, 0)) AS AMOUNT_FEE
            ,SUM(COALESCE(A.AMOUNT_COST, 0)) AS AMOUNT_COST
            ,SYSTIMESTAMP AS CREATEDDATE
            ,'CF_SUMM' AS CREATEDBY
            ,0 AS AMORT_FEE
            ,0 AS AMORT_COST
          FROM (SELECT MASTERID
                  ,BRCODE
                  ,CIFNO
                  ,FACNO
                  ,ACCTNO
                  ,DATASOURCE
                  ,CCY
                  ,AMOUNT_FEE
                  ,AMOUNT_COST
                FROM IFRS_LBM_STAFF_BENEFIT_SUMM
                WHERE DOWNLOAD_DATE = V_PREVDATE
                UNION ALL
                SELECT MASTERID
                  ,BRCODE
                  ,CIFNO
                  ,FACNO
                  ,ACCTNO
                  ,DATASOURCE
                  ,CCY
                  ,SUM(CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'Y' THEN - 1 * AMOUNT ELSE AMOUNT END
                      ELSE 0 END)
                  ,SUM(CASE WHEN FLAG_CF = 'C' THEN CASE WHEN FLAG_REVERSE = 'Y' THEN - 1 * AMOUNT ELSE AMOUNT END
                      ELSE 0 END)
                FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF
                WHERE ECFDATE = V_CURRDATE
                  AND STATUS IN ('ACT','PNL','REV')
                  AND SRCPROCESS NOT IN ('SL_TO_EIR')
                  AND TRXCODE = 'BENEFIT'
                GROUP BY MASTERID
                  ,BRCODE
                  ,CIFNO
                  ,FACNO
                  ,ACCTNO
                  ,DATASOURCE
                  ,CCY
                ) A
          GROUP BY A.MASTERID
            ,A.BRCODE
            ,A.CIFNO
            ,A.FACNO
            ,A.ACCTNO
            ,A.DATASOURCE
            ,A.CCY
          ) Z;

    COMMIT;

    -- UPDATE AMORT AMT FROM PREVDATE
    MERGE INTO IFRS_LBM_STAFF_BENEFIT_SUMM A
    USING (
          SELECT X.*
            ,Y.*
          FROM IFRS_LBM_STAFF_BENEFIT_SUMM X
          JOIN IFRS_PRC_DATE_AMORT Y ON Y.PREVDATE = X.DOWNLOAD_DATE
          ) B
    ON (A.DOWNLOAD_DATE = B.CURRDATE
       AND A.MASTERID = B.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET AMORT_FEE = B.AMORT_FEE
      ,AMORT_COST = B.AMORT_COST;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_STAFF_BENEFIT_SUMM','');

    COMMIT;
    END;