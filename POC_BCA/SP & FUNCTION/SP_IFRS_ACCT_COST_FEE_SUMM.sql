CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_COST_FEE_SUMM
IS
  V_CURRDATE    DATE;
  V_PREVDATE    DATE;
BEGIN
    SELECT MAX(CURRDATE),MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_COST_FEE_SUMM','');
    COMMIT;

    --delete first
    DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_COST_FEE_SUMM WHERE DOWNLOAD_DATE>=V_CURRDATE;
    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_COST_FEE_SUMM
    (  DOWNLOAD_DATE
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
                ,SUM(COALESCE(A.AMOUNT_FEE,0)) AS AMOUNT_FEE
                ,SUM(COALESCE(A.AMOUNT_COST,0)) AS AMOUNT_COST
                ,SYSTIMESTAMP AS CREATEDDATE,'CF_SUMM' AS CREATEDBY
                ,SUM(COALESCE(A.AMORT_FEE,0)) AS AMORT_FEE
                ,SUM(COALESCE(A.AMORT_COST,0)) AS AMORT_COST
          FROM ( SELECT  MASTERID
                        ,BRCODE
                        ,CIFNO
                        ,FACNO
                        ,ACCTNO
                        ,DATASOURCE
                        ,CCY
                        ,AMOUNT_FEE
                        ,AMOUNT_COST
                        ,AMORT_FEE
                        ,AMORT_COST
                 FROM IFRS_ACCT_COST_FEE_SUMM
                 WHERE DOWNLOAD_DATE=V_PREVDATE
                 UNION ALL
                 SELECT  MASTERID
                        ,BRCODE
                        ,CIFNO
                        ,FACNO
                        ,ACCTNO
                        ,DATASOURCE
                        ,CCY
                        ,SUM(CASE WHEN FLAG_CF='F' THEN CASE WHEN FLAG_REVERSE='Y' THEN -1 * AMOUNT ELSE AMOUNT END ELSE 0 END)
                        ,SUM(CASE WHEN FLAG_CF='C' THEN CASE WHEN FLAG_REVERSE='Y' THEN -1 * AMOUNT ELSE AMOUNT END ELSE 0 END)
                        , 0 AS AMORT_FEE
                        , 0 AS AMORT_COST
                 FROM IFRS_ACCT_COST_FEE
                 WHERE DOWNLOAD_DATE=V_CURRDATE
                 AND STATUS IN ('ACT','PNL')
                 AND SRCPROCESS NOT IN ('SL_TO_EIR','STOP_REV')
                 GROUP BY MASTERID,BRCODE,CIFNO,FACNO,ACCTNO,DATASOURCE,CCY
                  /*
                  union all
                  --20160509 src process prorate
                  select
                  masterid,brcode,cifno,facno,acctno,datasource,ccy
                  ,-1 * sum(case when flag_cf='F' then case when flag_reverse='Y' then -1 * amount else amount end else 0 end)
                  ,-1 * sum(case when flag_cf='C' then case when flag_reverse='Y' then -1 * amount else amount end else 0 end)
                        , 0 as amort_fee
                        , 0 as amort_cost
                  from IFRS_ACCT_COST_FEE
                  where DOWNLOAD_DATE=@v_currdate and status='PRO' and srcprocess='STOP_REV'
                  group by
                  masterid,brcode,cifno,facno,acctno,datasource,ccy
                  */
                )A
          GROUP BY A.MASTERID,A.BRCODE,A.CIFNO,A.FACNO,A.ACCTNO,A.DATASOURCE,A.CCY
          ) Z;
    COMMIT;
    -- update amort amt from prevdate
    --update dbo.IFRS_ACCT_COST_FEE_SUMM
    --set amort_fee=b.amort_fee, amort_cost=b.amort_cost
    --from (
    --    select x.*,y.* from IFRS_ACCT_COST_FEE_SUMM x join IFRS_PRC_DATE_AMORT y on y.prevdate=x.effdate
    --) b
    --where dbo.IFRS_ACCT_COST_FEE_SUMM.effdate=b.currdate
    --	and dbo.IFRS_ACCT_COST_FEE_SUMM.masterid=b.masterid

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_COST_FEE_SUMM','');
    COMMIT;
END;