CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SL_ACF_ACCRU
AS
  v_CURRDATE DATE ;
  V_PREVDATE DATE;

BEGIN

    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;


    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'');

    COMMIT;

    /******************************************************************************
    02. RESET
    *******************************************************************************/
    DELETE  /*+ PARALLEL(12) */ FROM IFRS_ACCT_SL_ACF
    WHERE   DOWNLOAD_DATE = V_CURRDATE
            AND DO_AMORT = 'N';

    DELETE  /*+ PARALLEL(12) */ FROM IFRS_ACCT_SL_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_CURRDATE
            AND CREATEDBY = 'SLACF02';

    COMMIT;

    /******************************************************************************
    03. INSERT ACF
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_SL_ACF
    ( DOWNLOAD_DATE ,
      FACNO ,
      CIFNO ,
      DATASOURCE ,
      N_UNAMORT_COST ,
      N_UNAMORT_FEE ,
      N_AMORT_COST ,
      N_AMORT_FEE ,
      N_ACCRU_COST ,
      N_ACCRU_FEE ,
      N_ACCRUFULL_COST ,
      N_ACCRUFULL_FEE ,
      ECFDATE ,
      CREATEDDATE ,
      CREATEDBY ,
      MASTERID ,
      ACCTNO ,
      DO_AMORT ,
      BRANCH ,
      ACF_CODE ,
      FLAG_AL
    )
    SELECT /*+ PARALLEL(12) */ V_CURRDATE AS DOWNLOAD_DATE ,
            M.FACILITY_NUMBER ,
            M.CUSTOMER_NUMBER ,
            M.DATA_SOURCE ,
            CASE WHEN ( V_CURRDATE - A.PREVDATE + 1 ) / A.I_DAYSCNT > 1--AS NUMERIC(32, 6)) > 1
                 THEN ( A.N_UNAMORT_COST - A.UNAMORT_COST_PREV )
                 ELSE (V_CURRDATE - A.PREVDATE + 1)/ A.I_DAYSCNT* ( A.N_UNAMORT_COST - A.UNAMORT_COST_PREV )
            END + A.UNAMORT_COST_PREV ,

            CASE WHEN ( V_CURRDATE - A.PREVDATE + 1 )/ A.I_DAYSCNT > 1--AS NUMERIC(32, 6)) > 1
                 THEN ( A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV )
                 ELSE (V_CURRDATE - A.PREVDATE + 1)/ A.I_DAYSCNT* ( A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV )
            END + A.UNAMORT_FEE_PREV ,

            (C.N_UNAMORT_COST) /*( a.n_unamort_cost + a.n_amort_cost )*/
            - ( CASE WHEN ( V_CURRDATE - A.PREVDATE + 1 )/ A.I_DAYSCNT > 1--AS NUMERIC(32, 6)) > 1
                     THEN ( A.N_UNAMORT_COST - A.UNAMORT_COST_PREV )
                     ELSE (V_CURRDATE - A.PREVDATE + 1)/ A.I_DAYSCNT* ( A.N_UNAMORT_COST - A.UNAMORT_COST_PREV )
            END + A.UNAMORT_COST_PREV ) ,

            (C.N_UNAMORT_FEE) /*( a.n_unamort_fee + a.n_amort_fee )*/
            - ( CASE WHEN ( V_CURRDATE - A.PREVDATE + 1 )/ A.I_DAYSCNT > 1--AS NUMERIC(32, 6)) > 1
                     THEN ( A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV )
                     ELSE (V_CURRDATE - A.PREVDATE + 1)/ A.I_DAYSCNT * ( A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV )
            END + A.UNAMORT_FEE_PREV ) ,

            CASE WHEN ( V_CURRDATE - A.PREVDATE + 1 )/ A.I_DAYSCNT > 1--AS NUMERIC(32, 6)) > 1
                 THEN ( A.N_UNAMORT_COST - A.UNAMORT_COST_PREV )
                  ELSE (V_CURRDATE - A.PREVDATE + 1)/ A.I_DAYSCNT* ( A.N_UNAMORT_COST - A.UNAMORT_COST_PREV )
            END - NVL(A.SW_ADJ_COST, 0) ,

            CASE WHEN ( V_CURRDATE - A.PREVDATE + 1 )/ A.I_DAYSCNT > 1--AS NUMERIC(32, 6)) > 1
                 THEN ( A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV )
                 ELSE (V_CURRDATE - A.PREVDATE + 1)/ A.I_DAYSCNT* ( A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV )
            END - NVL(A.SW_ADJ_FEE, 0) ,

            A.N_UNAMORT_COST - A.UNAMORT_COST_PREV
            - NVL(A.SW_ADJ_COST, 0) AS N_ACCRUFULL_COST ,
            A.N_UNAMORT_FEE - A.UNAMORT_FEE_PREV - NVL(A.SW_ADJ_FEE,0) AS N_ACCRUFULL_FEE ,
            A.DOWNLOAD_DATE ,
            SYSTIMESTAMP ,
            'SP_ACCT_SL_ACF_ACCRU 1' ,
            M.MASTERID ,
            M.ACCOUNT_NUMBER ,
            'N' DO_AMORT ,
            M.BRANCH_CODE ,
            '2' ACFCODE ,
	    M.FLAG_AL
    FROM IFRS_ACCT_SL_ECF A
    JOIN (SELECT M.MASTERID,M.ACCOUNT_NUMBER,M.BRANCH_CODE,M.FACILITY_NUMBER,M.CUSTOMER_NUMBER,M.DATA_SOURCE,M.IAS_CLASS AS FLAG_AL
          FROM IFRS_IMA_AMORT_CURR M
          LEFT JOIN (SELECT DISTINCT MASTERID,DOWNLOAD_DATE FROM IFRS_ACCT_CLOSED WHERE DOWNLOAD_DATE =V_CURRDATE)  D
            ON M.DOWNLOAD_DATE = D.DOWNLOAD_DATE
            AND M.MASTERID = D.MASTERID
          WHERE M.DOWNLOAD_DATE =V_CURRDATE
          AND D.MASTERID IS NULL
         ) M
      ON  A.MASTERID = M.MASTERID
    /*Adding to fixing n_amort_amount 20160504*/
    JOIN IFRS_ACCT_SL_ECF C
      ON C.AMORTSTOPDATE IS NULL
      AND C.MASTERID = A.MASTERID
      AND C.PMTDATE = C.PREVDATE
    WHERE A.PMTDATE <> A.PREVDATE
    AND A.PMTDATE > V_CURRDATE
    AND A.PREVDATE <= V_CURRDATE
    AND A.AMORTSTOPDATE IS NULL;
    /* Remarks.. Tunning script 20160602
    FROM    IMA_AMORT_CURR m
    JOIN IFRS_ACCT_SL_ECF a ON a.amortstopdate IS NULL
    AND a.masterid = m.masterid
    AND @v_currdate < a.pmtdate
    AND @v_currdate >= a.prevdate
    AND a.pmtdate <> a.prevdate*/
    /*Adding to fixing n_amort_amount 20160504
    JOIN IFRS_ACCT_SL_ECF C ON C.AMORTSTOPDATE IS NULL
    AND C.MASTERID = A.MASTERID
    AND C.PMTDATE = C.PREVDATE
    WHERE   --dont do if closed
    M.MASTERID NOT IN ( SELECT  MASTERID
    FROM    IFRS_ACCT_CLOSED
    WHERE   DOWNLOAD_DATE = V_V_CURRDATE )
    END REMARKS.. TUNNING SCRIPT 20160602*/


    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'ACF INSERTED');

    COMMIT;


    /******************************************************************************
    04. get sl_acf max(id) to process
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

    INSERT  /*+ PARALLEL(12) */ INTO TMP_P1( ID)
    SELECT  /*+ PARALLEL(12) */ MAX(ID) AS ID
    FROM    IFRS_ACCT_SL_ACF
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND DO_AMORT = 'N'
    GROUP BY MASTERID;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'P1');

    COMMIT;


    /******************************************************************************
    05. GET SUMM FEE
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1
    ( SUM_AMT ,
      DOWNLOAD_DATE ,
      FACNO ,
      CIFNO ,
      DATASOURCE ,
      ACCTNO ,
      MASTERID
    )
    SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT ,
            A.DOWNLOAD_DATE ,
            A.FACNO ,
            A.CIFNO ,
            A.DATASOURCE ,
            A.ACCTNO ,
            A.MASTERID
    FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y'THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                     A.ECFDATE DOWNLOAD_DATE ,
                     A.FACNO ,
                     A.CIFNO ,
                     A.DATASOURCE ,
                     A.ACCTNO ,
                     A.MASTERID
              FROM IFRS_ACCT_SL_COST_FEE_ECF A
              WHERE A.FLAG_CF = 'F' AND A.STATUS = 'ACT'
            ) A
    GROUP BY A.DOWNLOAD_DATE ,
             A.FACNO ,
             A.CIFNO ,
             A.DATASOURCE ,
             A.ACCTNO ,
             A.MASTERID;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'T1 FEE');

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'INSERT FEE');

    COMMIT;
    /******************************************************************************
    06. FEE
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_SL_COST_FEE_PREV

    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      ECFDATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      AMOUNT ,
      STATUS ,
      CREATEDDATE ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      FLAG_REVERSE ,
      BRCODE ,
      SRCPROCESS ,
      METHOD ,
      CREATEDBY ,
      SEQ ,
      AMOUNT_ORG ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.ECFDATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            B.AMOUNT / C.SUM_AMT * A.N_UNAMORT_FEE AS N_AMOUNT ,
            B.STATUS ,
            SYSTIMESTAMP ,
            A.ACCTNO ,
            A.MASTERID ,
            B.FLAG_CF ,
            B.FLAG_REVERSE ,
            B.BRCODE ,
            B.SRCPROCESS ,
            'SL' ,
            'SLACF02' ,
            '2' ,
            B.AMOUNT_ORG ,
            B.ORG_CCY ,
            B.ORG_CCY_EXRATE ,
            B.PRDTYPE ,
            B.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN TMP_P1 D ON A.ID = D.ID
    JOIN IFRS_ACCT_SL_COST_FEE_ECF B
      ON B.ECFDATE = A.ECFDATE
      AND A.MASTERID = B.MASTERID
      AND B.FLAG_CF = 'F'
      AND B.STATUS = 'ACT'
    JOIN TMP_T1 C
      ON C.DOWNLOAD_DATE = A.ECFDATE
      AND C.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND (
	   (
	    A.N_UNAMORT_FEE < 0
	    AND A.FLAG_AL = 'A'
	    )
	   OR (
	    A.N_UNAMORT_FEE > 0
	    AND A.FLAG_AL = 'L'
	    )
   )
    --AND a.n_unamort_fee < 0 ----CTBC 20180528
    --AND A.ID IN ( SELECT ID FROM TMP_P1 )
    ;


    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'FEE PREV');

    COMMIT;

    /******************************************************************************
    07. GET COST SUMM
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2';


    INSERT /*+ PARALLEL(12) */ INTO TMP_T2
    ( SUM_AMT ,
      DOWNLOAD_DATE ,
      FACNO ,
      CIFNO ,
      DATASOURCE ,
      ACCTNO ,
      MASTERID
    )
    SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT ,
            A.DOWNLOAD_DATE ,
            A.FACNO ,
            A.CIFNO ,
            A.DATASOURCE ,
            A.ACCTNO ,
            A.MASTERID
    FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                     A.ECFDATE DOWNLOAD_DATE ,
                     A.FACNO ,
                     A.CIFNO ,
                     A.DATASOURCE ,
                     A.ACCTNO ,
                     A.MASTERID
              FROM IFRS_ACCT_SL_COST_FEE_ECF A
              WHERE A.FLAG_CF = 'C' AND A.STATUS = 'ACT'
            ) A
    GROUP BY A.DOWNLOAD_DATE ,
             A.FACNO ,
             A.CIFNO ,
             A.DATASOURCE ,
             A.ACCTNO ,
             A.MASTERID;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'T2 COST');

    COMMIT;
    /******************************************************************************
    08. COST
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_SL_COST_FEE_PREV
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      ECFDATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      AMOUNT ,
      STATUS ,
      CREATEDDATE ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      FLAG_REVERSE ,
      BRCODE ,
      SRCPROCESS ,
      METHOD ,
      CREATEDBY ,
      SEQ ,
      AMOUNT_ORG ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.ECFDATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            B.AMOUNT / C.SUM_AMT* A.N_UNAMORT_COST AS N_AMOUNT ,
            B.STATUS ,
            SYSTIMESTAMP ,
            A.ACCTNO ,
            A.MASTERID ,
            B.FLAG_CF ,
            B.FLAG_REVERSE ,
            B.BRCODE ,
            B.SRCPROCESS ,
            'SL' ,
            'SLACF02' ,
            '2' ,
            B.AMOUNT_ORG ,
            B.ORG_CCY ,
            B.ORG_CCY_EXRATE ,
            B.PRDTYPE ,
            B.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN TMP_P1 D ON A.ID = D.ID
    JOIN IFRS_ACCT_SL_COST_FEE_ECF B
      ON B.ECFDATE = A.ECFDATE
      AND A.MASTERID = B.MASTERID
      AND B.FLAG_CF = 'C'
      AND B.STATUS = 'ACT'
    JOIN TMP_T2 C
      ON C.DOWNLOAD_DATE = A.ECFDATE
      AND C.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND (
	   (
	    A.N_UNAMORT_COST > 0
	    AND A.FLAG_AL = 'A'
	    )
	   OR (
	    A.N_UNAMORT_COST < 0
	    AND A.FLAG_AL = 'L'
	    )
    )
    --AND a.n_unamort_cost > 0 ----CTBC 20180528
    --AND A.ID IN ( SELECT ID FROM TMP_P1 )
    ;

    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'COST PREV');

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_ACCT_SL_ACF_ACCRU' ,'');

    COMMIT;

END;