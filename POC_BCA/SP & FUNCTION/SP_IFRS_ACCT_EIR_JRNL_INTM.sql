CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_JRNL_INTM
AS
  V_CURRDATE DATE ;
  V_PREVDATE DATE ;
  V_PARAM_DISABLE_ACCRU_PREV NUMBER(19);
  V_ROUND NUMBER(10) := 6;
  V_FUNCROUND NUMBER(10) := 1;

BEGIN

    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    --disable accru prev create on new ecf and return accrual to unamort
        --ADD YAHYA
    BEGIN
      SELECT  CASE WHEN COMMONUSAGE = 'Y' THEN 1  ELSE 0  END
      INTO V_PARAM_DISABLE_ACCRU_PREV
      FROM    TBLM_COMMONCODEHEADER
      WHERE   COMMONCODE = 'SCM005';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_PARAM_DISABLE_ACCRU_PREV := 0;
    END;
    --SET @param_disable_accru_prev = 1

    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;


    BEGIN
      SELECT CAST(VALUE1 AS NUMBER(10))
           , CAST(VALUE2 AS NUMBER(10))
      INTO V_ROUND, V_FUNCROUND
      FROM TBLM_COMMONCODEDETAIL
      WHERE COMMONCODE = 'SCM003';
    EXCEPTION
    --20171016 set default value
      WHEN NO_DATA_FOUND THEN
        V_ROUND := 6;
        V_FUNCROUND:=1;
    END;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_ACCT_EIR_ACF';

	INSERT /*+ PARALLEL(12) */ INTO GTMP_IFRS_ACCT_EIR_ACF
	(
		ID,
		DOWNLOAD_DATE,
		MASTERID,
		FACNO,
		CIFNO,
		ACCTNO,
		DATASOURCE,
		N_UNAMORT_COST,
		N_UNAMORT_FEE,
		N_AMORT_COST,
		N_AMORT_FEE,
		N_ACCRU_COST,
		N_ACCRU_FEE,
		N_ACCRUFULL_COST,
		N_ACCRUFULL_FEE,
		ECFDATE,
		CREATEDDATE,
		CREATEDBY,
		N_ACCRU_PREV_COST,
		N_ACCRU_PREV_FEE,
		N_AMORT_ADJ_COST,
		N_AMORT_ADJ_FEE,
		DO_AMORT,
		BRANCH,
		ACF_CODE,
		FLAG_AL,
		N_ACCRU_NOCF,
		N_UNAMORT_NOCF,
		N_UNAMORT_PREV_NOCF
	)
	SELECT /*+ PARALLEL(12) */
		ID,
		DOWNLOAD_DATE,
		MASTERID,
		FACNO,
		CIFNO,
		ACCTNO,
		DATASOURCE,
		N_UNAMORT_COST,
		N_UNAMORT_FEE,
		N_AMORT_COST,
		N_AMORT_FEE,
		N_ACCRU_COST,
		N_ACCRU_FEE,
		N_ACCRUFULL_COST,
		N_ACCRUFULL_FEE,
		ECFDATE,
		CREATEDDATE,
		CREATEDBY,
		N_ACCRU_PREV_COST,
		N_ACCRU_PREV_FEE,
		N_AMORT_ADJ_COST,
		N_AMORT_ADJ_FEE,
		DO_AMORT,
		BRANCH,
		ACF_CODE,
		FLAG_AL,
		N_ACCRU_NOCF,
		N_UNAMORT_NOCF,
		N_UNAMORT_PREV_NOCF
	FROM IFRS_ACCT_EIR_ACF
    WHERE DOWNLOAD_DATE >= V_PREVDATE
    AND DOWNLOAD_DATE <= V_CURRDATE;
	COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'');

    COMMIT;

    /******************************************************************************
    02. DELETE FIRST
    *******************************************************************************/
    DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_INTM
    WHERE   DOWNLOAD_DATE >= V_CURRDATE
    AND SUBSTR(SOURCEPROCESS, 1, 3) = 'EIR';

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS , PROCNAME , REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'1');

    COMMIT;

    /******************************************************************************
    03. PNL = DEFA0 + AMORT OF NEW COST FEE TODAY
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      IS_PNL ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'N' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR PNL 1' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            'Y' IS_PNL ,
            PRD_TYPE ,
            'ITRCG' ,
            CF_ID
    FROM    IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'PNL'
    AND METHOD = 'EIR';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'2');

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      IS_PNL ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            -1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'EIR PNL 2' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            'Y' IS_PNL ,
            PRD_TYPE ,
            'ACCRU' ,
            CF_ID
    FROM    IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'PNL'
    AND METHOD = 'EIR';
      COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'3');

    COMMIT;

    /******************************************************************************
    04. PNL = AMORT OF UNAMORT BY CURRDATE
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            -1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END ) ,
            SYSTIMESTAMP ,
            'EIR PNL 3' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ACCRU' ,
            CF_ID
    FROM    IFRS_ACCT_EIR_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'PNL';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'4');

    COMMIT;

    /******************************************************************************
    05. PNL2 = AMORT OF UNAMORT BY PREVDATE
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID ,
      METHOD
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            V_CURRDATE AS DOWNLOAD_DATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            -1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END ) ,
            SYSTIMESTAMP ,
            'EIR PNL 4' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ACCRU' ,
            CF_ID ,
            METHOD
    FROM    ( SELECT ACCTNO ,
                     SUM(AMOUNT) AS AMOUNT ,
                     PRDTYPE ,
                     BRCODE ,
                     CCY ,
                     CF_ID ,
                     CIFNO ,
                     DATASOURCE ,
                     DOWNLOAD_DATE ,
                     FACNO ,
                     FLAG_CF ,
                     FLAG_REVERSE ,
                     PRDCODE ,
                     TRXCODE ,
                     MASTERID ,
                     METHOD ,
                     STATUS
              FROM IFRS_ACCT_EIR_COST_FEE_PREV
              WHERE     DOWNLOAD_DATE = V_PREVDATE
              AND STATUS = 'PNL2'
              GROUP BY  ACCTNO ,
                        PRDTYPE ,
                        BRCODE ,
                        CCY ,
                        CF_ID ,
                        CIFNO ,
                        DATASOURCE ,
                        DOWNLOAD_DATE ,
                        FACNO ,
                        FLAG_CF ,
                        FLAG_REVERSE ,
                        PRDCODE ,
                        TRXCODE ,
                        MASTERID ,
                        METHOD ,
                        STATUS
          ) A
    WHERE   A.DOWNLOAD_DATE = V_PREVDATE
    AND A.STATUS = 'PNL2';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'5');

    COMMIT;
    /******************************************************************************
    06. DEFA0 NORMAL AMORTIZED COST/FEE
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'N' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR ACT 1' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRD_TYPE ,
            'ITRCG' ,
            CF_ID
    FROM    IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND METHOD = 'EIR';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'6');

    COMMIT;

    /******************************************************************************
    07. DEFA0 COME FROM DIFF TABLE
    *******************************************************************************/
    /*
    INSERT  INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'N' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR HRD 1' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ITRCG' ,
            CF_ID
    FROM    IFRS_ACCT_EIR_COST_FEE_ECF
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND SRCPROCESS = 'STAFFLOAN';

    */

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'7');

    COMMIT;
    /******************************************************************************
    08. REVERSE ACCRUAL
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            V_CURRDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            JOURNALCODE ,
            STATUS ,
            'Y' ,
            N_AMOUNT ,
            SYSTIMESTAMP ,
            'EIR REV ACCRU' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRANCH ,
            PRDTYPE ,
            JOURNALCODE ,
            CF_ID
    FROM    IFRS_ACCT_JOURNAL_INTM
    WHERE   DOWNLOAD_DATE = V_PREVDATE
    AND STATUS = 'ACT'
    AND NVL(TRXCODE, ' ') <> 'BENEFIT'
    AND JOURNALCODE IN ( 'ACCRU', 'ACRU4' ) -- include also no cost fee ecf
    AND REVERSE = 'N'
    AND SUBSTR(SOURCEPROCESS, 1, 3) = 'EIR';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'8');

    COMMIT;

    /******************************************************************************
    09. ACCRU FEE
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T5'  ;

    INSERT /*+ PARALLEL(12) */ INTO TMP_T5
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      N_AMOUNT ,
      ACCTNO ,
      MASTERID ,
      BRCODE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            ECFDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            SUM(CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END) AS N_AMOUNT ,
            ACCTNO ,
            MASTERID ,
            BRCODE ,
            PRDTYPE ,
            CF_ID
    FROM    IFRS_ACCT_EIR_COST_FEE_ECF
    WHERE   FLAG_CF = 'F' AND NVL(TRXCODE, ' ') <> 'BENEFIT' AND STATUS = 'ACT'
    GROUP BY FACNO ,
             CIFNO ,
             ECFDATE ,
             DATASOURCE ,
             PRDCODE ,
             TRXCODE ,
             CCY ,
             FLAG_REVERSE ,
             ACCTNO ,
             MASTERID ,
             BRCODE ,
             PRDTYPE ,
             CF_ID;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'9');

    COMMIT;

     /******************************************************************************
    10. INSERT INTO TMP_T6
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T6'  ;

    INSERT /*+ PARALLEL(12) */ INTO TMP_T6
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      SUM_AMT ,
      ACCTNO ,
      MASTERID ,
      BRCODE
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            SUM(N_AMOUNT) AS SUM_AMT ,
            ACCTNO ,
            MASTERID ,
            BRCODE
    FROM    TMP_T5 D
    GROUP BY FACNO ,
             CIFNO ,
             DOWNLOAD_DATE ,
             DATASOURCE ,
             ACCTNO ,
             MASTERID ,
             BRCODE;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'10');

    COMMIT;

    --EXECUTE IMMEDIATE 'drop index TMP_T5_idx1';
    --EXECUTE IMMEDIATE 'drop index TMP_T6_idx1';
    --EXECUTE IMMEDIATE 'create index TMP_T5_idx1 on TMP_T5(DOWNLOAD_DATE,masterid)';
    --EXECUTE IMMEDIATE 'create index TMP_T6_idx1 on TMP_T6(DOWNLOAD_DATE,masterid)';

     /******************************************************************************
    11. EIR ACCRU FEE
    *******************************************************************************/

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'ACCRU' ,
            'ACT' ,
            'N' ,
            ROUND(A.N_ACCRU_FEE * CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (B.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)),V_ROUND) ,
            SYSTIMESTAMP ,
            'EIR ACCRU FEE 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'F' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU' ,
            B.CF_ID
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN (SELECT TMP_T5.*,
            SUM(N_AMOUNT) OVER(PARTITION BY CIFNO,DOWNLOAD_DATE,DATASOURCE,ACCTNO,MASTERID,BRCODE) as SUM_AMT
           FROM TMP_T5)  B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
--    JOIN TMP_T6 C
--      ON C.DOWNLOAD_DATE = A.ECFDATE
--      AND C.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N';

    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'11');

    COMMIT;

    /******************************************************************************
    12. AMORT FEE
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            ROUND(A.N_ACCRU_FEE * CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE)/ CAST (B.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)),V_ROUND) ,
            SYSTIMESTAMP ,
            'EIR AMORT FEE 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'F' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU' ,
            B.CF_ID
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN (SELECT TMP_T5.*,
            SUM(N_AMOUNT) OVER(PARTITION BY CIFNO,DOWNLOAD_DATE,DATASOURCE,ACCTNO,MASTERID,BRCODE) as SUM_AMT
           FROM TMP_T5)  B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
--    JOIN TMP_T6 C
--      ON C.DOWNLOAD_DATE = A.ECFDATE
--      AND C.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'12');

    COMMIT;

    /******************************************************************************
    13. STOP REV DEFA0 FEE (20160619)
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO,
      CIFNO,
      DOWNLOAD_DATE,
      DATASOURCE,
      PRDCODE,
      TRXCODE,
      CCY,
      JOURNALCODE,
      STATUS,
      REVERSE,
      N_AMOUNT,
      CREATEDDATE,
      SOURCEPROCESS,
      ACCTNO,
      MASTERID,
      FLAG_CF,
      BRANCH,
      PRDTYPE,
      JOURNALCODE2,
      CF_ID
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO,
           A.CIFNO,
           A.DOWNLOAD_DATE,
           A.DATASOURCE,
           B.PRDCODE,
           B.TRXCODE,
           B.CCY,
           'DEFA0',
           'ACT',
           'N',
           ROUND(-1 * A.N_ACCRU_FEE * CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (B.SUM_AMT AS BINARY_DOUBLE) AS NUMBER (32, 20)),V_ROUND),
           SYSTIMESTAMP,
           'EIR DEFA0 FEE 1',
           A.ACCTNO,
           A.MASTERID,
           'F',
           B.BRCODE,
           B.PRDTYPE,
           'ITRCG',
           B.CF_ID
    FROM GTMP_IFRS_ACCT_EIR_ACF A
    JOIN (SELECT TMP_T5.*,
            SUM(N_AMOUNT) OVER(PARTITION BY CIFNO,DOWNLOAD_DATE,DATASOURCE,ACCTNO,MASTERID,BRCODE) as SUM_AMT
           FROM TMP_T5)  B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
--    JOIN TMP_T6 C
--      ON C.DOWNLOAD_DATE = A.ECFDATE
--      AND C.MASTERID = A.MASTERID
    --only for stop rev
    JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) D
      ON A.MASTERID = D.MASTERID
    WHERE A.DOWNLOAD_DATE = V_CURRDATE AND A.DO_AMORT = 'Y';
    --only for stop rev
    --and a.MASTERID in (select masterid from IFRS_ACCT_EIR_STOP_REV where DOWNLOAD_DATE=@v_currdate)
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'13');

    COMMIT;

    /******************************************************************************
    14. ACCRU COST
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T5' ;

    INSERT /*+ PARALLEL(12) */ INTO TMP_T5
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      N_AMOUNT ,
      ACCTNO ,
      MASTERID ,
      BRCODE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            ECFDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            SUM(CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END) AS N_AMOUNT ,
            ACCTNO ,
            MASTERID ,
            BRCODE ,
            PRDTYPE ,
            CF_ID
    FROM    IFRS_ACCT_EIR_COST_FEE_ECF
    WHERE   FLAG_CF = 'C' AND NVL(TRXCODE, ' ') <> 'BENEFIT' AND STATUS = 'ACT'
    GROUP BY FACNO ,
             CIFNO ,
             ECFDATE ,
             DATASOURCE ,
             PRDCODE ,
             TRXCODE ,
             CCY ,
             ACCTNO ,
             MASTERID ,
             BRCODE ,
             PRDTYPE ,
             CF_ID ,
             FLAG_REVERSE;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'14');

    COMMIT;

    /******************************************************************************
    15. INSERT INTO TMP_T6
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T6'  ;

    INSERT /*+ PARALLEL(12) */ INTO TMP_T6
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      SUM_AMT ,
      ACCTNO ,
      MASTERID ,
      BRCODE
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            SUM(N_AMOUNT) AS SUM_AMT ,
            ACCTNO ,
            MASTERID ,
            BRCODE
    FROM    TMP_T5 D
    GROUP BY FACNO ,
             CIFNO ,
             DOWNLOAD_DATE ,
             DATASOURCE ,
             ACCTNO ,
             MASTERID ,
             BRCODE;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'15');

    COMMIT;

    --EXECUTE IMMEDIATE 'drop index TMP_T5_idx1';
    --EXECUTE IMMEDIATE 'drop index TMP_T6_idx1';
    --EXECUTE IMMEDIATE 'create index TMP_T5_idx1 on TMP_T5(DOWNLOAD_DATE,masterid)';
    --EXECUTE IMMEDIATE 'create index TMP_T6_idx1 on TMP_T6(DOWNLOAD_DATE,masterid)';


    /******************************************************************************
    16. EIR ACCRU COST
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
                  CIFNO ,
                  DOWNLOAD_DATE ,
                  DATASOURCE ,
                  PRDCODE ,
                  TRXCODE ,
                  CCY ,
                  JOURNALCODE ,
                  STATUS ,
                  REVERSE ,
                  N_AMOUNT ,
                  CREATEDDATE ,
                  SOURCEPROCESS ,
                  ACCTNO ,
                  MASTERID ,
                  FLAG_CF ,
                  BRANCH ,
                  PRDTYPE ,
                  JOURNALCODE2 ,
                  CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'ACCRU' ,
            'ACT' ,
            'N' ,
            ROUND(A.N_ACCRU_COST  * CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)),V_ROUND) ,
            SYSTIMESTAMP ,
            'EIR ACCRU COST 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'C' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU' ,
            B.CF_ID
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG  ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'16');

    COMMIT;

    /******************************************************************************
    17. AMORT COST
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            ROUND(A.N_ACCRU_COST * CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)),V_ROUND) ,
            SYSTIMESTAMP ,
            'EIR AMORT COST 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'C' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU' ,
            B.CF_ID
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y'
    AND A.MASTERID NOT IN (select distinct masterid from IFRS_ACCT_SWITCH where DOWNLOAD_DATE = v_currdate); -- ADD 20180824;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'17');

    COMMIT;

    /******************************************************************************
    18. STOP REV DEFA0 COST 20160619
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO,
      CIFNO,
      DOWNLOAD_DATE,
      DATASOURCE,
      PRDCODE,
      TRXCODE,
      CCY,
      JOURNALCODE,
      STATUS,
      REVERSE,
      N_AMOUNT,
      CREATEDDATE,
      SOURCEPROCESS,
      ACCTNO,
      MASTERID,
      FLAG_CF,
      BRANCH,
      PRDTYPE,
      JOURNALCODE2,
      CF_ID
      )
    SELECT /*+ PARALLEL(12) */ A.FACNO,
           A.CIFNO,
           A.DOWNLOAD_DATE,
           A.DATASOURCE,
           B.PRDCODE,
           B.TRXCODE,
           B.CCY,
           'DEFA0',
           'ACT',
           'N',
           ROUND(-1 * A.N_ACCRU_COST * CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER (32, 20)),V_ROUND),
           SYSTIMESTAMP,
           'EIR AMORT COST 1',
           A.ACCTNO,
           A.MASTERID,
           'C',
           B.BRCODE,
           B.PRDTYPE,
           'ITRCG',
           B.CF_ID
    FROM GTMP_IFRS_ACCT_EIR_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    --stoprev
    JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE=V_CURRDATE) D
      ON A.MASTERID = D.MASTERID
    WHERE A.DOWNLOAD_DATE = V_CURRDATE AND A.DO_AMORT = 'Y';
    --stoprev
    --and a.MASTERID in (select masterid from IFRS_ACCT_EIR_STOP_REV where DOWNLOAD_DATE=@v_currdate)
     COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'18');


    COMMIT;

    /******************************************************************************
    19. UPDATE STATUS ACF
    *******************************************************************************/  -- 20160407 daniel s : set BLK before accru prev code
    -- update status accru prev for eir stop rev
    MERGE INTO IFRS_ACCT_EIR_ACCRU_PREV C
    USING (SELECT C.STATUS AS C_STATUS,C.MASTERID,C.DOWNLOAD_DATE
            FROM GTMP_IFRS_ACCT_EIR_ACF A
            JOIN IFRS_ACCT_EIR_STOP_REV E
              ON E.DOWNLOAD_DATE = V_CURRDATE
              AND E.MASTERID = A.MASTERID
            JOIN IFRS_ACCT_EIR_ACCRU_PREV C
              ON C.MASTERID = A.MASTERID
              AND C.STATUS = 'ACT'
              AND C.DOWNLOAD_DATE <= V_CURRDATE
            WHERE   A.DOWNLOAD_DATE = V_PREVDATE
            )A
    ON (A.MASTERID=C.MASTERID
        AND A.DOWNLOAD_DATE=C.DOWNLOAD_DATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET  C.STATUS = TO_CHAR (V_CURRDATE, 'YYYYMMDD')  || 'BLK';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'19');

    COMMIT;

    /******************************************************************************
    20. EIR ACCRU PREV
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID ,
      METHOD
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'ACCRU' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT  ELSE C.AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR ACCRU PREV' ,
            A.ACCTNO ,
            A.MASTERID ,
            C.FLAG_CF ,
            A.BRANCH ,
            C.PRDTYPE ,
            'ACCRU' ,
            C.CF_ID ,
            C.METHOD
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN IFRS_ACCT_EIR_ACCRU_PREV C ON C.MASTERID = A.MASTERID
    AND C.STATUS = 'ACT'
    AND C.DOWNLOAD_DATE <= V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'20');

    COMMIT;

    /******************************************************************************
    21. EIR AMORT PREV
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID ,
      METHOD
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT  ELSE C.AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR AMORT PREV' ,
            A.ACCTNO ,
            A.MASTERID ,
            C.FLAG_CF ,
            A.BRANCH ,
            C.PRDTYPE ,
            'ACCRU' ,
            C.CF_ID ,
            C.METHOD
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN IFRS_ACCT_EIR_ACCRU_PREV C
      ON C.MASTERID = A.MASTERID
      AND C.STATUS = TO_CHAR  (V_CURRDATE, 'YYYYMMDD')
      AND C.DOWNLOAD_DATE <= V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y'
    AND A.MASTERID NOT IN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE = V_CURRDATE);
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'21');


    COMMIT;

    /******************************************************************************
    22. accru prev with no acf for pnl ed acctno and disable accru prev param @ ecf main
    *******************************************************************************/

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID ,
      METHOD
    )
    SELECT  /*+ PARALLEL(12) */ C.FACNO ,
            C.CIFNO ,
            V_CURRDATE AS DOWNLOAD_DATE ,
            C.DATASOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT  ELSE C.AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR AMORT PREV2' ,
            C.ACCTNO ,
            C.MASTERID ,
            C.FLAG_CF ,
            P.BRANCH_CODE ,
            C.PRDTYPE ,
            'ACCRU' ,
            C.CF_ID ,
            C.METHOD
    FROM ( SELECT ACCTNO ,
                  AMORTDATE ,
                  SUM(AMOUNT) AS AMOUNT ,
                  PRDTYPE ,
                  CCY ,
                  CF_ID ,
                  CIFNO ,
                  DATASOURCE ,
                  DOWNLOAD_DATE ,
                  FACNO ,
                  FLAG_CF ,
                  FLAG_REVERSE ,
                  PRDCODE ,
                  TRXCODE ,
                  MASTERID ,
                  METHOD ,
                  STATUS
          FROM IFRS_ACCT_EIR_ACCRU_PREV
          WHERE     DOWNLOAD_DATE <= V_CURRDATE
          GROUP BY  ACCTNO ,
                    AMORTDATE ,
                    PRDTYPE ,
                    CCY ,
                    CF_ID ,
                    CIFNO ,
                    DATASOURCE ,
                    DOWNLOAD_DATE ,
                    FACNO ,
                    FLAG_CF ,
                    FLAG_REVERSE ,
                    PRDCODE ,
                    TRXCODE ,
                    MASTERID ,
                    METHOD ,
                    STATUS
        ) C
    JOIN IFRS_IMA_AMORT_CURR P
      ON P.MASTERID = C.MASTERID
    --20180310 change from ecf to acf
    LEFT JOIN GTMP_IFRS_ACCT_EIR_ACF A
      ON A.MASTERID = C.MASTERID
      AND A.DOWNLOAD_DATE = V_CURRDATE
    WHERE   C.STATUS = TO_CHAR  (V_CURRDATE, 'YYYYMMDD')
    AND C.DOWNLOAD_DATE <= V_CURRDATE
    AND A.MASTERID IS NULL;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'22');

    COMMIT;

    /******************************************************************************
    23. EIR switch amort of accru prev
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.PREV_FACNO ,
            A.PREV_CIFNO ,
            A.DOWNLOAD_DATE ,
            A.PREV_DATASOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT  ELSE C.AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR ACRU SW' ,
            A.PREV_ACCTNO ,
            A.PREV_MASTERID ,
            C.FLAG_CF ,
            A.PREV_BRCODE ,
            C.PRDTYPE ,
            'ACCRU' ,
            C.CF_ID
    FROM    IFRS_ACCT_SWITCH A
    JOIN IFRS_ACCT_EIR_ACCRU_PREV C
      ON C.MASTERID = A.PREV_MASTERID
      AND C.STATUS = TO_CHAR  (V_CURRDATE, 'YYYYMMDD')
      AND C.DOWNLOAD_DATE = V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.PREV_EIR_ECF = 'Y';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'23');

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'24');

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (
      FACNO
      ,CIFNO
      ,DOWNLOAD_DATE
      ,DATASOURCE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,JOURNALCODE
      ,STATUS
      ,REVERSE
      ,N_AMOUNT
      ,CREATEDDATE
      ,SOURCEPROCESS
      ,ACCTNO
      ,MASTERID
      ,FLAG_CF
      ,BRANCH
      ,PRDTYPE
      ,JOURNALCODE2
      ,CF_ID
    )
    SELECT /*+ PARALLEL(12) */ FACNO
      ,CIFNO
      ,DOWNLOAD_DATE
      ,DATASOURCE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,'DEFA0'
      ,'ACT'
      ,'Y'
      ,1 * (
       CASE
        WHEN FLAG_REVERSE = 'Y'
         THEN - 1 * AMOUNT
        ELSE AMOUNT
        END
       )
      ,SYSTIMESTAMP
      ,'EIR_REV_SWITCH'
      ,ACCTNO
      ,MASTERID
      ,FLAG_CF
      ,BRCODE
      ,PRDTYPE
      ,'ITRCG'
      ,CF_ID
     FROM IFRS_ACCT_EIR_COST_FEE_PREV
     WHERE DOWNLOAD_DATE = V_CURRDATE
      AND STATUS = 'REV'
      AND CREATEDBY = 'EIR_SWITCH';

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'25');

    COMMIT;

    -- REV2 = REV OF UNAMORT BY PREVDATE
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (
      FACNO
      ,CIFNO
      ,DOWNLOAD_DATE
      ,DATASOURCE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,JOURNALCODE
      ,STATUS
      ,REVERSE
      ,N_AMOUNT
      ,CREATEDDATE
      ,SOURCEPROCESS
      ,ACCTNO
      ,MASTERID
      ,FLAG_CF
      ,BRANCH
      ,PRDTYPE
      ,JOURNALCODE2
      ,CF_ID
    )
    SELECT /*+ PARALLEL(12) */ FACNO
      ,CIFNO
      ,V_CURRDATE
      ,DATASOURCE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,'DEFA0'
      ,'ACT'
      ,'Y'
      ,1 * (
       CASE
        WHEN FLAG_REVERSE = 'Y'
         THEN - 1 * AMOUNT
        ELSE AMOUNT
        END
       )
      ,SYSTIMESTAMP
      ,'EIR_REV_SWITCH'
      ,ACCTNO
      ,MASTERID
      ,FLAG_CF
      ,BRCODE
      ,PRDTYPE
      ,'ITRCG'
      ,CF_ID
    FROM IFRS_ACCT_EIR_COST_FEE_PREV
    WHERE DOWNLOAD_DATE = V_PREVDATE
      AND STATUS = 'REV2'
      AND CREATEDBY = 'EIR_SWITCH';

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'26');

    COMMIT;

    -- DEFA0 FOR NEW ACCT OF EIR SWITCH
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM (
      FACNO
      ,CIFNO
      ,DOWNLOAD_DATE
      ,DATASOURCE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,JOURNALCODE
      ,STATUS
      ,REVERSE
      ,N_AMOUNT
      ,CREATEDDATE
      ,SOURCEPROCESS
      ,ACCTNO
      ,MASTERID
      ,FLAG_CF
      ,BRANCH
      ,PRDTYPE
      ,JOURNALCODE2
      ,CF_ID
      )
     SELECT /*+ PARALLEL(12) */ FACNO
      ,CIFNO
      ,DOWNLOAD_DATE
      ,DATASOURCE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,'DEFA0'
      ,'ACT'
      ,'N'
      ,1 * (
       CASE
        WHEN FLAG_REVERSE = 'Y'
         THEN - 1 * AMOUNT
        ELSE AMOUNT
        END
       )
      ,SYSTIMESTAMP
      ,'EIR_SWITCH'
      ,ACCTNO
      ,MASTERID
      ,FLAG_CF
      ,BRCODE
      ,PRDTYPE
      ,'ITRCG'
      ,CF_ID
    FROM IFRS_ACCT_EIR_COST_FEE_PREV
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND STATUS = 'ACT'
      AND SEQ = '0';

    COMMIT;

    /******************************************************************************
    27. -- no cost fee ecf accrual journal intm
        -- no cost fee ecf accru
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRODUCT_CODE ,
            'EIR_NOCF' TRXCODE ,
            B.CURRENCY ,
            'ACRU4' ,
            'ACT' ,
            'N' ,
            --a.n_accru_nocf ,
            A.N_UNAMORT_PREV_NOCF + A.N_ACCRU_NOCF, --20171016 nocf is post reverse so post the whole amount
            SYSTIMESTAMP ,
            'EIR ACCRU NOCF' ,
            A.ACCTNO ,
            A.MASTERID ,
            'S',-- diubah mengikuti web 'N' ,                                                      -- nocf
            B.BRANCH_CODE ,
            B.PRODUCT_TYPE ,
            'ACRU4' ,
            NULL                                                       --cfid
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN IFRS_IMA_AMORT_CURR B
      ON B.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N'
    AND A.N_ACCRU_NOCF IS NOT NULL;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'27');

    COMMIT;

    /******************************************************************************
    28. NO COST FEE ECF AMORT
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
                  CIFNO ,
                  DOWNLOAD_DATE ,
                  DATASOURCE ,
                  PRDCODE ,
                  TRXCODE ,
                  CCY ,
                  JOURNALCODE ,
                  STATUS ,
                  REVERSE ,
                  N_AMOUNT ,
                  CREATEDDATE ,
                  SOURCEPROCESS ,
                  ACCTNO ,
                  MASTERID ,
                  FLAG_CF ,
                  BRANCH ,
                  PRDTYPE ,
                  JOURNALCODE2 ,
                  CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
                        A.CIFNO ,
                        A.DOWNLOAD_DATE ,
                        A.DATASOURCE ,
                        B.PRODUCT_CODE ,
                        'EIR_NOCF' TRXCODE ,
                        B.CURRENCY ,
                        'AMRT4' ,
                        'ACT' ,
                        'N' ,
                        A.N_ACCRU_NOCF ,
                        SYSTIMESTAMP ,
                        'EIR AMORT NOCF' ,
                        A.ACCTNO ,
                        A.MASTERID ,
                        'N' ,                                                      -- nocf
                        B.BRANCH_CODE ,
                        B.PRODUCT_TYPE ,
                        'AMRT4' ,
                        NULL                                                       --cfid
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    JOIN IFRS_IMA_AMORT_CURR B
      ON B.MASTERID = A.MASTERID
      AND B.DOWNLOAD_DATE = V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y'
    AND A.N_ACCRU_NOCF IS NOT NULL;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'28');

    COMMIT;

    /******************************************************************************
    29. pnl for no cost fee ecf for closed account and event change
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_NOCF';

    INSERT /*+ PARALLEL(12) */ INTO TMP_NOCF  ( MASTERID )
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
    FROM    IFRS_ACCT_CLOSED
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;


    INSERT /*+ PARALLEL(12) */ INTO TMP_NOCF  ( MASTERID )
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
    FROM    IFRS_ACCT_EIR_ECF
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND MASTERID NOT IN ( SELECT    DISTINCT MASTERID
                          FROM      IFRS_ACCT_CLOSED
                          WHERE     DOWNLOAD_DATE = V_CURRDATE );

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'29');

    COMMIT;


    /******************************************************************************
    30. EIR AMORT NOCF
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            V_CURRDATE AS DOWNLOAD_DATE ,
            A.DATASOURCE ,
            NVL(B.PRODUCT_CODE, C.PRODUCT_CODE) ,
            'EIR_NOCF' TRXCODE ,
            NVL(B.CURRENCY, C.CURRENCY) ,
            'AMRT4' ,
            'ACT' ,
            'Y' ,
            CASE WHEN A.DO_AMORT = 'Y' THEN A.N_UNAMORT_NOCF
                 ELSE A.N_UNAMORT_PREV_NOCF
            END ,
            SYSTIMESTAMP ,
            'EIR AMORT NOCF' ,
            A.ACCTNO ,
            A.MASTERID ,
            'N' ,                                                      -- nocf
            NVL(B.BRANCH_CODE, C.BRANCH_CODE) ,
            NVL(B.PRODUCT_TYPE, C.PRODUCT_TYPE) ,
            'AMRT4' ,
            NULL                                                       --cfid
    FROM    GTMP_IFRS_ACCT_EIR_ACF A
    LEFT JOIN IFRS_IMA_AMORT_CURR B ON B.MASTERID = A.MASTERID
      AND B.DOWNLOAD_DATE = V_CURRDATE
    LEFT JOIN IFRS_IMA_AMORT_PREV C ON C.MASTERID = A.MASTERID
      AND C.DOWNLOAD_DATE = V_PREVDATE--@v_currdate
    WHERE   A.ID IN (
        SELECT MAX(ID)
        FROM GTMP_IFRS_ACCT_EIR_ACF
        WHERE --DOWNLOAD_DATE >= V_PREVDATE
         --AND DOWNLOAD_DATE <= V_CURRDATE
         --AND
        MASTERID IN ( SELECT MASTERID FROM TMP_NOCF )
        GROUP BY  MASTERID
        )
    AND CASE WHEN A.DO_AMORT = 'Y' THEN A.N_UNAMORT_NOCF  ELSE A.N_UNAMORT_PREV_NOCF  END <> 0;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'30');

    COMMIT;

    /******************************************************************************
    31. -- 20160407 EIR stop reverse
        -- before EIR acf run
        -- reverse unamortized and amort accru if exist
        -- unamortized may be used by other process
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            V_CURRDATE AS DOWNLOAD_DATE ,
            A.DATASOURCE ,
            A.PRDCODE ,
            A.TRXCODE ,
            A.CCY ,
            'DEFA0' ,
            'ACT' ,
            'Y' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT  ELSE AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR STOP REV 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            A.FLAG_CF ,
            A.BRCODE ,
            A.PRDTYPE ,
            'ITRCG' ,
            A.CF_ID
    FROM    IFRS_ACCT_EIR_COST_FEE_PREV A -- 20130722 add join cond to pick latest cf prev
    JOIN VW_LAST_EIR_COST_FEE_PREV_YES C
      ON C.MASTERID = A.MASTERID
      AND C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
      AND NVL(C.SEQ,'') = NVL(A.SEQ,'')
    JOIN IFRS_ACCT_EIR_STOP_REV B
      ON B.DOWNLOAD_DATE = V_CURRDATE
    AND B.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_PREVDATE
    AND A.STATUS = 'ACT';
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'31');
    COMMIT;


    /******************************************************************************
    32. -- 20160407 amort yesterday accru
        -- block accru prev generation on SL_ECF
    *******************************************************************************/
    IF V_PARAM_DISABLE_ACCRU_PREV = 0
    THEN

      INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
      ( FACNO ,
        CIFNO ,
        DOWNLOAD_DATE ,
        DATASOURCE ,
        PRDCODE ,
        TRXCODE ,
        CCY ,
        JOURNALCODE ,
        STATUS ,
        REVERSE ,
        N_AMOUNT ,
        CREATEDDATE ,
        SOURCEPROCESS ,
        ACCTNO ,
        MASTERID ,
        FLAG_CF ,
        BRANCH ,
        PRDTYPE ,
        JOURNALCODE2 ,
        CF_ID
      )
      SELECT  /*+ PARALLEL(12) */ FACNO ,
              CIFNO ,
              V_CURRDATE ,
              DATASOURCE ,
              PRDCODE ,
              TRXCODE ,
              CCY ,
              'AMORT' ,
              STATUS ,
              'N' ,
              N_AMOUNT ,
              SYSTIMESTAMP ,
              'EIR STOP REV 2' ,
              ACCTNO ,
              X.MASTERID ,
              FLAG_CF ,
              BRANCH ,
              PRDTYPE ,
              'ACCRU' ,
              CF_ID
      FROM    IFRS_ACCT_JOURNAL_INTM X
      INNER JOIN (SELECT DISTINCT MASTERID
                  FROM    IFRS_ACCT_EIR_STOP_REV
                  WHERE   DOWNLOAD_DATE = V_CURRDATE ) Y
        ON X.MASTERID = Y.MASTERID
      WHERE   DOWNLOAD_DATE = V_PREVDATE
      AND STATUS = 'ACT'
      AND NVL(TRXCODE, ' ') <> 'BENEFIT'
      AND JOURNALCODE = 'ACCRU'
      AND REVERSE = 'N'
      AND SUBSTR(SOURCEPROCESS, 1, 3) = 'EIR';
                             /*   AND masterid IN (
                                SELECT  masterid
                                FROM    dbo.IFRS_ACCT_EIR_STOP_REV
                                WHERE   DOWNLOAD_DATE = @v_currdate ) */

      COMMIT;

    ELSE

      -- reverse accru
      INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
      ( FACNO ,
        CIFNO ,
        DOWNLOAD_DATE ,
        DATASOURCE ,
        PRDCODE ,
        TRXCODE ,
        CCY ,
        JOURNALCODE ,
        STATUS ,
        REVERSE ,
        N_AMOUNT ,
        CREATEDDATE ,
        SOURCEPROCESS ,
        ACCTNO ,
        MASTERID ,
        FLAG_CF ,
        BRANCH ,
        PRDTYPE ,
        JOURNALCODE2 ,
        CF_ID
      )
      SELECT  /*+ PARALLEL(12) */ FACNO ,
              CIFNO ,
              V_CURRDATE ,
              DATASOURCE ,
              PRDCODE ,
              TRXCODE ,
              CCY ,
              'DEFA0' ,
              STATUS ,
              'Y' ,
              -1 * N_AMOUNT ,
              SYSTIMESTAMP ,
              'EIR STOP REV 2' ,
              ACCTNO ,
              MASTERID ,
              FLAG_CF ,
              BRANCH ,
              PRDTYPE ,
              'ITRCG' ,
              CF_ID
      FROM    IFRS_ACCT_JOURNAL_INTM
      WHERE   DOWNLOAD_DATE = V_PREVDATE
      AND STATUS = 'ACT'
      AND JOURNALCODE = 'ACCRU'
      AND NVL(TRXCODE, ' ') <> 'BENEFIT'
      AND REVERSE = 'N'
      AND SUBSTR(SOURCEPROCESS, 1, 3) = 'EIR'
      AND MASTERID IN (
      SELECT  MASTERID
      FROM    IFRS_ACCT_EIR_STOP_REV
      WHERE   DOWNLOAD_DATE = V_CURRDATE );

      COMMIT;

    END IF;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'32');
    COMMIT;
      /*
      -- intm reverse data
      update dbo.IFRS_ACCT_JOURNAL_INTM
      set n_amount_idr = dbo.IFRS_ACCT_JOURNAL_INTM.N_AMOUNT * ISNULL (RATE_AMOUNT, 1)
      from psak_master_exchange_rate_curr b
      where  dbo.IFRS_ACCT_JOURNAL_INTM.CCY = b.CURRENCY
       and dbo.IFRS_ACCT_JOURNAL_INTM.DOWNLOAD_DATE = @v_currdate
       AND dbo.IFRS_ACCT_JOURNAL_INTM.REVERSE = 'Y'
      */

    /******************************************************************************
    33. 20180226 gain loss partial payment
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID ,
      METHOD
    )
    SELECT  /*+ PARALLEL(12) */ A.FACILITY_NUMBER ,
            A.CUSTOMER_NUMBER ,
            A.DOWNLOAD_DATE ,
            A.DATA_SOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT  ELSE C.AMOUNT  END ,
            SYSTIMESTAMP ,
            'EIR GAIN LOSS' ,
            A.ACCOUNT_NUMBER ,
            A.MASTERID ,
            C.FLAG_CF ,
            A.BRANCH_CODE ,
            C.PRDTYPE ,
            'ACCRU' ,
            C.CF_ID ,
            C.METHOD
    FROM    IFRS_IMA_AMORT_CURR A
    JOIN IFRS_ACCT_EIR_GAIN_LOSS C ON C.MASTERID = A.MASTERID
    AND C.DOWNLOAD_DATE = V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'33');
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG  ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_ACCT_EIR_JRNL_INTM' ,'');

    COMMIT;

END;