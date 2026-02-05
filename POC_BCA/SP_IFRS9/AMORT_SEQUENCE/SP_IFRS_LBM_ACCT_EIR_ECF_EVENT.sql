CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_ECF_EVENT
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  V_EFFDATEFLAG VARCHAR2(1);

  BEGIN
    SELECT MAX(CURRDATE)
    , MAX(PREVDATE) INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    SELECT COMMONUSAGE INTO V_EFFDATEFLAG
    FROM TBLM_COMMONCODEHEADER
    WHERE COMMONCODE = 'SCM004';

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_LBM_ACCT_EIR_ECF_EVENT','');

    COMMIT;

    -- RESET
    --UPDATE PMA SET EIR_STATUS='' WHERE DOWNLOAD_DATE=V_CURRDATE AND EIR_STATUS='Y';
    UPDATE /*+ PARALLEL(8) */ IFRS_IMA_AMORT_CURR
    SET EIR_STATUS = ''
        ,ECF_STATUS = ''
    WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

    UPDATE /*+ PARALLEL(8) */ IFRS_MASTER_ACCOUNT
    SET EIR_STATUS = ''
        ,ECF_STATUS = ''
    WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

    DELETE /*+ PARALLEL(8) */ IFRS_LBM_EVENT_CHANGES
    WHERE DOWNLOAD_DATE = V_CURRDATE;


    COMMIT;

    /*
    DELETE IFRS_LBM_EVENT_CHANGES_DETAILS
    WHERE DOWNLOAD_DATE = @V_CURRDATE
    */

    -- GET ACTIVE EIR ECF MASTERID
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACT_ECF_TMP';

    INSERT /*+ PARALLEL(8) */ INTO IFRS_ACT_ECF_TMP (MASTERID)
    SELECT /*+ PARALLEL(8) */ DISTINCT MASTERID
    FROM IFRS_LBM_ACCT_EIR_ECF
    WHERE AMORTSTOPDATE IS NULL;

    COMMIT;

    --AND DOWNLOAD_DATE < @V_CURRDATE
    --INTEREST RATE CHANGES
    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_EVENT_CHANGES (
        DOWNLOAD_DATE
        ,MASTERID
        ,ACCOUNT_NUMBER
        ,EFFECTIVE_DATE
        ,BEFORE_VALUE
        ,AFTER_VALUE
        ,EVENT_ID
        ,REMARKS
        ,CREATEDBY
        )
    SELECT /*+ PARALLEL(8) */ V_CURRDATE
        ,A.MASTERID
        ,A.ACCOUNT_NUMBER
        ,CASE
            WHEN V_EFFDATEFLAG = '1'
                THEN V_CURRDATE
            WHEN V_EFFDATEFLAG = '2'
                THEN A.NEXT_PAYMENT_DATE
            ELSE V_CURRDATE
            END
        ,C.INTEREST_RATE
        ,A.INTEREST_RATE
        ,0
        ,'Interest Rate Changes'
        ,'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
    FROM IFRS_IMA_AMORT_CURR A
    INNER JOIN IFRS_ACT_ECF_TMP B ON A.MASTERID = B.MASTERID
    INNER JOIN IFRS_IMA_AMORT_PREV C ON A.MASTERID = C.MASTERID
    WHERE (
            A.INTEREST_RATE <> C.INTEREST_RATE
            OR A.INTEREST_RATE_IDC <> C.INTEREST_RATE_IDC
            )
        AND (
            ABS(A.UNAMORT_COST_AMT) <> 0
            OR ABS(A.UNAMORT_FEE_AMT) <> 0
            )
        AND NVL(A.INTEREST_RATE, 0) > 0
        AND A.LOAN_DUE_DATE > V_CURRDATE
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.STAFF_LOAN_FLAG = 'Y';


    COMMIT;


    --LOAN DUE DATE CHANGES
    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_EVENT_CHANGES (
        DOWNLOAD_DATE
        ,MASTERID
        ,ACCOUNT_NUMBER
        ,EFFECTIVE_DATE
        ,BEFORE_VALUE
        ,AFTER_VALUE
        ,EVENT_ID
        ,REMARKS
        ,CREATEDBY
        )
    SELECT /*+ PARALLEL(8) */ V_CURRDATE
        ,A.MASTERID
        ,A.ACCOUNT_NUMBER
        ,CASE
            WHEN V_EFFDATEFLAG = '1'
                THEN V_CURRDATE
            WHEN V_EFFDATEFLAG = '2'
                THEN A.NEXT_PAYMENT_DATE
            ELSE V_CURRDATE
            END
        ,C.LOAN_DUE_DATE
        ,A.LOAN_DUE_DATE
        ,1
        ,'Loan Due Date Change'
        ,'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
    FROM IFRS_IMA_AMORT_CURR A
    INNER JOIN IFRS_ACT_ECF_TMP B ON A.MASTERID = B.MASTERID
    INNER JOIN IFRS_IMA_AMORT_PREV C ON A.MASTERID = C.MASTERID
    WHERE A.LOAN_DUE_DATE <> C.LOAN_DUE_DATE
        AND (
            ABS(A.UNAMORT_COST_AMT) <> 0
            OR ABS(A.UNAMORT_FEE_AMT) <> 0
            )
        AND A.LOAN_DUE_DATE > V_CURRDATE
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.STAFF_LOAN_FLAG = 'Y';


    COMMIT;


    --NEW COST/FEE
    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_EVENT_CHANGES (
        DOWNLOAD_DATE
        ,MASTERID
        ,ACCOUNT_NUMBER
        ,EFFECTIVE_DATE
        ,BEFORE_VALUE
        ,AFTER_VALUE
        ,EVENT_ID
        ,REMARKS
        ,CREATEDBY
        )
    SELECT /*+ PARALLEL(8) */ V_CURRDATE
        ,A.MASTERID
        ,A.ACCOUNT_NUMBER
        ,CASE
            WHEN V_EFFDATEFLAG = '1'
                THEN V_CURRDATE
            WHEN V_EFFDATEFLAG = '2'
                THEN A.NEXT_PAYMENT_DATE
            ELSE V_CURRDATE
            END
        ,0
        ,B.AMOUNT
        ,2
        ,CASE WHEN B.FLAG_CF='F'
					THEN CASE WHEN B.FLAG_REVERSE='N' THEN 'Additional/New Fee ' || B.TRX_CODE
							  WHEN B.FLAG_REVERSE='Y' THEN 'Reversal Fee ' || B.TRX_CODE
						END
				 WHEN B.FLAG_CF='C'
					THEN CASE WHEN B.FLAG_REVERSE='N' THEN 'Additional/New Cost ' || B.TRX_CODE
							  WHEN B.FLAG_REVERSE='Y' THEN 'Reversal Cost ' || B.TRX_CODE
						END
			END AS REMARKS
        ,'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
    FROM IFRS_IMA_AMORT_CURR A
    INNER JOIN IFRS_ACCT_COST_FEE B ON A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
    WHERE B.STATUS = 'ACT'
        AND B.METHOD = 'EIR'
        AND A.LOAN_DUE_DATE > V_CURRDATE
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.STAFF_LOAN_FLAG = 'Y';


    COMMIT;

        -- NEW STAFFLOAN EVENT
        INSERT  /*+ PARALLEL(8) */ INTO IFRS_LBM_EVENT_CHANGES
                ( DOWNLOAD_DATE ,
                  MASTERID ,
                  ACCOUNT_NUMBER ,
                  EFFECTIVE_DATE ,
                  BEFORE_VALUE ,
                  AFTER_VALUE ,
                  EVENT_ID ,
                  REMARKS ,
                  CREATEDBY
                )
                SELECT  /*+ PARALLEL(8) */ V_CURRDATE ,
                        A.MASTERID ,
                        A.ACCOUNT_NUMBER ,
                        V_CURRDATE ,
                        0 ,
                        0 ,
                        3 ,
                        'New Staff Loan Account' ,
                        'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
                FROM    IFRS_IMA_AMORT_CURR A
            INNER JOIN IFRS_PRODUCT_PARAM B ON A.DATA_SOURCE = B.DATA_SOURCE
                                     AND A.PRODUCT_TYPE = B.PRD_TYPE
                AND A.PRODUCT_CODE = B.PRD_CODE
                AND (A.CURRENCY = B.CCY OR B.CCY = 'ALL')
                WHERE   A.LOAN_START_DATE = V_CURRDATE
                    AND   (B.IS_STAF_LOAN IN ('1','Y') OR A.STAFF_LOAN_FLAG = 'Y')
                    AND   A.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    /*
  -- REPAYMENT EVENT
        INSERT  INTO IFRS_LBM_EVENT_CHANGES
                ( DOWNLOAD_DATE ,
                  MASTERID ,
                  ACCOUNT_NUMBER ,
                  EFFECTIVE_DATE ,
                  BEFORE_VALUE ,
                  AFTER_VALUE ,
                  EVENT_ID ,
                  REMARKS ,
                  CREATEDBY
                )
                SELECT  @V_CURRDATE ,
                        A.MASTERID ,
                        A.ACCOUNT_NUMBER ,
                        @V_CURRDATE ,
                        0 ,
                        1 ,
                        4 ,
                        'REPAYMENT ACCOUNT' ,
                        'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
                FROM    IFRS_IMA_AMORT_CURR A
            INNER JOIN IFRS_MASTER_ACCOUNT B ON A.MASTERID = B.MASTERID
                                     AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                AND A.CURRENCY = B.CURRENCY
                WHERE   B.FLAG_REPAYMENT = '1'
      AND   A.DOWNLOAD_DATE = @V_CURRDATE
      AND A.MASTERID IN (
       SELECT MASTERID FROM IFRS_MASTER_ACCOUNT
       WHERE DOWNLOAD_DATE = @V_PREVDATE
       AND FLAG_REPAYMENT = '0'
       )
     AND A.AMORT_TYPE = 'EIR'

 -- DISBURSE REVERSAL EVENT
        INSERT  INTO IFRS_LBM_EVENT_CHANGES
                ( DOWNLOAD_DATE ,
                  MASTERID ,
                  ACCOUNT_NUMBER ,
                  EFFECTIVE_DATE ,
                  BEFORE_VALUE ,
                  AFTER_VALUE ,
                  EVENT_ID ,
                  REMARKS ,
                  CREATEDBY
                )
                SELECT  @V_CURRDATE ,
                        A.MASTERID ,
                        A.ACCOUNT_NUMBER ,
                        @V_CURRDATE ,
                        0 ,
                        0 ,
                        5 ,
                        'REVERSAL DISBURSE ACCOUNT' ,
                        'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
                FROM    IFRS_IMA_AMORT_CURR A
            INNER JOIN IFRS_MASTER_ACCOUNT B ON A.MASTERID = B.MASTERID
                                     AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                AND A.CURRENCY = B.CURRENCY
                WHERE   B.FLAG_REVERSAL = 'Y'
      AND   A.DOWNLOAD_DATE = @V_CURRDATE
      AND A.MASTERID IN (
       SELECT MASTERID FROM IFRS_MASTER_ACCOUNT
       WHERE DOWNLOAD_DATE = @V_PREVDATE
       AND FLAG_REVERSAL = 'N'
       )
    AND A.AMORT_TYPE = 'EIR'

        INSERT  INTO IFRS_LBM_EVENT_CHANGES_DETAILS
                ( DOWNLOAD_DATE ,
                  MASTERID ,
                  ACCOUNT_NUMBER ,
                  BEFORE_VALUE ,
                  AFTER_VALUE ,
                  EVENT_ID ,
                  REMARKS ,
                  CREATEDBY
                )
                SELECT  A.DOWNLOAD_DATE ,
                        A.MASTERID ,
                        A.ACCOUNT_NUMBER ,
                        B.BEFORE_VALUE ,
                        B.AFTER_VALUE ,
                        B.EVENT_ID ,
                        B.REMARKS ,
                        'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
                FROM    IFRS_IMA_AMORT_CURR A
                        INNER JOIN IFRS_LBM_EVENT_CHANGES B ON A.MASTERID = B.MASTERID
                                                           AND A.DOWNLOAD_DATE = B.EFFECTIVE_DATE
                WHERE   A.DOWNLOAD_DATE = @V_CURRDATE
      AND   A.AMORT_TYPE = 'EIR'
      AND   A.MASTERID NOT IN (SELECT DISTINCT MASTERID
                                      FROM      IFRS_ACCT_CLOSED
                                      WHERE     DOWNLOAD_DATE = @V_CURRDATE)
           */
    /*REMARK CTBC
        -- NEW NOCF EVENT
        INSERT  INTO IFRS_LBM_EVENT_CHANGES
                ( DOWNLOAD_DATE ,
                  MASTERID ,
                  ACCOUNT_NUMBER ,
                  EFFECTIVE_DATE ,
                  BEFORE_VALUE ,
                  AFTER_VALUE ,
                  EVENT_ID ,
                  REMARKS ,
                  CREATEDBY
                )
                SELECT  @V_CURRDATE ,
                        A.MASTERID ,
                        A.ACCOUNT_NUMBER ,
                        @V_CURRDATE ,
                        0 ,
                        0 ,
                        4 ,
                        'NEW NOCF ACCOUNT' ,
                        'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
                FROM    IFRS_IMA_AMORT_CURR A
            INNER JOIN IFRS_PRODUCT_PARAM B ON A.DATA_SOURCE = B.DATA_SOURCE
                AND A.PRODUCT_TYPE = B.PRD_TYPE
                AND A.PRODUCT_CODE = B.PRD_CODE
                AND (A.CURRENCY = B.CCY OR B.CCY = 'ALL')
                WHERE   A.LOAN_START_DATE = @V_CURRDATE
                AND   (B.IS_STAF_LOAN IN ('N') OR A.STAFF_LOAN_FLAG = 'N')
                AND   A.DOWNLOAD_DATE = @V_CURRDATE
                AND A.MASTERID NOT IN (
                                      SELECT DISTINCT MASTERID
                                      FROM IFRS_LBM_EVENT_CHANGES
                                      WHERE DOWNLOAD_DATE = @V_CURRDATE
                                      )

                AND A.LOAN_DUE_DATE > @V_CURRDATE
                AND A.ACCOUNT_STATUS = 'A'

      */
    -- NEW RESTRUCT EVENT
    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_EVENT_CHANGES (
        DOWNLOAD_DATE
        ,MASTERID
        ,ACCOUNT_NUMBER
        ,EFFECTIVE_DATE
        ,BEFORE_VALUE
        ,AFTER_VALUE
        ,EVENT_ID
        ,REMARKS
        ,CREATEDBY
        )
    SELECT /*+ PARALLEL(8) */ V_CURRDATE
        ,A.MASTERID
        ,A.ACCOUNT_NUMBER
        ,V_CURRDATE
        ,B.PREV_ACCTNO
        ,B.ACCTNO
        ,5
        ,'Account Restructure'
        ,'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
    FROM IFRS_IMA_AMORT_CURR A
    INNER JOIN IFRS_ACCT_SWITCH B ON A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.ACCOUNT_NUMBER = B.ACCTNO
        AND B.ACCTNO <> B.PREV_ACCTNO
    WHERE A.DOWNLOAD_DATE = V_CURRDATE
    AND A.STAFF_LOAN_FLAG = 'Y';


    COMMIT;

    -- PARTIAL PAYMENT EVENT
    INSERT /*+ PARALLEL(8) */ INTO IFRS_LBM_EVENT_CHANGES (
        DOWNLOAD_DATE
        ,MASTERID
        ,ACCOUNT_NUMBER
        ,EFFECTIVE_DATE
        ,BEFORE_VALUE
        ,AFTER_VALUE
        ,EVENT_ID
        ,REMARKS
        ,CREATEDBY
        )
    SELECT /*+ PARALLEL(8) */ V_CURRDATE
        ,A.MASTERID
        ,A.ACCOUNT_NUMBER
        ,V_CURRDATE
        ,0
        ,B.ORG_CCY_AMT
        ,6
        ,'Partial Payment'
        ,'SP_IFRS_LBM_ACCT_EIR_ECF_EVENT'
    FROM IFRS_IMA_AMORT_CURR A
    INNER JOIN IFRS_TRANSACTION_DAILY B ON A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND B.TRX_CODE = 'PP'
    WHERE A.DOWNLOAD_DATE = V_CURRDATE
        AND A.ACCOUNT_STATUS = 'A'
        AND A.OUTSTANDING > 0
        AND A.STAFF_LOAN_FLAG = 'Y';COMMIT;


    MERGE INTO IFRS_IMA_AMORT_CURR IMA
    USING
    (   SELECT DISTINCT MASTERID
         FROM IFRS_LBM_EVENT_CHANGES
         WHERE EFFECTIVE_DATE = V_CURRDATE
         AND MASTERID NOT IN
         (
            SELECT DISTINCT MASTERID
            FROM IFRS_ACCT_CLOSED
            WHERE DOWNLOAD_DATE = V_CURRDATE
         )
    ) RES
    ON (RES.MASTERID = IMA.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET IMA.EIR_STATUS = 'Y'
        ,IMA.ECF_STATUS = 'Y';

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_LBM_ACCT_EIR_ECF_EVENT','');

    COMMIT;

END;