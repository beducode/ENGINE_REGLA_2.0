CREATE OR REPLACE PROCEDURE SP_IFRS_SL_MAIN_PROC
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;

BEGIN
    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT CURRDATE, PREVDATE
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(  V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_SL_MAIN_PROC','');

    DELETE IFRS_ACF_SL_MSTR
    WHERE EFFDATE >= V_CURRDATE;

    DELETE IFRS_ACF_SL_MSTR_REV
    WHERE EFFDATE >= V_CURRDATE;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','CLEAN UP done');

    COMMIT;

    /******************************************************************************
    02. INSERT INTO IFRS_ACF_SL_MSTR
    *******************************************************************************/
    INSERT INTO IFRS_ACF_SL_MSTR
    ( EFFDATE,
      ACCOUNT_NUMBER,
      TRX_CODE,
      CCY,
      PRD_CODE,
      START_AMORT_DATE,
      ORIGINAL_VALUE_ORG,
      ORIGINAL_VALUE,
      DRCR,
      IFRS_STATUS,
      EXCHANGE_RATE--,
      --brcode
    )
    SELECT V_CURRDATE,
           ACCOUNT_NUMBER,
           A.TRX_CODE,
           A.CCY,
           A.PRD_TYPE,
           V_CURRDATE,
           --  left(b.IFRS_TXN_CLASS,1),
           --  case when b.SL_EXP_LIFE is not null then dateadd(mm,b.sl_exp_life, @currdate) else null end,
           /*
           case when left(b.IFRS_TXN_CLASS,1) = 'F' and a.DEBET_CREDIT_FLAG = 'C' then -a.ORG_CCY_AMT
           when left(b.IFRS_TXN_CLASS,1) = 'C' and a.DEBET_CREDIT_FLAG = 'D' then a.ORG_CCY_AMT
           else -a.ORG_CCY_AMT end,
           case when left(b.IFRS_TXN_CLASS,1) = 'F' and a.DEBET_CREDIT_FLAG = 'C' then -a.EQV_LCY_AMT
           when left(b.IFRS_TXN_CLASS,1) = 'C' and a.DEBET_CREDIT_FLAG = 'D' then a.EQV_LCY_AMT
           else -a.EQV_LCY_AMT end,
           */
           A.ORG_CCY_AMT ,
           A.EQV_LCY_AMT,
           A.DEBET_CREDIT_FLAG,
           'ACT',
           ORG_CCY_AMT/EQV_LCY_AMT
           /*,a.BRANCH_CODE*/
    FROM IFRS_TRANSACTION_DAILY A
    WHERE TRX_CODE IN (SELECT DISTINCT TRX_CODE
                       FROM (SELECT DISTINCT TRX_CODE FROM IFRS_TRANSACTION_PARAM WHERE AMORT_TYPE = 'SL') A
                       LEFT JOIN (SELECT DISTINCT TRX_CODE AS TRX_CODE_EIR FROM IFRS_TRANSACTION_PARAM  WHERE AMORT_TYPE = 'EIR') B
                       ON A.TRX_CODE = B.TRX_CODE_EIR
                       WHERE B.TRX_CODE_EIR IS NULL
                       )
    AND DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    /******************************************************************************
    03. UPDATE PRODUCT CODE
    *******************************************************************************/
    MERGE INTO  IFRS_ACF_SL_MSTR A
    USING IFRS_MASTER_ACCOUNT B
    ON (A.EFFDATE = B.DOWNLOAD_DATE
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.PRD_CODE = B.PRODUCT_CODE ;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','INS 1 done');

    COMMIT;
    /******************************************************************************
    04. INSERT INTO IFRS_ACF_SL_MSTR (SL)
    *******************************************************************************/
    INSERT INTO IFRS_ACF_SL_MSTR
    ( EFFDATE,
      ACCOUNT_NUMBER,
      TRX_CODE,
      CCY,
      PRD_CODE,
      START_AMORT_DATE,
      ORIGINAL_VALUE_ORG,
      ORIGINAL_VALUE,
      DRCR,
      IFRS_STATUS,
      EXCHANGE_RATE--,
      --brcode
    )
    SELECT V_CURRDATE,
           ACCOUNT_NUMBER,
           A.TRX_CODE,
           A.CCY,
           A.PRD_TYPE,
           V_CURRDATE,
           A.ORG_CCY_AMT ,
           A.EQV_LCY_AMT,
           A.DEBET_CREDIT_FLAG,
           'ACT',
           ORG_CCY_AMT/EQV_LCY_AMT
    FROM IFRS_TRANSACTION_DAILY A
    JOIN IFRS_TRANSACTION_PARAM B
      ON  A.TRX_CODE = B.TRX_CODE
      AND (A.PRD_CODE = B.PRD_CODE OR NVL(B.PRD_CODE,'ALL')= 'ALL')
      AND AMORT_TYPE = 'SL'
    WHERE A.TRX_CODE IN (SELECT DISTINCT TRX_CODE
                         FROM (SELECT DISTINCT TRX_CODE FROM IFRS_TRANSACTION_PARAM WHERE AMORT_TYPE = 'SL') A
                         JOIN (SELECT DISTINCT TRX_CODE AS TRX_CODE_EIR FROM IFRS_TRANSACTION_PARAM WHERE AMORT_TYPE = 'EIR' ) B

                         ON A.TRX_CODE = B.TRX_CODE_EIR
                        )
    AND DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','INS 2 done');

    /******************************************************************************
    05. UPDATE IFRS_ACF_SL_MSTR
    *******************************************************************************/
    MERGE INTO IFRS_ACF_SL_MSTR A
    USING IFRS_TRANSACTION_PARAM B
    ON ((A.PRD_CODE = B.PRD_CODE OR NVL(B.PRD_CODE,'ALL')= 'ALL')
         AND A.TRX_CODE = B.TRX_CODE
         AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FLAG_CF = SUBSTR(B.IFRS_TXN_CLASS, 1,1),
        A.END_AMORT_DATE =  CASE WHEN B.SL_EXP_LIFE IS NOT NULL THEN (FN_PMTDATE(START_AMORT_DATE,B.sl_exp_life)+ INTERVAL '-1' DAY) ELSE NULL END,
        A.ORIGINAL_VALUE = CASE WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'F' AND A.DRCR = 'C' THEN -A.ORIGINAL_VALUE
                                WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'C' AND A.DRCR = 'D' THEN A.ORIGINAL_VALUE
                                WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'F' AND A.DRCR = 'D' THEN A.ORIGINAL_VALUE
                                WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'C' AND A.DRCR = 'C' THEN -A.ORIGINAL_VALUE END ,
        A.ORIGINAL_VALUE_ORG = CASE WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'F' AND A.DRCR = 'C' THEN -A.ORIGINAL_VALUE_ORG
                                    WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'C' AND A.DRCR = 'D' THEN A.ORIGINAL_VALUE_ORG
                                    WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'F' AND A.DRCR = 'D' THEN A.ORIGINAL_VALUE_ORG
                                    WHEN SUBSTR(B.IFRS_TXN_CLASS, 1,1) = 'C' AND A.DRCR = 'C' THEN -A.ORIGINAL_VALUE_ORG END ;
    COMMIT;


    MERGE INTO IFRS_ACF_SL_MSTR A
    USING IFRS_MASTER_ACCOUNT B
    ON (A.EFFDATE = B.DOWNLOAD_DATE
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.MASTERID = B.MASTERID,
        A.END_AMORT_DATE = NVL(A.END_AMORT_DATE,B.LOAN_DUE_DATE),
     A.CIFNO = B.CUSTOMER_NUMBER,
     A.FACNO = B.FACILITY_NUMBER,
     A.DATA_SOURCE = B.DATA_SOURCE,
     A.PRD_CODE = B.PRODUCT_CODE,
     A.BRCODE = B.BRANCH_CODE;

    COMMIT;


    MERGE INTO IFRS_ACF_SL_MSTR A
    USING IFRS_MASTER_ACCOUNT B
    ON (A.EFFDATE = B.DOWNLOAD_DATE
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.EFFDATE = V_CURRDATE
        AND A.EXCHANGE_RATE IS NULL
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.EXCHANGE_RATE = B.EXCHANGE_RATE,
        A.ORIGINAL_VALUE = A.ORIGINAL_VALUE_ORG * B.EXCHANGE_RATE;
    COMMIT;


    UPDATE IFRS_ACF_SL_MSTR
    SET SL_AMORT_DAILY = (ORIGINAL_VALUE_ORG/((END_AMORT_DATE -START_AMORT_DATE)+1)*-1 )
    WHERE EFFDATE = V_CURRDATE
    AND IFRS_STATUS = 'ACT'
    AND SL_AMORT_DAILY IS NULL ;

    COMMIT;

    MERGE INTO IFRS_ACF_SL_MSTR A
    USING IFRS_MASTER_ACCOUNT B
    ON ( A.EFFDATE = B.DOWNLOAD_DATE
      AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
      AND A.START_AMORT_DATE = B.LOAN_START_DATE
      AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.MASTERID = B.MASTERID,
        A.END_AMORT_DATE = NVL(A.END_AMORT_DATE,B.LOAN_DUE_DATE),
        A.CIFNO = B.CUSTOMER_NUMBER,
        A.FACNO = B.FACILITY_NUMBER,
        A.DATA_SOURCE = B.DATA_SOURCE,
        A.PRD_CODE = B.PRODUCT_CODE;

    COMMIT;

    UPDATE IFRS_ACF_SL_MSTR
    SET IFRS_STATUS = 'FRZ'
    WHERE MASTERID IS NULL
    AND EFFDATE = V_CURRDATE;


    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','UPDATE done');

    COMMIT;

    /******************************************************************************
    06. CAPTURE REVERSAL
    *******************************************************************************/
    INSERT INTO IFRS_ACF_SL_MSTR_REV
    ( MASTERID
     ,TRX_CODE
     ,CCY
     ,PRD_CODE
     ,START_AMORT_DATE
     ,END_AMORT_DATE
     ,FLAG_CF
     ,ORIGINAL_VALUE
     ,ORIGINAL_VALUE_ORG
     ,AMORT_VALUE
     ,AMORT_VALUE_ORG
     ,UNAMORT_VALUE
     ,UNAMORT_VALUE_ORG
     ,CLOSING_AMOUNT
     ,CLOSING_AMOUNT_ORG
     ,ITRCG_FLAG
     ,EXCHANGE_RATE
     ,EFFDATE
     ,ACCOUNT_NUMBER
     ,BRCODE
     ,CIFNO
     ,FACNO
     ,DATA_SOURCE
     ,DRCR
    )
    SELECT  MASTERID
           ,TRX_CODE
           ,CCY
           ,PRD_CODE
           ,START_AMORT_DATE
           ,END_AMORT_DATE
           ,FLAG_CF
           ,ORIGINAL_VALUE
           ,ORIGINAL_VALUE_ORG
           ,AMORT_VALUE
           ,AMORT_VALUE_ORG
           ,UNAMORT_VALUE
           ,UNAMORT_VALUE_ORG
           ,CLOSING_AMOUNT
           ,CLOSING_AMOUNT_ORG
           ,ITRCG_FLAG
           ,EXCHANGE_RATE
           ,EFFDATE
           ,ACCOUNT_NUMBER
           ,BRCODE
           ,CIFNO
           ,FACNO
           ,DATA_SOURCE
           ,DRCR
    FROM IFRS_ACF_SL_MSTR
    WHERE EFFDATE = V_CURRDATE
    AND ((ORIGINAL_VALUE < 0 AND FLAG_CF = 'C') OR (ORIGINAL_VALUE > 0 AND FLAG_CF = 'F'));

    COMMIT;

    /******************************************************************************
    07. for reverse if masterid is missing take from previous masterid
    *******************************************************************************/
    MERGE INTO IFRS_ACF_SL_MSTR_REV A
    USING IFRS_MASTER_ACCOUNT B
    ON (A.EFFDATE = V_CURRDATE
        AND B.DOWNLOAD_DATE = V_PREVDATE
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.EFFDATE = V_CURRDATE
        AND A.MASTERID IS NULL
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.MASTERID = B.MASTERID,
        A.END_AMORT_DATE = NVL(A.END_AMORT_DATE,B.LOAN_DUE_DATE),
        A.CIFNO = B.CUSTOMER_NUMBER,
        A.FACNO = B.FACILITY_NUMBER,
        A.DATA_SOURCE = B.DATA_SOURCE,
        A.PRD_CODE = B.PRODUCT_CODE,
        A.BRCODE = B.BRANCH_CODE ;

    COMMIT;

    MERGE INTO IFRS_ACF_SL_MSTR_REV A
    USING IFRS_ACF_SL_MSTR B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND B.EFFDATE = V_PREVDATE
        AND A.ORIGINAL_VALUE = B.ORIGINAL_VALUE*-1
        AND A.EFFDATE = V_CURRDATE
        AND A.MASTERID IS NULL
       )
    WHEN MATCHED THEN
    UPDATE SET A.MASTERID = B.MASTERID ;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','REVERSE done');

    DELETE FROM IFRS_ACF_SL_MSTR
    WHERE EFFDATE = V_CURRDATE
    AND ((ORIGINAL_VALUE < 0 AND FLAG_CF = 'C') OR (ORIGINAL_VALUE > 0 AND FLAG_CF = 'F'))  ;

    COMMIT;

    UPDATE IFRS_ACF_SL_MSTR
    SET IFRS_STATUS = 'PNL'
    WHERE END_AMORT_DATE <= V_CURRDATE
    AND IFRS_STATUS = 'ACT'
    AND EFFDATE = V_CURRDATE;

    COMMIT;

    UPDATE IFRS_ACF_SL_MSTR
    SET IFRS_STATUS = 'PNL'
    WHERE END_AMORT_DATE > V_CURRDATE
    AND IFRS_STATUS = 'ACT'
    AND MASTERID IN (SELECT DISTINCT MASTERID
                     FROM IFRS_MASTER_ACCOUNT
                     WHERE DOWNLOAD_DATE = V_CURRDATE
                     AND ACCOUNT_STATUS <> 'A'
                     )
    AND EFFDATE = V_CURRDATE;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','UPDATE 2 done');
    /*
    delete IFRS_ACCT_COST_FEE
    where CREATEDBY = 'SL PROCESS'
      and DOWNLOAD_DATE = @currdate


    delete IFRS_ACCT_COST_FEE
    where CREATEDBY = 'SP_IFRS_TRAN_DAILY'
      and DOWNLOAD_DATE = @currdate
      AND METHOD = 'SL'


    insert IFRS_ACCT_COST_FEE
    (
      DOWNLOAD_DATE,
      CREATEDBY,
      masterid,
      BRCODE,
      ccy,
      TRX_CODE,
      cifno,
      facno,
      acctno,
      DATASOURCE,
      PRD_CODE,
      FLAG_CF,
      FLAG_REVERSE,
      METHOD,
      STATUS,
      SRCPROCESS,
      AMOUNT,
      ORG_CCY,
      ORG_CCY_EXRATE
    )
    select
      @currdate,
      'SL PROCESS',
      masterid,
      brcode,
      ccy,
      TRX_CODE,
      cifno,
      facno,
      account_number,
      data_source,
      PRD_CODE,
      FLAG_CF,
      'N',
      'SL',
      IFRS_STATUS,
      'SL PROCESS',
      ORIGINAL_VALUE,
      ccy,
      EXCHANGE_RATE
    from IFRS_ACF_SL_MSTR
    where effdate = @currdate
    --and START_AMORT_DATE = @currdate
    */
    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','INS ACCT COST FEE done');


    COMMIT;


    INSERT INTO IFRS_ACF_SL_MSTR
    (MASTERID
    ,TRX_CODE
    ,CCY
    ,PRD_CODE
    ,START_AMORT_DATE
    ,END_AMORT_DATE
    ,FLAG_CF
    ,ORIGINAL_VALUE
    ,ORIGINAL_VALUE_ORG
    ,AMORT_VALUE
    ,AMORT_VALUE_ORG
    ,UNAMORT_VALUE
    ,UNAMORT_VALUE_ORG
    ,CLOSING_AMOUNT
    ,CLOSING_AMOUNT_ORG
    ,IFRS_STATUS
    ,ITRCG_FLAG
    ,EXCHANGE_RATE
    ,EFFDATE
    ,ACCOUNT_NUMBER
    ,DATA_SOURCE
    ,FACNO
    ,CIFNO
    ,BRCODE
    ,DRCR
    )
    SELECT MASTERID
          ,TRX_CODE
          ,CCY
          ,PRD_CODE
          ,START_AMORT_DATE
          ,END_AMORT_DATE
          ,FLAG_CF
          ,ORIGINAL_VALUE
          ,ORIGINAL_VALUE_ORG
          ,AMORT_VALUE
          ,AMORT_VALUE_ORG
          ,UNAMORT_VALUE
          ,UNAMORT_VALUE_ORG
          ,CLOSING_AMOUNT
          ,CLOSING_AMOUNT_ORG
          ,IFRS_STATUS
          ,ITRCG_FLAG
          ,EXCHANGE_RATE
          ,V_CURRDATE
          ,ACCOUNT_NUMBER
          ,DATA_SOURCE
          ,FACNO
          ,CIFNO
          ,BRCODE
          ,DRCR
    FROM IFRS_ACF_SL_MSTR
    WHERE EFFDATE = V_PREVDATE
    AND IFRS_STATUS = 'ACT';


    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','INS SL MSTR prevdate done');

    --reverse
    --mark the ACF_SL_MSTR first

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_REV_LS';

    INSERT INTO TMP_REV_LS
    ( ACCOUNT_NUMBER,
      TRX_CODE,
      FLAG_CF,
      AMOUNT,
      ID ,
      CNT,
      STAT
    )
    SELECT ACCOUNT_NUMBER
         , TRX_CODE
         , FLAG_CF
         , ORIGINAL_VALUE_ORG
         , ID_SL, SUM(CNT) OVER (PARTITION BY ACCOUNT_NUMBER, TRX_CODE, FLAG_CF, ORIGINAL_VALUE_ORG ORDER BY ID_SL)
         , 'NEW' AS STAT
    FROM (SELECT ACCOUNT_NUMBER, TRX_CODE, FLAG_CF, ORIGINAL_VALUE_ORG, ID_SL, 1 AS CNT
          FROM IFRS_ACF_SL_MSTR_REV
          WHERE EFFDATE = V_CURRDATE
         ) REV_CHECK  ;


    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_CF_LS';

    INSERT INTO TMP_CF_LS
    (
      ACCOUNT_NUMBER,
      TRX_CODE,
      FLAG_CF,
      AMOUNT,
      ID,
      CNT,
      STAT
    )
    SELECT ACCOUNT_NUMBER
         , TRX_CODE
         , FLAG_CF
         , ORIGINAL_VALUE_ORG
         , ID_SL, SUM(CNT) OVER (PARTITION BY ACCOUNT_NUMBER, TRX_CODE, FLAG_CF, ORIGINAL_VALUE_ORG ORDER BY ID_SL)
         , 'NEW' AS STAT
    FROM (SELECT A.ACCOUNT_NUMBER, A.TRX_CODE, A.FLAG_CF, ORIGINAL_VALUE_ORG, A.ID_SL, 1 AS CNT
          FROM IFRS_ACF_SL_MSTR A
          JOIN ( SELECT DISTINCT ACCOUNT_NUMBER, TRX_CODE, AMOUNT*-1 AS AMT FROM TMP_REV_LS) B
            ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER AND A.TRX_CODE = B.TRX_CODE
            AND A.ORIGINAL_VALUE_ORG= B.AMT
          WHERE EFFDATE = V_CURRDATE
         ) REV_CF   ;

    COMMIT;

    MERGE INTO TMP_REV_LS A
    USING TMP_CF_LS B
    ON (A.CNT = B.CNT
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.TRX_CODE = B.TRX_CODE
        AND A.AMOUNT = B.AMOUNT*-1
        AND A.FLAG_CF = B.FLAG_CF
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.STAT = CASE WHEN B.ACCOUNT_NUMBER IS NULL THEN 'FRZ' ELSE 'ACT' END;


    MERGE INTO TMP_CF_LS A
    USING TMP_REV_LS B
    ON (A.CNT = B.CNT
            AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
            AND A.TRX_CODE = B.TRX_CODE
            AND A.AMOUNT = B.AMOUNT*-1
            AND A.FLAG_CF = B.FLAG_CF
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.STAT = CASE WHEN B.ACCOUNT_NUMBER IS NULL THEN 'FRZ' ELSE 'ACT' END ;

    COMMIT;

    MERGE INTO IFRS_ACF_SL_MSTR A
    USING TMP_CF_LS B
    ON (A.ID_SL = B.ID
        AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS_STATUS = CASE WHEN B.STAT = 'ACT' THEN 'REV' ELSE 'ACT' END;



    MERGE INTO IFRS_ACF_SL_MSTR_REV A
    USING TMP_REV_LS B
    ON (A.ID_SL = B.ID
        AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS_STATUS = CASE WHEN B.STAT = 'ACT' THEN 'REV' ELSE 'ACT' END;

    COMMIT;



    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','REVERSE 2 done');


    DELETE FROM IFRS_ACCT_COST_FEE
    WHERE CREATEDBY = 'SL PROC REV'
      AND DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;
    ---COST FEE for reverse
    INSERT INTO IFRS_ACCT_COST_FEE
    ( DOWNLOAD_DATE,
      CREATEDBY,
      MASTERID,
      BRCODE,
      CCY,
      TRX_CODE,
      CIFNO,
      FACNO,
      ACCTNO,
      DATASOURCE,
      PRD_CODE,
      FLAG_CF,
      FLAG_REVERSE,
      METHOD,
      STATUS,
      SRCPROCESS,
      AMOUNT,
      ORG_CCY,
      ORG_CCY_EXRATE
    )
    SELECT V_CURRDATE,
          'SL PROC REV',
          MASTERID,
          BRCODE,
          CCY,
          TRX_CODE,
          CIFNO,
          FACNO,
          ACCOUNT_NUMBER,
          DATA_SOURCE,
          PRD_CODE,
          FLAG_CF,
          'Y',
          'SL',
          IFRS_STATUS,
          'SL PROCESS',
          ORIGINAL_VALUE,
          CCY,
          EXCHANGE_RATE
    FROM IFRS_ACF_SL_MSTR_REV
    WHERE EFFDATE = V_CURRDATE;
    --and START_AMORT_DATE = @currdate

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','REVERSE INS ACCT COST FEE done');


    --close account
    UPDATE IFRS_ACF_SL_MSTR
    SET CLOSING_AMOUNT = -ORIGINAL_VALUE,
        CLOSING_AMOUNT_ORG = -ORIGINAL_VALUE_ORG,
        UNAMORT_VALUE = 0,
        UNAMORT_VALUE_ORG = 0,
        AMORT_VALUE = -ORIGINAL_VALUE,
        AMORT_VALUE_ORG = -ORIGINAL_VALUE_ORG,
        IFRS_STATUS = 'CLS'
    WHERE END_AMORT_DATE <= V_CURRDATE
    AND IFRS_STATUS = 'ACT'
    AND EFFDATE = V_CURRDATE;

    COMMIT;


    --close account not in IMA
    UPDATE IFRS_ACF_SL_MSTR
    SET CLOSING_AMOUNT = -ORIGINAL_VALUE,
        CLOSING_AMOUNT_ORG = -ORIGINAL_VALUE_ORG,
        UNAMORT_VALUE = 0,
        UNAMORT_VALUE_ORG = 0,
        AMORT_VALUE = -ORIGINAL_VALUE,
        AMORT_VALUE_ORG = -ORIGINAL_VALUE_ORG,
        IFRS_STATUS = 'CLS'
    WHERE MASTERID NOT IN (SELECT MASTERID FROM IFRS_MASTER_ACCOUNT
                           WHERE DOWNLOAD_DATE = V_CURRDATE)
    AND IFRS_STATUS = 'ACT'
    AND EFFDATE = V_CURRDATE;

    COMMIT;

    --close when not active
    UPDATE IFRS_ACF_SL_MSTR
    SET CLOSING_AMOUNT = -ORIGINAL_VALUE,
        CLOSING_AMOUNT_ORG = -ORIGINAL_VALUE_ORG,
        UNAMORT_VALUE = 0,
        UNAMORT_VALUE_ORG = 0,
        AMORT_VALUE = -ORIGINAL_VALUE,
        AMORT_VALUE_ORG = -ORIGINAL_VALUE_ORG,
        IFRS_STATUS = 'CLS'
    WHERE MASTERID IN (SELECT MASTERID FROM IFRS_MASTER_ACCOUNT
                       WHERE DOWNLOAD_DATE = V_CURRDATE AND ACCOUNT_STATUS <> 'A')
    AND IFRS_STATUS = 'ACT'
    AND EFFDATE = V_CURRDATE;

    COMMIT;

    UPDATE IFRS_ACF_SL_MSTR
    SET AMORT_VALUE = ROUND((ORIGINAL_VALUE/(END_AMORT_DATE -START_AMORT_DATE+1))*(V_CURRDATE -START_AMORT_DATE+1)*-1,2),
        AMORT_VALUE_ORG = ROUND((ORIGINAL_VALUE_ORG/(END_AMORT_DATE -START_AMORT_DATE+1))*(V_CURRDATE -START_AMORT_DATE+1)*-1,2),
        UNAMORT_VALUE = ORIGINAL_VALUE - ROUND(((ORIGINAL_VALUE/(END_AMORT_DATE -START_AMORT_DATE+1))*(V_CURRDATE -START_AMORT_DATE+1)),2),
        UNAMORT_VALUE_ORG = ORIGINAL_VALUE_ORG - ROUND(((ORIGINAL_VALUE_ORG/(END_AMORT_DATE -START_AMORT_DATE+1))*(V_CURRDATE -START_AMORT_DATE+1)),2)
    WHERE IFRS_STATUS = 'ACT'
    AND EFFDATE = V_CURRDATE;

    COMMIT;

    --detect mssing acc for SL in IMA
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IMA_LS';

    INSERT INTO TMP_IMA_LS(MASTERID)
    SELECT DISTINCT MASTERID
    FROM IFRS_ACF_SL_MSTR
    WHERE EFFDATE = V_CURRDATE
    AND MASTERID NOT IN (SELECT DISTINCT MASTERID
                         FROM IFRS_MASTER_ACCOUNT
                         WHERE DOWNLOAD_DATE = V_CURRDATE
                        );
    COMMIT;
    /*
    select * into #ima from ifrs_master_account
    where master_account_id in (select distinct masterid from #ima_ls)
    and download_date = @prevdate

    update #ima
    set account_status = 'S',
        download_date = @currdate,
     outstanding = 0,
    -- OUTSTANDING_PROFIT = 0,
     OUTSTANDING_PASTDUE = 0,
     PLAFOND = 0

    delete ifrs_master_account
    where download_date = @currdate
    and account_status = 'S'

    insert ifrs_master_account
    select * from #ima
    */
    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','CLOSE ACCT done');

    /*
    update ifrs_master_account
    set unamor_origination_fee_amt_sl = 0,
        unamor_trans_cost_amt_sl = 0,
     unamor_amt_total_sl = 0,
     UNAMOR_ORG_FEE_AMT_SL_LCY = 0,
     UNAMOR_TRANS_COST_AMT_SL_LCY = 0,
     UNAMOR_AMT_TOTAL_SL_LCY = 0,
     initial_unamor_fee_sl_lcy = 0,
     initial_unamor_cost_sl_lcy = 0,
     initial_unamor_total_sl_lcy = 0,
     initial_unamor_fee_sl_org = 0,
     initial_unamor_cost_sl_org = 0,
     initial_unamor_total_sl_org = 0
    where download_Date = @currdate

    update a
    set a.unamor_origination_fee_amt_sl = b.fee,
        a.unamor_trans_cost_amt_sl = b.cost,
     a.unamor_amt_total_sl = b.fee+b.cost,
     a.UNAMOR_ORG_FEE_AMT_SL_LCY = b.fee_lcy,
     a.UNAMOR_TRANS_COST_AMT_SL_LCY = b.cost_lcy,
     a.UNAMOR_AMT_TOTAL_SL_LCY = b.fee_lcy+b.cost_lcy,
     a.initial_unamor_fee_sl_lcy = b.fee_ini,
     a.initial_unamor_cost_sl_lcy = b.cost_ini,
     a.initial_unamor_total_sl_lcy = b.fee_ini+b.cost_ini,
     a.initial_unamor_fee_sl_org = b.fee_ini_org,
     a.initial_unamor_cost_sl_org = b.cost_ini_org,
     a.initial_unamor_total_sl_org = b.fee_ini_org+b.cost_ini_org
    from ifrs_master_account a join
      (select masterid, sum(case when flag_cf = 'F' then UNAMORT_VALUE_ORG else 0 end) as fee,
          sum(case when flag_cf = 'C' then UNAMORT_VALUE_ORG else 0 end) as cost,
       sum(case when flag_cf = 'F' then unamort_value else 0 end) as fee_lcy,
          sum(case when flag_cf = 'C' then unamort_value else 0 end) as cost_lcy,
       sum(case when flag_cf = 'F' then ORIGINAL_VALUE else 0 end) as fee_ini,
          sum(case when flag_cf = 'C' then ORIGINAL_VALUE else 0 end) as cost_ini,
       sum(case when flag_cf = 'F' then ORIGINAL_VALUE_ORG else 0 end) as fee_ini_org,
          sum(case when flag_cf = 'C' then ORIGINAL_VALUE_ORG else 0 end) as cost_ini_org
       from IFRS_ACF_SL_MSTR where effdate = @currdate and ifrs_status = 'ACT'
       group by masterid) b
     on a.master_account_id = b.masterid
    where a.download_date = @currdate

      --initial
      update a
      set a.initial_unamor_fee_sl_org = fee_amt_org,
       a.initial_unamor_cost_sl_org = cost_amt_org,
       a.initial_unamor_fee_sl_lcy = fee_amt_lcy,
       a.initial_unamor_cost_sl_lcy = cost_amt_lcy,
       a.INITIAL_UNAMORT_TOTAL = fee_amt_org+cost_amt_org,
       a.initial_unamor_total_sl_org = fee_amt_lcy+cost_amt_lcy
      from ifrs_master_account a
      join (select masterid,
        sum(case when flag_cf = 'F' then amount else 0 end) as fee_amt_lcy,
        sum(case when flag_cf = 'C' then amount else 0 end) as cost_amt_lcy,
        sum(case when flag_cf = 'F' then amount_org else 0 end) as fee_amt_org,
        sum(case when flag_cf = 'C' then amount_org else 0 end) as cost_amt_org
     from IFRS_ACCT_COST_FEE
        where effdate <= @currdate and status in ('PNL','ACT') and method = 'SL'
     group by masterid ) b on a.MASTERID = b.masterid
      where a.download_Date = @currdate
      */

    UPDATE IFRS_ACF_SL_MSTR
    SET SL_AMORT_DAILY = (ORIGINAL_VALUE_ORG/(END_AMORT_DATE -START_AMORT_DATE+1))*-1
    WHERE EFFDATE = V_CURRDATE
    AND IFRS_STATUS = 'ACT'
    AND SL_AMORT_DAILY IS NULL   ;

    COMMIT;


    --SL SWITCH
    MERGE INTO IFRS_ACF_SL_MSTR A
    USING  IFRS_ACCT_SWITCH B
    ON ( A.ACCOUNT_NUMBER = B.PREV_ACCTNO
            AND A.EFFDATE = B.DOWNLOAD_DATE
            AND A.IFRS_STATUS = 'ACT'
            AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS_STATUS = 'SWC' ;
    COMMIT;


    MERGE INTO IFRS_ACF_SL_MSTR A
    USING  IFRS_ACCT_SWITCH B
    ON ( A.ACCOUNT_NUMBER = B.ACCTNO
            AND A.BRCODE = B.PREV_BRCODE
            AND A.EFFDATE = B.DOWNLOAD_DATE
            AND A.IFRS_STATUS = 'ACT'
            AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS_STATUS = 'SWC' ;
    COMMIT;

    MERGE INTO IFRS_ACF_SL_MSTR A
    USING  IFRS_ACCT_SWITCH B
    ON ( A.ACCOUNT_NUMBER = B.ACCTNO
            AND A.PRD_CODE = B.PREV_PRDCODE
            AND A.EFFDATE = B.DOWNLOAD_DATE
            AND A.IFRS_STATUS = 'ACT'
            AND A.EFFDATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS_STATUS = 'SWC' ;

    COMMIT;

    INSERT INTO IFRS_ACF_SL_MSTR
    (MASTERID
    ,TRX_CODE
    ,CCY
    ,PRD_CODE
    ,START_AMORT_DATE
    ,END_AMORT_DATE
    ,FLAG_CF
    ,ORIGINAL_VALUE
    ,ORIGINAL_VALUE_ORG
    ,AMORT_VALUE
    ,AMORT_VALUE_ORG
    ,UNAMORT_VALUE
    ,UNAMORT_VALUE_ORG
    ,CLOSING_AMOUNT
    ,CLOSING_AMOUNT_ORG
    ,IFRS_STATUS
    ,ITRCG_FLAG
    ,EXCHANGE_RATE
    ,EFFDATE
    ,ACCOUNT_NUMBER
    ,DATA_SOURCE
    ,FACNO
    ,CIFNO
    ,BRCODE
    ,DRCR
    ,SL_AMORT_DAILY
    )
    SELECT A.MASTERID
          ,TRX_CODE
          ,CCY
          ,B.PRODUCT_CODE
          ,START_AMORT_DATE
          ,END_AMORT_DATE
          ,FLAG_CF
          ,ORIGINAL_VALUE
          ,ORIGINAL_VALUE_ORG
          ,AMORT_VALUE
          ,AMORT_VALUE_ORG
          ,UNAMORT_VALUE
          ,UNAMORT_VALUE_ORG
          ,CLOSING_AMOUNT
          ,CLOSING_AMOUNT_ORG
          ,'ACT'
          ,ITRCG_FLAG
          ,A.EXCHANGE_RATE
          ,V_CURRDATE
          ,B.ACCOUNT_NUMBER
          ,A.DATA_SOURCE
          ,FACNO
          ,CIFNO
          ,B.BRANCH_CODE
          ,DRCR
          ,SL_AMORT_DAILY
    FROM IFRS_ACF_SL_MSTR A
    JOIN IFRS_MASTER_ACCOUNT B
    ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
    AND A.EFFDATE = B.DOWNLOAD_DATE
    WHERE EFFDATE = V_CURRDATE
    AND IFRS_STATUS = 'SWC';
    COMMIT;

    INSERT INTO IFRS_ACF_SL_MSTR
    (MASTERID
    ,TRX_CODE
    ,CCY
    ,PRD_CODE
    ,START_AMORT_DATE
    ,END_AMORT_DATE
    ,FLAG_CF
    ,ORIGINAL_VALUE
    ,ORIGINAL_VALUE_ORG
    ,AMORT_VALUE
    ,AMORT_VALUE_ORG
    ,UNAMORT_VALUE
    ,UNAMORT_VALUE_ORG
    ,CLOSING_AMOUNT
    ,CLOSING_AMOUNT_ORG
    ,IFRS_STATUS
    ,ITRCG_FLAG
    ,EXCHANGE_RATE
    ,EFFDATE
    ,ACCOUNT_NUMBER
    ,DATA_SOURCE
    ,FACNO
    ,CIFNO
    ,BRCODE
    ,DRCR
    ,SL_AMORT_DAILY
    )
    SELECT A.MASTERID
          ,TRX_CODE
          ,CCY
          ,B.PRODUCT_CODE
          ,START_AMORT_DATE
          ,END_AMORT_DATE
          ,FLAG_CF
          ,ORIGINAL_VALUE
          ,ORIGINAL_VALUE_ORG
          ,AMORT_VALUE
          ,AMORT_VALUE_ORG
          ,UNAMORT_VALUE
          ,UNAMORT_VALUE_ORG
          ,CLOSING_AMOUNT
          ,CLOSING_AMOUNT_ORG
          ,'ACT'
          ,ITRCG_FLAG
          ,A.EXCHANGE_RATE
          ,V_CURRDATE
          ,B.ACCOUNT_NUMBER
          ,A.DATA_SOURCE
          ,FACNO
          ,CIFNO
          ,B.BRANCH_CODE
          ,DRCR
          ,SL_AMORT_DAILY
    FROM IFRS_ACF_SL_MSTR A
    JOIN IFRS_MASTER_ACCOUNT B
    ON A.ACCOUNT_NUMBER = B.PREVIOUS_ACCOUNT_NUMBER
    AND A.EFFDATE = B.DOWNLOAD_DATE
    WHERE EFFDATE = V_CURRDATE
    AND IFRS_STATUS = 'SWC';

    COMMIT;


    --SL SWITCH
    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING IFRS_ACF_SL_MSTR B
    ON (A.MASTERID = B.MASTERID
            AND A.DOWNLOAD_DATE = B.EFFDATE
            AND A.DOWNLOAD_DATE = V_CURRDATE
            AND B.IFRS_STATUS = 'ACT'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.UNAMORT_FEE_AMT = CASE WHEN B.FLAG_CF = 'F' THEN B.UNAMORT_VALUE_ORG ELSE 0 END,
        A.UNAMORT_COST_AMT= CASE WHEN B.FLAG_CF = 'C' THEN B.UNAMORT_VALUE_ORG ELSE 0 END;commit;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_SL_MAIN_PROC','UPD IMA done');


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_REV_LS';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_CF_LS';

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_SL_MAIN_PROC','');

    COMMIT;

END;