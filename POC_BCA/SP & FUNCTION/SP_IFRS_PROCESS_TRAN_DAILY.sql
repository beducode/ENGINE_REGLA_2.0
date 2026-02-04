CREATE OR REPLACE PROCEDURE SP_IFRS_PROCESS_TRAN_DAILY
AS
  V_CURRDATE DATE ;
  V_PREVDATE DATE;
BEGIN

    SELECT  MAX(CURRDATE) ,MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT  ;


    INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE, DTM,OPS ,PROCNAME ,REMARK)
    VALUES(  V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'') ;
    COMMIT;


    --DELETE FIRST
    DELETE  FROM IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE >= V_CURRDATE;
    COMMIT;


    -- FEE AND COST
    INSERT  INTO IFRS_ACCT_COST_FEE
    ( DOWNLOAD_DATE ,
      MASTERID ,
      BRCODE ,
      CIFNO ,
      FACNO ,
      ACCTNO ,
      DATASOURCE ,
      PRD_TYPE ,
      PRD_CODE ,
      TRX_CODE ,
      CCY ,
      FLAG_CF ,
      FLAG_REVERSE ,
      METHOD ,
      STATUS ,
      SRCPROCESS ,
      AMOUNT ,
      CREATEDDATE ,
      CREATEDBY,
      TRX_REFF_NUMBER,
      SOURCE_TABLE,
      TRX_LEVEL
    )
    SELECT  A.DOWNLOAD_DATE EFFDATE ,
            A.MASTERID,
            A.BRANCH_CODE BRCODE ,
            NULL CIFNO ,
            A.FACILITY_NUMBER FACNO ,
            A.ACCOUNT_NUMBER ACCTNO ,
            A.DATA_SOURCE DATASOURCE ,
            A.PRD_TYPE ,
            A.PRD_CODE ,
            A.TRX_CODE ,
            A.CCY CCY ,
            SUBSTR(COALESCE(B.IFRS_TXN_CLASS, 'F'), 1, 1) FLAG_CF ,
            SUBSTR(COALESCE(A.DEBET_CREDIT_FLAG, 'X'), 1, 1) FLAG_REVERSE ,
            'X' METHOD ,
            'ACT' STATUS ,
            'TRAN_DAILY' SRCPROCESS ,
            A.ORG_CCY_AMT AS AMOUNT ,
            SYSTIMESTAMP CREATEDDATE ,
            'SP_IFRS_TRAN_DAILY' CREATEDBY,
            TRX_REFERENCE_NUMBER  ,
            'TRANS_DAILY'SOURCE_TABLE,
            TRX_LEVEL
    FROM    IFRS_TRANSACTION_DAILY A
    JOIN ( SELECT DISTINCT  DATA_SOURCE ,
                            PRD_TYPE ,
                            PRD_CODE ,
                            TRX_CODE ,
                            CCY ,
                            IFRS_TXN_CLASS
            FROM     IFRS_TRANSACTION_PARAM
            WHERE    IFRS_TXN_CLASS IN ( 'FEE', 'COST' )
            AND AMORTIZATION_FLAG = '1' --flag Y
          ) B
      ON (B.DATA_SOURCE  = A.DATA_SOURCE OR NVL(B.DATA_SOURCE,'ALL') = 'ALL')
      AND (B.PRD_TYPE = A.PRD_TYPE  OR NVL(B.PRD_TYPE,'ALL') = 'ALL')
      AND (B.PRD_CODE = A.PRD_CODE  OR NVL(LTRIM(B.PRD_CODE),'ALL') = 'ALL')
      AND B.TRX_CODE  = A.TRX_CODE
      AND (B.CCY = A.CCY OR B.CCY = 'ALL')
      LEFT JOIN TMP_EXCLUDE_PP C ON A.DOWNLOAD_DATE=C.DOWNLOAD_DATE AND A.MASTERID=C.MASTERID AND A.TRX_CODE='PP' --additional for case 31 dec 2019 reverse fee pp
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    and c.masterid is null;--additional for case 31 dec 2019 reverse fee pp
    --AND A.ACCOUNT_NUMBER <> A.FACILITY_NUMBER
    COMMIT;


     -- start for case 31 dec 2019 reversal fee pp
    INSERT  INTO IFRS_ACCT_COST_FEE
    ( DOWNLOAD_DATE ,
      MASTERID ,
      BRCODE ,
      CIFNO ,
      FACNO ,
      ACCTNO ,
      DATASOURCE ,
      PRD_TYPE ,
      PRD_CODE ,
      TRX_CODE ,
      CCY ,
      FLAG_CF ,
      FLAG_REVERSE ,
      METHOD ,
      STATUS ,
      SRCPROCESS ,
      AMOUNT ,
      CREATEDDATE ,
      CREATEDBY,
      TRX_REFF_NUMBER,
      SOURCE_TABLE,
      TRX_LEVEL
    )
    SELECT  A.DOWNLOAD_DATE EFFDATE ,
            A.MASTERID,
            A.BRANCH_CODE BRCODE ,
            NULL CIFNO ,
            A.FACILITY_NUMBER FACNO ,
            A.ACCOUNT_NUMBER ACCTNO ,
            A.DATA_SOURCE DATASOURCE ,
            A.PRD_TYPE ,
            A.PRD_CODE ,
            A.TRX_CODE ,
            A.CCY CCY ,
            SUBSTR(COALESCE(B.IFRS_TXN_CLASS, 'F'), 1, 1) FLAG_CF ,
            SUBSTR(COALESCE(A.DEBET_CREDIT_FLAG, 'X'), 1, 1) FLAG_REVERSE ,
            'X' METHOD ,
            'ACT' STATUS ,
            'TRAN_DAILY' SRCPROCESS ,
            A.ORG_CCY_AMT AS AMOUNT ,
            SYSTIMESTAMP CREATEDDATE ,
            'SP_IFRS_TRAN_DAILY' CREATEDBY,
            TRX_REFERENCE_NUMBER  ,
            'TRANS_DAILY'SOURCE_TABLE,
            TRX_LEVEL
    FROM    TMP_IFRS_TRANSACTION_PP A
    JOIN ( SELECT DISTINCT  DATA_SOURCE ,
                            PRD_TYPE ,
                            PRD_CODE ,
                            TRX_CODE ,
                            CCY ,
                            IFRS_TXN_CLASS
            FROM     IFRS_TRANSACTION_PARAM
            WHERE    IFRS_TXN_CLASS IN ( 'FEE', 'COST' )
            AND AMORTIZATION_FLAG = '1' --flag Y
          ) B
      ON (B.DATA_SOURCE  = A.DATA_SOURCE OR NVL(B.DATA_SOURCE,'ALL') = 'ALL')
      AND (B.PRD_TYPE = A.PRD_TYPE  OR NVL(B.PRD_TYPE,'ALL') = 'ALL')
      AND (B.PRD_CODE = A.PRD_CODE  OR NVL(LTRIM(B.PRD_CODE),'ALL') = 'ALL')
      AND B.TRX_CODE  = A.TRX_CODE
      AND (B.CCY = A.CCY OR B.CCY = 'ALL')
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;
  -- end for case 31 dec 2019 reversal fee pp


  /*20171129 INSERT FROM COST FEE UNPROCESSED FROM PREVDATE*/
    INSERT  INTO IFRS_ACCT_COST_FEE
    ( DOWNLOAD_DATE ,
      MASTERID ,
      BRCODE ,
      CIFNO ,
      FACNO ,
      ACCTNO ,
      DATASOURCE ,
      PRD_TYPE ,
      PRD_CODE ,
      TRX_CODE ,
      CCY ,
      FLAG_CF ,
      FLAG_REVERSE ,
      METHOD ,
      STATUS ,
      SRCPROCESS ,
      AMOUNT ,
      CREATEDDATE ,
      CREATEDBY,
      TRX_REFF_NUMBER ,
      SOURCE_TABLE,
      TRX_LEVEL
    )
    SELECT  V_CURRDATE ,
            MASTERID ,
            BRCODE ,
            CIFNO ,
            FACNO ,
            ACCTNO ,
            DATASOURCE ,
            PRD_TYPE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            FLAG_CF ,
            CASE WHEN FLAG_AL = 'A'
                      THEN --ASSETS
                        CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'N'  THEN 'C' ELSE 'D' END
                             ELSE CASE WHEN FLAG_REVERSE = 'N' THEN 'D' ELSE 'C' END
                             END
                 ELSE --LIAB
                      CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'N' THEN 'C' ELSE 'Y' END
                           ELSE CASE WHEN FLAG_REVERSE = 'N' THEN 'D' ELSE 'C' END
                           END
                 END AS FLAG_REVERSE ,
            METHOD ,
            'ACT' ,
            SRCPROCESS ,
            AMOUNT ,
            CREATEDDATE ,
            CREATEDBY,
            TRX_REFF_NUMBER  ,
            SOURCE_TABLE,
            TRX_LEVEL
    FROM IFRS_ACCT_COST_FEE
    WHERE DOWNLOAD_DATE = V_PREVDATE
    AND STATUS = 'NPRCD';
    COMMIT;

/*
-- COST dijadikan satu script
        INSERT  INTO IFRS_ACCT_COST_FEE
                ( DOWNLOAD_DATE ,
                  MASTERID ,
                  BRCODE ,
                  CIFNO ,
                  FACNO ,
                  ACCTNO ,
                  DATASOURCE ,
                  PRD_TYPE ,
                  PRD_CODE ,
                  TRX_CODE ,
                  CCY ,
                  FLAG_CF ,
                  FLAG_REVERSE ,
                  METHOD ,
                  STATUS ,
                  SRCPROCESS ,
                  AMOUNT ,
                  CREATEDDATE ,
                  CREATEDBY



                )
                SELECT  A.DOWNLOAD_DATE EFFDATE ,
                        A.MASTER_ACCOUNT_ID MASTERID ,
                        A.BRANCH_CODE BRCODE ,
                        NULL CIFNO ,
                        A.FACILITY_NUMBER FACNO ,
                        A.ACCOUNT_NUMBER ACCTNO ,
                        A.DATA_SOURCE DATASOURCE ,
                        A.PRD_TYPE ,
                        A.PRD_CODE ,
                        A.TRX_CODE ,
                        A.CCY CCY ,
                        'C' FLAG_CF ,
                        SUBSTRING(COALESCE(A.DEBET_CREDIT_FLAG, 'X'), 1, 1) FLAG_REVERSE ,
                        'X' METHOD ,
                        'PARAM' STATUS ,
                        'TRAN_DAILY' SRCPROCESS ,
                        A.ORG_CCY_AMT AS AMOUNT ,
                        CURRENT_TIMESTAMP CREATEDDATE ,
                        'SP_PSAK_TRAN_DAILY' CREATEDBY
                FROM    TRANSACTION_DAILY A
                        JOIN ( SELECT DISTINCT
                                        DATA_SOURCE ,
                                        PRD_TYPE ,
                                        PRD_CODE ,
                                        TRX_CODE ,
                                        CCY
                               FROM     IFRS_MASTER_TRANSACTION_PARAM
                               WHERE    IFRS_TXN_CLASS = 'COST'
                                        AND AMORTIZATION_FLAG = 'Y'
                             ) B ON B.DATA_SOURCE = A.DATA_SOURCE
                                    AND B.PRD_TYPE = A.PRD_TYPE
                                    AND B.PRD_CODE = A.PRD_CODE
                                    AND B.TRX_CODE = A.TRX_CODE
                                    AND B.CCY = A.CCY
                WHERE   A.DOWNLOAD_DATE = @V_CURRDATE

*/



    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'INSERTED') ;
    COMMIT;


-- UPDATE INFO FROM IMA_CURR
/* FD 30042018: update set data source disini juga, where nya hanya by masterid saja */

    MERGE INTO IFRS_ACCT_COST_FEE A
    USING IFRS_IMA_AMORT_CURR B
    ON (B.MASTERID = A.MASTERID
        --AND B.DATA_SOURCE = DBO.IFRS_ACCT_COST_FEE.DATASOURCE
        AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.CIFNO=B.CUSTOMER_NUMBER
      , A.PRD_CODE=B.PRODUCT_CODE
      , A.PRD_TYPE=B.PRODUCT_TYPE
      , A.DATASOURCE=B.DATA_SOURCE
      , A.BRCODE=B.BRANCH_CODE
      , A.FACNO=B.FACILITY_NUMBER;
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE , DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'UPD FROM IMA');

    /*
    -- UPDATE STATUS FROM TRAN PARAM

        UPDATE  DBO.IFRS_ACCT_COST_FEE
        SET     FLAG_CF = SUBSTRING(COALESCE(B.IFRS_TXN_CLASS, 'F'), 1, 1) ,
                STATUS = 'ACT'
        FROM    IFRS_MASTER_TRANSACTION_PARAM B
        WHERE   B.DATA_SOURCE = DBO.IFRS_ACCT_COST_FEE.DATASOURCE
                AND B.PRD_TYPE = DBO.IFRS_ACCT_COST_FEE.PRD_TYPE
                AND B.PRD_CODE = DBO.IFRS_ACCT_COST_FEE.PRD_CODE
                AND B.TRX_CODE = DBO.IFRS_ACCT_COST_FEE.TRX_CODE
                AND B.CCY = DBO.IFRS_ACCT_COST_FEE.CCY
                AND DBO.IFRS_ACCT_COST_FEE.DOWNLOAD_DATE = @V_CURRDATE
                AND DBO.IFRS_ACCT_COST_FEE.SRCPROCESS = 'TRAN_DAILY'
                AND B.AMORTIZATION_FLAG = 'Y'
    */

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'UPD FROM TRAN PARAM');
    COMMIT;


-- UPDATE FLAG_AL
    MERGE INTO IFRS_ACCT_COST_FEE A
    USING IFRS_PRODUCT_PARAM B
    ON ((B.DATA_SOURCE = A.DATASOURCE OR NVL(B.DATA_SOURCE,'ALL') = 'ALL')
              AND (B.PRD_TYPE = A.PRD_TYPE  OR NVL(B.PRD_TYPE,'ALL') = 'ALL')
              AND (B.PRD_CODE = A.PRD_CODE  OR NVL(B.PRD_CODE,'ALL') = 'ALL')
              AND (B.CCY = A.CCY OR NVL(B.CCY,'ALL') = 'ALL')
              AND A.SRCPROCESS = 'TRAN_DAILY'
              AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FLAG_AL=NVL(B.FLAG_AL, 'A');
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE , DTM , OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'UPD FROM PROD PARAM') ;


--UPDATE AMOUNT AND REV FLAG
    UPDATE  IFRS_ACCT_COST_FEE
    SET AMOUNT = CASE WHEN FLAG_AL = 'A' THEN CASE WHEN FLAG_CF = 'F' THEN -1 * AMOUNT
                                                   ELSE AMOUNT
                                                   END
                      ELSE CASE WHEN FLAG_CF = 'C' THEN -1 * AMOUNT
                                ELSE AMOUNT
                                END
                      END ,
        FLAG_REVERSE = CASE WHEN FLAG_AL = 'A'  THEN --ASSETS
                                                 CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'C' THEN 'N'
                                                                                   ELSE 'Y'
                                                                                   END
                                                      ELSE CASE WHEN FLAG_REVERSE = 'D' THEN 'N'
                                                                ELSE 'Y'
                                                                END
                                                      END
                            ELSE --LIAB
                              CASE WHEN FLAG_CF = 'F'  THEN CASE WHEN FLAG_REVERSE = 'C' THEN 'N'
                                                                 ELSE 'Y'
                                                                 END
                                   ELSE CASE WHEN FLAG_REVERSE = 'D' THEN 'N'
                                             ELSE 'Y'
                                             END
                                   END
                        END
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND SRCPROCESS = 'TRAN_DAILY';
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'UPD AMT REV' ) ;


    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_PROCESS_TRAN_DAILY' ,'');
    COMMIT;

END;