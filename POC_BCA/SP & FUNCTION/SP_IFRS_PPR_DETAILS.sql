CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_DETAILS
 AS
    V_CURRDATE DATE;
BEGIN

     SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

     --GET  PREPAYMENT DETAILS
     MERGE INTO IFRS_PPR_DETAILS_2 D
     USING (
             SELECT
                A.REPORT_DATE,
                A.ACCOUNT_NUMBER,
                A.CURRENCY,
                A.OUTSTANDING,
                A.PREPAYMENT,
                A.SCHEDULE,
                A.ACTUAL,
                A.RATE_AMOUNT,
                A.SMM,
                A.PREPAYMENT_SEGMENT,
                A.SEGMENTATION_ID,
                A.REVOLVING_FLAG,
                A.PRODUCT_CODE,
                A.DATA_SOURCE
            FROM IFRS_PPR_PROCESS A
            WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
        ) S ON (D.REPORT_DATE=S.REPORT_DATE
                AND D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER)
    WHEN NOT MATCHED THEN
    INSERT (REPORT_DATE,
            ACCOUNT_NUMBER,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            PREPAYMENT_SEGMENT,
          --  SEGMENTATION_ID,
            --REVOLVING_FLAG,
            --PRODUCT_CODE,
            DATA_SOURCE)
    VALUES (S.REPORT_DATE,
            S.ACCOUNT_NUMBER,
            S.CURRENCY,
            S.OUTSTANDING,
            S.PREPAYMENT,
            S.SCHEDULE,
            S.ACTUAL,
            S.RATE_AMOUNT,
            S.SMM,
            S.PREPAYMENT_SEGMENT,
           -- S.SEGMENTATION_ID,
            --S.REVOLVING_FLAG,
            --S.PRODUCT_CODE,
            S.DATA_SOURCE);
    COMMIT;

END;