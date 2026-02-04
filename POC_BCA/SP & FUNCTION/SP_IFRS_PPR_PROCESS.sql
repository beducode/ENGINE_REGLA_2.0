CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_PROCESS
AS
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

     --TRUNCATE IFRS_PPR_PROCESS
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PPR_PROCESS';
    COMMIT;

    --PREPAYMENT PROCESS
    MERGE INTO IFRS_PPR_PROCESS D
     USING (
             SELECT
                AA.DOWNLOAD_DATE,
                AA.REPORT_DATE,
                AA.ACCOUNT_NUMBER,
                AA.CURRENCY,
                AA.OUTSTANDING,
                AA.PREPAYMENT,
                AA.SCHEDULE,
                AA.ACTUAL,
                AA.RATE_AMOUNT,
                AA.SMM,
                AA.PREPAYMENT_SEGMENT,
                AA.SEGMENTATION_ID,
                C.SEGMENT SEGMENTATION_NAME,
                D.PKID PREPAYMENT_RULE_ID,
                D.PREPAYMENT_RULE_NAME,
                AA.REVOLVING_FLAG,
                AA.PRODUCT_CODE,
                AA.DATA_SOURCE,
                D.LATE_PAYMENT_FLAG
            FROM (
                SELECT
                    LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                    A.REPORT_DATE,
                    A.DEAL_ID ACCOUNT_NUMBER,
                    A.CCY CURRENCY,
                    A.OUTSTANDING,
                    A.PREPAYMENT,
                    SUM(A.SCHEDULE) SCHEDULE,
                    A.ACTUAL,
                    B.RATE_AMOUNT,
                    CASE WHEN (A.OUTSTANDING*B.RATE_AMOUNT)=0 THEN 0
                    ELSE ROUND(((A.ACTUAL*B.RATE_AMOUNT)-SUM(A.SCHEDULE*B.RATE_AMOUNT))/(A.OUTSTANDING*B.RATE_AMOUNT),4)
                    END  SMM,
                    A.RESERVED_VARCHAR_2 PREPAYMENT_SEGMENT,
                   '249' AS SEGMENTATION_ID,
                    A.REVOLVING_FLAG,
                    A.PRODUCT_CODE,
                    'ILS' DATA_SOURCE
                FROM TEST_PPRV2 A
                    INNER JOIN IFRS_PPR_MASTER_EXCHANGE_RATE B ON A.REPORT_DATE=B.DOWNLOAD_DATE
                        AND A.CCY=B.CURRENCY
                WHERE A.REVOLVING_FLAG IN (0)
                    AND A.ACTUAL>0
                    AND A.RESERVED_VARCHAR_2 IS NOT NULL
                    AND A.REPORT_DATE = LAST_DAY(V_CURRDATE)
                GROUP BY
                        A.REPORT_DATE,
                        A.DEAL_ID ,
                        A.CCY,
                        A.OUTSTANDING,
                        A.PREPAYMENT,
                        A.ACTUAL,
                        B.RATE_AMOUNT,
                        A.RESERVED_VARCHAR_2,
                        A.REVOLVING_FLAG,
                        A.PRODUCT_CODE
                )AA
                INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER C ON AA.SEGMENTATION_ID=C.PKID
                INNER JOIN IFRS_PREPAYMENT_RULES_CONFIG D ON C.PKID=D.SEGMENTATION_ID
            ) S ON (D.REPORT_DATE=S.REPORT_DATE
                    AND D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER)
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            REPORT_DATE,
            ACCOUNT_NUMBER,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            PREPAYMENT_SEGMENT,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            PREPAYMENT_RULE_ID,
            PREPAYMENT_RULE_NAME,
            REVOLVING_FLAG,
            PRODUCT_CODE,
            DATA_SOURCE,
            LATE_PAYMENT_FLAG)
    VALUES (S.DOWNLOAD_DATE,
            S.REPORT_DATE,
            S.ACCOUNT_NUMBER,
            S.CURRENCY,
            S.OUTSTANDING,
            S.PREPAYMENT,
            S.SCHEDULE,
            S.ACTUAL,
            S.RATE_AMOUNT,
            S.SMM,
            S.PREPAYMENT_SEGMENT,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.PREPAYMENT_RULE_ID,
            S.PREPAYMENT_RULE_NAME,
            S.REVOLVING_FLAG,
            S.PRODUCT_CODE,
            S.DATA_SOURCE,
            S.LATE_PAYMENT_FLAG
            );
    COMMIT;

END;