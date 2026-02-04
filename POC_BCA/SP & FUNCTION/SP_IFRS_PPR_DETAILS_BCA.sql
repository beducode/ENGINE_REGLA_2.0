CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_DETAILS_BCA
AS
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

    MERGE INTO IFRS_PPR_DETAILS D
    USING (
            SELECT
                AA.REPORT_DATE,
               -- AA.MASTERID,
                AA.ACCOUNT_NUMBER,
                AA.CCY,
                AA.OUTSTANDING,
                AA.PREPAYMENT,
                AA.SCHEDULE,
                AA.ACTUAL,
                AA.OUTSTANDING*B.RATE_AMOUNT IDR_OUTSTANDING,
                AA.PREPAYMENT*B.RATE_AMOUNT IDR_PREPAYMENT,
                SUM(AA.SCHEDULE*B.RATE_AMOUNT) IDR_SCHEDULE,
                AA.ACTUAL*B.RATE_AMOUNT IDR_ACTUAL,
                AA.PREPAYMENT_SEGMENT,
                AA.REVOLVING_FLAG,
                AA.PRODUCT_CODE,
                CASE WHEN (AA.OUTSTANDING*B.RATE_AMOUNT)=0 THEN 0
                ELSE ROUND(((AA.ACTUAL*B.RATE_AMOUNT)-SUM(AA.SCHEDULE*B.RATE_AMOUNT))/(AA.OUTSTANDING*B.RATE_AMOUNT),4)
                END  SMM,
                AA.DATA_SOURCE
            FROM (
                SELECT
                    A.REPORT_DATE,
                   -- '-' MASTERID,
                    A.DEAL_ID ACCOUNT_NUMBER,
                    A.CCY,
                    A.OUTSTANDING,
                    A.PREPAYMENT,
                    SUM(A.SCHEDULE) SCHEDULE,
                    A.ACTUAL,
                    A.RESERVED_VARCHAR_2 PREPAYMENT_SEGMENT,
                    A.REVOLVING_FLAG,
                    A.PRODUCT_CODE,
                    'ILS' DATA_SOURCE
                FROM TEST_PPRV2 A
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
                    A.RESERVED_VARCHAR_2,
                    A.REVOLVING_FLAG,
                    A.PRODUCT_CODE
        )AA
            INNER JOIN IFRS_PPR_MASTER_EXCHANGE_RATE B ON AA.REPORT_DATE=B.DOWNLOAD_DATE
                   AND AA.CCY=B.CURRENCY
        GROUP BY
            AA.REPORT_DATE,
           -- AA.MASTERID,
            AA.ACCOUNT_NUMBER,
            AA.CCY,
            AA.OUTSTANDING,
            AA.PREPAYMENT,
            AA.SCHEDULE,
            AA.ACTUAL,
            B.RATE_AMOUNT ,
            AA.PREPAYMENT_SEGMENT,
            AA.REVOLVING_FLAG,
            AA.PRODUCT_CODE,
            AA.DATA_SOURCE
    /*
            SELECT A.REPORT_DATE,
                A.DEAL_ID ACCOUNT_NUMBER,
                A.CCY,
                A.OUTSTANDING,
                A.PREPAYMENT,
                A.SCHEDULE,
                A.ACTUAL,
                A.OUTSTANDING*B.RATE_AMOUNT IDR_OUTSTANDING,
                A.PREPAYMENT*B.RATE_AMOUNT IDR_PREPAYMENT,
                SUM(A.SCHEDULE*B.RATE_AMOUNT) IDR_SCHEDULE,
                A.ACTUAL*B.RATE_AMOUNT IDR_ACTUAL,
                A.RESERVED_VARCHAR_2 PREPAYMENT_SEGMENT,
                A.REVOLVING_FLAG,
                A.PRODUCT_CODE,
                CASE WHEN (A.OUTSTANDING*B.RATE_AMOUNT)!=0 THEN (A.PREPAYMENT*B.RATE_AMOUNT)/(A.OUTSTANDING*B.RATE_AMOUNT)
                ELSE 0
                END SMM
            FROM TEST_PPRV2 A
                INNER JOIN IFRS_PPR_MASTER_EXCHANGE_RATE B ON A.REPORT_DATE=B.DOWNLOAD_DATE
                    AND A.CCY=B.CURRENCY
            WHERE A.REVOLVING_FLAG IN (0)
               -- AND A.PREPAYMENT*B.RATE_AMOUNT>=0
                AND A.ACTUAL*B.RATE_AMOUNT>0
                AND REPORT_DATE = LAST_DAY(V_CURRDATE)
            GROUP BY A.REPORT_DATE,
                A.DEAL_ID,
                A.CCY,
                A.OUTSTANDING,
                B.RATE_AMOUNT,
                A.PREPAYMENT,
                A.SCHEDULE,
                A.ACTUAL,
                A.RESERVED_VARCHAR_2,
                A.REVOLVING_FLAG,
                A.PRODUCT_CODE
        */
           ) S ON (D.REPORT_DATE=S.REPORT_DATE
                   AND D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER)
    WHEN NOT MATCHED THEN
    INSERT (REPORT_DATE,
            --MASTERID,
            ACCOUNT_NUMBER,
            CURRENCY,
            PREPAYMENT_SEGMENT,
            OUTSTANDING,
            SCHEDULE,
            ACTUAL,
            IDR_OUTSTANDING,
            IDR_PREPAYMENT,
            IDR_SCHEDULE,
            IDR_ACTUAL,
            SMM,
            DATA_SOURCE)
    VALUES (S.REPORT_DATE,
           -- S.MASTERID,
            S.ACCOUNT_NUMBER,
            S.CCY,
            S.PREPAYMENT_SEGMENT,
            S.OUTSTANDING,
            S.SCHEDULE,
            S.ACTUAL,
            S.IDR_OUTSTANDING,
            S.IDR_PREPAYMENT,
            S.IDR_SCHEDULE,
            S.IDR_ACTUAL,
            S.SMM,
            S.DATA_SOURCE);
    COMMIT;
END;