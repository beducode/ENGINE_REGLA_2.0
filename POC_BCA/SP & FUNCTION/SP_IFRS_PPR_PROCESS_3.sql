CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_PROCESS_3
AS
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

    --TRUNCATE TMP_IFRS_PPR_ACCOUNT_NUMBER
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PPR_ACCOUNT_NUMBER';
    COMMIT;

    --GET ACCOUNT NUMBER PROCESS
    INSERT INTO TMP_IFRS_PPR_ACCOUNT_NUMBER (DOWNLOAD_DATE,ACCOUNT_NUMBER)
    SELECT
        LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
        DEAL_ID ACCOUNT_NUMBER
    FROM TEST_PREPAYMENTT --IFRS_PPR_PROCESS
    WHERE REPORT_DATE=LAST_DAY(V_CURRDATE);
    COMMIT;

    --TRUNCATE TMP_PPR_ACCOUNT_PAYM_SETTING
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PPR_ACCOUNT_PAYM_SETTING';
    COMMIT;

    --GET ACCOUNT NUMBER FOR PAYMENT SETTING
    INSERT INTO TMP_PPR_ACCOUNT_PAYM_SETTING (DOWNLOAD_DATE,
                                              DATE_START,
                                              ACCOUNT_NUMBER)
    SELECT
        A.DOWNLOAD_DATE,
        MAX(B.DATE_START) DATE_START,
        A.ACCOUNT_NUMBER
    FROM TMP_IFRS_PPR_ACCOUNT_NUMBER A ---TEST_PREPAYMENTT A---IFRS_PPR_DETAILS_3 A
        INNER JOIN IFRS_MASTER_PAYMENT_SETTING B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
           AND LAST_DAY(V_CURRDATE) BETWEEN B.DATE_START AND B.DATE_END
    WHERE COMPONENT_TYPE IN (0,2)
    GROUP BY
        A.DOWNLOAD_DATE,
        A.ACCOUNT_NUMBER;
    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PPR_MASTER_PAYMENT_SETTING';
    COMMIT;

    --GET PAYMENT SETTING
    INSERT INTO TMP_PPR_MASTER_PAYMENT_SETTING (DOWNLOAD_DATE,
                                            MASTERID,
                                            ACCOUNT_NUMBER,
                                            COMPONENT_TYPE,
                                            INCREMENTS,
                                            DATE_START,
                                            DATE_END)
    SELECT
        MAX(C.DOWNLOAD_DATE) DOWNLOAD_DATE,
        C.MASTERID,
        C.ACCOUNT_NUMBER,
        C.COMPONENT_TYPE,
        C.INCREMENTS,C.DATE_START,C.DATE_END
    FROM TMP_PPR_ACCOUNT_PAYM_SETTING A
    INNER JOIN IFRS_MASTER_PAYMENT_SETTING C ON A.ACCOUNT_NUMBER=C.ACCOUNT_NUMBER
        AND A.DOWNLOAD_DATE=C.DOWNLOAD_DATE
        AND A.DATE_START=C.DATE_START
    WHERE C.COMPONENT_TYPE IN (0,2)
    GROUP BY
        C.MASTERID,
        C.ACCOUNT_NUMBER,
        C.COMPONENT_TYPE,
        C.INCREMENTS,
        C.DATE_START,
        C.DATE_END;
    COMMIT;



     --TRUNCATE IFRS_PPR_PROCESS
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PPR_PROCESS';
    COMMIT;

    MERGE INTO IFRS_PPR_PROCESS D
    USING (
        SELECT
                LAST_DAY(V_CURRDATE)  DOWNLOAD_DATE,
                A.REPORT_DATE,
                C.MASTERID,
                A.DEAL_ID ACCOUNT_NUMBER,
                A.CURRENCY,
                A.OS OUTSTANDING,
                A.PREPAYMENT,
                SUM(A.SCHEDULED) SCHEDULE,
                A.ACTUAL,
                B.RATE_AMOUNT,
                CASE WHEN (A.OS*B.RATE_AMOUNT)=0 THEN 0
                ELSE ROUND(((A.ACTUAL*B.RATE_AMOUNT)-SUM(A.SCHEDULED*B.RATE_AMOUNT))/(A.OS*B.RATE_AMOUNT),4)
                END  SMM,
                F.PKID SEGMENTATION_ID,
                F.SEGMENT SEGMENTATION_NAME,
                E.PKID PREPAYMENT_RULE_ID,
                E.PREPAYMENT_RULE_NAME,
                A.DEAL_TYPE PRODUCT_CODE,
                C.INCREMENTS,
                C.COMPONENT_TYPE,
                'ILS' DATA_SOURCE,
                E.LATE_PAYMENT_FLAG
            FROM TEST_PREPAYMENTT A
                INNER JOIN IFRS_MASTER_EXCHANGE_RATE B ON A.REPORT_DATE = B.DOWNLOAD_DATE
                    AND A.CURRENCY=B.CURRENCY
                INNER JOIN TMP_PPR_MASTER_PAYMENT_SETTING C ON A.DEAL_ID = C.ACCOUNT_NUMBER
                    AND LAST_DAY(V_CURRDATE) BETWEEN  C.DATE_START AND C.DATE_END
               -- INNER JOIN IFRS_MASTER_ACCOUNT_MONTHLY D ON A.REPORT_DATE=D.DOWNLOAD_DATE
                 --   AND A.DEAL_ID=D.ACCOUNT_NUMBER
                 INNER JOIN TEST_FINSTO_EAD_CONVERSION D ON SUBSTR(A.SEGMENTASI,1,2)=D.POOL_ID
                INNER JOIN IFRS_PREPAYMENT_RULES_CONFIG E ON D.EAD_RULE_ID=E.PKID
                INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER F ON E.SEGMENTATION_ID=F.PKID
            WHERE
                A.ACTUAL>0
                AND A.REPORT_DATE = LAST_DAY(V_CURRDATE)
                AND E.LATE_PAYMENT_FLAG = 0
                AND A.REPORT_DATE >= E.CUT_OFF_DATE
            GROUP BY
                    A.REPORT_DATE,
                    C.MASTERID,
                    A.DEAL_ID ,
                    A.CURRENCY,
                    A.OS,
                    A.PREPAYMENT,
                    A.ACTUAL,
                    B.RATE_AMOUNT,
                    A.DEAL_TYPE,
                    F.PKID,
                    F.SEGMENT,
                    E.PKID,
                    E.PREPAYMENT_RULE_NAME,
                    E.LATE_PAYMENT_FLAG,
                    C.INCREMENTS,
                    C.COMPONENT_TYPE
    /*
            SELECT
                AA.DOWNLOAD_DATE,
                AA.REPORT_DATE,
                AA.MASTERID,
                AA.ACCOUNT_NUMBER,
                AA.CURRENCY,
                AA.OUTSTANDING,
                AA.PREPAYMENT,
                AA.SCHEDULE,
                AA.ACTUAL,
                AA.RATE_AMOUNT,
                AA.SMM,
                AA.SEGMENTATION_ID,
                C.SEGMENT SEGMENTATION_NAME,
                D.PKID PREPAYMENT_RULE_ID,
                D.PREPAYMENT_RULE_NAME,
                AA.PRODUCT_CODE,
                AA.INCREMENTS,
                AA.COMPONENT_TYPE,
                AA.DATA_SOURCE,
                D.LATE_PAYMENT_FLAG,
                D.CUT_OFF_DATE
            FROM (
            SELECT
                LAST_DAY(V_CURRDATE)  DOWNLOAD_DATE,
                A.REPORT_DATE,
                C.MASTERID,
                A.DEAL_ID ACCOUNT_NUMBER,
                A.CURRENCY,
                A.OS OUTSTANDING,
                A.PREPAYMENT,
                SUM(A.SCHEDULED) SCHEDULE,
                A.ACTUAL,
                B.RATE_AMOUNT,
                CASE WHEN (A.OS*B.RATE_AMOUNT)=0 THEN 0
                ELSE ROUND(((A.ACTUAL*B.RATE_AMOUNT)-SUM(A.SCHEDULED*B.RATE_AMOUNT))/(A.OS*B.RATE_AMOUNT),4)
                END  SMM,
                --SUBSTR(A.SEGMENTASI,1,2) SEGMENTATION_ID,
                '252' AS SEGMENTATION_ID,
                A.DEAL_TYPE PRODUCT_CODE,
                C.INCREMENTS,
                C.COMPONENT_TYPE,
                'ILS' DATA_SOURCE
            FROM TEST_PREPAYMENTT A
                INNER JOIN IFRS_PPR_MASTER_EXCHANGE_RATE B ON A.REPORT_DATE = B.DOWNLOAD_DATE
                    AND A.CURRENCY=B.CURRENCY
                INNER JOIN TMP_PPR_MASTER_PAYMENT_SETTING C ON A.DEAL_ID = C.ACCOUNT_NUMBER
                    AND LAST_DAY(V_CURRDATE) BETWEEN  C.DATE_START AND C.DATE_END
            WHERE
                A.ACTUAL>0
                AND A.REPORT_DATE = LAST_DAY(V_CURRDATE)
            GROUP BY
                    A.REPORT_DATE,
                    C.MASTERID,
                    A.DEAL_ID ,
                    A.CURRENCY,
                    A.OS,
                    A.PREPAYMENT,
                    A.ACTUAL,
                    B.RATE_AMOUNT,
                    A.DEAL_TYPE,
                    A.SEGMENTASI,
                    C.INCREMENTS,
                    C.COMPONENT_TYPE
            )AA
                INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER C ON AA.SEGMENTATION_ID=C.PKID
                INNER JOIN IFRS_PREPAYMENT_RULES_CONFIG D ON C.PKID=D.SEGMENTATION_ID
            WHERE D.LATE_PAYMENT_FLAG = 0
            AND AA.REPORT_DATE>=D.CUT_OFF_DATE
            */
           ) S ON (D.REPORT_DATE=S.REPORT_DATE
                   AND D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER)
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            REPORT_DATE,
            MASTERID,
            ACCOUNT_NUMBER,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            PREPAYMENT_RULE_ID,
            PREPAYMENT_RULE_NAME,
            PRODUCT_CODE,
            INCREMENTS,
            COMPONENT_TYPE,
            DATA_SOURCE,
            LATE_PAYMENT_FLAG--,
           -- CUT_OFF_DATE
            )
    VALUES (S.DOWNLOAD_DATE,
            S.REPORT_DATE,
            S.MASTERID,
            S.ACCOUNT_NUMBER,
            S.CURRENCY,
            S.OUTSTANDING,
            S.PREPAYMENT,
            S.SCHEDULE,
            S.ACTUAL,
            S.RATE_AMOUNT,
            S.SMM,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.PREPAYMENT_RULE_ID,
            S.PREPAYMENT_RULE_NAME,
            S.PRODUCT_CODE,
            S.INCREMENTS,
            S.COMPONENT_TYPE,
            S.DATA_SOURCE,
            S.LATE_PAYMENT_FLAG--,
            --S.CUT_OFF_DATE
            );
    COMMIT;

     --UPDATE REVOLVING AND PREPAYMENT SEGMENT
    MERGE INTO IFRS_PPR_PROCESS D
    USING (SELECT
                DOWNLOAD_DATE,
                ACCOUNT_NUMBER,
                CUSTOMER_NUMBER,
                CUSTOMER_NAME,
                ACCOUNT_STATUS,
                BI_COLLECTABILITY,
                REVOLVING_FLAG,
                PREPAYMENT_SEGMENT
           FROM IFRS_MASTER_ACCOUNT_MONTHLY
           WHERE DOWNLOAD_DATE = LAST_DAY(V_CURRDATE)
           ) S ON (D.REPORT_DATE = S.DOWNLOAD_DATE
                   AND D.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        CUSTOMER_NUMBER     =   S.CUSTOMER_NUMBER,
        CUSTOMER_NAME       =   S.CUSTOMER_NAME,
        ACCOUNT_STATUS      =   S.ACCOUNT_STATUS,
        BI_COLLECTABILITY   =   S.BI_COLLECTABILITY,
        REVOLVING_FLAG      =   S.REVOLVING_FLAG,
        PREPAYMENT_SEGMENT  =   S.PREPAYMENT_SEGMENT;
    COMMIT;

    --DELETE REVOLFING FLAG<>0
    DELETE FROM IFRS_PPR_PROCESS
    WHERE REVOLVING_FLAG!=0 OR BI_COLLECTABILITY IN ('3','4','5','C');
    COMMIT;

END;