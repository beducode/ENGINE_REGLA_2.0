CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_HEADER_BCA
AS
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

    --TRUNCATE TMP_PPR_ACCOUNT_PAYM_SETTING
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PPR_ACCOUNT_PAYM_SETTING';
    COMMIT;

    --GET ACCOUNT NUMBER FOR PAYMENT SETTING
    INSERT INTO TMP_PPR_ACCOUNT_PAYM_SETTING (DOWNLOAD_DATE,
                                              DATE_START,
                                              ACCOUNT_NUMBER)
    SELECT
        MAX(DOWNLOAD_DATE) DOWNLOAD_DATE,
        MAX(B.DATE_START) DATE_START,
        A.ACCOUNT_NUMBER
    FROM IFRS_PPR_DETAILS A
        INNER JOIN IFRS_MASTER_PAYMENT_SETTING B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
           AND LAST_DAY(V_CURRDATE) BETWEEN B.DATE_START AND B.DATE_END
    WHERE COMPONENT_TYPE=0
        AND A.ACCOUNT_NUMBER IN (SELECT DEAL_ID FROM TEST_PPRV2
                                 WHERE REPORT_DATE=LAST_DAY(V_CURRDATE))
    GROUP BY
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
    WHERE C.COMPONENT_TYPE=0
    GROUP BY
        C.MASTERID,
        C.ACCOUNT_NUMBER,
        C.COMPONENT_TYPE,
        C.INCREMENTS,
        C.DATE_START,
        C.DATE_END;
    COMMIT;

    --UPDATE/INSERT PPR HEADER
    MERGE INTO IFRS_PPR_HEADER D
    USING (
            SELECT
                XX.DOWNLOAD_DATE,
                XX.ACCOUNT_NUMBER,
                --XX.MASTERID,
                XX.CUSTOMER_NUMBER,
                XX.CUSTOMER_NAME,
                XX.SEGMENTATION_ID,
                XX.SEGMENTATION_NAME,
                XX.PREPAYMENT_RULE_ID,
                XX.PREPAYMENT_RULE_NAME,
                XX.PRODUCT_CODE,
                XX.PREPAYMENT_SEGMENT,
                XX.AVERAGE_SMM,
                ROUND(1-POWER((1-XX.AVERAGE_SMM),B.INCREMENTS),4) PREPAYMENT_RATE
            FROM (
                    SELECT AA.DOWNLOAD_DATE,
                        AA.ACCOUNT_NUMBER,
                        AA.MASTERID,
                        AA.CUSTOMER_NUMBER,
                        AA.CUSTOMER_NAME,
                        AA.SEGMENTATION_ID,
                        C.PREPAYMENT_RULE_NAME SEGMENTATION_NAME,
                        C.PKID PREPAYMENT_RULE_ID,
                        C.PREPAYMENT_RULE_NAME,
                        AA.PRODUCT_CODE,
                        AA.PREPAYMENT_SEGMENT,
                        AA.AVERAGE_SMM
                    FROM(
                    SELECT LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                        A.ACCOUNT_NUMBER,
                         '-' AS MASTERID,
                        '-' AS CUSTOMER_NUMBER,
                        '-' AS CUSTOMER_NAME,
                        '249' AS SEGMENTATION_ID,
                        '' AS SEGMENTATION_NAME,
                        '' AS PREPAYMENT_RULE_ID,
                        B.PRODUCT_CODE,
                        A.PREPAYMENT_SEGMENT,
                        ROUND(AVG(A.SMM),4) AVERAGE_SMM
                    FROM IFRS_PPR_DETAILS A
                    INNER JOIN TEST_PPRV2 B ON A.ACCOUNT_NUMBER=B.DEAL_ID AND A.REPORT_DATE=B.REPORT_DATE
                    WHERE A.ACCOUNT_NUMBER IN (SELECT DEAL_ID FROM TEST_PPRV2
                                               WHERE REPORT_DATE=LAST_DAY(V_CURRDATE) AND RESERVED_VARCHAR_2 IS NOT NULL)
                    GROUP BY
                        LAST_DAY(V_CURRDATE),
                        A.ACCOUNT_NUMBER,
                        A.PREPAYMENT_SEGMENT,
                        B.PRODUCT_CODE
                    )AA
                    INNER JOIN IFRS_PREPAYMENT_RULES_CONFIG C ON AA.SEGMENTATION_ID=C.SEGMENTATION_ID
                    WHERE LATE_PAYMENT_FLAG=0
                /*
                SELECT LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                    A.ACCOUNT_NUMBER,
                    A.PREPAYMENT_SEGMENT,
                    ROUND(AVG(A.SMM),4) AVERAGE_SMM
                FROM IFRS_PPR_DETAILS A
                WHERE A.ACCOUNT_NUMBER IN (SELECT DEAL_ID FROM TEST_PPRV2
                                           WHERE REPORT_DATE=LAST_DAY(V_CURRDATE) AND RESERVED_VARCHAR_2 IS NOT NULL)
                GROUP BY
                    LAST_DAY(V_CURRDATE),
                    A.ACCOUNT_NUMBER,
                    A.PREPAYMENT_SEGMENT
               */
            )XX
            INNER JOIN TMP_PPR_MASTER_PAYMENT_SETTING B ON XX.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
                   AND XX.DOWNLOAD_DATE BETWEEN B.DATE_START AND B.DATE_END
            WHERE COMPONENT_TYPE=0
          ) S ON (D.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER
                  AND D.PREPAYMENT_SEGMENT = S.PREPAYMENT_SEGMENT)
    WHEN MATCHED THEN
    UPDATE SET
        DOWNLOAD_DATE           =   S.DOWNLOAD_DATE,
        --ACCOUNT_NUMBER          =   S.ACCOUNT_NUMBER,
        CUSTOMER_NUMBER         =   S.CUSTOMER_NUMBER,
        CUSTOMER_NAME           =   S.CUSTOMER_NAME,
        SEGMENTATION_ID         =   S.SEGMENTATION_ID,
        SEGMENTATION_NAME       =   S.SEGMENTATION_NAME,
        PREPAYMENT_RULE_ID      =   S.PREPAYMENT_RULE_ID,
        PREPAYMENT_RULE_NAME    =   S.PREPAYMENT_RULE_NAME,
        PRODUCT_CODE            =   S.PRODUCT_CODE,
        --PREPAYMENT_SEGMENT      =   S.PREPAYMENT_SEGMENT,
        AVERAGE_SMM             =   S.AVERAGE_SMM,
        PREPAYMENT_RATE         =   S.PREPAYMENT_RATE,
        UPDATEDDATE             =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            PREPAYMENT_RULE_ID,
            PREPAYMENT_RULE_NAME,
            PRODUCT_CODE,
            PREPAYMENT_SEGMENT,
            AVERAGE_SMM,
            PREPAYMENT_RATE)
    VALUES (S.DOWNLOAD_DATE,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.CUSTOMER_NAME,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.PREPAYMENT_RULE_ID,
            S.PREPAYMENT_RULE_NAME,
            S.PRODUCT_CODE,
            S.PREPAYMENT_SEGMENT,
            S.AVERAGE_SMM,
            S.PREPAYMENT_RATE);
    COMMIT;

END;