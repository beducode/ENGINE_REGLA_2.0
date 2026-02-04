CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_CARD_DATA_IMA
AS
    V_CURRDATE DATE;

BEGIN

    SELECT  CURRDATE
    INTO V_CURRDATE
    FROM IFRS_PRC_DATE_LGD ;

    --POPULASI KARTU KREDIT INDIVIDUAL
    --UPDATE/ INSERT TABLE IFRS_LGD_ER_NPL_ACCT
    MERGE INTO IFRS_LGD_ER_NPL_ACCT_CARD D
    USING (
            --KARTU KREDIT INDIVIDUAL
            SELECT
                LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                A.MASTERID,
                A.ACCOUNT_NUMBER,
                A.CUSTOMER_NUMBER,
                A.CUSTOMER_NAME,
                B.LOSS_DATE NPL_DATE,
                B.CLOSED_DATE,
                B.BI_COLLECTABILITY_NPL ,
                A.RESERVED_VARCHAR_2 BI_COLLECTABILITY_CLOSED,
                B.OUTSTANDING_FIRST_NPL OUTSTANDING_DEFAULT,
                A.SEGMENT LGD_CUSTOMER_TYPE,
                A.LGD_SEGMENT SEGMENTATION_ID,
                A.SEGMENT SEGMENTATION_NAME,
                C.PKID LGD_RULE_ID,
                C.LGD_RULE_NAME,
                A.PLAFOND,
                A.PRODUCT_CODE,
                A.PRODUCT_TYPE,
                A.CURRENCY,
                A.CURRENCY ORIGINAL_CURRENCY,
                A.EXCHANGE_RATE,
                A.INTEREST_RATE,
                ''COLLATERAL_VALUE_FOR_IMPAIR,
                FN_LGD_DAYS_30_360 (NPL_DATE,RESERVED_DATE_5)/360 DAILY_BASIS,
                D.CHARGEOFF_STATUS      VALUATION_TYPE
            FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                INNER JOIN IFRS_LGD_NPL_ACCOUNT B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
                LEFT JOIN IFRS_LGD_RULES_CONFIG C ON A.LGD_RULE_ID=C.PKID
                    --AND A.LGD_RULE_ID=B.PKID
                INNER JOIN IFRS_STG_CRD_CHARGE D ON A.CUSTOMER_NUMBER=SUBSTR(D.CUSTOMER_NUMBER,6,11)
                    AND A.DOWNLOAD_DATE=D.DOWNLOAD_DATE
            WHERE
                A.DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
                AND A.DATA_SOURCE='CRD'
                AND RESERVED_VARCHAR_2 IN ('I','O')
                AND A.ACCOUNT_STATUS IN ('C','W')
                AND RESERVED_AMOUNT_4 >= 5
               AND A.BI_COLLECTABILITY IN ('C','3','4','5')
            UNION ALL
           --POPULASI KARTU KREDIT LOKAL
            SELECT
                LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                A.MASTERID,
                A.ACCOUNT_NUMBER,
                A.CUSTOMER_NUMBER,
                A.CUSTOMER_NAME,
                B.LOSS_DATE NPL_DATE,
                B.CLOSED_DATE,
                B.BI_COLLECTABILITY_NPL ,
                A.RESERVED_VARCHAR_2 BI_COLLECTABILITY_CLOSED,
                B.OUTSTANDING_FIRST_NPL OUTSTANDING_DEFAULT,
                A.SEGMENT LGD_CUSTOMER_TYPE,
                A.LGD_SEGMENT SEGMENTATION_ID,
                A.SEGMENT SEGMENTATION_NAME,
                C.PKID LGD_RULE_ID,
                C.LGD_RULE_NAME,
                A.PLAFOND,
                A.PRODUCT_CODE,
                A.PRODUCT_TYPE,
                A.CURRENCY,
                A.CURRENCY ORIGINAL_CURRENCY,
                A.EXCHANGE_RATE,
                A.INTEREST_RATE,
                ''COLLATERAL_VALUE_FOR_IMPAIR,
                FN_LGD_DAYS_30_360 (NPL_DATE,RESERVED_DATE_5)/360 DAILY_BASIS,
                D.CHARGEOFF_STATUS      VALUATION_TYPE
            FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                INNER JOIN IFRS_LGD_NPL_ACCOUNT B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
                LEFT JOIN IFRS_LGD_RULES_CONFIG C ON A.LGD_RULE_ID=C.PKID
                 --   AND A.LGD_RULE_ID=B.PKID
                INNER JOIN IFRS_STG_CRD_CHARGE D ON A.CUSTOMER_NUMBER=SUBSTR(D.CUSTOMER_NUMBER,6,11)
                    AND A.DOWNLOAD_DATE=D.DOWNLOAD_DATE
            WHERE A.DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
                AND A.DATA_SOURCE='ILS'
                AND A.ACCOUNT_STATUS = 'C'
                AND A.BI_COLLECTABILITY IN ('C','3','4','5')
                AND PRODUCT_CODE='101'
            )S ON (D.ACCOUNT_NUMBER     =   S.ACCOUNT_NUMBER
               AND D.CUSTOMER_NUMBER   =   S.CUSTOMER_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        DOWNLOAD_DATE               =   S.DOWNLOAD_DATE,
        MASTER_ID                   =   S.MASTERID,
        CUSTOMER_NAME               =   S.CUSTOMER_NAME,
        SEGMENTATION_ID             =   S.SEGMENTATION_ID,
        SEGMENTATION_NAME           =   S.SEGMENTATION_NAME,
        LGD_RULE_ID                 =   S.LGD_RULE_ID,
        LGD_RULE_NAME               =   S.LGD_RULE_NAME,
        NPL_DATE                    =   S.NPL_DATE,
        CLOSED_DATE                 =   S.CLOSED_DATE,
        BI_COLLECTABILITY_NPL       =   S.BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED    =   S.BI_COLLECTABILITY_CLOSED,
        OUTSTANDING_DEFAULT         =   S.OUTSTANDING_DEFAULT,
        LGD_CUSTOMER_TYPE           =   S.LGD_CUSTOMER_TYPE,
        PRODUCT_CODE                =   S.PRODUCT_CODE,
        CURRENCY                    =   S.CURRENCY,
        ORIGINAL_CURRENCY           =   S.ORIGINAL_CURRENCY,
        INTEREST_RATE               =   S.INTEREST_RATE,
        UPDATEDDATE                 =   SYSDATE,
        VALUATION_TYPE              =   S.VALUATION_TYPE
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            LGD_RULE_ID,
            LGD_RULE_NAME,
            NPL_DATE,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            OUTSTANDING_DEFAULT,
            LGD_CUSTOMER_TYPE,
            PRODUCT_CODE,
            CURRENCY,
            ORIGINAL_CURRENCY,
            INTEREST_RATE,
            VALUATION_TYPE
            )
    VALUES (S.DOWNLOAD_DATE,
            S.MASTERID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.CUSTOMER_NAME,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.LGD_RULE_ID,
            S.LGD_RULE_NAME,
            S.NPL_DATE,
            S.CLOSED_DATE,
            S.BI_COLLECTABILITY_NPL,
            S.BI_COLLECTABILITY_CLOSED,
            S.OUTSTANDING_DEFAULT,
            S.LGD_CUSTOMER_TYPE,
            S.PRODUCT_CODE,
            S.CURRENCY,
            S.ORIGINAL_CURRENCY,
            S.INTEREST_RATE,
            S.VALUATION_TYPE
            );
    COMMIT;

    /*
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_LGD_DETAIL_CCI';
    COMMIT;
    --POPULASI KARTU KREDIT INDIVIDUAL DETAIL
    INSERT INTO TMP_IFRS_LGD_DETAIL_CCI (DOWNLOAD_DATE,
                                        MASTERID,
                                        ACCOUNT_NUMBER,
                                        CUSTOMER_NUMBER,
                                        ACCOUNT_STATUS,
                                        CURRENCY,
                                        OUTSTANDING,
                                        IS_EFFECTIVE,
                                        DATA_SOURCE )
    SELECT
        DOWNLOAD_DATE,
        MASTERID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        ACCOUNT_STATUS,
        CURRENCY,
        OUTSTANDING,
        '1' IS_EFFECTIVE,
        DATA_SOURCE
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
    WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
        AND A.DATA_SOURCE='CRD'
        AND RESERVED_VARCHAR_2 IN ('I','O')
        AND A.ACCOUNT_STATUS='C'
        AND RESERVED_AMOUNT_4>=5
        AND A.BI_COLLECTABILITY IN ('C','3','4','5') ;
    */
    /*
    UNION
    SELECT
        DOWNLOAD_DATE,
        MASTERID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        ACCOUNT_STATUS,
        CURRENCY,
        OUTSTANDING,
        '1' IS_EFFECTIVE,
        DATA_SOURCE
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
    WHERE DOWNLOAD_DATE=LAST_DAY(ADD_MONTHS(V_CURRDATE,-1))
        AND A.DATA_SOURCE='CRD'
        AND RESERVED_VARCHAR_2 IN ('I','O')
        AND A.ACCOUNT_STATUS='C'
        -- AND RESERVED_AMOUNT_4>=5
        AND A.BI_COLLECTABILITY IN ('C','3','4','5') ;
     */
     COMMIT;

    /*
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_LGD_DETAIL_CCL';
    COMMIT;
    --POPULASI KARTU KREDIT LOCAL DETAIL
     INSERT INTO TMP_IFRS_LGD_DETAIL_CCL (DOWNLOAD_DATE,
                                        MASTERID,
                                        ACCOUNT_NUMBER,
                                        CUSTOMER_NUMBER,
                                        ACCOUNT_STATUS,
                                        CURRENCY,
                                        OUTSTANDING,
                                        IS_EFFECTIVE,
                                        DATA_SOURCE )
    SELECT
        DOWNLOAD_DATE,
        MASTERID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        ACCOUNT_STATUS,
        CURRENCY,
        OUTSTANDING,
        '1' IS_EFFECTIVE,
        DATA_SOURCE
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
    --WHERE DOWNLOAD_DATE=LAST_DAY('1-SEP-2018')
    WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
        AND ACCOUNT_STATUS='C'
        AND PRODUCT_CODE='101'
        AND DATA_SOURCE='ILS'
        AND BI_COLLECTABILITY IN ('C','3','4','5')
        AND RESERVED_DATE_5 IS NOT NULL
    UNION
    SELECT
        DOWNLOAD_DATE,
        MASTERID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        ACCOUNT_STATUS,
        CURRENCY,
        OUTSTANDING,
        '1' IS_EFFECTIVE,
        DATA_SOURCE
    FROM IFRS_MASTER_ACCOUNT_MONTHLY A
    WHERE DOWNLOAD_DATE=LAST_DAY(ADD_MONTHS(V_CURRDATE,-1))
        AND ACCOUNT_STATUS='C'
        AND PRODUCT_CODE='101'
        AND DATA_SOURCE='ILS'
        AND BI_COLLECTABILITY IN ('C','3','4','5')
        AND RESERVED_DATE_5 IS NOT NULL;
    COMMIT;
    */

       --UPDATE/ INSERT TABLE IFRS_LGD_DATA_DETAIL
    MERGE INTO IFRS_LGD_DATA_DETAIL_IMA D
    USING (
            SELECT
                A.DOWNLOAD_DATE,
                A.MASTERID,
                A.ACCOUNT_NUMBER,
                A.CUSTOMER_NUMBER,
                A.ACCOUNT_STATUS,
                B.CHARGEOFF_DATE PAYMENT_DATE,
                A.CURRENCY,
                B.CHARGEOFF_AMOUNT RECOVERY_AMOUNT,
                '' RECOVERY_TYPE,
                '1' IS_EFFECTIVE,
                A.DATA_SOURCE
            FROM IFRS_MASTER_ACCOUNT_MONTHLY A
                INNER JOIN IFRS_STG_CRD_CHARGE B ON A.CUSTOMER_NUMBER=SUBSTR(B.CUSTOMER_NUMBER,6,11)
                    AND A.DOWNLOAD_DATE=B.DOWNLOAD_DATE
            WHERE A.DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
                AND A.DATA_SOURCE='CRD'
                AND RESERVED_VARCHAR_2 IN ('I','O')
                AND A.ACCOUNT_STATUS IN ('C','W')
                AND RESERVED_AMOUNT_4>=5
                AND A.BI_COLLECTABILITY IN ('C','3','4','5')
        )S ON (D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER
                AND D.CUSTOMER_NUMBER=S.CUSTOMER_NUMBER
                AND NVL(D.PAYMENT_DATE,'1-JAN-1900')=NVL(S.PAYMENT_DATE,'1-JAN-1900'))
    WHEN MATCHED THEN
    UPDATE SET
        MASTER_ID       =   S.MASTERID,
        ACCOUNT_STATUS  =   S.ACCOUNT_STATUS,
        CURRENCY        =   S.CURRENCY,
        RECOVERY_AMOUNT =   S.RECOVERY_AMOUNT,
        RECOVERY_TYPE   =   S.RECOVERY_TYPE,
        IS_EFFECTIVE    =   S.IS_EFFECTIVE,
        DATA_SOURCE     =   S.DATA_SOURCE,
        UPDATEDDATE     =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            ACCOUNT_STATUS,
            PAYMENT_DATE,
            CURRENCY,
            RECOVERY_AMOUNT,
            IS_EFFECTIVE,
            DATA_SOURCE)
    VALUES (S.MASTERID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.ACCOUNT_STATUS,
            S.PAYMENT_DATE,
            S.CURRENCY,
            S.RECOVERY_AMOUNT,
            S.IS_EFFECTIVE,
            S.DATA_SOURCE)
     ;
    COMMIT;

  /*
    --UPDATE/ INSERT TABLE IFRS_LGD_DATA_DETAIL
    MERGE INTO IFRS_LGD_DATA_DETAIL_IMA D
    USING (
            SELECT
                DOWNLOAD_DATE,
                MASTERID,
                ACCOUNT_NUMBER,
                CUSTOMER_NUMBER,
                ACCOUNT_STATUS,
                CASE WHEN OUTSTANDING=0 THEN DOWNLOAD_DATE
                    WHEN OUTSTANDING2>OUTSTANDING  THEN DOWNLOAD_DATE
                END PAYMENT_DATE,
                CURRENCY,
                CASE WHEN (OUTSTANDING2 IS NULL OR OUTSTANDING2=0) AND OUTSTANDING!=0  THEN 0
                    WHEN OUTSTANDING2=OUTSTANDING THEN 0
                    WHEN OUTSTANDING2>OUTSTANDING  THEN OUTSTANDING
                    WHEN OUTSTANDING=0 THEN 0
                END RECOVERY_AMOUNT,
                '' RECOVERY_TYPE,
                IS_EFFECTIVE,
                DATA_SOURCE
            FROM (
                SELECT
                    DOWNLOAD_DATE,
                    MASTERID,
                    ACCOUNT_NUMBER,
                    CUSTOMER_NUMBER,
                    ACCOUNT_STATUS,
                    CURRENCY,
                    OUTSTANDING,
                    LAG(OUTSTANDING) OVER (PARTITION BY ACCOUNT_NUMBER,CUSTOMER_NUMBER ORDER BY  DOWNLOAD_DATE) OUTSTANDING2,
                    IS_EFFECTIVE,
                    DATA_SOURCE
                FROM (
                    SELECT DOWNLOAD_DATE,
                        MASTERID,
                        ACCOUNT_NUMBER,
                        CUSTOMER_NUMBER,
                        ACCOUNT_STATUS,
                        CURRENCY,
                        OUTSTANDING,
                        IS_EFFECTIVE,
                        DATA_SOURCE
                    FROM TMP_IFRS_LGD_DETAIL_CCI
                    /*
                    UNION
                    SELECT DOWNLOAD_DATE,
                        MASTERID,
                        ACCOUNT_NUMBER,
                        CUSTOMER_NUMBER,
                        ACCOUNT_STATUS,
                        CURRENCY,
                        OUTSTANDING,
                        IS_EFFECTIVE,
                        DATA_SOURCE
                    FROM TMP_IFRS_LGD_DETAIL_CCL
                /*
                    SELECT
                        DOWNLOAD_DATE,
                        MASTERID,
                        ACCOUNT_NUMBER,
                        CUSTOMER_NUMBER,
                        ACCOUNT_STATUS,
                        CURRENCY,
                        OUTSTANDING,
                        '1' IS_EFFECTIVE,
                        DATA_SOURCE
                    FROM IFRS_MASTER_ACCOUNT
                    WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
                        AND DATA_SOURCE='ILS'
                        AND BI_COLLECTABILITY IN ('C','3','4','5')
                        AND RESERVED_DATE_5 IS NOT NULL
                    UNION
                    SELECT
                        DOWNLOAD_DATE,
                        MASTERID,
                        ACCOUNT_NUMBER,
                        CUSTOMER_NUMBER,
                        ACCOUNT_STATUS,
                        CURRENCY,
                        OUTSTANDING,
                        '1' IS_EFFECTIVE,
                        DATA_SOURCE
                    FROM IFRS_MASTER_ACCOUNT
                    WHERE DOWNLOAD_DATE=LAST_DAY(ADD_MONTHS(V_CURRDATE,-1))
                        AND DATA_SOURCE='ILS'
                        AND BI_COLLECTABILITY IN ('C','3','4','5')
                        AND RESERVED_DATE_5 IS NOT NULL

                )AA
            )BB
            WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
        )S ON (D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER
                AND D.CUSTOMER_NUMBER=S.CUSTOMER_NUMBER
                AND NVL(D.PAYMENT_DATE,'1-JAN-1900')=NVL(S.PAYMENT_DATE,'1-JAN-1900'))
    WHEN MATCHED THEN
    UPDATE SET
        MASTER_ID       =   S.MASTERID,
        ACCOUNT_STATUS  =   S.ACCOUNT_STATUS,
        CURRENCY        =   S.CURRENCY,
        RECOVERY_AMOUNT =   S.RECOVERY_AMOUNT,
        RECOVERY_TYPE   =   S.RECOVERY_TYPE,
        IS_EFFECTIVE    =   S.IS_EFFECTIVE,
        DATA_SOURCE     =   S.DATA_SOURCE,
        UPDATEDDATE     =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            ACCOUNT_STATUS,
            PAYMENT_DATE,
            CURRENCY,
            RECOVERY_AMOUNT,
            IS_EFFECTIVE,
            DATA_SOURCE)
    VALUES (S.MASTERID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.ACCOUNT_STATUS,
            S.PAYMENT_DATE,
            S.CURRENCY,
            S.RECOVERY_AMOUNT,
            S.IS_EFFECTIVE,
            S.DATA_SOURCE)
     ;
    COMMIT;

    */
END;