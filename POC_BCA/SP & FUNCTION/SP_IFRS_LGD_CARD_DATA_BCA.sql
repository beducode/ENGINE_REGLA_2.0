CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_CARD_DATA_BCA
AS
    V_CURRDATE DATE;
BEGIN

    SELECT  CURRDATE
    INTO V_CURRDATE
    FROM IFRS_PRC_DATE_LGD ;

    --UPDATE /INSERT TABLE IFRS_LGD_ER_NPL_ACCT
    MERGE INTO IFRS_LGD_ER_NPL_ACCT D
    USING (
            SELECT
                CASE WHEN A.LOSS_DATE=LAST_DAY(A.LOSS_DATE) THEN A.LOSS_DATE
                ELSE LAST_DAY(A.LOSS_DATE) END      AS DOWNLOAD_DATE,
                NVL(C.PKID,0)                       AS MASTER_ID,
                A.DEAL_ID                           AS ACCOUNT_NUMBER,
                A.CUSTOMER_NR                       AS CUSTOMER_NUMBER,
                A.CUSTOMER_NAME                     AS CUSTOMER_NAME,
                A.LOSS_DATE                         AS NPL_DATE,
                A.CLOSED_DATE                       AS CLOSED_DATE,
                A.DEFAULT_STATUS_AT_LOSS_DATE       AS BI_COLLECTABILITY_NPL,
                A.DEFAULT_STATUS_AT_CLOSE_DATE      AS BI_COLLECTABILITY_CLOSED,
                A.TOTAL_LOSS_AMT                    AS OUTSTANDING_DEFAULT,
                ''	                                AS LGD_CUSTOMER_TYPE,
                F.PKID                              AS SEGMENTATION_ID,
                F.SEGMENT                           AS SEGMENTATION_NAME,
                E.PKID                              AS LGD_RULE_ID,
                E.LGD_RULE_NAME                     AS LGD_RULE_NAME,
                ''                                  AS PLAFOND,
                ''                                  AS PRODUCT_CODE,
                ''                                  AS PRODUCT_TYPE,
                A.CURRENCY                          AS CURRENCY,
                A.ORIGINAL_CURRENCY                 AS ORIGINAL_CURRENCY,
                ''                                  AS EXCHANGE_RATE,
                A.DISCOUNT_RATE                     AS INTEREST_RATE,
                ''                                  AS COLLATERAL_VALUE_FOR_IMPAIR,
                A.VALUATION_TYPE                    AS VALUATION_TYPE
            FROM TEST_LGD_CARD A
                LEFT JOIN IFRS_MASTERID C ON A.DEAL_ID = C.MASTER_ACCOUNT_CODE
                INNER JOIN TEST_FINSTO_LGD_CONVERSION D ON A.POOL_ID = D.POOL_ID
                INNER JOIN IFRS_LGD_RULES_CONFIG E ON D.LGD_RULE_ID	= E.PKID
                INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER F ON E.SEGMENTATION_ID=F.PKID
            WHERE A."Date"= V_CURRDATE--LAST_DAY(V_CURRDATE)
               -- AND
                --A.VALUATION_TYPE='FULLPAID'
            ) S ON (D.ACCOUNT_NUMBER  = S.ACCOUNT_NUMBER
                AND D.CUSTOMER_NUMBER = S.CUSTOMER_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        --DOWNLOAD_DATE               =   S.DOWNLOAD_DATE,
        MASTER_ID                   =   S.MASTER_ID,
        CUSTOMER_NAME               =   S.CUSTOMER_NAME,
        NPL_DATE                    =   S.NPL_DATE,
        CLOSED_DATE                 =   S.CLOSED_DATE,
        BI_COLLECTABILITY_NPL       =   S.BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED    =   S.BI_COLLECTABILITY_CLOSED,
        OUTSTANDING_DEFAULT         =   S.OUTSTANDING_DEFAULT,
        LGD_CUSTOMER_TYPE           =   S.LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID             =   S.SEGMENTATION_ID,
        SEGMENTATION_NAME           =   S.SEGMENTATION_NAME,
        LGD_RULE_ID                 =   S.LGD_RULE_ID,
        LGD_RULE_NAME               =   S.LGD_RULE_NAME,
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
            NPL_DATE,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            OUTSTANDING_DEFAULT,
            LGD_CUSTOMER_TYPE,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            LGD_RULE_ID,
            LGD_RULE_NAME,
            PRODUCT_CODE,
            CURRENCY,
            ORIGINAL_CURRENCY,
            INTEREST_RATE,
            VALUATION_TYPE
            )
    VALUES (S.DOWNLOAD_DATE,
            S.MASTER_ID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.CUSTOMER_NAME,
            S.NPL_DATE,
            S.CLOSED_DATE,
            S.BI_COLLECTABILITY_NPL,
            S.BI_COLLECTABILITY_CLOSED,
            S.OUTSTANDING_DEFAULT,
            S.LGD_CUSTOMER_TYPE,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.LGD_RULE_ID,
            S.LGD_RULE_NAME,
            S.PRODUCT_CODE,
            S.CURRENCY,
            S.ORIGINAL_CURRENCY,
            S.INTEREST_RATE,
            S.VALUATION_TYPE
            );
    COMMIT;

    --UPDATE /INSERT TABLE IFRS_LGD_DATA_DETAIL
    MERGE INTO IFRS_LGD_DATA_DETAIL D
    USING (
            SELECT
                C.PKID AS MASTER_ID,
                A.DEAL_ID AS ACCOUNT_NUMBER,
                A.CUSTOMER_NR AS CUSTOMER_NUMBER,
                B.RECOVERY_DATE AS PAYMENT_DATE,
                G.PKID       AS SEGMENTATION_ID,
                D.ACCOUNT_STATUS,
                A.CURRENCY,
                B.RECOVERY_AMOUNT,
                B.RECOVERY_TYPE,
                B.IS_EFFECTIVE,
                CASE WHEN B.SOURCE_SYSTEM='Cardlink' THEN 'CRD' END DATA_SOURCE
            FROM TEST_LGD_CARD A
                INNER JOIN TEST_LGD_CARD_RECOV B ON A.DEAL_ID = B.RECOVERY_EXTERNAL_REFERENCE
                LEFT JOIN IFRS_MASTERID C ON A.DEAL_ID = C.MASTER_ACCOUNT_CODE
                LEFT JOIN TMP_IFRS_MASTER_ACCOUNT D ON B.RECOVERY_EXTERNAL_REFERENCE = D.ACCOUNT_NUMBER
                    AND B.RECOVERY_DATE = D.DOWNLOAD_DATE
                INNER JOIN TEST_FINSTO_LGD_CONVERSION E ON A.POOL_ID = E.POOL_ID
                INNER JOIN IFRS_LGD_RULES_CONFIG F ON E.LGD_RULE_ID	= F.PKID
                INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER G ON F.SEGMENTATION_ID=G.PKID
            WHERE B.RECOVERY_DATE = V_CURRDATE --LAST_DAY(V_CURRDATE)
            --AND VALUATION_TYPE='FULLPAID'
        ) S ON (D.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER
                AND D.CUSTOMER_NUMBER = S.CUSTOMER_NUMBER
                AND NVL(D.PAYMENT_DATE,'1-JAN-1900') = NVL(S.PAYMENT_DATE,'1-JAN-1900'))
    WHEN MATCHED THEN
    UPDATE SET
        MASTER_ID       =   S.MASTER_ID,
        ACCOUNT_STATUS  =   S.ACCOUNT_STATUS,
        SEGMENTATION_ID =   S.SEGMENTATION_ID,
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
            SEGMENTATION_ID,
            PAYMENT_DATE,
            CURRENCY,
            RECOVERY_AMOUNT,
            IS_EFFECTIVE,
            DATA_SOURCE)
    VALUES (S.MASTER_ID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.ACCOUNT_STATUS,
            S.SEGMENTATION_ID,
            S.PAYMENT_DATE,
            S.CURRENCY,
            S.RECOVERY_AMOUNT,
            S.IS_EFFECTIVE,
            S.DATA_SOURCE);
    COMMIT;
END;