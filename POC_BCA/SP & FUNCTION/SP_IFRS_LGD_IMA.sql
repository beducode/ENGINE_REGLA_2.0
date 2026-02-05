CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_IMA
AS
   V_CURRDATE DATE;
BEGIN

    SELECT  CURRDATE
    INTO V_CURRDATE
    FROM IFRS_PRC_DATE_LGD ;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LGD_PROCESS';
    COMMIT;

    INSERT INTO IFRS_LGD_PROCESS(DOWNLOAD_DATE,
                                PRODUCT_CODE,
                                PRODUCT_NAME,
                                MASTER_ID,
                                ACCOUNT_NUMBER,
                                CUSTOMER_NUMBER,
                                CUSTOMER_NAME,
                                ACCOUNT_STATUS,
                                LGD_CUSTOMER_TYPE,
                                SEGMENTATION_ID,
                                SEGMENTATION_NAME,
                                LGD_RULE_ID,
                                LGD_RULE_NAME,
                                CURRENCY,
                                ORIGINAL_CURRENCY,
                                NPL_DATE,
                                CLOSED_DATE,
                                BI_COLLECTABILITY_NPL,
                                BI_COLLECTABILITY_CLOSED,
                                TOTAL_LOSS_AMT,
                                DISCOUNT_RATE,
                                DAILY_BASIS,
                                REC_AMOUNT,
                                RECOVERY_DATE,
                                DATA_SOURCE)
    SELECT
        A.DOWNLOAD_DATE,
        A.PRODUCT_CODE,
        C.PRD_DESC PRODUCT_NAME,
        A.MASTER_ID,
        A.ACCOUNT_NUMBER,
        A.CUSTOMER_NUMBER,
        A.CUSTOMER_NAME,
        B.ACCOUNT_STATUS,
        A.LGD_CUSTOMER_TYPE,
        A.SEGMENTATION_ID,
        A.SEGMENTATION_NAME,
        A.LGD_RULE_ID,
        A.LGD_RULE_NAME,
        A.CURRENCY,
        A.ORIGINAL_CURRENCY,
        A.NPL_DATE,
        A.CLOSED_DATE,
        A.BI_COLLECTABILITY_NPL,
        A.BI_COLLECTABILITY_CLOSED,
        A.OUTSTANDING_DEFAULT  TOTAL_LOSS_AMT,
        A.INTEREST_RATE DISCOUNT_RATE,
        FN_LGD_DAYS_30_360 (A.NPL_DATE,B.PAYMENT_DATE)/360 DAILY_BASIS,
        (RECOVERY_AMOUNT/POWER((1+INTEREST_RATE),
            FN_LGD_DAYS_30_360 (A.NPL_DATE,B.PAYMENT_DATE)/360)
        ) REC_AMOUNT,
        B.PAYMENT_DATE  RECOVERY_DATE,
        B.DATA_SOURCE
    FROM IFRS_LGD_ER_NPL_ACCT_IMA A
        INNER JOIN IFRS_LGD_DATA_DETAIL_IMA B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
        INNER JOIN IFRS_MASTER_PRODUCT_PARAM C ON A.PRODUCT_CODE=C.PRD_CODE
            AND C.DATA_SOURCE IN ('ILS','CARD')
           -- AND PRODUCT_CODE IN ('101','CARDS')
    WHERE
        --OUTSTANDING_DEFAULT!=0
        --AND
        CLOSED_DATE IS NOT NULL
        AND B.PAYMENT_DATE = LAST_DAY(V_CURRDATE);
    COMMIT;

    --POPUPASI LGD CC INDIVIDUAL DAN LOCAL
    --UPDATE/INSERT IFRS_LGD WITH WORKOUT PERIOD=0
    MERGE INTO IFRS_LGD_CARD_IMA D
    USING (
            SELECT
                    A.DOWNLOAD_DATE,
                    A.PRODUCT_CODE,
                    A.PRODUCT_NAME,
                    A.MASTER_ID,
                    A.ACCOUNT_NUMBER,
                    A.CUSTOMER_NUMBER,
                    A.CUSTOMER_NAME,
                    A.LGD_CUSTOMER_TYPE,
                    A.SEGMENTATION_ID,
                    A.SEGMENTATION_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    MIN(A.NPL_DATE) NPL_DATE,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    SUM(A.REC_AMOUNT) RECOV_AMT_BF_NPV,
                    MAX(A.RECOVERY_DATE) LAST_RECOV_DATE,
                    ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 AS RECOV_PERCENTAGE,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    SUM(A.REC_AMOUNT) AS RECOVERY_AMOUNT,
                    100-ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 LOSS_RATE,
                    A.DATA_SOURCE,
                    B.WORKOUT_PERIOD
                FROM IFRS_LGD_PROCESS A
                    LEFT JOIN IFRS_LGD_RULES_CONFIG B ON A.SEGMENTATION_ID=B.SEGMENTATION_ID
                        AND A.LGD_RULE_ID=B.PKID
                WHERE
                    ---WORKOUT_PERIOD = 0
                    --AND
                    PRODUCT_CODE IN ('101','CARD')
                    AND DOWNLOAD_DATE = LAST_DAY(V_CURRDATE)
                GROUP BY
                    A.DOWNLOAD_DATE,
                    A.PRODUCT_CODE,
                    A.PRODUCT_NAME,
                    A.MASTER_ID,
                    A.ACCOUNT_NUMBER,
                    A.CUSTOMER_NUMBER,
                    A.CUSTOMER_NAME,
                    A.LGD_CUSTOMER_TYPE,
                    A.SEGMENTATION_ID,
                    A.SEGMENTATION_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    A.DATA_SOURCE,
                    B.WORKOUT_PERIOD
           ) S ON (D.ACCOUNT_NUMBER=S.ACCOUNT_NUMBER
                   AND D.PRODUCT_CODE=S.PRODUCT_CODE)
     WHEN MATCHED THEN
     UPDATE SET
        DOWNLOAD_DATE               =   S.DOWNLOAD_DATE,
        PRODUCT_NAME                =   S.PRODUCT_NAME,
        MASTER_ID                   =   S.MASTER_ID,
        CUSTOMER_NUMBER             =   S.CUSTOMER_NUMBER,
        CUSTOMER_NAME               =   S.CUSTOMER_NAME,
        LGD_CUSTOMER_TYPE           =   S.LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID             =   S.SEGMENTATION_ID,
        SEGMENTATION_NAME           =   S.SEGMENTATION_NAME,
        CURRENCY                    =   S.CURRENCY,
        ORIGINAL_CURRENCY           =   S.ORIGINAL_CURRENCY,
        NPL_DATE                    =   S.NPL_DATE,
        CLOSED_DATE                 =   S.CLOSED_DATE,
        BI_COLLECTABILITY_NPL       =   S.BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED    =   S.BI_COLLECTABILITY_CLOSED,
        RECOV_AMT_BF_NPV            =   S.RECOV_AMT_BF_NPV,
        LAST_RECOV_DATE             =   S.LAST_RECOV_DATE,
        RECOV_PERCENTAGE            =   S.RECOV_PERCENTAGE,
        TOTAL_LOSS_AMT              =   S.TOTAL_LOSS_AMT,
        DISCOUNT_RATE               =   S.DISCOUNT_RATE,
        RECOVERY_AMOUNT             =   S.RECOVERY_AMOUNT,
        LOSS_RATE                   =   S.LOSS_RATE,
        DATA_SOURCE                 =   S.DATA_SOURCE,
        UPDATEDDATE                 =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            PRODUCT_CODE,
            PRODUCT_NAME,
            MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            LGD_CUSTOMER_TYPE,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            CURRENCY,
            ORIGINAL_CURRENCY,
            NPL_DATE,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            RECOV_AMT_BF_NPV,
            LAST_RECOV_DATE,
            RECOV_PERCENTAGE,
            TOTAL_LOSS_AMT,
            DISCOUNT_RATE,
            RECOVERY_AMOUNT,
            LOSS_RATE,
            DATA_SOURCE)
    VALUES (S.DOWNLOAD_DATE,
            S.PRODUCT_CODE,
            S.PRODUCT_NAME,
            S.MASTER_ID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.CUSTOMER_NAME,
            S.LGD_CUSTOMER_TYPE,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.CURRENCY,
            S.ORIGINAL_CURRENCY,
            S.NPL_DATE,
            S.CLOSED_DATE,
            S.BI_COLLECTABILITY_NPL,
            S.BI_COLLECTABILITY_CLOSED,
            S.RECOV_AMT_BF_NPV,
            S.LAST_RECOV_DATE,
            S.RECOV_PERCENTAGE,
            S.TOTAL_LOSS_AMT,
            S.DISCOUNT_RATE,
            S.RECOVERY_AMOUNT,
            S.LOSS_RATE,
            S.DATA_SOURCE);
    COMMIT;

/*
    ---TRUNCATE CLOSED DATE WO PERIOD
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LGD_WOPERIOD';
    COMMIT;

    ---INSERT CLOSED DATE WO PERIOD
    INSERT INTO TMP_LGD_WOPERIOD (ACCOUNT_NUMBER,
                                  NPL_DATE,
                                  CLOSED_DATE)
    SELECT
        A.ACCOUNT_NUMBER,
        A.NPL_DATE,
        MAX(CASE WHEN A.ACCOUNT_STATUS='C' THEN CLOSED_DATE
            ELSE A.RECOVERY_DATE END) CLOSED_DATE
    FROM IFRS_LGD_PROCESS A
        LEFT JOIN IFRS_LGD_RULES_CONFIG B ON A.SEGMENTATION_ID=B.SEGMENTATION_ID
            AND A.LGD_RULE_ID=B.PKID
    WHERE   B.WORKOUT_PERIOD!=0
            AND TO_CHAR(A.RECOVERY_DATE,'MM-YYYY') BETWEEN  TO_CHAR(A.NPL_DATE,'MM-YYYY')
            AND TO_CHAR(ADD_MONTHS(A.NPL_DATE, 12 * WORKOUT_PERIOD),'MM-YYYY')
            AND DOWNLOAD_DATE = LAST_DAY(V_CURRDATE)
    GROUP BY A.ACCOUNT_NUMBER,
        A.NPL_DATE,
        A.CLOSED_DATE;
    COMMIT;

    --UPDATE/INSERT IFRS_LGD WITH WORKOUT PERIOD!=0
    MERGE INTO IFRS_LGD D
    USING (
            SELECT
                    A.DOWNLOAD_DATE,
                    A.PRODUCT_CODE,
                    A.PRODUCT_NAME,
                    A.MASTER_ID,
                    A.ACCOUNT_NUMBER,
                    A.CUSTOMER_NUMBER,
                    A.CUSTOMER_NAME,
                    A.LGD_CUSTOMER_TYPE,
                    A.SEGMENTATION_ID,
                    A.SEGMENTATION_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    MIN(A.NPL_DATE) NPL_DATE,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    SUM(A.REC_AMOUNT) RECOV_AMT_BF_NPV,
                    MAX(A.RECOVERY_DATE) LAST_RECOV_DATE,
                    ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 AS RECOV_PERCENTAGE,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    SUM(A.REC_AMOUNT) AS RECOVERY_AMOUNT,
                    100-ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 LOSS_RATE,
                    A.DATA_SOURCE,
                    B.WORKOUT_PERIOD
                FROM IFRS_LGD_PROCESS A
                    LEFT JOIN IFRS_LGD_RULES_CONFIG B ON A.SEGMENTATION_ID=B.SEGMENTATION_ID
                        AND A.LGD_RULE_ID=B.PKID
                WHERE WORKOUT_PERIOD!=0
                    AND TO_CHAR(RECOVERY_DATE,'MM-YYYY') BETWEEN  TO_CHAR(A.NPL_DATE,'MM-YYYY')
                    AND TO_CHAR(ADD_MONTHS(A.NPL_DATE,12 * B.WORKOUT_PERIOD),'MM-YYYY')
                    AND DOWNLOAD_DATE=LAST_DAY(V_CURRDATE)
                GROUP BY
                    A.DOWNLOAD_DATE,
                    A.PRODUCT_CODE,
                    A.PRODUCT_NAME,
                    A.MASTER_ID,
                    A.ACCOUNT_NUMBER,
                    A.CUSTOMER_NUMBER,
                    A.CUSTOMER_NAME,
                    A.LGD_CUSTOMER_TYPE,
                    A.SEGMENTATION_ID,
                    A.SEGMENTATION_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    A.DATA_SOURCE,
                    B.WORKOUT_PERIOD
           ) S ON (D.ACCOUNT_NUMBER = S.ACCOUNT_NUMBER
                   AND D.PRODUCT_CODE = S.PRODUCT_CODE)
     WHEN MATCHED THEN
     UPDATE SET
        DOWNLOAD_DATE               =   S.DOWNLOAD_DATE,
        PRODUCT_NAME                =   S.PRODUCT_NAME,
        MASTER_ID                   =   S.MASTER_ID,
        CUSTOMER_NUMBER             =   S.CUSTOMER_NUMBER,
        CUSTOMER_NAME               =   S.CUSTOMER_NAME,
        LGD_CUSTOMER_TYPE           =   S.LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID             =   S.SEGMENTATION_ID,
        SEGMENTATION_NAME           =   S.SEGMENTATION_NAME,
        CURRENCY                    =   S.CURRENCY,
        ORIGINAL_CURRENCY           =   S.ORIGINAL_CURRENCY,
        NPL_DATE                    =   S.NPL_DATE,
        CLOSED_DATE                 =   S.CLOSED_DATE,
        BI_COLLECTABILITY_NPL       =   S.BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED    =   S.BI_COLLECTABILITY_CLOSED,
        RECOV_AMT_BF_NPV            =   S.RECOV_AMT_BF_NPV,
        LAST_RECOV_DATE             =   S.LAST_RECOV_DATE,
        RECOV_PERCENTAGE            =   S.RECOV_PERCENTAGE,
        TOTAL_LOSS_AMT              =   S.TOTAL_LOSS_AMT,
        DISCOUNT_RATE               =   S.DISCOUNT_RATE,
        RECOVERY_AMOUNT             =   S.RECOVERY_AMOUNT,
        LOSS_RATE                   =   S.LOSS_RATE,
        DATA_SOURCE                 =   S.DATA_SOURCE,
        UPDATEDDATE                 =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (DOWNLOAD_DATE,
            PRODUCT_CODE,
            PRODUCT_NAME,
            MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            LGD_CUSTOMER_TYPE,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            CURRENCY,
            ORIGINAL_CURRENCY,
            NPL_DATE,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            RECOV_AMT_BF_NPV,
            LAST_RECOV_DATE,
            RECOV_PERCENTAGE,
            TOTAL_LOSS_AMT,
            DISCOUNT_RATE,
            RECOVERY_AMOUNT,
            LOSS_RATE,
            DATA_SOURCE)
    VALUES (S.DOWNLOAD_DATE,
            S.PRODUCT_CODE,
            S.PRODUCT_NAME,
            S.MASTER_ID,
            S.ACCOUNT_NUMBER,
            S.CUSTOMER_NUMBER,
            S.CUSTOMER_NAME,
            S.LGD_CUSTOMER_TYPE,
            S.SEGMENTATION_ID,
            S.SEGMENTATION_NAME,
            S.CURRENCY,
            S.ORIGINAL_CURRENCY,
            S.NPL_DATE,
            S.CLOSED_DATE,
            S.BI_COLLECTABILITY_NPL,
            S.BI_COLLECTABILITY_CLOSED,
            S.RECOV_AMT_BF_NPV,
            S.LAST_RECOV_DATE,
            S.RECOV_PERCENTAGE,
            S.TOTAL_LOSS_AMT,
            S.DISCOUNT_RATE,
            S.RECOVERY_AMOUNT,
            S.LOSS_RATE,
            S.DATA_SOURCE);
    COMMIT;

    /*
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LGD';
    COMMIT;
    INSERT INTO IFRS_LGD(DOWNLOAD_DATE,
            PRODUCT_CODE,
            PRODUCT_NAME,
            MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            LGD_CUSTOMER_TYPE,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            CURRENCY,
            ORIGINAL_CURRENCY,
            NPL_DATE,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            RECOV_AMT_BF_NPV,
            LAST_RECOV_DATE,
            RECOV_PERCENTAGE,
            TOTAL_LOSS_AMT,
            DISCOUNT_RATE,
            RECOVERY_AMOUNT,
            LOSS_RATE,
            DATA_SOURCE	)
    SELECT
        DOWNLOAD_DATE,
        PRODUCT_CODE,
        PRODUCT_NAME,
        MASTER_ID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID,
        SEGMENTATION_NAME,
        CURRENCY,
        ORIGINAL_CURRENCY,
        MIN(NPL_DATE) NPL_DATE,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        SUM(REC_AMOUNT) RECOV_AMT_BF_NPV,
        MAX(RECOVERY_DATE) LAST_RECOV_DATE,
        ROUND(SUM(REC_AMOUNT)/TOTAL_LOSS_AMT,4)*100 AS RECOV_PERCENTAGE,
        TOTAL_LOSS_AMT,
        DISCOUNT_RATE,
        SUM(REC_AMOUNT) AS RECOVERY_AMOUNT,
        100-ROUND(SUM(REC_AMOUNT)/TOTAL_LOSS_AMT,4)*100 LOSS_RATE,
        DATA_SOURCE
    FROM IFRS_LGD_PROCESS
    GROUP BY
        DOWNLOAD_DATE,
        PRODUCT_CODE,
        PRODUCT_NAME,
        MASTER_ID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID,
        SEGMENTATION_NAME,
        CURRENCY,
        ORIGINAL_CURRENCY,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        TOTAL_LOSS_AMT,
        DISCOUNT_RATE,
        DATA_SOURCE;
    COMMIT;
    */
/*
        INSERT INTO IFRS_LGD_PROCESS(DOWNLOAD_DATE,
            PRODUCT_CODE,
            PRODUCT_NAME,
            MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            LGD_CUSTOMER_TYPE,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            CURRENCY,
            ORIGINAL_CURRENCY,
            NPL_DATE,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            RECOV_AMT_BF_NPV,
            LAST_RECOV_DATE,
            RECOV_PERCENTAGE,
            --DAILY_BASIS,
            TOTAL_LOSS_AMT,
            DISCOUNT_RATE,
            RECOVERY_AMOUNT,
            LOSS_RATE,
            DATA_SOURCE	)
    SELECT
        DOWNLOAD_DATE,
        PRODUCT_CODE,
        PRODUCT_NAME,
        MASTER_ID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID,
        SEGMENTATION_NAME,
        CURRENCY,
        ORIGINAL_CURRENCY,
        MIN(NPL_DATE) NPL_DATE,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        SUM(REC_AMOUNT) RECOV_AMT_BF_NPV,
        MAX(RECOVERY_DATE) LAST_RECOV_DATE,
        ROUND(SUM(REC_AMOUNT)/TOTAL_LOSS_AMT,4)*100 AS RECOV_PERCENTAGE,
        TOTAL_LOSS_AMT,
        DISCOUNT_RATE,
        SUM(REC_AMOUNT) AS RECOVERY_AMOUNT,
        100-ROUND(SUM(REC_AMOUNT)/TOTAL_LOSS_AMT,4)*100 LOSS_RATE,
        DATA_SOURCE
    FROM (
        SELECT
            A.DOWNLOAD_DATE,
            A.PRODUCT_CODE,
            C.PRODUCT_DESCRIPTION PRODUCT_NAME,
            A.MASTER_ID,
            A.ACCOUNT_NUMBER,
            A.CUSTOMER_NUMBER,
            A.CUSTOMER_NAME,
            A.LGD_CUSTOMER_TYPE,
            A.SEGMENTATION_ID,
            A.SEGMENTATION_NAME,
            A.CURRENCY,
            A.ORIGINAL_CURRENCY,
            A.NPL_DATE,
            A.CLOSED_DATE,
            A.BI_COLLECTABILITY_NPL,
            A.BI_COLLECTABILITY_CLOSED,
            A.OUTSTANDING_DEFAULT  TOTAL_LOSS_AMT,
            A.INTEREST_RATE DISCOUNT_RATE,
            FN_LGD_DAYS_30_360 (A.NPL_DATE,B.PAYMENT_DATE)/360 DAILY_BASIS,
            (RECOVERY_AMOUNT/POWER((1+INTEREST_RATE),
                FN_LGD_DAYS_30_360 (A.NPL_DATE,B.PAYMENT_DATE)/360)
            ) REC_AMOUNT,
            B.PAYMENT_DATE  RECOVERY_DATE,
            B.DATA_SOURCE
        FROM IFRS_LGD_ER_NPL_ACCT A
            INNER JOIN IFRS_LGD_DATA_DETAIL B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
            INNER JOIN IFRS_PRODUCT_PARAM C ON A.PRODUCT_CODE=C.PRD_CODE AND C.DATA_SOURCE='ILS'
    )AA
    GROUP BY  DOWNLOAD_DATE,
            PRODUCT_CODE,
            PRODUCT_NAME,
            MASTER_ID,
            ACCOUNT_NUMBER,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME,
            LGD_CUSTOMER_TYPE,
            SEGMENTATION_ID,
            SEGMENTATION_NAME,
            CURRENCY,
            ORIGINAL_CURRENCY,
            CLOSED_DATE,
            BI_COLLECTABILITY_NPL,
            BI_COLLECTABILITY_CLOSED,
            DAILY_BASIS,
            TOTAL_LOSS_AMT,
            DISCOUNT_RATE,
            DATA_SOURCE;
    COMMIT;
    */
END;