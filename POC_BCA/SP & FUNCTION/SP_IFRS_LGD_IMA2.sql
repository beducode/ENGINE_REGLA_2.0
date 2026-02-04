CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_IMA2
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
        LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
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
    FROM IFRS_LGD_ER_NPL_ACCT A
        INNER JOIN IFRS_LGD_DATA_DETAIL B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
        INNER JOIN IFRS_MASTER_PRODUCT_PARAM C ON A.PRODUCT_CODE=C.PRD_CODE
            AND C.DATA_SOURCE IN ('ILS','CARD')
    WHERE --OUTSTANDING_DEFAULT!=0
       -- AND
        (CLOSED_DATE IS NOT NULL
        AND --B.PAYMENT_DATE = LAST_DAY(V_CURRDATE)
            A.DOWNLOAD_DATE	= LAST_DAY(V_CURRDATE))
        OR
        (CLOSED_DATE IS NOT NULL
         AND A.VALUATION_TYPE='FULLPAID'
         AND --B.PAYMENT_DATE = LAST_DAY(V_CURRDATE)
            A.DOWNLOAD_DATE	= LAST_DAY(V_CURRDATE))
        ;
    COMMIT;

    --UPDATE/INSERT IFRS_LGD WITH WORKOUT PERIOD=0 AND KARTU KREDIT INDIVIDUAL,LOKAL
    MERGE INTO IFRS_LGD D
    USING (
            SELECT
                    LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                    A.PRODUCT_CODE,
                    A.PRODUCT_NAME,
                    A.MASTER_ID,
                    A.ACCOUNT_NUMBER,
                    A.CUSTOMER_NUMBER,
                    A.CUSTOMER_NAME,
                    A.LGD_CUSTOMER_TYPE,
                    A.SEGMENTATION_ID,
                    A.SEGMENTATION_NAME,
                    A.LGD_RULE_ID,
                    A.LGD_RULE_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    MIN(A.NPL_DATE) NPL_DATE,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    SUM(A.REC_AMOUNT) RECOV_AMT_BF_NPV,
                    MAX(A.RECOVERY_DATE) LAST_RECOV_DATE,
                    CASE WHEN SUM(A.REC_AMOUNT)=0 THEN 0 ELSE ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 END RECOV_PERCENTAGE,
                    --ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 AS RECOV_PERCENTAGE,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    SUM(A.REC_AMOUNT) AS RECOVERY_AMOUNT,
                    CASE WHEN SUM(A.REC_AMOUNT)=0 THEN 0 ELSE 100-ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 END LOSS_RATE,
                    --100-ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 LOSS_RATE,
                    A.DATA_SOURCE--,
                    --C.WORKOUT_PERIOD
                FROM IFRS_LGD_PROCESS A
                    INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER B ON A.SEGMENTATION_ID=B.PKID
                    INNER JOIN IFRS_LGD_RULES_CONFIG C ON B.PKID=C.SEGMENTATION_ID
                WHERE
                /*
                    (WORKOUT_PERIOD = 0
                     AND PRODUCT_CODE NOT IN ('101','CARDS')
                     AND A.RECOVERY_DATE  = LAST_DAY(V_CURRDATE))
                     OR
                     --KARTU KREDIT INDIVIDUAL DAN LOCAL
                    (PRODUCT_CODE IN ('101','CARDS')
                    AND A.RECOVERY_DATE  = LAST_DAY(V_CURRDATE))*/

                    --WORKOUT_PERIOD = 0
                    --AND
                    A.ACCOUNT_STATUS IN ('C','W')
                    AND --A.RECOVERY_DATE  = LAST_DAY(V_CURRDATE)
                        A.DOWNLOAD_DATE	= LAST_DAY(V_CURRDATE)
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
                    A.LGD_RULE_ID,
                    A.LGD_RULE_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    A.DATA_SOURCE--,
                    --C.WORKOUT_PERIOD
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
        LGD_RULE_ID                     =   S.LGD_RULE_ID,
        LGD_RULE_NAME               =   S.LGD_RULE_NAME,
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
            LGD_RULE_ID,
            LGD_RULE_NAME,
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
            S.LGD_RULE_ID,
            S.LGD_RULE_NAME,
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
        --MAX(CASE WHEN A.ACCOUNT_STATUS='C' THEN CLOSED_DATE
           -- ELSE A.RECOVERY_DATE END) CLOSED_DATE
        A.RECOVERY_DATE CLOSED_DATE
    FROM IFRS_LGD_PROCESS A
        INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER B ON A.SEGMENTATION_ID=B.PKID
        INNER JOIN IFRS_LGD_RULES_CONFIG C ON B.PKID=C.SEGMENTATION_ID
    WHERE
            --C.WORKOUT_PERIOD != 0
            --AND
            MONTHS_BETWEEN (A.DOWNLOAD_DATE, NPL_DATE )>= 12 * WORKOUT_PERIOD
            AND A.ACCOUNT_STATUS NOT IN ('C','W')
            AND A.RECOVERY_DATE  = LAST_DAY(V_CURRDATE)
   /* GROUP BY A.ACCOUNT_NUMBER,
        A.NPL_DATE,
        A.CLOSED_DATE
     ;
    COMMIT;*/

    --UPDATE/INSERT IFRS_LGD WITH WORKOUT PERIOD!=0
    MERGE INTO IFRS_LGD D
    USING (
            SELECT
                    LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                    A.PRODUCT_CODE,
                    A.PRODUCT_NAME,
                    A.MASTER_ID,
                    A.ACCOUNT_NUMBER,
                    A.CUSTOMER_NUMBER,
                    A.CUSTOMER_NAME,
                    A.LGD_CUSTOMER_TYPE,
                    A.SEGMENTATION_ID,
                    A.SEGMENTATION_NAME,
                    A.LGD_RULE_ID,
                    A.LGD_RULE_NAME,
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
                    A.DATA_SOURCE--,
                   -- C.WORKOUT_PERIOD
                FROM IFRS_LGD_PROCESS A
                    INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER B ON A.SEGMENTATION_ID=B.PKID
                    INNER JOIN IFRS_LGD_RULES_CONFIG C ON B.PKID=C.SEGMENTATION_ID
                WHERE
                    --C.WORKOUT_PERIOD !=0
                    --AND
                    MONTHS_BETWEEN (A.DOWNLOAD_DATE, NPL_DATE )>= 12 * WORKOUT_PERIOD
                    AND A.ACCOUNT_STATUS NOT IN ('C','W')
                    AND A.DOWNLOAD_DATE	= LAST_DAY(V_CURRDATE) --A.RECOVERY_DATE = LAST_DAY(V_CURRDATE)
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
                    A.LGD_RULE_ID,
                    A.LGD_RULE_NAME,
                    A.CURRENCY,
                    A.ORIGINAL_CURRENCY,
                    A.CLOSED_DATE,
                    A.BI_COLLECTABILITY_NPL,
                    A.BI_COLLECTABILITY_CLOSED,
                    A.TOTAL_LOSS_AMT,
                    A.DISCOUNT_RATE,
                    A.DATA_SOURCE--,
                    --C.WORKOUT_PERIOD
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
        LGD_RULE_ID                     =   S.LGD_RULE_ID,
        LGD_RULE_NAME               =   S.LGD_RULE_NAME,
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
            LGD_RULE_ID,
            LGD_RULE_NAME,
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
            S.LGD_RULE_ID,
            S.LGD_RULE_NAME,
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
    --POPUPASI LGD KARTU KREDIT INDIVIDUAL DAN LOCAL
    MERGE INTO IFRS_LGD D
    USING (
            SELECT
                    V_CURRDATE DOWNLOAD_DATE,
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
                    PRODUCT_CODE IN ('101','CARD')
                    AND A.RECOVERY_DATE  = V_CURRDATE--LAST_DAY(V_CURRDATE)
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
*/
END;