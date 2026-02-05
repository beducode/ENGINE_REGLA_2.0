CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_WORKOUT(V_EFF_DATE DATE)
AS
   V_DATE DATE;
   V_WORKOUT_PERIOD NUMBER;
BEGIN
    V_WORKOUT_PERIOD := 36;

    DELETE IFRS_LGD_WORKOUT
    WHERE EFF_DATE = V_EFF_DATE
    AND DATA_SOURCE = 'ILS';
    COMMIT;

    V_DATE := '31 OCT 2006';

    WHILE V_DATE <= V_EFF_DATE LOOP
        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LGD_PROCESS';

        /*=========================================================
        1. Insert from calculation table
        ===========================================================*/
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
                                    RECOV_AMT_BF_NPV,
                                    REC_AMOUNT,
                                    RECOVERY_DATE,
                                    DATA_SOURCE,
                                    LGD_FLAG)
        SELECT
            LAST_DAY(V_DATE) DOWNLOAD_DATE,
            NVL(A.PRODUCT_CODE,0) PRODUCT_CODE,
            NVL(C.PRD_DESC,'-') PRODUCT_NAME,
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
            RECOVERY_AMOUNT RECOV_AMT_BF_NPV,
            (RECOVERY_AMOUNT/POWER((1+INTEREST_RATE),
                FN_LGD_DAYS_30_360 (A.NPL_DATE,B.PAYMENT_DATE)/360)
            ) REC_AMOUNT,
            B.PAYMENT_DATE  RECOVERY_DATE,
            'ILS' DATA_SOURCE,
            A.LGD_FLAG
        FROM IFRS_LGD_ER_NPL_ACCT_WORKOUT A
            LEFT JOIN IFRS_LGD_DATA_DETAIL_WORKOUT B ON A.EFF_DATE = B.EFF_DATE AND A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
            JOIN IFRS_MASTER_PRODUCT_PARAM C ON A.PRODUCT_CODE=C.PRD_CODE
                AND C.DATA_SOURCE = 'ILS'
        WHERE A.EFF_DATE = V_EFF_DATE
            AND CLOSED_DATE IS NOT NULL
            AND A.DOWNLOAD_DATE	= LAST_DAY(V_DATE)
            AND A.ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER FROM IFRS_LGD WHERE EFF_DATE = V_EFF_DATE AND SPECIAL_REASON = 'ADJ_REC_AMOUNT')
            AND A.ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER FROM IFRS_LGD WHERE EFF_DATE = V_EFF_DATE AND SPECIAL_REASON = 'INCLUDED')
            ;
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
                                    RECOV_AMT_BF_NPV,
                                    REC_AMOUNT,
                                    RECOVERY_DATE,
                                    DATA_SOURCE,
                                    LGD_FLAG,
                                    SPECIAL_REASON)
        SELECT
            LAST_DAY(V_DATE) DOWNLOAD_DATE,
            NVL(A.PRODUCT_CODE,0) PRODUCT_CODE,
            NVL(C.PRD_DESC,'-') PRODUCT_NAME,
            A.MASTER_ID,
            A.ACCOUNT_NUMBER,
            A.CUSTOMER_NUMBER,
            A.CUSTOMER_NAME,
            'C' ACCOUNT_STATUS,
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
            FN_LGD_DAYS_30_360 (A.NPL_DATE,CASE WHEN NVL(B.PAYMENT_DATE, A.CLOSED_DATE) < A.CLOSED_DATE THEN NVL(B.PAYMENT_DATE, A.CLOSED_DATE) ELSE A.CLOSED_DATE END)/360 DAILY_BASIS,
            RECOVERY_AMOUNT RECOV_AMT_BF_NPV,
            (RECOVERY_AMOUNT/POWER((1+INTEREST_RATE),
                FN_LGD_DAYS_30_360 (A.NPL_DATE,CASE WHEN NVL(B.PAYMENT_DATE, A.CLOSED_DATE) < A.CLOSED_DATE THEN NVL(B.PAYMENT_DATE, A.CLOSED_DATE) ELSE A.CLOSED_DATE END)/360)
            ) REC_AMOUNT,
            CASE WHEN NVL(B.PAYMENT_DATE, A.CLOSED_DATE) < A.CLOSED_DATE THEN NVL(B.PAYMENT_DATE, A.CLOSED_DATE) ELSE A.CLOSED_DATE END RECOVERY_DATE,
            'ILS' DATA_SOURCE,
            A.LGD_FLAG,
            'ADJ_REC_AMOUNT' SPECIAL_REASON
        FROM IFRS_LGD_ER_NPL_ACCT_WORKOUT A
        JOIN
            (
                SELECT A2.ACCOUNT_NUMBER,
                    A2.ADJ_REC_AMOUNT_BF_NPV RECOVERY_AMOUNT,
                    B2.PAYMENT_DATE
                FROM TBLU_LGD_ADJ_REC_AMOUNT_BF_NPV A2
                LEFT JOIN
                (
                    SELECT ACCOUNT_NUMBER,
                        MAX(PAYMENT_DATE) PAYMENT_DATE
                    FROM IFRS_LGD_DATA_DETAIL_WORKOUT
                    WHERE EFF_DATE = V_EFF_DATE
                    GROUP BY ACCOUNT_NUMBER
                ) B2
                ON A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
                JOIN IFRS_LGD C2
                ON A2.ACCOUNT_NUMBER = C2.ACCOUNT_NUMBER
                AND C2.EFF_DATE = V_EFF_DATE
                AND C2.SPECIAL_REASON = 'ADJ_REC_AMOUNT'
            )B ON A.ACCOUNT_NUMBER=B.ACCOUNT_NUMBER
            JOIN IFRS_MASTER_PRODUCT_PARAM C ON A.PRODUCT_CODE=C.PRD_CODE
                AND C.DATA_SOURCE = 'ILS'
        WHERE A.EFF_DATE = V_EFF_DATE
            AND CLOSED_DATE IS NOT NULL
            AND A.DOWNLOAD_DATE	= LAST_DAY(V_DATE);
        COMMIT;

        INSERT INTO IFRS_LGD_WORKOUT
        (
            EFF_DATE,
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
            DATA_SOURCE,
            LGD_FLAG,
            SPECIAL_REASON
        )
        SELECT
            V_EFF_DATE,
            LAST_DAY(V_DATE) DOWNLOAD_DATE,
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
            SUM(A.RECOV_AMT_BF_NPV) RECOV_AMT_BF_NPV,
            MAX(A.RECOVERY_DATE) LAST_RECOV_DATE,
            ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 AS RECOV_PERCENTAGE,
            A.TOTAL_LOSS_AMT,
            A.DISCOUNT_RATE,
            SUM(A.REC_AMOUNT) AS RECOVERY_AMOUNT,
            100-ROUND(SUM(A.REC_AMOUNT)/A.TOTAL_LOSS_AMT,4)*100 LOSS_RATE,
            'ILS' DATA_SOURCE,
            A.LGD_FLAG,
            A.SPECIAL_REASON
        FROM IFRS_LGD_PROCESS A
            LEFT JOIN IFRS_LGD_RULES_CONFIG B ON A.SEGMENTATION_ID=B.SEGMENTATION_ID
                AND A.LGD_RULE_ID=B.PKID
        WHERE A.DOWNLOAD_DATE	= LAST_DAY(V_DATE) AND VALUATION_TYPE IS NULL
            --AND PRODUCT_CODE NOT IN ('101','CARDS')
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
            A.DATA_SOURCE,
            A.LGD_FLAG,
            A.SPECIAL_REASON;
        COMMIT;

        V_DATE := ADD_MONTHS(V_DATE,1);
    END LOOP;

    INSERT INTO IFRS_LGD_WORKOUT
    (
        EFF_DATE,
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
        DATA_SOURCE,
        LGD_FLAG,
        SPECIAL_REASON
    )
    SELECT V_EFF_DATE,
        LAST_DAY(A.FIRST_NPL_DATE) DOWNLOAD_DATE,
        A.PRODUCT_CODE,
        NVL(C.PRD_DESC,'-') PRODUCT_NAME,
        NVL(D.PKID,0) MASTER_ID,
        A.ACCOUNT_NUMBER,
        A.CUSTOMER_NUMBER,
        A.CUSTOMER_NAME,
        NULL LGD_CUSTOMER_TYPE,
        E.SEGMENTATION_ID   AS SEGMENTATION_ID,
        F.SEGMENT           AS SEGMENTATION_NAME,
        E.PKID              AS LGD_RULE_ID,
        E.LGD_RULE_NAME     AS LGD_RULE_NAME,
        'IDR' CURRENCY,
        A.CURRENCY ORIGINAL_CURRENCY,
        A.FIRST_NPL_DATE,
        CASE WHEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) < B.CLOSED_DATE THEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) ELSE B.CLOSED_DATE END AS CLOSED_DATE,
        NULL BI_COLLECTABILITY_NPL,
        NULL BI_COLLECTABILITY_CLOSED,
        B.ADJ_REC_AMOUNT_BF_NPV RECOV_AMT_BF_NPV,
        CASE WHEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) < B.CLOSED_DATE THEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) ELSE B.CLOSED_DATE END LAST_RECOV_DATE,
        ROUND((B.ADJ_REC_AMOUNT_BF_NPV/POWER((1+A.INTEREST_RATE), FN_LGD_DAYS_30_360 (A.FIRST_NPL_DATE,CASE WHEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) < B.CLOSED_DATE THEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) ELSE B.CLOSED_DATE END)/360))/B.ADJ_REC_AMOUNT_BF_NPV,4)*100 AS RECOV_PERCENTAGE,
        B.ADJ_REC_AMOUNT_BF_NPV TOTAL_LOSS_AMT,
        A.INTEREST_RATE DISCOUNT_RATE,
        (B.ADJ_REC_AMOUNT_BF_NPV/POWER((1+A.INTEREST_RATE),
            FN_LGD_DAYS_30_360 (A.FIRST_NPL_DATE,B.CLOSED_DATE)/360)
        )  AS RECOVERY_AMOUNT,
        100-ROUND((B.ADJ_REC_AMOUNT_BF_NPV/POWER((1+A.INTEREST_RATE), FN_LGD_DAYS_30_360 (A.FIRST_NPL_DATE,CASE WHEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) < B.CLOSED_DATE THEN ADD_MONTHS(LAST_DAY(A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) ELSE B.CLOSED_DATE END)/360))/B.ADJ_REC_AMOUNT_BF_NPV,4)*100  LOSS_RATE,
        'ILS' DATA_SOURCE,
        A.LGD_FLAG,
        'INCLUDED' AS SPECIAL_REASON
    FROM
    (
        SELECT A2.*
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT A3.ACCOUNT_NUMBER,
                MAX(A3.DOWNLOAD_DATE) MAX_DOWNLOAD_DATE
            FROM TMP_LGD_IMA A3
            JOIN TBLU_LGD_INCLUDED_LOAN B3
            ON A3.DOWNLOAD_DATE <= LAST_DAY(B3.CLOSED_DATE)
            AND A3.ACCOUNT_NUMBER = B3.ACCOUNT_NUMBER
            AND B3.ACCOUNT_NUMBER NOT IN
            (SELECT ACCOUNT_NUMBER FROM IFRS_LGD_WORKOUT WHERE EFF_DATE = V_EFF_DATE)
            AND B3.ACCOUNT_NUMBER NOT IN
            (SELECT ACCOUNT_NUMBER FROM TBLU_LGD_EXCLUDED_LOAN)
            GROUP BY A3.ACCOUNT_NUMBER
        ) B2
        ON A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
        AND A2.DOWNLOAD_DATE = B2.MAX_DOWNLOAD_DATE
    ) A
    JOIN TBLU_LGD_INCLUDED_LOAN B
    ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
    JOIN IFRS_MASTER_PRODUCT_PARAM C ON A.PRODUCT_CODE=C.PRD_CODE
            AND C.DATA_SOURCE = 'ILS'
    LEFT JOIN IFRS_MASTERID D ON A.ACCOUNT_NUMBER = D.MASTER_ACCOUNT_CODE
    JOIN
      (
        Select a.pkid as LGD_SEGMENTATION_ID, b.pkid as SEGMENT_RULE_ID--, a.sub_segment, b.sub_segment
        from ifrs_mstr_segment_rules_header a
        join ifrs_mstr_segment_rules_header b
        on a.segment_type = 'LGD_SEG'
        and b.segment_type = 'PORTFOLIO_SEG'
        and a.group_Segment = b.group_segment
      ) D
         ON A.SEGMENT_RULE_ID = D.SEGMENT_RULE_ID
      INNER JOIN IFRS_LGD_RULES_CONFIG E ON D.LGD_SEGMENTATION_ID = E.SEGMENTATION_ID
      INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER F
         ON E.SEGMENTATION_ID = F.PKID;
    COMMIT;
END;