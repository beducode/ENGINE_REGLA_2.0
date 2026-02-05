CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_DATA_BCA_WORKOUT(V_EFF_DATE DATE)
AS
    V_WORKOUT_PERIOD NUMBER;
BEGIN
    V_WORKOUT_PERIOD := 36;

    DELETE IFRS_LGD_ER_NPL_ACCT_WORKOUT
    WHERE EFF_DATE = V_EFF_DATE;
    COMMIT;

    DELETE IFRS_LGD_DATA_DETAIL_WORKOUT
    WHERE EFF_DATE = V_EFF_DATE;
    COMMIT;

    INSERT INTO IFRS_LGD_ER_NPL_ACCT_WORKOUT (EFF_DATE,
                                      DOWNLOAD_DATE,
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
                                      DATA_SOURCE,
                                      LGD_FLAG)
       SELECT  DISTINCT
              V_EFF_DATE,
              LAST_DAY (A.FIRST_NPL_DATE) AS DOWNLOAD_DATE,
              NVL (B.PKID, 0)     AS MASTER_ID,
              A.ACCOUNT_NUMBER    AS ACCOUNT_NUMBER,
              A.CUSTOMER_NUMBER   AS CUSTOMER_NUMBER,
              MAX(A.CUSTOMER_NAME)AS CUSTOMER_NAME,
              A.FIRST_NPL_DATE    AS NPL_DATE,
              CASE WHEN ADD_MONTHS(LAST_DAY (A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) < A.CLOSED_DATE THEN ADD_MONTHS(LAST_DAY (A.FIRST_NPL_DATE), V_WORKOUT_PERIOD) ELSE A.CLOSED_DATE END AS CLOSED_DATE,
              ' '                 AS BI_COLLECTABILITY_NPL,
              ' '                 AS BI_COLLECTABILITY_CLOSED,
              ROUND(A.FIRST_NPL_OS * C.RATE_AMOUNT,2)  AS OUTSTANDING_DEFAULT,
              ' '                 AS LGD_CUSTOMER_TYPE,
              E.SEGMENTATION_ID   AS SEGMENTATION_ID,
              F.SEGMENT           AS SEGMENTATION_NAME,
              E.PKID              AS LGD_RULE_ID,
              E.LGD_RULE_NAME     AS LGD_RULE_NAME,
              A.PRODUCT_CODE      AS PRODUCT_CODE,
              'IDR'               AS CURRENCY,
              A.CURRENCY          AS ORIGINAL_CURRENCY,
              A.INTEREST_RATE     AS INTEREST_RATE,
              'ILS',
              A.LGD_FLAG
         FROM TMP_LGD_IMA A
              LEFT JOIN IFRS_MASTERID B ON A.ACCOUNT_NUMBER = B.MASTER_ACCOUNT_CODE
              JOIN (Select case when download_date < '31 JAN 2011' then last_day(download_date) else download_date end download_date, currency, rate_amount from ifrs_master_exchange_rate) c
                on last_day(A.first_npl_date) = c.download_date
                and A.currency = c.currency
              INNER JOIN
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
                 ON E.SEGMENTATION_ID = F.PKID
        WHERE A.ACCOUNT_NUMBER NOT IN
        (SELECT ACCOUNT_NUMBER FROM TBLU_LGD_EXCLUDED_LOAN)
        AND A.ACCOUNT_NUMBER NOT IN (SELECT DISTINCT ACCOUNT_NUMBER FROM IFRS_LGD WHERE EFF_DATE = V_EFF_DATE)
        AND NVL(A.SEGMENT_RULE_ID,0) != 0
        GROUP BY
              B.PKID,
              A.ACCOUNT_NUMBER,
              A.CUSTOMER_NUMBER,
              A.FIRST_NPL_DATE,
              A.CLOSED_DATE,
              A.FIRST_NPL_OS,
              C.RATE_AMOUNT,
              E.SEGMENTATION_ID,
              F.SEGMENT,
              E.PKID,
              E.LGD_RULE_NAME,
              A.PRODUCT_CODE,
              A.CURRENCY,
              A.INTEREST_RATE,
              A.LGD_FLAG
--        OR (NVL(A.FLAG,' ') = 'L' AND NVL(E.WORKOUT_PERIOD,0) > 0 AND V_CURRDATE - A.CLOSED_DATE >= NVL(E.WORKOUT_PERIOD,0))
        ;

    COMMIT;

    --UPDATE /INSERT TABLE IFRS_LGD_DATA_DETAIL_WORKOUT_K

    INSERT INTO IFRS_LGD_DATA_DETAIL_WORKOUT (EFF_DATE,
                                      MASTER_ID,
                                      ACCOUNT_NUMBER,
                                      CUSTOMER_NUMBER,
                                      ACCOUNT_STATUS,
                                      SEGMENTATION_ID,
                                      PAYMENT_DATE,
                                      CURRENCY,
                                      RECOVERY_AMOUNT,
                                      IS_EFFECTIVE,
                                      DATA_SOURCE)
       SELECT V_EFF_DATE,
              A.MASTER_ID       AS MASTER_ID,
              A.ACCOUNT_NUMBER,
              A.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
              B.ACCOUNT_STATUS,
              A.SEGMENTATION_ID AS SEGMENTATION_ID,
              B.DOWNLOAD_DATE AS PAYMENT_DATE,
              A.CURRENCY,
              Round(B.RECOVERY_AMOUNT * c.rate_amount,2) RECOVERY_AMOUNT,
              1 IS_EFFECTIVE,
              A.DATA_SOURCE
         FROM IFRS_LGD_ER_NPL_ACCT_WORKOUT A
              JOIN TMP_LGD_IMA B ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
              AND B.DOWNLOAD_DATE <= A.CLOSED_DATE
              JOIN (Select case when download_date < '31 JAN 2011' then last_day(download_date) else download_date end download_date, currency, rate_amount from ifrs_master_exchange_rate) c
                on last_day(b.first_npl_date) = c.download_date
                and b.currency = c.currency
        WHERE
            A.EFF_DATE = V_EFF_DATE
            AND B.RECOVERY_AMOUNT > 0
            ;
    COMMIT;

    /* INSERT PREVIOUS LGD DATA */
    INSERT INTO IFRS_LGD_ER_NPL_ACCT_WORKOUT (EFF_DATE,
                                      DOWNLOAD_DATE,
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
                                      DATA_SOURCE,
                                      LGD_FLAG)
    SELECT
        EFF_DATE,
        DOWNLOAD_DATE,
        MASTER_ID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        NPL_DATE,
        CASE WHEN ADD_MONTHS(LAST_DAY(NPL_DATE), V_WORKOUT_PERIOD) < CLOSED_DATE THEN ADD_MONTHS(LAST_DAY(NPL_DATE), V_WORKOUT_PERIOD) ELSE CLOSED_DATE END AS CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        TOTAL_LOSS_AMT OUTSTANDING_DEFAULT,
        LGD_CUSTOMER_TYPE,
        SEGMENTATION_ID,
        SEGMENTATION_NAME,
        LGD_RULE_ID,
        LGD_RULE_NAME,
        PRODUCT_CODE,
        CURRENCY,
        ORIGINAL_CURRENCY,
        DISCOUNT_RATE INTEREST_RATE,
        DATA_SOURCE,
        LGD_FLAG
    FROM IFRS_LGD
    WHERE EFF_DATE = V_EFF_DATE
    AND DATA_SOURCE = 'ILS';
    COMMIT;


    INSERT INTO IFRS_LGD_DATA_DETAIL_WORKOUT
    (
        EFF_DATE,
        MASTER_ID,
        ACCOUNT_NUMBER,
        CUSTOMER_NUMBER,
        ACCOUNT_STATUS,
        SEGMENTATION_ID,
        PAYMENT_DATE,
        CURRENCY,
        EXCHANGE_RATE,
        RECOVERY_AMOUNT,
        RECOVERY_TYPE,
        IS_EFFECTIVE,
        DATA_SOURCE,
        CREATEDBY,
        CREATEDDATE,
        CREATEDHOST,
        UPDATEDBY,
        UPDATEDDATE,
        UPDATEDHOST
    )
    SELECT V_EFF_DATE EFF_DATE,
        A.MASTER_ID,
        A.ACCOUNT_NUMBER,
        A.CUSTOMER_NUMBER,
        A.ACCOUNT_STATUS,
        A.SEGMENTATION_ID,
        A.PAYMENT_DATE,
        A.CURRENCY,
        A.EXCHANGE_RATE,
        A.RECOVERY_AMOUNT,
        A.RECOVERY_TYPE,
        A.IS_EFFECTIVE,
        A.DATA_SOURCE,
        A.CREATEDBY,
        SYSDATE CREATEDDATE,
        A.CREATEDHOST,
        A.UPDATEDBY,
        A.UPDATEDDATE,
        A.UPDATEDHOST
    FROM IFRS_LGD_DATA_DETAIL A
    JOIN IFRS_LGD B
    ON A.EFF_DATE = B.EFF_DATE
    AND A.EFF_DATE = V_EFF_DATE
    AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
    AND B.DATA_SOURCE = 'ILS'
    AND A.PAYMENT_DATE <= ADD_MONTHS(LAST_DAY(B.NPL_DATE), V_WORKOUT_PERIOD);
    COMMIT;

    END;