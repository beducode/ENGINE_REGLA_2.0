CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_CARD(V_EFF_DATE DATE)
AS
   V_MAX_DATE DATE;
BEGIN
    DELETE IFRS_LGD
    WHERE EFF_DATE = V_EFF_DATE
    AND DATA_SOURCE = 'CRD';
    COMMIT;

    SELECT MAX(EFF_DATE)
    INTO V_MAX_DATE
    FROM IFRS_LGD WHERE DATA_SOURCE = 'CRD';

    INSERT INTO IFRS_LGD
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
        CURRENCY,
        ORIGINAL_CURRENCY,
        NPL_DATE,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        TOTAL_LOSS_AMT,
        RECOV_AMT_BF_NPV,
        LAST_RECOV_DATE,
        RECOV_PERCENTAGE,
        DISCOUNT_RATE,
        LOSS_RATE,
        RECOVERY_AMOUNT,
        DATA_SOURCE,
        LGD_RULE_ID,
        LGD_RULE_NAME,
        LGD_FLAG,
        CREATEDDATE,
        CREATEDBY,
        CREATEDHOST
    )
    SELECT V_EFF_DATE EFF_DATE,
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
        NPL_DATE,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        TOTAL_LOSS_AMT,
        RECOV_AMT_BF_NPV,
        LAST_RECOV_DATE,
        RECOV_PERCENTAGE,
        DISCOUNT_RATE,
        LOSS_RATE,
        RECOVERY_AMOUNT,
        DATA_SOURCE,
        LGD_RULE_ID,
        LGD_RULE_NAME,
        LGD_FLAG,
        CREATEDDATE,
        CREATEDBY,
        CREATEDHOST
    FROM IFRS_LGD
    WHERE EFF_DATE = V_MAX_DATE
    AND DATA_SOURCE = 'CRD'
    AND DOWNLOAD_DATE < '1 JAN 20'; --Temporary hardcode
    COMMIT;

    INSERT INTO IFRS_LGD
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
        CURRENCY,
        ORIGINAL_CURRENCY,
        NPL_DATE,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        TOTAL_LOSS_AMT,
        RECOV_AMT_BF_NPV,
        LAST_RECOV_DATE,
        RECOV_PERCENTAGE,
        DISCOUNT_RATE,
        LOSS_RATE,
        RECOVERY_AMOUNT,
        DATA_SOURCE,
        LGD_RULE_ID,
        LGD_RULE_NAME,
        LGD_FLAG
    )
    Select distinct V_EFF_DATE as Effective_date,
    a.first_npl_date,
    a.Product_Code,
    NULL Product_Name,
    a.masterid,
    a.Account_number,
    a.Customer_number,
    a.customer_name,
    null lgd_customer_type,
    case when a.segment_rule_id in (52,20463, 20462) then '142' else '143' end segmentation_id,
    case when a.segment_rule_id in (52,20463, 20462) then 'CREDIT CARD ORGANIZATION' else 'CREDIT CARD INDIVIDUAL' end segmentation_name,
    a.currency,
    a.currency original_currency,
    a.first_npl_date npl_date,
    a.download_date closed_date,
    null default_status_at_loss_date,
    null default_status_at_close_date,
    a.first_npl_os total_loss_amt,
    a.first_npl_os recovery_amt_bf_npv,
    a.download_date last_recovery_date,
    0 Recovery_percentage,
    case
           when a.INTEREST_RATE = 0.0027 or a.INTEREST_RATE = 0.0021
               then a.INTEREST_RATE * 100
           when a.INTEREST_RATE >= 1
               then (a.INTEREST_RATE * 12) / 100
           else
               a.INTEREST_RATE
           end discount_rate,
    0 loss_rate,
    (a.first_npl_os/POWER(1+case
           when a.INTEREST_RATE = 0.0027 or a.INTEREST_RATE = 0.0021
               then a.INTEREST_RATE * 100
           when a.INTEREST_RATE >= 1
               then (a.INTEREST_RATE * 12) / 100
           else
               a.INTEREST_RATE
           end,
            FN_LGD_DAYS_30_360 (A.first_npl_date,A.download_date)/360)
        ) Recovery_amount,
    'CRD' data_source,
    case when a.segment_rule_id in (52,20463, 20462) then '12' else '11' end lgd_rule_id,
    case when a.segment_rule_id in (52,20463, 20462) then 'LGD_CREDIT_CARD_ORGANIZATION' else 'LGD_CREDIT_CARD' end lgd_rule_name,
    'C'
    FROM TMP_LGD_IMA A
    JOIN
    (
        SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
        FROM TMP_LGD_IMA
        WHERE DATA_SOURCE = 'CRD'
        AND ACCOUNT_STATUS = 'C'
        AND OUTSTANDING = 0
        GROUP BY ACCOUNT_NUMBER
    ) b
    on b.ACCOUNT_NUMBER = a.ACCOUNT_NUMBER
    and a.download_date = b.MIN_DOWNLOAD_DATE
--    and a.download_date between V_MAX_DATE + 1 and V_EFF_DATE --Temporary hardcode
    and a.download_date between '1 JAN 2020' and V_EFF_DATE
    JOIN
    (
        SELECT A2.ACCOUNT_NUMBER
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT ACCOUNT_NUMBER, MAX(DOWNLOAD_DATE) MAX_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            WHERE DATA_SOURCE = 'CRD'
            GROUP BY ACCOUNT_NUMBER
        ) B2
        ON A2.DOWNLOAD_DATE = B2.MAX_DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
        AND A2.ACCOUNT_STATUS = 'C'
        AND A2.OUTSTANDING = 0
    ) c
    on A.ACCOUNT_NUMBER = C.ACCOUNT_NUMBER
    and a.account_number not in
    (
    Select account_number from ifrs_lgd
    where eff_date = V_EFF_DATE
    )
    and a.account_number not in
    (select account_number from TBLU_LGD_EXCLUDED_CARD)
    and a.first_npl_os > 0;

    COMMIT;

    INSERT INTO IFRS_LGD
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
        CURRENCY,
        ORIGINAL_CURRENCY,
        NPL_DATE,
        CLOSED_DATE,
        BI_COLLECTABILITY_NPL,
        BI_COLLECTABILITY_CLOSED,
        TOTAL_LOSS_AMT,
        RECOV_AMT_BF_NPV,
        LAST_RECOV_DATE,
        RECOV_PERCENTAGE,
        DISCOUNT_RATE,
        LOSS_RATE,
        RECOVERY_AMOUNT,
        DATA_SOURCE,
        LGD_RULE_ID,
        LGD_RULE_NAME,
        LGD_FLAG
    )
    Select distinct V_EFF_DATE as Effective_date,
    a.Download_date,
    a.Product_Code,
    NULL Product_Name,
    a.masterid,
    a.Account_number,
    a.Customer_number,
    a.customer_name,
    null lgd_customer_type,
    case when a.segment_rule_id in (52,20463, 20462) then '142' else '143' end segmentation_id,
    case when a.segment_rule_id in (52,20463, 20462) then 'CREDIT CARD ORGANIZATION' else 'CREDIT CARD INDIVIDUAL' end segmentation_name,
    a.currency,
    a.currency original_currency,
    a.download_date npl_date,
    case
        when b.CHARGEOFF_AMOUNT = 0
            then nvl(b.MIN_DOWNLOAD_DATE, b.CHARGEOFF_DATE)
        else
            b.CHARGEOFF_DATE
    end closed_date,
    null default_status_at_loss_date,
    null default_status_at_close_date,
    a.first_npl_os total_loss_amt,
    case when b.chargeoff_status = 'FULLPAID' then a.first_npl_os when b.chargeoff_amount > a.first_npl_os then 0 else a.first_npl_os - b.chargeoff_amount end recovery_amt_bf_npv,
    case
        when b.CHARGEOFF_AMOUNT = 0
            then nvl(b.MIN_DOWNLOAD_DATE, b.CHARGEOFF_DATE)
        else
            b.CHARGEOFF_DATE
    end last_recovery_date,
    0 Recovery_percentage,
    case
           when case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end = 0.0027 or case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end = 0.0021
               then case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end * 100
           when case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end >= 1
               then (case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end * 12) / 100
           else
               case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end
           end discount_rate,
    0 loss_rate,
    (case when b.chargeoff_status = 'FULLPAID' then a.first_npl_os when b.chargeoff_amount > a.first_npl_os then 0 else a.first_npl_os - b.chargeoff_amount end/POWER(
        (1+
           case
           when case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end = 0.0027 or case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end = 0.0021
               then case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end * 100
           when case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end >= 1
               then (case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end * 12) / 100
           else
               case when a.download_date >= '1 JAN 2019' THEN ROUND(a.INTEREST_RATE,2) / 100 else ROUND(a.INTEREST_RATE,2) end
           end),
            FN_LGD_DAYS_30_360 (A.download_date,case
                                                    when b.CHARGEOFF_AMOUNT = 0
                                                        then nvl(b.MIN_DOWNLOAD_DATE, b.CHARGEOFF_DATE)
                                                    else
                                                        b.CHARGEOFF_DATE
                end) / 360)
        ) Recovery_amount,
    'CRD' data_source,
    case when a.segment_rule_id in (52,20463, 20462) then '12' else '11' end lgd_rule_id,
    case when a.segment_rule_id in (52,20463, 20462) then 'LGD_CREDIT_CARD_ORGANIZATION' else 'LGD_CREDIT_CARD' end lgd_rule_name,
    SUBSTR(b.chargeoff_status,1,1)
    from ifrs_lgd_first_npl_date A
    join (select *
          from ifrs_stg_crd_charge a
                   left join (SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
                              FROM TMP_LGD_IMA
                              WHERE DATA_SOURCE = 'CRD'
                                AND ACCOUNT_STATUS = 'C'
                                AND OUTSTANDING = 0
                              GROUP BY ACCOUNT_NUMBER) b on a.CUSTOMER_NUMBER = b.ACCOUNT_NUMBER) b
    on b.customer_number = a.account_number
    and a.download_date <= last_day(b.chargeoff_date)
    --and b.chargeoff_date between V_MAX_DATE + 1 and V_EFF_DATE --Temporary hardcode
    and b.chargeoff_date between '1 JAN 2020' and V_EFF_DATE
    and case when b.chargeoff_status = 'FULLPAID' then 1 else b.chargeoff_amount end > 0
    and a.account_number not in
    (
    Select account_number from ifrs_lgd
    where eff_date = V_EFF_DATE
    )
    and a.account_number not in
    (select account_number from TBLU_LGD_EXCLUDED_CARD)
    and a.first_npl_os > 0;

    COMMIT;

END;