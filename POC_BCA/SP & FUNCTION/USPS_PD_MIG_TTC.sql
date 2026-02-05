CREATE OR REPLACE PROCEDURE USPS_PD_MIG_TTC (
    v_eff_date    date,
    v_pd_rule_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN
    OPEN Cur_out FOR

    SELECT D.PD_RULE_NAME "PD Model", A.EFF_DATE "Period",
    A.BUCKET_ID "Bucket Id", B.BUCKET_NAME "Bucket Name",
    ROUND(A.TTC * 100, 6) "Average Default Rate (%)"
    FROM IFRS_PD_MIG_TTC A
    JOIN IFRS_BUCKET_DETAIL B
    ON A.BUCKET_GROUP = B.BUCKET_GROUP
    AND A.BUCKET_ID = B.BUCKET_ID
    JOIN IFRS_PD_RULES_CONFIG D
    ON A.PD_RULE_ID = D.PKID
    AND A.PD_RULE_ID = TO_CHAR(v_pd_rule_id)
    AND A.MODEL_ID = 0
    AND A.EFF_DATE = TO_CHAR(v_eff_date,'dd MON yyyy')
    ORDER BY B.BUCKET_ID;
END;