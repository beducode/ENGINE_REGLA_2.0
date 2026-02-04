CREATE OR REPLACE PROCEDURE USPS_PD_ASSET_CORRELATION
(
    v_eff_date    date,
    v_pd_rule_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR

        SELECT DISTINCT
            B.PD_RULE_NAME AS "PD Model",
            A.EFF_DATE AS "Period",
            A.BUCKET_ID AS "Bucket ID",
            C.BUCKET_NAME AS "Bucket Name",
            ROUND(D.PD, 6) AS "PD %",
            ROUND(A.ASSET_CORRELATION, 6) AS "Asset Correlation %"
        FROM IFRS_PD_VAS_CORRELATION A
        JOIN IFRS_PD_RULES_CONFIG B
            ON A.PD_RULE_ID = B.PKID
        JOIN IFRS_BUCKET_DETAIL C
            ON A.BUCKET_ID = C.BUCKET_ID
            AND A.BUCKET_GROUP = C.BUCKET_GROUP
        JOIN IFRS_PD_VAS D
            ON D.PD_RULE_ID = A.PD_RULE_ID
            AND D.BUCKET_ID = A.BUCKET_ID
            AND D.BUCKET_GROUP = A.BUCKET_GROUP
            AND A.EFF_DATE = D.EFF_DATE
        WHERE A.PD_RULE_ID = v_pd_rule_id
            AND A.EFF_DATE = v_eff_date
        ORDER BY A.BUCKET_ID;

END;