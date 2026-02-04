CREATE OR REPLACE PROCEDURE USPS_PD_MIG_ODR (
    v_eff_date    date,
    v_pd_rule_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN
    OPEN Cur_out FOR
    SELECT
        D.PD_RULE_NAME "PD Model",
        A.EFF_DATE "Period To",
        ROUND(ODR * 100, 6) AS "Default Rate (%)"
    FROM IFRS_PD_MIG_ODR A
    JOIN IFRS_PD_RULES_CONFIG D
    ON A.PD_RULE_ID = D.PKID
    AND A.PD_RULE_ID = TO_CHAR(v_pd_rule_id)
    AND A.EFF_DATE <= TO_CHAR(v_eff_date,'dd MON yyyy')
    ORDER BY A.EFF_DATE DESC;
END;