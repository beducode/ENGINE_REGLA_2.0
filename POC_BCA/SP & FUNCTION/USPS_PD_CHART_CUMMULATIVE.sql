CREATE OR REPLACE PROCEDURE  USPS_PD_CHART_CUMMULATIVE
(
    v_eff_date    date,
    v_pd_rule_id  number,
    v_bucket_id   number,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR

    SELECT A.BUCKET_ID, A.FL_YEAR, ROUND(A.CUMULATIVE_PD, 6) CUMULATIVE_PD
    FROM IFRS_PD_VAS_CUMULATIVE A
    WHERE A.BUCKET_ID = v_bucket_id
    AND A.EFF_DATE = v_eff_date
    AND A.PD_RULE_ID = v_pd_rule_id;

END;