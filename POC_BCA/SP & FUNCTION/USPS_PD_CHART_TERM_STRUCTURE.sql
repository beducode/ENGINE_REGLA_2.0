CREATE OR REPLACE PROCEDURE  USPS_PD_CHART_TERM_STRUCTURE
(
    v_eff_date    date,
    v_pd_rule_id  number,
    v_model_id   number,
    v_bucket_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR

    SELECT DISTINCT ROUND(A.PD, 6) PD, A.FL_YEAR, A.MODEL_ID
    FROM IFRS_PD_TERM_STRUCTURE A
    WHERE A.EFF_DATE = v_eff_date
    AND A.PD_RULE_ID = v_pd_rule_id
    AND A.MODEL_ID = v_model_id
    AND A.BUCKET_ID = v_bucket_id
    AND A.TM_TYPE = 'YEAR';

END;