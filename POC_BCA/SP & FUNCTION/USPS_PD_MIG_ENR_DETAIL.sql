CREATE OR REPLACE PROCEDURE  USPS_PD_MIG_ENR_DETAIL (
    v_bucket_id number,
    v_eff_date    date,
    v_pd_rule_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_common_code_1 varchar2(50);
    v_common_code_2 varchar2(50);
    v_query_temp varchar2(500);
    v_query varchar2(4500);
BEGIN

    SELECT
        CASE WHEN CALC_METHOD LIKE 'A%' THEN 'Account Number'
             WHEN CALC_METHOD LIKE 'C%' THEN 'Customer Number'
             ELSE ''
        END,
        CASE WHEN CALC_METHOD LIKE '%OS' THEN 'Outstanding in IDR'
             WHEN CALC_METHOD LIKE '%VS' THEN 'Fair Value'
             WHEN CALC_METHOD LIKE '%NOA' THEN 'Number of Account'
             WHEN CALC_METHOD LIKE '%NOC' THEN 'Number of Customer'
             ELSE ''
         END
    INTO v_common_code_1, v_common_code_2
    FROM IFRS_PD_RULES_CONFIG
    WHERE PKID = v_pd_rule_id;

    IF(LENGTH(v_common_code_1) > 0)
    THEN
        v_query_temp :=
        ' A.PD_UNIQUE_ID AS "' || v_common_code_1 || '",
        A.CUSTOMER_NAME "Customer Name",
        B.IMPAIRMENT_BUCKET "Bucket From",
        C.IMPAIRMENT_BUCKET "Bucket To",
        A.CALC_AMOUNT AS "' || v_common_code_2 || '" ';
    ELSE
        v_query_temp :=
        ' A.CUSTOMER_NAME "Customer Name",
        B.IMPAIRMENT_BUCKET "Bucket From",
        C.IMPAIRMENT_BUCKET "Bucket To" ';
    END IF;

    v_query :=
        'SELECT ' || v_query_temp || '
            FROM IFRS_PD_MIGRATION_DETAIL A
            JOIN IFRS_BUCKET_DETAIL B
            ON A.BUCKET_GROUP = B.BUCKET_GROUP
            AND A.BUCKET_FROM = B.BUCKET_ID
            JOIN IFRS_BUCKET_DETAIL C
            ON A.BUCKET_GROUP = C.BUCKET_GROUP
            AND A.BUCKET_TO = C.BUCKET_ID
            JOIN IFRS_PD_RULES_CONFIG D
            ON A.PD_RULE_ID = D.PKID
            AND A.PD_RULE_ID = ' ||  TO_CHAR(v_pd_rule_id) || '
            AND A.EFF_DATE = ''' || TO_CHAR(v_eff_date, 'dd-MON-yyyy') || '''
            AND B.BUCKET_ID = ' || TO_CHAR(v_bucket_id) || '
        ORDER BY A.BUCKET_TO, A.PD_UNIQUE_ID';

    OPEN Cur_out FOR v_query;

END;