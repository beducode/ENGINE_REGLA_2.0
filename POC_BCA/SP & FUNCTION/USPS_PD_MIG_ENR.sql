CREATE OR REPLACE PROCEDURE USPS_PD_MIG_ENR (
    v_eff_date    date,
    v_pd_rule_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query varchar2(4000);
    v_ColumnName varchar2(4000);
    v_Count number(18);
BEGIN

    SELECT COUNT(*)
    INTO v_Count
    FROM IFRS_PD_MIG_ENR A2
    JOIN IFRS_BUCKET_DETAIL B2
    ON A2.BUCKET_GROUP = B2.BUCKET_GROUP
    AND A2.BUCKET_FROM = B2.BUCKET_ID
    AND A2.PD_RULE_ID = v_pd_rule_id
    AND A2.EFF_DATE = v_eff_date
    JOIN VW_IFRS_MAX_BUCKET C2
    ON B2.BUCKET_GROUP = C2.BUCKET_GROUP
    AND B2.BUCKET_ID <= C2.MAX_BUCKET_ID;

    SELECT LISTAGG('''' || IMPAIRMENT_BUCKET || ''' AS "' || IMPAIRMENT_BUCKET || '"', ',') WITHIN GROUP (ORDER BY BUCKET_ID)
        INTO v_ColumnName
    FROM
    (
        SELECT B2.BUCKET_ID,
            B2.IMPAIRMENT_BUCKET
        FROM IFRS_PD_MIG_ENR A2
        JOIN IFRS_BUCKET_DETAIL B2
        ON A2.BUCKET_GROUP = B2.BUCKET_GROUP
        AND A2.BUCKET_FROM = B2.BUCKET_ID
        AND A2.PD_RULE_ID = v_pd_rule_id
        AND A2.EFF_DATE = v_eff_date
        JOIN VW_IFRS_MAX_BUCKET C2
        ON B2.BUCKET_GROUP = C2.BUCKET_GROUP
        AND B2.BUCKET_ID <= C2.MAX_BUCKET_ID
        GROUP BY B2.BUCKET_ID,
            B2.IMPAIRMENT_BUCKET
    ) A;

    v_Query := 'SELECT A.*, B.Total FROM
                (
                    SELECT * FROM
                    (
                        SELECT DISTINCT
                            A.BASE_DATE "Period From",
                            A.EFF_DATE "Period To",
                            B.BUCKET_ID "Bucket ID",
                            B.IMPAIRMENT_BUCKET AS "Bucket Name",
                            C.IMPAIRMENT_BUCKET AS BUCKET_TO,
                            A.CALC_AMOUNT
                        FROM IFRS_PD_MIG_ENR A
                        JOIN IFRS_BUCKET_DETAIL B
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP
                        AND A.BUCKET_FROM = B.BUCKET_ID
                        JOIN IFRS_BUCKET_DETAIL C
                        ON A.BUCKET_GROUP = C.BUCKET_GROUP
                        AND A.BUCKET_TO = C.BUCKET_ID
                        JOIN IFRS_PD_RULES_CONFIG D
                        ON A.PD_RULE_ID = D.PKID
                        AND A.PD_RULE_ID = ' || TO_CHAR(v_pd_rule_id) || '
                        AND A.EFF_DATE BETWEEN  ADD_MONTHS(''' || TO_CHAR(v_eff_date,'dd MON yyyy') || ''', -1 * D.HISTORICAL_DATA) AND ''' || TO_CHAR(v_eff_date,'dd MON yyyy') || '''
                    ) TMP
                    PIVOT
                    ( SUM(CALC_AMOUNT)
                        FOR BUCKET_TO IN (' || v_ColumnName || ')
                    ) PVT
                ) A
                JOIN
                (
                    SELECT
                        B.BUCKET_ID,
                        SUM(A.CALC_AMOUNT) AS TOTAL
                    FROM IFRS_PD_MIG_ENR A
                    JOIN IFRS_BUCKET_DETAIL B
                    ON A.BUCKET_GROUP = B.BUCKET_GROUP
                    AND A.BUCKET_FROM = B.BUCKET_ID
                    JOIN IFRS_PD_RULES_CONFIG D
                    ON A.PD_RULE_ID = D.PKID
                    AND A.PD_RULE_ID = ' || TO_CHAR(v_pd_rule_id) || '
                    AND A.EFF_DATE BETWEEN  ADD_MONTHS(''' || TO_CHAR(v_eff_date,'dd MON yyyy') || ''', -1 * D.HISTORICAL_DATA) AND ''' || TO_CHAR(v_eff_date,'dd MON yyyy') || '''
                    GROUP BY B.BUCKET_ID
                ) B ON A."Bucket ID" = B.BUCKET_ID
                ORDER BY A."Period To" DESC, A."Bucket ID"';

    IF v_Count > 0 THEN
    OPEN Cur_out FOR v_Query;
    END IF;
END;