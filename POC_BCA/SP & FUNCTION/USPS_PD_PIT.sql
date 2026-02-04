CREATE OR REPLACE PROCEDURE  USPS_PD_PIT
(
    v_eff_date    date,
    v_pd_rule_id  number,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_query VARCHAR2(4000);
    v_columnAlias VARCHAR2(4000);
    v_columnName VARCHAR2(4000);
    v_Count number(18);
BEGIN

        SELECT COUNT(*)
        INTO v_Count
        FROM IFRS_PD_VAS_PIT
        WHERE PD_RULE_ID = v_pd_rule_id
        AND EFF_DATE = v_eff_date;

        SELECT LISTAGG('"' || TO_CHAR(FL_YEAR) || '" AS "' || TO_CHAR(FL_YEAR) || '"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_PD_VAS_PIT
            WHERE PD_RULE_ID = v_pd_rule_id
            AND EFF_DATE = v_eff_date
        );

        SELECT LISTAGG(TO_CHAR(FL_YEAR),',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnName
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_PD_VAS_PIT
            WHERE PD_RULE_ID = v_pd_rule_id
            AND EFF_DATE = v_eff_date
        );

        v_query := 'SELECT PD_RULE_NAME AS "PD Model", EFF_DATE AS "Period", BUCKET_ID AS "Bucket ID", BUCKET_NAME AS "Bucket Name", ' || v_columnAlias || ' FROM
                    (
                        SELECT
                            B.PD_RULE_NAME, A.EFF_DATE, A.BUCKET_ID, C.BUCKET_NAME, FL_YEAR, round(PIT,6) PIT
                        FROM IFRS_PD_VAS_PIT A
                        JOIN IFRS_PD_RULES_CONFIG B
                        ON A.PD_RULE_ID	= B.PKID
                        JOIN IFRS_BUCKET_DETAIL C
                        ON A.BUCKET_GROUP = C.BUCKET_GROUP
                        AND C.BUCKET_ID = A.BUCKET_ID
                        WHERE A.PD_RULE_ID = '|| TO_CHAR(v_pd_rule_id) ||'
                        AND A.EFF_DATE = '''|| TO_CHAR(v_eff_date,'dd MON yyyy') ||'''
                    ) A
                    PIVOT
                    (
                        SUM(PIT)
                        FOR FL_YEAR IN (' || v_columnName ||')
                    )
                    ORDER BY BUCKET_ID
                    ';

        IF v_Count > 0 THEN
        OPEN Cur_out FOR v_query;
        END IF;
END;