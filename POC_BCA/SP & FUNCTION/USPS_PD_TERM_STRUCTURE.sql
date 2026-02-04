CREATE OR REPLACE PROCEDURE USPS_PD_TERM_STRUCTURE
(
    v_pd_rule_id IN NUMBER,
    v_model_id IN NUMBER,
    v_tm_type IN VARCHAR2,
    v_eff_date IN DATE,
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
            FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = v_pd_rule_id
            AND MODEL_ID = v_model_id
            AND TM_TYPE = v_tm_type
            AND EFF_DATE = v_eff_date;

    IF (v_tm_type = 'YEAR') THEN
        SELECT LISTAGG('"' || FL_YEAR || '" AS "MPD Y' || FL_YEAR || ' %"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = v_pd_rule_id
            AND MODEL_ID = v_model_id
            AND TM_TYPE = v_tm_type
            AND EFF_DATE = v_eff_date
        );

        SELECT LISTAGG(FL_YEAR,',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnName
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = v_pd_rule_id
            AND MODEL_ID = v_model_id
            AND TM_TYPE = v_tm_type
            AND EFF_DATE = v_eff_date
        );

        v_query := 'SELECT
                        FL_MODEL_NAME "PD Model",
                        EFF_DATE "Period",
                        BUCKET_ID "Bucket ID",
                        BUCKET_NAME "Bucket Name",
                        ' || v_columnAlias || '
                     FROM
                    (
                    SELECT C.FL_MODEL_NAME, A.EFF_DATE, B.BUCKET_ID, B.BUCKET_NAME, FL_YEAR, ROUND(NVL(PD,0) * 100, 6) PD
                        FROM IFRS_PD_TERM_STRUCTURE A
                        JOIN IFRS_BUCKET_DETAIL B
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP
                        AND A.BUCKET_ID = B.BUCKET_ID
                        AND A.PD_RULE_ID = ' || TO_CHAR(v_pd_rule_id) ||'
                        AND A.TM_TYPE = ''' || v_tm_type || '''
                        AND A.EFF_DATE = '''|| TO_CHAR(v_eff_date,'dd MON yyyy') ||'''
                        ' || CASE WHEN v_model_id = 0 THEN ' LEFT ' END || ' JOIN IFRS_FL_MODEL_VAR C
                        ON A.MODEL_ID = C.PKID
                        AND A.MODEL_ID = ' || TO_CHAR(v_model_id) || '
                    ) A
                    PIVOT
                    (
                        SUM(PD)
                        FOR FL_YEAR IN (' || v_columnName ||')
                    )
                    ORDER BY BUCKET_ID';
    ELSIF (v_tm_type = 'MONTH') THEN

        v_query := 'SELECT
                        FL_MODEL_NAME "PD Model",
                        FL_DATE "Period",
                        BUCKET_ID "Bucket ID",
                        BUCKET_NAME "Bucket Name",
                        PD "MPD Rate %"
                     FROM
                    (
                    SELECT C.FL_MODEL_NAME, A.EFF_DATE, B.BUCKET_ID, B.BUCKET_NAME, A.FL_YEAR, A.FL_MONTH, A.FL_DATE, ROUND(NVL(OVERRIDE_PD,0) * 100, 6) PD
                        FROM IFRS_PD_TERM_STRUCTURE A
                        JOIN IFRS_BUCKET_DETAIL B
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP
                        AND A.BUCKET_ID = B.BUCKET_ID
                        AND A.PD_RULE_ID = ' || TO_CHAR(v_pd_rule_id) ||'
                        AND A.TM_TYPE = ''' || v_tm_type || '''
                        ' || CASE WHEN v_model_id = 0 THEN ' LEFT ' END || ' JOIN IFRS_FL_MODEL_VAR C
                        ON A.MODEL_ID = C.PKID
                        AND A.MODEL_ID = ' || TO_CHAR(v_model_id) || '
                        AND A.EFF_DATE = '''|| TO_CHAR(v_eff_date,'dd MON yyyy') ||'''
                    ) A
                    ORDER BY FL_YEAR, FL_DATE, BUCKET_ID';

    ELSIF (v_tm_type = 'OVERRIDE') THEN

        SELECT COUNT(*)
            INTO v_Count
            FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = v_pd_rule_id
            AND MODEL_ID = v_model_id
            AND TM_TYPE = 'YEAR'
            AND EFF_DATE = v_eff_date;

       SELECT LISTAGG('"' || FL_YEAR || '" AS "MPD Y' || FL_YEAR || ' %"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = v_pd_rule_id
            AND MODEL_ID = v_model_id
            AND TM_TYPE = 'YEAR'
            AND EFF_DATE = v_eff_date
        );

        SELECT LISTAGG(FL_YEAR,',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnName
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = v_pd_rule_id
            AND MODEL_ID = v_model_id
            AND TM_TYPE = 'YEAR'
            AND EFF_DATE = v_eff_date
        );

        v_query := 'SELECT
                        FL_MODEL_NAME "PD Model",
                        EFF_DATE "Period",
                        BUCKET_ID "Bucket ID",
                        BUCKET_NAME "Bucket Name",
                        ' || v_columnAlias || '
                     FROM
                    (
                    SELECT C.FL_MODEL_NAME, A.EFF_DATE, B.BUCKET_ID, B.BUCKET_NAME, FL_YEAR, ROUND(NVL(OVERRIDE_PD,0) * 100, 6) PD
                        FROM IFRS_PD_TERM_STRUCTURE A
                        JOIN IFRS_BUCKET_DETAIL B
                        ON A.BUCKET_GROUP = B.BUCKET_GROUP
                        AND A.BUCKET_ID = B.BUCKET_ID
                        AND A.PD_RULE_ID = ' || TO_CHAR(v_pd_rule_id) ||'
                        AND A.TM_TYPE = ''YEAR''
                        AND A.EFF_DATE = '''|| TO_CHAR(v_eff_date,'dd MON yyyy') ||'''
                        ' || CASE WHEN v_model_id = 0 THEN ' LEFT ' END || ' JOIN IFRS_FL_MODEL_VAR C
                        ON A.MODEL_ID = C.PKID
                        AND A.MODEL_ID = ' || TO_CHAR(v_model_id) || '
                    ) A
                    PIVOT
                    (
                        SUM(PD)
                        FOR FL_YEAR IN (' || v_columnName ||')
                    )
                    ORDER BY BUCKET_ID';
    END IF;

    IF(v_Count > 0 OR v_tm_type = 'MONTH') THEN
    OPEN Cur_out FOR v_query;
    END IF;
END;