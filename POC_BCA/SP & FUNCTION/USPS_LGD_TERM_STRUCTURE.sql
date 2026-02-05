CREATE OR REPLACE PROCEDURE USPS_LGD_TERM_STRUCTURE
(
    V_PERIOD DATE DEFAULT '1900-01-01',
    V_ModelId NUMBER DEFAULT 0,
    v_tm_type IN VARCHAR2,
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
            FROM IFRS_LGD_TERM_STRUCTURE
            WHERE LGD_RULE_ID = V_ModelId
--            AND TM_TYPE = v_tm_type
            AND EFF_DATE = V_PERIOD;

    IF (v_tm_type = 'YEAR') THEN
        SELECT LISTAGG('"' || FL_YEAR || '" AS "LGD Y' || FL_YEAR || ' %"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_LGD_TERM_STRUCTURE
            WHERE LGD_RULE_ID = V_ModelId
  --          AND TM_TYPE = v_tm_type
            AND EFF_DATE = V_PERIOD
        );

        SELECT LISTAGG(FL_YEAR,',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnName
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_LGD_TERM_STRUCTURE
            WHERE LGD_RULE_ID = V_ModelId
    --        AND TM_TYPE = v_tm_type
            AND EFF_DATE = V_PERIOD
        );

        v_query := 'SELECT
                        LGD_RULE_NAME "LGD Model",
                        EFF_DATE "Period",
                        ' || v_columnAlias || '
                     FROM
                    (
                    SELECT C.LGD_RULE_NAME, A.EFF_DATE, A.FL_YEAR, ROUND(LGD, 6) LGD
                        FROM IFRS_LGD_TERM_STRUCTURE A
                        JOIN IFRS_LGD_RULES_CONFIG C
                        ON A.LGD_RULE_ID = C.PKID
                        AND A.LGD_RULE_ID = ' || TO_CHAR(V_ModelId) || '
                        AND A.EFF_DATE = '''|| TO_CHAR(V_PERIOD,'dd MON yyyy') ||'''
                    ) A
                    PIVOT
                    (
                        SUM(LGD)
                        FOR FL_YEAR IN (' || v_columnName ||')
                    )
                    ORDER BY EFF_DATE';
    ELSIF (v_tm_type = 'MONTH') THEN

        v_query := 'SELECT
                       LGD_RULE_NAME "LGD Model",
                        EFF_DATE "Period",
                        LGD "LGD Rate %"
                     FROM
                    (
                    SELECT C.LGD_RULE_NAME, A.EFF_DATE, A.FL_YEAR, A.FL_MONTH, A.FL_DATE, ROUND(OVERRIDE_LGD, 6) LGD
                       FROM IFRS_LGD_TERM_STRUCTURE A
                       JOIN IFRS_LGD_RULES_CONFIG C
                       ON A.LGD_RULE_ID = C.PKID
                       AND A.LGD_RULE_ID = ' || TO_CHAR(V_ModelId) || '
                       AND A.EFF_DATE = '''|| TO_CHAR(V_PERIOD,'dd MON yyyy') ||'''
                    ) A
                    ORDER BY FL_YEAR, FL_DATE';
    ELSIF (v_tm_type = 'OVERRIDE') THEN
        SELECT LISTAGG('"' || FL_YEAR || '" AS "LGD Y' || FL_YEAR || ' %"',',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnAlias
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_LGD_TERM_STRUCTURE
            WHERE LGD_RULE_ID = V_ModelId
  --          AND TM_TYPE = v_tm_type
            AND EFF_DATE = V_PERIOD
        );

        SELECT LISTAGG(FL_YEAR,',')
        WITHIN GROUP (ORDER BY FL_YEAR)
        INTO v_columnName
        FROM
        (
            SELECT DISTINCT FL_YEAR
            FROM IFRS_LGD_TERM_STRUCTURE
            WHERE LGD_RULE_ID = V_ModelId
    --        AND TM_TYPE = v_tm_type
            AND EFF_DATE = V_PERIOD
        );

        v_query := 'SELECT
                        LGD_RULE_NAME "LGD Model",
                        EFF_DATE "Period",
                        ' || v_columnAlias || '
                     FROM
                    (
                    SELECT C.LGD_RULE_NAME, A.EFF_DATE, A.FL_YEAR, ROUND(OVERRIDE_LGD, 6) LGD
                        FROM IFRS_LGD_TERM_STRUCTURE A
                        JOIN IFRS_LGD_RULES_CONFIG C
                        ON A.LGD_RULE_ID = C.PKID
                        AND A.LGD_RULE_ID = ' || TO_CHAR(V_ModelId) || '
                        AND A.EFF_DATE = '''|| TO_CHAR(V_PERIOD,'dd MON yyyy') ||'''
                    ) A
                    PIVOT
                    (
                        SUM(LGD)
                        FOR FL_YEAR IN (' || v_columnName ||')
                    )
                    ORDER BY EFF_DATE';
    END IF;

    IF(v_Count > 0 OR v_tm_type = 'MONTH') THEN
    OPEN Cur_out FOR v_query;
    END IF;
END;