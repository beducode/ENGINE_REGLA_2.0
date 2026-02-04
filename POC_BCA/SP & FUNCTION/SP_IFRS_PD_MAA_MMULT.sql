CREATE OR REPLACE PROCEDURE SP_IFRS_PD_MAA_MMULT
AS
   v_CURRDATE DATE;
   v_MAX  NUMBER;
   v_CURSOR NUMBER;
BEGIN

    SELECT CURRDATE
	INTO v_CURRDATE
	FROM IFRS_PRC_DATE;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_RUNNING_DATE';

    INSERT INTO TMP_IFRS_PD_RUNNING_DATE
	(
        EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        BUCKET_GROUP,
        HISTORICAL_DATA,
        POPULATION_MONTH
	)
	SELECT
	    v_CURRDATE AS EFF_DATE,
		LAST_DAY(ADD_MONTHS(v_CURRDATE, A.INCREMENT_PERIOD * -1)) AS BASE_DATE,
	    A.PKID AS PD_RULE_ID,
		A.BUCKET_GROUP,
		A.HISTORICAL_DATA,
		A.POPULATION_MONTH
	FROM IFRS_PD_RULES_CONFIG A
	WHERE NVL(A.ACTIVE_FLAG,0) = 1
	AND IS_DELETED = 0
	AND PD_METHOD ='MAA'
	AND A.DERIVED_PD_MODEL IS NULL;

    COMMIT;

    DELETE IFRS_PD_MAA_MMULT
    WHERE EFF_DATE = v_CURRDATE
        AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM TMP_IFRS_PD_RUNNING_DATE);

    COMMIT;

    v_CURSOR := 1;

    SELECT MAX(EXPECTED_LIFE)
        INTO v_MAX
    FROM TMP_IFRS_PD_RUNNING_DATE;

    WHILE v_CURSOR <= v_MAX LOOP
        IF v_CURSOR = 1 THEN
            INSERT INTO IFRS_PD_MAA_MMULT
            (
                EFF_DATE,
                BASE_DATE,
                PD_RULE_ID,
                PROJECTION_DATE,
                SEQUENCE,
                BUCKET_GROUP,
                BUCKET_FROM,
                BUCKET_TO,
                MMULT
            )
               SELECT
                    B.EFF_DATE,
                    B.BASE_DATE,
                    A.PD_RULE_ID,
                    A.EFF_DATE AS PROJECTION_DATE,
                    1 AS SEQUENCE,
                    A.BUCKET_GROUP,
                    A.BUCKET_FROM,
                    A.BUCKET_TO,
                    A.FLOWRATE
                FROM IFRS_PD_MAA_FLOWRATE A
                INNER JOIN TMP_IFRS_PD_RUNNING_DATE B
                    ON A.PD_RULE_ID = B.PD_RULE_ID
                    AND A.EFF_DATE = ADD_MONTHS(B.EFF_DATE, B.HISTORICAL_DATA * -1)
                ORDER BY A.PD_RULE_ID, A.BUCKET_FROM, A.BUCKET_TO;
        ELSE
            INSERT INTO IFRS_PD_MAA_MMULT
            (
                EFF_DATE,
                BASE_DATE,
                PD_RULE_ID,
                PROJECTION_DATE,
                SEQUENCE,
                BUCKET_GROUP,
                BUCKET_FROM,
                BUCKET_TO,
                MMULT
            )
               SELECT
                    EFF_DATE,
                    BASE_DATE,
                    PD_RULE_ID,
                    ADD_MONTHS(PROJECTION_DATE,  1) AS PROJECTION_DATE,
                    v_CURSOR AS SEQUENCE,
                    BUCKET_GROUP,
                    BUCKET_FROM,
                    BUCKET_TO,
                    MMULT
                FROM
                    (
                        SELECT A.EFF_DATE,
                            A.BASE_DATE,
                            A.PD_RULE_ID,
                            A.BUCKET_GROUP,
                            A.BUCKET_FROM,
                            B.BUCKET_TO,
                            B.PROJECTION_DATE,
                            SUM(A.MMULT * B.MMULT) AS MMULT
                        FROM IFRS_PD_MAA_MMULT A
                        INNER JOIN IFRS_PD_MAA_MMULT B
                        ON A.PD_RULE_ID = B.PD_RULE_ID
                            AND A.EFF_DATE = B.EFF_DATE
                            AND A.BUCKET_TO = B.BUCKET_FROM
                            AND B.SEQUENCE = v_CURSOR - 1
                        INNER JOIN TMP_IFRS_PD_RUNNING_DATE C
                        ON C.PD_RULE_ID = A.PD_RULE_ID
                            --AND B.PROJECTION_DATE = ADD_MONTHS(v_CURRDATE,  v_CURSOR - 2)
                        WHERE A.EFF_DATE = v_CURRDATE
                            AND A.SEQUENCE = 1
                            AND C.EXPECTED_LIFE >= v_CURSOR
                        GROUP BY A.BASE_DATE,
                            A.EFF_DATE,
                            A.PD_RULE_ID,
                            A.BUCKET_GROUP,
                            A.BUCKET_FROM,
                            B.BUCKET_TO,
                            B.PROJECTION_DATE
                    )
                ORDER BY PD_RULE_ID, BUCKET_FROM, BUCKET_TO;
        END IF;

        COMMIT;

        v_CURSOR := v_CURSOR + 1;

    END LOOP;

END;