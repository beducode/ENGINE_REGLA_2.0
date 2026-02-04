CREATE OR REPLACE PROCEDURE SP_IFRS_PD_VAS_CUMULATIVE (V_EFF_DATE DATE)
AS
	v_minyear number(10);
	v_maxyear number(10);
	v_year number(10);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_RUNNING_DATE';

	-- GET PD_RULE_ID FOR VASICEK METHOD
	INSERT INTO TMP_IFRS_PD_RUNNING_DATE
	(
        EFF_DATE,
        PD_RULE_ID,
        BUCKET_GROUP
	)
	SELECT
	    v_EFF_DATE AS EFF_DATE,
	    A.PKID AS PD_RULE_ID,
		A.BUCKET_GROUP
	FROM IFRS_PD_RULES_CONFIG A
	WHERE NVL(A.ACTIVE_FLAG,0) = 1
	AND IS_DELETED = 0
	AND PD_METHOD ='VAS'
	AND DERIVED_PD_MODEL IS NULL;

	COMMIT;

	DELETE IFRS_PD_VAS_CUMULATIVE
	WHERE EFF_DATE = v_EFF_DATE
	AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM TMP_IFRS_PD_RUNNING_DATE);

	COMMIT;

    SELECT MIN(FL_YEAR), MAX(FL_YEAR)
    INTO v_minyear, v_maxyear
    FROM IFRS_PD_VAS_PIT
    WHERE EFF_DATE = v_EFF_DATE;

    v_year := v_minyear;

    WHILE v_year <= v_maxyear LOOP
        IF v_year = v_minyear THEN
            INSERT INTO IFRS_PD_VAS_CUMULATIVE
            (
                EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                CUMULATIVE_PD
            )
            SELECT EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                PIT
            FROM IFRS_PD_VAS_PIT
            WHERE EFF_DATE = v_EFF_DATE
            AND FL_YEAR = v_year;

            COMMIT;
        ELSE
            INSERT INTO IFRS_PD_VAS_CUMULATIVE
            (
                EFF_DATE,
                PD_RULE_ID,
                BUCKET_GROUP,
                BUCKET_ID,
                FL_YEAR,
                CUMULATIVE_PD
            )
            SELECT A.EFF_DATE,
                A.PD_RULE_ID,
                A.BUCKET_GROUP,
                A.BUCKET_ID,
                B.FL_YEAR,
                A.CUMULATIVE_PD + (1-A.CUMULATIVE_PD)*B.PIT CUMULATIVE_PD
            FROM IFRS_PD_VAS_CUMULATIVE A
            JOIN IFRS_PD_VAS_PIT B
            ON A.EFF_DATE = B.EFF_DATE
            AND A.EFF_DATE = v_EFF_DATE
            AND A.PD_RULE_ID = B.PD_RULE_ID
            AND A.BUCKET_ID = B.BUCKET_ID
            AND A.FL_YEAR = v_year - 1
            AND B.FL_YEAR = v_year;

            COMMIT;
        END IF;

        v_year := v_year + 1;
    END LOOP;
END;