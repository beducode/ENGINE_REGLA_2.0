CREATE OR REPLACE PROCEDURE SP_IFRS_PD_VAS(V_EFF_DATE DATE)
AS
	v_count number;
	v_minZeroBucketID number(10);
	v_maxZeroBucketID number(10);
	v_minPD number;
	v_maxPD number;
	v_prevPD number;
	v_i number(10);
	v_bucketGroup varchar2(30);
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

	-- ============================== Start PD Interpolation from SNP ==============================
	SELECT BUCKET_GROUP
	INTO v_bucketGroup
	FROM IFRS_BUCKET_HEADER
	WHERE PKID =
	(
        SELECT MAX(PKID) FROM IFRS_BUCKET_HEADER
        WHERE OPTION_GROUPING = 'SNP'
        AND IS_DELETED = 0
	);

	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_VAS_SNP';

	INSERT INTO TMP_IFRS_PD_VAS_SNP
	(
        PKID,
        EFF_DATE,
        BUCKET_GROUP,
        BUCKET_ID,
        PD
	)
	SELECT A.BUCKET_ID PKID,
        v_EFF_DATE,
        A.BUCKET_GROUP,
        A.BUCKET_ID,
        B.PD
	FROM IFRS_BUCKET_DETAIL A
	LEFT JOIN TBLU_SNP_RATING B
	ON B.RATING_CODE = A.BUCKET_NAME
	WHERE A.BUCKET_GROUP = v_bucketGroup;

	COMMIT;

	--Apply min value
	MERGE INTO TMP_IFRS_PD_VAS_SNP A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MIN_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_SNP_RATING B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
              B.MIN_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	--Apply max value
	MERGE INTO TMP_IFRS_PD_VAS_SNP A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MAX_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_SNP_RATING B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
              B.MAX_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	SELECT COUNT(*),  MIN(BUCKET_ID)
	INTO v_count, v_minZeroBucketID
	FROM TMP_IFRS_PD_VAS_SNP
	WHERE NVL(PD,0) = 0;

	WHILE v_count > 0 LOOP
        SELECT MIN(BUCKET_ID)
        INTO v_maxZeroBucketID
        FROM TMP_IFRS_PD_VAS_SNP
        WHERE BUCKET_ID > v_minZeroBucketID
        AND NVL(PD,0) > 0;

        SELECT PD
        INTO v_minPD
        FROM TMP_IFRS_PD_VAS_SNP
        WHERE BUCKET_ID = v_minZeroBucketID - 1;

        SELECT PD
        INTO v_maxPD
        FROM TMP_IFRS_PD_VAS_SNP
        WHERE BUCKET_ID = v_maxZeroBucketID;

        v_i := v_minZeroBucketID;

        WHILE v_i < v_maxZeroBucketID LOOP
            SELECT PD
            INTO v_prevPD
            FROM TMP_IFRS_PD_VAS_SNP
            WHERE BUCKET_ID = v_i-1;

            UPDATE TMP_IFRS_PD_VAS_SNP
            SET PD = v_prevPD + (v_maxPD - v_minPD)/(v_maxZeroBucketID - (v_minZeroBucketID - 1))
            WHERE BUCKET_ID = v_i;

            COMMIT;

            v_i := v_i + 1;
        END LOOP;

        SELECT COUNT(*),  MIN(BUCKET_ID)
        INTO v_count, v_minZeroBucketID
        FROM TMP_IFRS_PD_VAS_SNP
        WHERE NVL(PD,0) = 0;
	END LOOP;

	-- ============================== End PD Interpolation from SNP ==============================

	-- =========================== Start PD Interpolation from Pefindo ===========================
	SELECT BUCKET_GROUP
	INTO v_bucketGroup
	FROM IFRS_BUCKET_HEADER
	WHERE PKID =
	(
        SELECT MAX(PKID) FROM IFRS_BUCKET_HEADER
        WHERE OPTION_GROUPING = 'PEF'
        AND IS_DELETED = 0
	);

	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_VAS_PEF';

	INSERT INTO TMP_IFRS_PD_VAS_PEF
	(
        PKID,
        EFF_DATE,
        BUCKET_GROUP,
        BUCKET_ID,
        PD
	)
	SELECT A.BUCKET_ID PKID,
        v_EFF_DATE,
        A.BUCKET_GROUP,
        A.BUCKET_ID,
        B.PD
	FROM IFRS_BUCKET_DETAIL A
	LEFT JOIN TBLU_PEFINDO_RATING B
	ON B.RATING_CODE = A.BUCKET_NAME
	WHERE A.BUCKET_GROUP = v_bucketGroup;

	COMMIT;

	--Apply min value
	MERGE INTO TMP_IFRS_PD_VAS_PEF A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MIN_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_PEFINDO_RATING B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
              B.MIN_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	--Apply max value
	MERGE INTO TMP_IFRS_PD_VAS_PEF A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MAX_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_PEFINDO_RATING B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
              B.MAX_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	SELECT COUNT(*),  MIN(BUCKET_ID)
	INTO v_count, v_minZeroBucketID
	FROM TMP_IFRS_PD_VAS_PEF
	WHERE NVL(PD,0) = 0;

	WHILE v_count > 0 LOOP
        SELECT MIN(BUCKET_ID)
        INTO v_maxZeroBucketID
        FROM TMP_IFRS_PD_VAS_PEF
        WHERE BUCKET_ID > v_minZeroBucketID
        AND NVL(PD,0) > 0;

        SELECT PD
        INTO v_minPD
        FROM TMP_IFRS_PD_VAS_PEF
        WHERE BUCKET_ID = v_minZeroBucketID - 1;

        SELECT PD
        INTO v_maxPD
        FROM TMP_IFRS_PD_VAS_PEF
        WHERE BUCKET_ID = v_maxZeroBucketID;

        v_i := v_minZeroBucketID;

        WHILE v_i < v_maxZeroBucketID LOOP
            SELECT PD
            INTO v_prevPD
            FROM TMP_IFRS_PD_VAS_PEF
            WHERE BUCKET_ID = v_i-1;

            UPDATE TMP_IFRS_PD_VAS_PEF
            SET PD = v_prevPD + (v_maxPD - v_minPD)/(v_maxZeroBucketID - (v_minZeroBucketID - 1))
            WHERE BUCKET_ID = v_i;

            COMMIT;

            v_i := v_i + 1;
        END LOOP;

        SELECT COUNT(*),  MIN(BUCKET_ID)
        INTO v_count, v_minZeroBucketID
        FROM TMP_IFRS_PD_VAS_PEF
        WHERE NVL(PD,0) = 0;
	END LOOP;

	-- =========================== End PD Interpolation from Pefindo ===========================

	-- ============================== Start PD Interpolation from SNP FI ==============================
	SELECT BUCKET_GROUP
	INTO v_bucketGroup
	FROM IFRS_BUCKET_HEADER
	WHERE PKID =
	(
        SELECT MAX(PKID) FROM IFRS_BUCKET_HEADER
        WHERE OPTION_GROUPING = 'SNPFI'
        AND IS_DELETED = 0
	);

	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_VAS_SNP_FI';

	INSERT INTO TMP_IFRS_PD_VAS_SNP_FI
	(
        PKID,
        EFF_DATE,
        BUCKET_GROUP,
        BUCKET_ID,
        PD
	)
	SELECT A.BUCKET_ID PKID,
        v_EFF_DATE,
        A.BUCKET_GROUP,
        A.BUCKET_ID,
        B.PD
	FROM IFRS_BUCKET_DETAIL A
	LEFT JOIN TBLU_SNP_RATING_FI B
	ON B.RATING_CODE = A.BUCKET_NAME
	WHERE A.BUCKET_GROUP = v_bucketGroup;

	COMMIT;

	--Apply min value
	MERGE INTO TMP_IFRS_PD_VAS_SNP_FI A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MIN_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_SNP_RATING_FI B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
              B.MIN_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	--Apply max value
	MERGE INTO TMP_IFRS_PD_VAS_SNP_FI A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MAX_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_SNP_RATING_FI B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
              B.MAX_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	SELECT COUNT(*),  MIN(BUCKET_ID)
	INTO v_count, v_minZeroBucketID
	FROM TMP_IFRS_PD_VAS_SNP_FI
	WHERE NVL(PD,0) = 0;

	WHILE v_count > 0 LOOP
        SELECT MIN(BUCKET_ID)
        INTO v_maxZeroBucketID
        FROM TMP_IFRS_PD_VAS_SNP_FI
        WHERE BUCKET_ID > v_minZeroBucketID
        AND NVL(PD,0) > 0;

        SELECT PD
        INTO v_minPD
        FROM TMP_IFRS_PD_VAS_SNP_FI
        WHERE BUCKET_ID = v_minZeroBucketID - 1;

        SELECT PD
        INTO v_maxPD
        FROM TMP_IFRS_PD_VAS_SNP_FI
        WHERE BUCKET_ID = v_maxZeroBucketID;

        v_i := v_minZeroBucketID;

        WHILE v_i < v_maxZeroBucketID LOOP
            SELECT PD
            INTO v_prevPD
            FROM TMP_IFRS_PD_VAS_SNP_FI
            WHERE BUCKET_ID = v_i-1;

            UPDATE TMP_IFRS_PD_VAS_SNP_FI
            SET PD = v_prevPD + (v_maxPD - v_minPD)/(v_maxZeroBucketID - (v_minZeroBucketID - 1))
            WHERE BUCKET_ID = v_i;

            COMMIT;

            v_i := v_i + 1;
        END LOOP;

        SELECT COUNT(*),  MIN(BUCKET_ID)
        INTO v_count, v_minZeroBucketID
        FROM TMP_IFRS_PD_VAS_SNP_FI
        WHERE NVL(PD,0) = 0;
	END LOOP;

	-- ============================== End PD Interpolation from SNP FI ==============================

	-- =========================== Start PD Interpolation from Pefindo FI ===========================
	SELECT BUCKET_GROUP
	INTO v_bucketGroup
	FROM IFRS_BUCKET_HEADER
	WHERE PKID =
	(
        SELECT MAX(PKID) FROM IFRS_BUCKET_HEADER
        WHERE OPTION_GROUPING = 'PEFFI'
        AND IS_DELETED = 0
	);

	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_VAS_PEF_FI';

	INSERT INTO TMP_IFRS_PD_VAS_PEF_FI
	(
        PKID,
        EFF_DATE,
        BUCKET_GROUP,
        BUCKET_ID,
        PD
	)
	SELECT A.BUCKET_ID PKID,
        v_EFF_DATE,
        A.BUCKET_GROUP,
        A.BUCKET_ID,
        B.PD
	FROM IFRS_BUCKET_DETAIL A
	LEFT JOIN TBLU_PEFINDO_RATING_FI B
	ON B.RATING_CODE = A.BUCKET_NAME
	WHERE A.BUCKET_GROUP = v_bucketGroup;

	COMMIT;

	--Apply min value
	MERGE INTO TMP_IFRS_PD_VAS_PEF_FI A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MIN_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_PEFINDO_RATING_FI B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD < B.MIN_VALUE / 100 THEN
              B.MIN_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	--Apply max value
	MERGE INTO TMP_IFRS_PD_VAS_PEF_FI A
	USING
	(
        SELECT A2.BUCKET_ID, A2.MAX_VALUE
        FROM IFRS_BUCKET_DETAIL A2
        JOIN TBLU_PEFINDO_RATING_FI B2
        ON B2.RATING_CODE = A2.BUCKET_NAME
        WHERE A2.BUCKET_GROUP = v_bucketGroup
	) B
	ON (A.BUCKET_ID = B.BUCKET_ID)
	WHEN MATCHED THEN
	UPDATE SET
	A.PD = CASE WHEN A.PD > B.MAX_VALUE / 100 AND B.MAX_VALUE != 0 THEN
              B.MAX_VALUE
           ELSE
              A.PD
           END;
	COMMIT;

	SELECT COUNT(*),  MIN(BUCKET_ID)
	INTO v_count, v_minZeroBucketID
	FROM TMP_IFRS_PD_VAS_PEF_FI
	WHERE NVL(PD,0) = 0;

	WHILE v_count > 0 LOOP
        SELECT MIN(BUCKET_ID)
        INTO v_maxZeroBucketID
        FROM TMP_IFRS_PD_VAS_PEF_FI
        WHERE BUCKET_ID > v_minZeroBucketID
        AND NVL(PD,0) > 0;

        SELECT PD
        INTO v_minPD
        FROM TMP_IFRS_PD_VAS_PEF_FI
        WHERE BUCKET_ID = v_minZeroBucketID - 1;

        SELECT PD
        INTO v_maxPD
        FROM TMP_IFRS_PD_VAS_PEF_FI
        WHERE BUCKET_ID = v_maxZeroBucketID;

        v_i := v_minZeroBucketID;

        WHILE v_i < v_maxZeroBucketID LOOP
            SELECT PD
            INTO v_prevPD
            FROM TMP_IFRS_PD_VAS_PEF_FI
            WHERE BUCKET_ID = v_i-1;

            UPDATE TMP_IFRS_PD_VAS_PEF_FI
            SET PD = v_prevPD + (v_maxPD - v_minPD)/(v_maxZeroBucketID - (v_minZeroBucketID - 1))
            WHERE BUCKET_ID = v_i;

            COMMIT;

            v_i := v_i + 1;
        END LOOP;

        SELECT COUNT(*),  MIN(BUCKET_ID)
        INTO v_count, v_minZeroBucketID
        FROM TMP_IFRS_PD_VAS_PEF_FI
        WHERE NVL(PD,0) = 0;
	END LOOP;

	-- =========================== End PD Interpolation from Pefindo FI ===========================

	DELETE IFRS_PD_VAS
	WHERE EFF_DATE = v_EFF_DATE
	AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM TMP_IFRS_PD_RUNNING_DATE);

	COMMIT;

	INSERT INTO IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM TMP_IFRS_PD_RUNNING_DATE A
	JOIN TMP_IFRS_PD_VAS_SNP B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP;

	COMMIT;

	INSERT INTO IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM TMP_IFRS_PD_RUNNING_DATE A
	JOIN TMP_IFRS_PD_VAS_PEF B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP;

	COMMIT;

	INSERT INTO IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM TMP_IFRS_PD_RUNNING_DATE A
	JOIN TMP_IFRS_PD_VAS_SNP_FI B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP;

	COMMIT;

	INSERT INTO IFRS_PD_VAS
	(
		EFF_DATE,
		PD_RULE_ID,
		BUCKET_GROUP,
		BUCKET_ID,
		PD
	)
	SELECT B.EFF_DATE,
		A.PD_RULE_ID,
		A.BUCKET_GROUP,
		B.BUCKET_ID,
		B.PD
	FROM TMP_IFRS_PD_RUNNING_DATE A
	JOIN TMP_IFRS_PD_VAS_PEF_FI B
	ON A.BUCKET_GROUP = B.BUCKET_GROUP;

	COMMIT;
END;