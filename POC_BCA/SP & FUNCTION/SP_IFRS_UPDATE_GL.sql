CREATE OR REPLACE PROCEDURE SP_IFRS_UPDATE_GL
AS
V_CURRDATE DATE;
V_PREVDATE DATE;
BEGIN

SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_AMORT;
SELECT PREVDATE INTO V_PREVDATE FROM IFRS_PRC_DATE_AMORT;

MERGE INTO IFRS_MASTER_ACCOUNT A
	USING IFRS_MASTER_ACCOUNT B
	ON (A.DOWNLOAD_DATE = V_CURRDATE
		AND B.DOWNLOAD_DATE = V_PREVDATE
		AND A.MASTERID = B.MASTERID
	    AND V_CURRDATE <> LAST_DAY(V_CURRDATE)
		)
	WHEN MATCHED THEN
	UPDATE
	SET A.IFRS9_CLASS = B.IFRS9_CLASS,
        A.SPPI_RESULT = B.SPPI_RESULT,
        A.BM_RESULT = B.BM_RESULT;

	COMMIT;

	-----------------------------------------------------------------------------------------------------------------
    --UPDATE BM_RESULT
    -----------------------------------------------------------------------------------------------------------------
    SP_IFRS_GENERATE_RULE('BM');
    SP_IFRS_RULE_DATA;

    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING
    (
         SELECT MASTERID,
            BMRESULT
         FROM GTMP_IFRS_SCENARIO_DATA A2
         JOIN
         (
            SELECT BMID,
                BMRESULT
            FROM IFRS_AC_BM_HEADER_HIST A3
                JOIN
                (   SELECT MAX(PKID) AS MAX_PKID
                    FROM IFRS_AC_BM_HEADER_HIST
                WHERE EFFECTIVE_DATE <= V_CURRDATE
                    AND TRUNC(REVIEWEDDATE) <= V_CURRDATE
                    AND STATUS = 1
                GROUP BY BMID
            ) B3
            ON A3.PKID = B3.MAX_PKID
         ) B2
         ON A2.DOWNLOAD_DATE = V_CURRDATE
         AND A2.RULE_ID = B2.BMID
         WHERE A2.DATA_SOURCE = 'ILS'
    ) B
    ON (A.DOWNLOAD_DATE = V_CURRDATE AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET A.BM_RESULT = B.BMRESULT;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE SPPI_RESULT
    -----------------------------------------------------------------------------------------------------------------
    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING
    (
        SELECT DISTINCT A2.SPPIRESULT,
            A2.PRODUCTCODE
        FROM
        (   SELECT PKID,
            CASE WHEN OVERRIDEFLAG = 1 THEN
                OVERRIDERESULT
            ELSE
                SPPIRESULT
            END SPPIRESULT,
            PRODUCTCODE
            FROM IFRS_AC_SPPI_HEADER_HIST A3
            CROSS APPLY
            (
                SELECT REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                         '[^,]+',
                                         1,
                                         LEVEL)
                             AS PRODUCTCODE
                     FROM DUAL
                CONNECT BY REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                         '[^,]+',
                                         1,
                                         LEVEL)
                             IS NOT NULL
            )
            WHERE EFFECTIVE_DATE <= V_CURRDATE
                    AND TRUNC(REVIEWEDDATE) <= V_CURRDATE
            AND NVL(STATUS,1) = 1
        ) A2
        JOIN
        (   SELECT PRODUCTCODE, MAX(PKID) AS MAX_PKID
            FROM IFRS_AC_SPPI_HEADER_HIST A3
        CROSS APPLY
        (
                SELECT REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                     '[^,]+',
                                     1,
                                     LEVEL)
                         AS PRODUCTCODE
                 FROM DUAL
                CONNECT BY REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                     '[^,]+',
                                     1,
                                     LEVEL)
                         IS NOT NULL
        )
            WHERE EFFECTIVE_DATE <= V_CURRDATE
            AND TRUNC(REVIEWEDDATE) <= V_CURRDATE
            AND STATUS = 1
            GROUP BY PRODUCTCODE
        ) B2
        ON A2.PKID = B2.MAX_PKID
        AND A2.PRODUCTCODE = B2.PRODUCTCODE
    ) B
    ON (A.PRODUCT_CODE = B.PRODUCTCODE AND A.DOWNLOAD_DATE = V_CURRDATE)
    WHEN MATCHED THEN
    UPDATE
    SET A.SPPI_RESULT = B.SPPIRESULT;

    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING
    (
        SELECT DISTINCT A2.SPPIRESULT,
            A2.PRODUCTCODE
        FROM
        (   SELECT PKID,
            CASE WHEN OVERRIDEFLAG = 1 THEN
                OVERRIDERESULT
            ELSE
                SPPIRESULT
            END SPPIRESULT,
            PRODUCTCODE
            FROM IFRS_AC_SPPI_HEADER_HIST A3
            CROSS APPLY
            (
                SELECT REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                         '[^,]+',
                                         1,
                                         LEVEL)
                             AS PRODUCTCODE
                     FROM DUAL
                CONNECT BY REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                         '[^,]+',
                                         1,
                                         LEVEL)
                             IS NOT NULL
            )
            WHERE EFFECTIVE_DATE <= V_CURRDATE
                    AND TRUNC(REVIEWEDDATE) <= V_CURRDATE
            AND NVL(STATUS,1) = 1
        ) A2
        JOIN
        (   SELECT PRODUCTCODE, MAX(PKID) AS MAX_PKID
            FROM IFRS_AC_SPPI_HEADER_HIST A3
        CROSS APPLY
        (
                SELECT REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                     '[^,]+',
                                     1,
                                     LEVEL)
                         AS PRODUCTCODE
                 FROM DUAL
                CONNECT BY REGEXP_SUBSTR (A3.PRODUCT_CODE,
                                     '[^,]+',
                                     1,
                                     LEVEL)
                         IS NOT NULL
        )
            WHERE EFFECTIVE_DATE <= V_CURRDATE
            AND TRUNC(REVIEWEDDATE) <= V_CURRDATE
            AND STATUS = 1
            GROUP BY PRODUCTCODE
        ) B2
        ON A2.PKID = B2.MAX_PKID
        AND A2.PRODUCTCODE = B2.PRODUCTCODE
    ) B
    ON (A.RESERVED_VARCHAR_5 = B.PRODUCTCODE AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE = 'KTP')
    WHEN MATCHED THEN
    UPDATE
    SET A.SPPI_RESULT = B.SPPIRESULT;

    COMMIT;

    -----------------------------------------------------------------------------------------------------------------
    --UPDATE IFRS9_CLASS
    -----------------------------------------------------------------------------------------------------------------
    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING
    (
        SELECT A2.BMRESULT,
            A2.SPPIRESULT,
            CASE WHEN NVL(ASSETCLASSOVERRIDE  , ' ') = ' ' THEN ASSETCLASS ELSE ASSETCLASSOVERRIDE END ASSETCLASS
        FROM IFRS_AC_RESULT_HIST A2
        JOIN
        (   SELECT BMRESULT,
                    SPPIRESULT,
                MAX(PKID) AS MAX_PKID
                    FROM IFRS_AC_RESULT_HIST
            WHERE EFFECTIVE_DATE <= V_CURRDATE
                    AND TRUNC(REVIEWEDDATE) <= V_CURRDATE
            AND NVL(STATUS,1) = 1
            GROUP BY BMRESULT,
                SPPIRESULT
        ) B2
        ON A2.PKID = B2.MAX_PKID
        AND A2.BMRESULT = B2.BMRESULT
        AND A2.SPPIRESULT = B2.SPPIRESULT
    ) B
    ON (NVL(A.BM_RESULT,' ') = B.BMRESULT
        AND NVL(A.SPPI_RESULT, ' ') = B.SPPIRESULT
        AND A.DOWNLOAD_DATE = V_CURRDATE)
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS9_CLASS = B.ASSETCLASS;

    COMMIT;

    UPDATE IFRS_MASTER_ACCOUNT
        SET IFRS9_CLASS = 'AMORT'
        WHERE DOWNLOAD_DATE = V_CURRDATE
        AND DATA_SOURCE IN ('CRD','LIMIT');COMMIT;

    MERGE INTO IFRS_MASTER_ACCOUNT A
    USING IFRS_AC_OVERRIDE B
    ON (A.MASTER_ACCOUNT_CODE = B.MASTER_ACCOUNT_CODE
          AND A.DOWNLOAD_DATE = V_CURRDATE
          AND B.DOWNLOAD_DATE = V_CURRDATE
          AND B.STATUS = 1
        )
    WHEN MATCHED THEN
    UPDATE
    SET A.IFRS9_CLASS = B.OVERRIDERESULT;

    COMMIT;

-- TAMBAHAN UPDATE - WILLY 29 OCT 2019 FOR BCA
    /**************************************************
    UPDATE KIK EBA DAN KTP TRADING
    **************************************************/

    UPDATE ifrs_master_account a
    SET A.SPPI_RESULT = 'SPPI'
    where download_date = V_CURRDATE and a.data_source = 'KTP'
    and a.reserved_varchar_5 = 'CB-EBA-FR'
    AND A.RESERVED_VARCHAR_7 = 'EBA-INPO-01';COMMIT;

    UPDATE ifrs_master_account a
    SET A.SPPI_RESULT = 'NONSPPI'
    where download_date = V_CURRDATE and a.data_source = 'KTP'
    and a.reserved_varchar_5 = 'CB-EBA-FR'
    AND A.RESERVED_VARCHAR_7 = 'EBA-JAMA01';   COMMIT;

    UPDATE ifrs_master_account a
    SET A.SPPI_RESULT = 'NONSPPI'
    where download_date = V_CURRDATE and a.data_source = 'KTP'
    AND PRODUCT_GROUP = 'BOND' AND SEGMENT IS NULL AND CR_STAGE = 1 AND RESERVED_VARCHAR_4 = 'TRD';    COMMIT;

    UPDATE IFRS_MASTER_ACCOUNT
        SET IFRS9_CLASS = CASE
                               WHEN SPPI_RESULT = 'NONSPPI' THEN 'FVTPL'
                               WHEN SPPI_RESULT = 'SPPI' AND RESERVED_VARCHAR_4 = 'HTM' THEN 'AMORT'
                               WHEN SPPI_RESULT = 'SPPI' AND RESERVED_VARCHAR_4 = 'AFS' THEN 'FVTOCI'
                               WHEN SPPI_RESULT = 'SPPI' AND RESERVED_VARCHAR_4 = 'TRD' THEN 'FVTPL'
                               WHEN DATA_SOURCE = 'PBMM' THEN 'AMORT'
                          END
        WHERE DATA_SOURCE in ('PBMM', 'KTP')
        AND DOWNLOAD_DATE = LAST_DAY(V_CURRDATE);

        COMMIT;

    UPDATE IFRS_MASTER_ACCOUNT
        SET IFRS9_CLASS = 'AMORT'
        WHERE DOWNLOAD_DATE = V_CURRDATE
        AND DATA_SOURCE = 'KTP'
        AND IFRS9_CLASS IS NULL;COMMIT;
	/* FRANS 07052018
	ADD STEP TO RELOAD CURRENCY TABLE
	*/
	INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_INITIAL_UPDATE','RELOAD MASTER CURRENCY TABLE');

	COMMIT;

	EXECUTE IMMEDIATE 'TRUNCATE TABLE TBLM_CURRENCY';

	INSERT INTO TBLM_CURRENCY (
		CCY
		,CCY_TYPE
		,CCY_DESC
		,CREATEDBY
		,CREATEDDATE
	)
	SELECT CURRENCY
		,CURRENCY
		,COALESCE(CURRENCY_DESC, 'N/A') --CURRENCY_DESC
		,'SP_IFRS_INITIAL_UPDATE'
		,SYSTIMESTAMP
	FROM IFRS_MASTER_EXCHANGE_RATE
	WHERE DOWNLOAD_DATE = V_CURRDATE;

	COMMIT;

	-- SHU 20180830
	-- ADD STEP TO UPDATE GL_CONSTNAME

	INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_INITIAL_UPDATE','UPDATE GL_CONSTNAME');

	COMMIT;

	SP_IFRS_GENERATE_RULE('GL');
	SP_IFRS_RULE_DATA_AMORT;

	MERGE INTO IFRS_MASTER_ACCOUNT A
    USING
    (
        SELECT A2.RULE_NAME AS GL_CONSTNAME,
            B2.MASTERID
        FROM IFRS_SCENARIO_RULES_HEADER A2
        JOIN GTMP_IFRS_SCENARIO_DATA B2
        ON B2.DOWNLOAD_DATE = V_CURRDATE
        AND B2.RULE_ID = A2.PKID
        AND A2.RULE_TYPE = 'GL'
    ) B
    ON (A.DOWNLOAD_DATE = V_CURRDATE AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET A.GL_CONSTNAME = B.GL_CONSTNAME;

    COMMIT;

	INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_INITIAL_UPDATE','');

	END;