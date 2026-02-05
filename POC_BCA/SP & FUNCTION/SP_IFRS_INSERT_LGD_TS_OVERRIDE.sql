CREATE OR REPLACE PROCEDURE SP_IFRS_INSERT_LGD_TS_OVERRIDE(v_uploadId number)
AS
    V_EFF_DATE DATE;
BEGIN
    SELECT DISTINCT EFF_DATE
    INTO V_EFF_DATE
    FROM TBLU_LGD_AFL_OVERRIDE
    WHERE UPLOADID = v_uploadId;

    /*===========================================================================================
    Initialize before update override_LGD
    ===========================================================================================*/
    UPDATE IFRS_LGD_TERM_STRUCTURE
    SET OVERRIDE_LGD = LGD
    WHERE EFF_DATE = V_EFF_DATE;
    COMMIT;

    /*===========================================================================================
    Update LGD AFL
    ===========================================================================================*/
    MERGE INTO IFRS_LGD_TERM_STRUCTURE A
    USING TBLU_LGD_AFL_OVERRIDE B
    ON (A.EFF_DATE = V_EFF_DATE
        AND B.EFF_DATE = A.EFF_DATE
        AND A.LGD_RULE_NAME = B.LGD_RULE_NAME
        AND A.FL_YEAR = B.FL_YEAR
    )
    WHEN MATCHED THEN
    UPDATE SET
        A.OVERRIDE_LGD = B.LGD_OVERRIDE,
        A.UPDATEDBY = B.APPROVEDBY,
        A.UPDATEDDATE = B.APPROVEDDATE,
        A.UPDATEDHOST = B.APPROVEDHOST;
    COMMIT;


    /*===========================================================================================
    Update term structure using max(year) from LGD AFL if provided year less than available year
    ===========================================================================================*/
    MERGE INTO IFRS_LGD_TERM_STRUCTURE A
    USING (
        SELECT A2.EFF_DATE,
            A2.LGD_RULE_NAME,
            A2.FL_YEAR,
            A2.APPROVEDBY,
            A2.APPROVEDDATE,
            A2.APPROVEDHOST,
            A2.LGD_OVERRIDE
        FROM TBLU_LGD_AFL_OVERRIDE A2
        JOIN
        (
            SELECT EFF_DATE,
                LGD_RULE_NAME,
                MAX(FL_YEAR) MAX_YEAR
            FROM TBLU_LGD_AFL_OVERRIDE
            WHERE EFF_DATE = V_EFF_DATE
            GROUP BY EFF_DATE,
                LGD_RULE_NAME
        ) B2
        ON A2.EFF_DATE = B2.EFF_DATE
        AND A2.LGD_RULE_NAME = B2.LGD_RULE_NAME
        AND A2.FL_YEAR = B2.MAX_YEAR
        JOIN
        (
            SELECT LGD_RULE_NAME,
            MAX(FL_YEAR) MAX_YEAR
            FROM IFRS_LGD_TERM_STRUCTURE
            WHERE EFF_DATE = V_EFF_DATE
            GROUP BY LGD_RULE_NAME
        ) C2
        ON B2.LGD_RULE_NAME = C2.LGD_RULE_NAME
        AND B2.MAX_YEAR < C2.MAX_YEAR
    ) B
    ON (B.EFF_DATE = A.EFF_DATE
        AND A.LGD_RULE_NAME = B.LGD_RULE_NAME
        AND A.FL_YEAR > B.FL_YEAR
    )
    WHEN MATCHED THEN
    UPDATE SET
        A.OVERRIDE_LGD = B.LGD_OVERRIDE,
        A.UPDATEDBY = B.APPROVEDBY,
        A.UPDATEDDATE = B.APPROVEDDATE,
        A.UPDATEDHOST = B.APPROVEDHOST;
    COMMIT;

    /*===========================================================================================
    Insert term structure if LGD AFL not exists in existing term structure
    ===========================================================================================*/
    INSERT INTO IFRS_LGD_TERM_STRUCTURE
    (
        EFF_DATE,
        BASE_DATE,
        LGD_RULE_ID,
        LGD_RULE_NAME,
        FL_SEQ,
        FL_YEAR,
        FL_MONTH,
        FL_DATE,
        LGD,
        PRODUCTION_LGD,
        OVERRIDE_LGD,
        PRC_FLAG,
        CREATEDBY,
        CREATEDDATE,
        CREATEDHOST
    )
    SELECT A.EFF_DATE
        , ADD_MONTHS(A.EFF_DATE, -12) BASE_DATE
        , B.PKID LGD_RULE_ID
        , A.LGD_RULE_NAME
        , A.FL_YEAR AS FL_SEQ
        , A.FL_YEAR AS FL_YEAR
        , 12 AS FL_MONTH
        , ADD_MONTHS(A.EFF_DATE, (A.FL_YEAR - 1) * 12) FL_DATE
        , 0 LGD
        , 0 PRODUCTION_LGD
        , LGD_OVERRIDE OVERRIDE_LGD
        , 'M' PRC_FLAG
        , A.APPROVEDBY CREATEDBY
        , A.APPROVEDDATE CREATEDDATE
        , A.APPROVEDHOST CREATEDHOST
    FROM TBLU_LGD_AFL_OVERRIDE A
    JOIN IFRS_LGD_RULES_CONFIG B
    ON A.LGD_RULE_NAME = B.LGD_RULE_NAME
    LEFT JOIN
    (
        SELECT DISTINCT LGD_RULE_NAME
        FROM IFRS_LGD_TERM_STRUCTURE
        WHERE EFF_DATE = V_EFF_DATE
    ) C
    ON (A.LGD_RULE_NAME = C.LGD_RULE_NAME)
    WHERE A.UPLOADID = v_uploadId
    AND C.LGD_RULE_NAME IS NULL
    ORDER BY B.PKID,
        A.FL_YEAR;
    COMMIT;

END;