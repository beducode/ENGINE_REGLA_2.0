CREATE OR REPLACE PROCEDURE SP_IFRS_INSERT_PD_TS_OVERRIDE(v_uploadId number)
AS
    V_EFF_DATE DATE;
BEGIN
    SELECT DISTINCT EFF_DATE
    INTO V_EFF_DATE
    FROM TBLU_PD_AFL_OVERRIDE
    WHERE UPLOADID = v_uploadId;

    /*===========================================================================================
    Initialize before update override_pd
    ===========================================================================================*/
    UPDATE IFRS_PD_TERM_STRUCTURE
    SET OVERRIDE_PD = PD
    WHERE EFF_DATE = V_EFF_DATE;
    COMMIT;

    /*===========================================================================================
    Update PD AFL
    ===========================================================================================*/
    MERGE INTO IFRS_PD_TERM_STRUCTURE A
    USING TBLU_PD_AFL_OVERRIDE B
    ON (A.EFF_DATE = B.EFF_DATE
        AND B.UPLOADID = v_uploadId
        AND A.PD_RULE_NAME = B.PD_RULE_NAME
        AND A.BUCKET_ID = B.BUCKET_ID
        AND A.FL_YEAR = B.FL_YEAR
        AND (A.MODEL_ID > 0 OR A.PD_RULE_ID IN (SELECT PKID FROM IFRS_PD_RULES_CONFIG WHERE PD_METHOD = 'VAS'))
    )
    WHEN MATCHED THEN
    UPDATE SET
        A.OVERRIDE_PD = B.MPD_OVERRIDE,
        A.UPDATEDBY = B.APPROVEDBY,
        A.UPDATEDDATE = B.APPROVEDDATE,
        A.UPDATEDHOST = B.APPROVEDHOST;
    COMMIT;

    /*===========================================================================================
    Update PD Bank Internal
    ===========================================================================================*/
    MERGE INTO IFRS_PD_TERM_STRUCTURE A
    USING TBLU_PD_AFL_OVERRIDE B
    ON (A.EFF_DATE = B.EFF_DATE
        AND B.UPLOADID = v_uploadId
        AND A.PD_RULE_ID = 23
        AND B.PD_RULE_NAME = 'PD_CORPORATE'
        AND A.BUCKET_ID = B.BUCKET_ID
        AND A.FL_YEAR = B.FL_YEAR
    )
    WHEN MATCHED THEN
    UPDATE SET
        A.OVERRIDE_PD = B.MPD_OVERRIDE,
        A.UPDATEDBY = B.APPROVEDBY,
        A.UPDATEDDATE = B.APPROVEDDATE,
        A.UPDATEDHOST = B.APPROVEDHOST;
    COMMIT;

    MERGE INTO IFRS_PD_TERM_STRUCTURE A
    USING
    (
        SELECT A2.EFF_DATE,
            23 PD_RULE_ID,
            0 MODEL_ID,
            'BR9_1' BUCKET_GROUP,
            12 BUCKET_ID,
            A2.FL_SEQ,
            AVG(A2.OVERRIDE_PD) MPD_OVERRIDE,
            B2.APPROVEDBY,
            B2.APPROVEDDATE,
            B2.APPROVEDHOST
        FROM IFRS_PD_TERM_STRUCTURE A2
        JOIN
        (
            SELECT DISTINCT EFF_DATE
                , PD_RULE_NAME
                , APPROVEDBY
                , APPROVEDDATE
                , APPROVEDHOST
            FROM TBLU_PD_AFL_OVERRIDE
            WHERE UPLOADID = v_uploadId
        ) B2
        ON A2.EFF_DATE = B2.EFF_DATE
        AND A2.PD_RULE_ID = 1
        AND A2.PD_RULE_NAME = B2.PD_RULE_NAME
        AND A2.MODEL_ID = (SELECT MAX(MODEL_ID) FROM IFRS_PD_TERM_STRUCTURE WHERE EFF_DATE = V_EFF_DATE AND PD_RULE_ID = 1 AND MODEL_ID > 0)
        AND A2.BUCKET_ID < 8
        GROUP BY
            A2.EFF_DATE,
            A2.FL_SEQ,
            B2.APPROVEDBY,
            B2.APPROVEDDATE,
            B2.APPROVEDHOST
    ) B
    ON (A.EFF_DATE = B.EFF_DATE
        AND A.PD_RULE_ID = B.PD_RULE_ID
        AND A.BUCKET_ID = B.BUCKET_ID
        AND A.FL_SEQ = B.FL_SEQ
    )
    WHEN MATCHED THEN
    UPDATE SET
        A.OVERRIDE_PD = B.MPD_OVERRIDE,
        A.UPDATEDBY = B.APPROVEDBY,
        A.UPDATEDDATE = B.APPROVEDDATE,
        A.UPDATEDHOST = B.APPROVEDHOST;
    COMMIT;

    /*===========================================================================================
    Insert term structure if PD AFL not exists in existing term structure
    ===========================================================================================*/
    INSERT INTO IFRS_PD_TERM_STRUCTURE
    (
        EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        PD_RULE_NAME,
        MODEL_ID,
        BUCKET_GROUP,
        BUCKET_ID,
        FL_SEQ,
        FL_YEAR,
        FL_MONTH,
        FL_DATE,
        PD,
        PRODUCTION_PD,
        OVERRIDE_PD,
        PRC_FLAG,
        TM_TYPE,
        CREATEDBY,
        CREATEDDATE,
        CREATEDHOST
    )
    SELECT A.EFF_DATE
        , ADD_MONTHS(A.EFF_DATE, B.INCREMENT_PERIOD * -1) BASE_DATE
        , B.PD_RULE_ID
        , A.PD_RULE_NAME
        , B.MODEL_ID
        , B.BUCKET_GROUP
        , A.BUCKET_ID
        , A.FL_YEAR AS FL_SEQ
        , A.FL_YEAR AS FL_YEAR
        , 12 AS FL_MONTH
        , ADD_MONTHS(A.EFF_DATE, (A.FL_YEAR - 1) * 12) FL_DATE
        , 0 PD
        , 0 PRODUCTION_PD
        , MPD_OVERRIDE OVERRIDE_PD
        , 'M' PRC_FLAG
        , 'YEARLY' TM_TYPE
        , A.APPROVEDBY CREATEDBY
        , A.APPROVEDDATE CREATEDDATE
        , A.APPROVEDHOST CREATEDHOST
    FROM TBLU_PD_AFL_OVERRIDE A
    JOIN
    (
        SELECT A2.EFF_DATE,
            A2.PD_RULE_NAME,
            B2.PKID PD_RULE_ID,
            B2.INCREMENT_PERIOD,
            B2.BUCKET_GROUP,
            MAX(C2.PKID) MODEL_ID
        FROM TBLU_PD_AFL_OVERRIDE A2
        JOIN IFRS_PD_RULES_CONFIG B2
        ON A2.EFF_DATE = V_EFF_DATE
        AND A2.PD_RULE_NAME = B2.PD_RULE_NAME
        JOIN IFRS_FL_MODEL_VAR_PEN C2
        ON B2.PKID = C2.DEPENDENT_VAR_VALUE
        AND C2.DEPENDENT_VAR_TYPE = 'PD'
        AND C2.STATUS = 1
        AND C2.REVIEWEDDATE <= A2.EFF_DATE
        GROUP BY A2.EFF_DATE,
            A2.PD_RULE_NAME,
            B2.PKID,
            B2.INCREMENT_PERIOD,
            B2.BUCKET_GROUP
    ) B
    ON A.EFF_DATE = B.EFF_DATE
    AND A.PD_RULE_NAME = B.PD_RULE_NAME
    JOIN IFRS_FL_MODEL_VAR_PEN C
    ON B.MODEL_ID = C.PKID
    LEFT JOIN
    (
        SELECT DISTINCT PD_RULE_NAME
        FROM IFRS_PD_TERM_STRUCTURE
        WHERE EFF_DATE = V_EFF_DATE
        AND MODEL_ID > 0
    ) D
    ON (B.PD_RULE_NAME = D.PD_RULE_NAME)
    WHERE D.PD_RULE_NAME IS NULL
    ORDER BY B.PD_RULE_ID,
        A.FL_YEAR,
        A.BUCKET_ID;
    COMMIT;
END;