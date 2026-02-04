CREATE OR REPLACE PROCEDURE USPS_IFRS_AC_RESULT
(
    v_PKID IN NUMBER DEFAULT 0,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
    SELECT  A.REFPKID AS BMID,
            CAST(B.SPPIID AS NUMERIC(18, 0)) AS SPPIID,
            A.BMRESULT,
            NVL(C.OVERRIDERESULT, C.SPPIRESULT) AS SPPIRESULT,
            D.ASSETCLASS,
            A.EFFECTIVE_DATE,
            A.CREATEDBY,
            A.CREATEDDATE,
            A.CREATEDHOST
    FROM IFRS_AC_BM_HEADER_PEND A
    CROSS APPLY
    (
        SELECT REGEXP_SUBSTR (A.SPPIID,
                                 '[^,]+',
                                 1,
                                 LEVEL)
                     AS SPPIID
             FROM DUAL
        CONNECT BY REGEXP_SUBSTR (A.SPPIID,
                                 '[^,]+',
                                 1,
                                 LEVEL)
                     IS NOT NULL
    ) B
    JOIN IFRS_AC_SPPI_HEADER C
    ON B.SPPIID = C.PKID
    JOIN IFRS_AC_MAIN_MAPPING D
    ON A.BMRESULT = D.BMRESULT
    AND C.SPPIRESULT = D.SPPIRESULT
    WHERE A.PKID = v_PKID
    UNION
    SELECT  A.BMID AS BMID,
            CAST(B.SPPIID AS NUMERIC(18, 0)) AS SPPIID,
            A.BMRESULT,
            NVL(C.OVERRIDERESULT, C.SPPIRESULT) AS SPPIRESULT,
            D.ASSETCLASS,
            A.EFFECTIVE_DATE,
            A.CREATEDBY,
            A.CREATEDDATE,
            A.CREATEDHOST
    FROM IFRS_AC_RESULT A
    CROSS APPLY
    (
        SELECT REGEXP_SUBSTR (A.SPPIID,
                                 '[^,]+',
                                 1,
                                 LEVEL)
                     AS SPPIID
             FROM DUAL
        CONNECT BY REGEXP_SUBSTR (A.SPPIID,
                                 '[^,]+',
                                 1,
                                 LEVEL)
                     IS NOT NULL
    ) B
    JOIN IFRS_AC_SPPI_HEADER C
    ON B.SPPIID = C.PKID
    JOIN IFRS_AC_MAIN_MAPPING D
    ON A.BMRESULT = D.BMRESULT
    AND C.SPPIRESULT = D.SPPIRESULT
    WHERE A.BMID IN (
            SELECT REFPKID
            FROM IFRS_AC_BM_HEADER_PEND
            WHERE PKID = v_PKID
    )
    AND A.SPPIID NOT IN (
            SELECT DISTINCT REGEXP_SUBSTR(TRIM(SPPIID), '[^,]+', 1, LEVEL) AS SPPIID
            FROM IFRS_AC_BM_HEADER_PEND
            WHERE PKID = v_PKID
            CONNECT BY REGEXP_SUBSTR(TRIM(SPPIID), '[^,]+', 1, LEVEL) IS NOT NULL
    );

END;