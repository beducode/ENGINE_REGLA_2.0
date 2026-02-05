CREATE OR REPLACE PROCEDURE  USPS_LOOKUPUSERACTIVITY
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
        SELECT '' AS LookupCode, '--ALL--' AS LookupName FROM dual
            UNION
        SELECT LookupCode, LookupName
        FROM tblM_Lookup
        ORDER BY LookupName;

END;