CREATE OR REPLACE PROCEDURE USPS_LOOKUPUSERNAME
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
        SELECT '' AS UserID, '--ALL--' AS Username FROM dual
            UNION
        SELECT UserName AS UserID, UserName || '-' || FullName AS Username
        FROM tblM_User
        ORDER BY Username;

END;