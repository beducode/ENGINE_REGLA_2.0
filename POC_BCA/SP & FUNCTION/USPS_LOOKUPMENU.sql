CREATE OR REPLACE PROCEDURE USPS_LOOKUPMENU
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
        SELECT '' AS ScreenID, '--ALL--' AS Title FROM dual
            UNION
        SELECT ScreenID, Title
        FROM tblM_Screen
        where Active = 1 and ISMENU = 1
        ORDER BY Title;

END;