CREATE OR REPLACE PROCEDURE USPS_PIVOTCOLUMNS
(
    V_Pivotid VARCHAR2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN
OPEN Cur_out FOR
SELECT * FROM tblS_PivotColumns nolock where PivotID = V_Pivotid order by Area, AreaIndex;

END;