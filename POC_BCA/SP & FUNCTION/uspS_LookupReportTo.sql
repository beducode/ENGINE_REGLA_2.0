CREATE OR REPLACE PROCEDURE uspS_LookupReportTo
    (Cur_out OUT SYS_REFCURSOR)
AS
BEGIN

   OPEN  Cur_out FOR
      SELECT USERID AS "ReportTo"  ,
             UserName || '_' || FullName AS "ReportToName"
        FROM tblM_User
        WHERE IsUserActive = 1
        ORDER BY UserName ;

END;