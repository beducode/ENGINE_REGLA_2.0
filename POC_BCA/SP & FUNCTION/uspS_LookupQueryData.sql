CREATE OR REPLACE PROCEDURE uspS_LookupQueryData (
   Cur_out OUT SYS_REFCURSOR)
AS
BEGIN
   OPEN Cur_out FOR
      SELECT *
        FROM (SELECT '[Create New Query]' AS "QueryID", 0 AS "PKID" FROM DUAL)
      UNION ALL
      (SELECT QUERYNAME AS "QueryID", PKID AS "PKID" FROM tblM_QueryData);
END;