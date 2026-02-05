CREATE OR REPLACE PROCEDURE uspS_LookupScreenGroupH
    (Cur_out OUT SYS_REFCURSOR)
AS
BEGIN

   OPEN  Cur_out FOR
      SELECT CAST(PKID AS VARCHAR2(10)) || '-' || ScreenGroupID as "Screen Group"  ,
             PKID as "SCREENGROUPID"
        FROM tblM_ScreenGroupHeader  ;

END;