CREATE OR REPLACE PROCEDURE uspS_LookupModule
    (Cur_out OUT SYS_REFCURSOR)
AS
BEGIN

   OPEN  Cur_out FOR
      SELECT DESCRIPTION Module  ,
             ModuleID
        FROM tblM_Module  ;

END;