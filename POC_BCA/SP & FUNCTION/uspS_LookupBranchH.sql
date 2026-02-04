CREATE OR REPLACE PROCEDURE uspS_LookupBranchH
(Cur_out          OUT SYS_REFCURSOR)
AS
BEGIN
    OPEN Cur_out FOR
    select (cast(PKID as varchar2(10)) || '-' || Name) as "Branch Group" , PKID as "BranchGroupID" from tblM_BranchGroupHeader   ;
END;