CREATE OR REPLACE procedure       USPS_Test_Dapper (
    v_PKID NUMBER default 0,
    Cur_out OUT SYS_REFCURSOR
)
as
begin

OPEN Cur_out FOR
--select Count(*)
select PKID, GROUP_SEGMENT, SEGMENT, SUB_SEGMENT, SEGMENT_TYPE
from IFRS_MSTR_SEGMENT_RULES_HEADER
where PKID = v_PKID;

end;