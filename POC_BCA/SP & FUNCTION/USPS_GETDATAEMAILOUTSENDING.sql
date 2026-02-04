CREATE OR REPLACE PROCEDURE USPS_GETDATAEMAILOUTSENDING
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN
    OPEN Cur_out FOR
select * from TBLT_EMAILOUT
    where status = 'XXXX';
    --select * from TBLT_EMAILOUT
    --where status = 'Sending' or status = 'Failed';
END;