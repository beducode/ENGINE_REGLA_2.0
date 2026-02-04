CREATE OR REPLACE procedure       USPS_ECL_EAD_EFFDATE
(
    Cur_out OUT SYS_REFCURSOR
)
as
begin

open Cur_out for
select distinct TO_CHAR(DOWNLOAD_DATE, 'yyyy-MM-dd') PERIOD, TO_CHAR(DOWNLOAD_DATE, 'dd MON yyyy') DESCRIPTION
 from ifrs_ccf_header
 UNION
 select distinct TO_CHAR(DOWNLOAD_DATE, 'yyyy-MM-dd') PERIOD, TO_CHAR(DOWNLOAD_DATE, 'dd MON yyyy') DESCRIPTION
 from ifrs_prepayment_header;
 end;