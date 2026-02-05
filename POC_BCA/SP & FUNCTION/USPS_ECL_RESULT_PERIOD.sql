CREATE OR REPLACE Procedure      USPS_ECL_RESULT_PERIOD
(
      Cur_out OUT SYS_REFCURSOR
)
as
begin

    OPEN Cur_out FOR
    select DISTINCT DOWNLOAD_DATE CODE, Trim(TO_CHAR( DOWNLOAD_DATE, 'Month')) || ' ' || TO_CHAR( DOWNLOAD_DATE, 'yyyy') as "DESCRIPTION"
    from ifrs_ecl_result_header ORDER BY DOWNLOAD_DATE DESC;

end;