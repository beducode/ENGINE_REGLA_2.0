CREATE OR REPLACE PROCEDURE USPS_LGD_CMB_PERIOD
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
    Select  distinct DOWNLOAD_DATE, Trim(TO_CHAR( DOWNLOAD_DATE, 'Month')) || ' ' || TO_CHAR( DOWNLOAD_DATE, 'yyyy') as "DESCRIPTION"
    from ifrs_lgd_expected_recovery
    order by DOWNLOAD_DATE DESC;
END;