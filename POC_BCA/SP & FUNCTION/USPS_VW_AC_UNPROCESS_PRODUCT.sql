CREATE OR REPLACE PROCEDURE  USPS_VW_AC_UNPROCESS_PRODUCT
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
        SELECT PKID,
            DATA_SOURCE,
            PRD_GROUP,
            PRD_TYPE,
            PRD_CODE,
            PRD_DESC,
            CCY
        FROM VW_AC_UNPROCESS_PRODUCT;

END;