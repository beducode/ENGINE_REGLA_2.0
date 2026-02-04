CREATE OR REPLACE PROCEDURE  USPS_LGD_GETMODEL
(
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
        select PKID, LGD_RULE_NAME
        from ifrs_lgd_rules_config;

END;