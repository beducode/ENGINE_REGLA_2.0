CREATE OR REPLACE PROCEDURE  USPS_LGD_SUMMARY
(
    V_PERIOD DATE DEFAULT '2000-01-01',
    V_ModelId Number DEFAULT 0,
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    OPEN Cur_out FOR
    Select B.DOWNLOAD_DATE ,
           B.LGD_RULE_ID,
           A.LGD_RULE_NAME,
           C.SEGMENT,
           B.OUTSTANDING_NPL,
           B.RECOVERY_AMOUNT,
           B.PV_RECOVERY_AMOUNT,
           B.RECOVERY_RATE,
           B.LGD_EXPECTED_RECOVERY
    from ifrs_lgd_rules_config A
           join ifrs_lgd_expected_recovery B on (A.PKID = B.LGD_RULE_ID)
           join IFRS_MSTR_SEGMENT_RULES_HEADER C  on (A.SEGMENTATION_ID = C.PKID)
    Where B.DOWNLOAD_DATE=V_PERIOD and B.LGD_RULE_ID=V_ModelId;

END;