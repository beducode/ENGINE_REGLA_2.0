CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_EXPECT_RECOV_IMA
 AS
    V_CURRDATE DATE;
BEGIN
    SELECT  CURRDATE
    INTO V_CURRDATE
    FROM IFRS_PRC_DATE_LGD ;

    --UPDATE/ INSERT IFRS_LGD_EXPECTED_RECOVERY
    MERGE INTO IFRS_LGD_EXPECTED_RECOVERY_IMA D
    USING (
            SELECT LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                  A.SEGMENTATION_ID,
                  A.SEGMENTATION_NAME,
                  B.PKID LGD_RULE_ID,
                  B.LGD_RULE_NAME,
                  B.CALC_METHOD,
                  --B.WORKOUT_PERIOD,
                  SUM(A.TOTAL_LOSS_AMT) OUTSTANDING_NPL,
                  SUM(A.RECOV_AMT_BF_NPV) PV_RECOVERY_AMOUNT,
                  ROUND(SUM(A.RECOV_AMT_BF_NPV)/SUM(A.TOTAL_LOSS_AMT),4) RECOVERY_RATE,
                  1-ROUND(SUM(A.RECOV_AMT_BF_NPV)/SUM(A.TOTAL_LOSS_AMT),4) LGD_EXPECTED_RECOVERY
            FROM IFRS_LGD_IMA A
                INNER JOIN IFRS_LGD_RULES_CONFIG B ON A.SEGMENTATION_ID = B.SEGMENTATION_ID
            WHERE A.ACCOUNT_NUMBER IN (SELECT ACCOUNT_NUMBER FROM IFRS_LGD_PROCESS
                                       WHERE RECOVERY_DATE = LAST_DAY(V_CURRDATE)
                                       )
            GROUP BY A.DOWNLOAD_DATE,
                  A.SEGMENTATION_ID,
                  A.SEGMENTATION_NAME,
                  B.PKID,
                  B.LGD_RULE_NAME,
                  B.CALC_METHOD--,
                  --B.WORKOUT_PERIOD
            ) S ON (D.PERIOD = S.DOWNLOAD_DATE
                AND D.RULE_ID = S.LGD_RULE_ID)
    WHEN MATCHED THEN
    UPDATE SET
            RULE_NAME               =   S.LGD_RULE_NAME,
            CONFIG_ID               =   S.SEGMENTATION_ID,
            CALC_METHOD             =   S.CALC_METHOD,
            --WORKOUT_PERIOD          =   S.WORKOUT_PERIOD,
            OUTSTANDING_NPL         =   S.OUTSTANDING_NPL,
            PV_RECOVERY_AMOUNT      =   S.PV_RECOVERY_AMOUNT,
            RECOVERY_RATE           =   S.RECOVERY_RATE,
            LGD_EXPECTED_RECOVERY   =   S.LGD_EXPECTED_RECOVERY,
            UPDATEDDATE             =   SYSDATE
    WHEN NOT MATCHED THEN
    INSERT (PERIOD,
            RULE_ID,
            RULE_NAME,
            CONFIG_ID,
            CALC_METHOD,
            --WORKOUT_PERIOD,
            OUTSTANDING_NPL,
            PV_RECOVERY_AMOUNT,
            RECOVERY_RATE,
            LGD_EXPECTED_RECOVERY)
    VALUES(S.DOWNLOAD_DATE,
           S.LGD_RULE_ID,
           S.LGD_RULE_NAME,
           S.SEGMENTATION_ID,
           S.CALC_METHOD,
           --S.WORKOUT_PERIOD,
           S.OUTSTANDING_NPL,
           S.PV_RECOVERY_AMOUNT,
           S.RECOVERY_RATE,
           S.LGD_EXPECTED_RECOVERY);
    COMMIT;
END;