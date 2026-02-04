CREATE OR REPLACE PROCEDURE SP_IFRS_PPR_HEADER_3
AS
    V_CURRDATE DATE;
    V_PROCESS NUMBER;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_LGD;

    SELECT COUNT(1)
    INTO V_PROCESS
    FROM IFRS_PPR_PROCESS
    WHERE DOWNLOAD_DATE=LAST_DAY(V_CURRDATE);

    --UPDATE/INSERT PREPAYMENT HEADER
    IF V_PROCESS!=0 THEN
    BEGIN
        MERGE INTO IFRS_PREPAYMENT_HEADER D
        USING (
                SELECT
                    LAST_DAY(V_CURRDATE) DOWNLOAD_DATE,
                    A.SEGMENTATION_ID,
                    C.SEGMENT SEGMENTATION_NAME,
                    D.PKID PREPAYMENT_RULE_ID,
                    D.PREPAYMENT_RULE_NAME,
                    AVG(A.SMM)AVERAGE_SMM ,
                    ROUND(1-POWER((1-AVG(A.SMM)),
                        CASE WHEN A.INCREMENTS = 1 THEN 12
                             WHEN A.INCREMENTS = 3 THEN 4
                             WHEN A.INCREMENTS = 6 THEN 2
                        END ),4
                    ) PREPAYMENT_RATE,
                    CASE WHEN A.INCREMENTS = 1 THEN 12
                         WHEN A.INCREMENTS = 3 THEN 4
                         WHEN A.INCREMENTS = 6 THEN 2
                    END DURATION
                FROM IFRS_PREPAYMENT_DETAILS A
                   INNER JOIN IFRS_MSTR_SEGMENT_RULES_HEADER C ON A.SEGMENTATION_ID=C.PKID
                   INNER JOIN IFRS_PREPAYMENT_RULES_CONFIG D ON C.PKID=D.SEGMENTATION_ID
                WHERE
                       A.COMPONENT_TYPE IN (0,2)
                       AND A.REPORT_DATE >= D.CUT_OFF_DATE
                       AND REPORT_DATE<=LAST_DAY(V_CURRDATE)
                GROUP BY
                    A.SEGMENTATION_ID,
                    C.SEGMENT,
                    D.PKID,
                    D.PREPAYMENT_RULE_NAME,
                    A.INCREMENTS
              ) S ON (D.DOWNLOAD_DATE = S.DOWNLOAD_DATE
                      AND D.SEGMENTATION_ID = S.SEGMENTATION_ID
                      AND D.DURATION = S.DURATION
                      )
        WHEN MATCHED THEN
        UPDATE SET
            SEGMENTATION_NAME       =   S.SEGMENTATION_NAME,
            PREPAYMENT_RULE_ID      =   S.PREPAYMENT_RULE_ID,
            PREPAYMENT_RULE_NAME    =   S.PREPAYMENT_RULE_NAME,
            AVERAGE_SMM             =   S.AVERAGE_SMM,
            PREPAYMENT_RATE         =   S.PREPAYMENT_RATE,
            UPDATEDDATE             =   SYSDATE
        WHEN NOT MATCHED THEN
        INSERT (DOWNLOAD_DATE,
                SEGMENTATION_ID,
                SEGMENTATION_NAME,
                PREPAYMENT_RULE_ID,
                PREPAYMENT_RULE_NAME,
                AVERAGE_SMM,
                PREPAYMENT_RATE,
                DURATION)
        VALUES (S.DOWNLOAD_DATE,
                S.SEGMENTATION_ID,
                S.SEGMENTATION_NAME,
                S.PREPAYMENT_RULE_ID,
                S.PREPAYMENT_RULE_NAME,
                S.AVERAGE_SMM,
                S.PREPAYMENT_RATE,
                S.DURATION);
        COMMIT;
    END;
    END IF;
END;