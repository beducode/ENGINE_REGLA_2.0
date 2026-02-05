CREATE OR REPLACE PROCEDURE TEST_UPDATE_PP
as
  v_date date;
  V_PREV DATE;
begin
    --SP_IFRS_PD_SEQUENCE;
    v_date := '31-JUL-2023';
    V_PREV := '30-JUN-2023';
--
--
    while v_date <= '30-jun-2025' loop
--

MERGE INTO IFRS.IFRS_PREPAYMENT_DETAIL A
USING (SELECT PREPAYMENT_RULE_NAME, SEGMENTATION_ID FROM IFRS.IFRS_PREPAYMENT_RULES_CONFIG)B
ON (A.SEGMENTATION_ID = B.SEGMENTATION_ID AND A.DOWNLOAD_DATE = V_DATE)
WHEN MATCHED THEN UPDATE
SET A. PREPAYMENT_SEGMENT = B.PREPAYMENT_RULE_NAME;
COMMIT;

DELETE IFRS.IFRS_PREPAYMENT_HEADER WHERE DOWNLOAD_DATE = V_DATE;COMMIT;

INSERT INTO IFRS.IFRS_PREPAYMENT_HEADER (DOWNLOAD_DATE,
                                       SEGMENTATION_ID,
                                       SEGMENTATION_NAME,
                                       PREPAYMENT_RULE_ID,
                                       PREPAYMENT_RULE_NAME,
                                       AVERAGE_SMM,
                                       PREPAYMENT_RATE,
                                       DURATION)
        SELECT V_DATE DOWNLOAD_DATE,
               A.SEGMENTATION_ID,
               NVL (C.SEGMENT, '-') SEGMENTATION_NAME,
               NVL (D.PKID, 0) PREPAYMENT_RULE_ID,
               NVL (D.PREPAYMENT_RULE_NAME, '-') PREPAYMENT_RULE_NAME,
               AVG (A.SMM) AVERAGE_SMM,
               ROUND (
                  1
                  - POWER (
                       (1 - AVG (A.SMM)),
                       CASE
                          WHEN A.INCREMENTS = 1 THEN 12
                          WHEN A.INCREMENTS = 3 THEN 4
                          WHEN A.INCREMENTS = 6 THEN 2
                          WHEN A.INCREMENTS = 12 THEN 1
                       END),
                  4)
                  PREPAYMENT_RATE,
               CASE
                  WHEN A.INCREMENTS = 1 THEN 12
                  WHEN A.INCREMENTS = 3 THEN 4
                  WHEN A.INCREMENTS = 6 THEN 2
                  WHEN A.INCREMENTS = 12 THEN 1
               END
                  DURATION
          FROM IFRS.IFRS_PREPAYMENT_DETAIL A
               LEFT JOIN IFRS.IFRS_MSTR_SEGMENT_RULES_HEADER C
                  ON A.SEGMENTATION_ID = C.PKID
               LEFT JOIN IFRS.IFRS_PREPAYMENT_RULES_CONFIG D
                  ON C.PKID = D.SEGMENTATION_ID
         WHERE     A.DOWNLOAD_DATE <= V_DATE
               AND D.AVERAGE_METHOD = 'Simple'
               AND A.SMM >= 0
               AND A.DOWNLOAD_dATE <= V_DATE
               AND A.DURATION = 12
      GROUP BY A.SEGMENTATION_ID,
               C.SEGMENT,
               D.PKID,
               D.PREPAYMENT_RULE_NAME,
               A.INCREMENTS;

   COMMIT;

         v_date := ADD_MONTHS(V_DATE,1);
         V_PREV := ADD_MONTHS(V_PREV,1);

       update IFRS.IFRS_DATE_DAY1
       set currdate = v_date,
           PREVDATE = V_PREV;
       commit;

    end loop;
end;