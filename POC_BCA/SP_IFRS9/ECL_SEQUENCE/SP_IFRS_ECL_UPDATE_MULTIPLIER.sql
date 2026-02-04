CREATE OR REPLACE PROCEDURE SP_IFRS_ECL_UPDATE_MULTIPLIER (
   v_ECLID           NUMBER DEFAULT (0),
   v_DOWNLOADDATE    DATE DEFAULT NULL)
AS
BEGIN
   UPDATE IFRS_MASTER_ACCOUNT IMA
      SET IMA.RESERVED_RATE_8 = 1
    WHERE DOWNLOAD_DATE = v_DOWNLOADDATE;

   COMMIT;

   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (SELECT A2.RULE_ID, A2.MULTIPLIER
                 FROM IFRS_ECL_MULTIPLIER A2
                      JOIN (  SELECT RULE_ID,
                                     MAX (EFFECTIVE_DATE) MAX_EFFECTIVE_DATE
                                FROM IFRS_ECL_MULTIPLIER
                               WHERE EFFECTIVE_DATE <= v_DOWNLOADDATE
                            GROUP BY RULE_ID) B2
                         ON A2.RULE_ID = B2.RULE_ID
                            AND A2.EFFECTIVE_DATE = B2.MAX_EFFECTIVE_DATE
                      JOIN (  SELECT RULE_ID,
                                     EFFECTIVE_DATE,
                                     MAX (CREATEDDATE) MAX_CREATEDDATE
                                FROM IFRS_ECL_MULTIPLIER
                               WHERE EFFECTIVE_DATE <= v_DOWNLOADDATE
                            GROUP BY RULE_ID, EFFECTIVE_DATE) C2
                         ON     A2.RULE_ID = C2.RULE_ID
                            AND A2.EFFECTIVE_DATE = C2.EFFECTIVE_DATE
                            AND A2.CREATEDDATE = C2.MAX_CREATEDDATE) B
           ON (B.RULE_ID = A.SEGMENT_RULE_ID
               AND A.DOWNLOAD_DATE = v_DOWNLOADDATE)
   WHEN MATCHED
   THEN
      UPDATE SET A.RESERVED_RATE_8 = B.MULTIPLIER;

   COMMIT;

   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT
                      SUBSTR (SWIFT_CODE, 1, 8) SWIFT_CODE,
                      NVL (MULTIPLIER, 1) MULTIPLIER
                 FROM TBLU_RATING_BANK
                WHERE DOWNLOAD_DATE =
                         (SELECT MAX (DOWNLOAD_DATE)
                            FROM TBLU_RATING_BANK
                           WHERE DOWNLOAD_DATE <= v_DOWNLOADDATE)) B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_1, 1, 8)
               AND A.DOWNLOAD_DATE = v_DOWNLOADDATE)
   WHEN MATCHED
   THEN
      UPDATE SET
         A.RESERVED_RATE_8 = B.MULTIPLIER
              WHERE ( (A.DATA_SOURCE = 'KTP'
                       AND PRODUCT_CODE LIKE 'PLACEMENT%')
                     OR (A.DATA_SOURCE = 'RKN'));

   COMMIT;

   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT
                      SUBSTR (SWIFT_CODE, 1, 8) SWIFT_CODE,
                      NVL (MULTIPLIER, 1) MULTIPLIER
                 FROM TBLU_RATING_BANK
                WHERE DOWNLOAD_DATE =
                         (SELECT MAX (DOWNLOAD_DATE)
                            FROM TBLU_RATING_BANK
                           WHERE DOWNLOAD_DATE <= v_DOWNLOADDATE)) B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_15, 1, 8)
               AND A.DOWNLOAD_DATE = v_DOWNLOADDATE)
   WHEN MATCHED
   THEN
      UPDATE SET
         A.RESERVED_RATE_8 = B.MULTIPLIER
              WHERE (DATA_SOURCE = 'BTRD'
                     AND RESERVED_VARCHAR_23 IN ('2', '3'))
                    OR (    DATA_SOURCE = 'BTRD'
                        AND RESERVED_VARCHAR_23 IN ('4', '5')
                        AND RESERVED_FLAG_10 = 1)
                    OR DATA_SOURCE IN ('ILS', 'LIMIT');

   COMMIT;

   -- UPDATE MUTFUND MULTIPLIER FROM MEDALLION

   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (SELECT *
                 FROM IFRS_MDL_NONGOV
                WHERE DOWNLOAD_DATE = V_DOWNLOADDATE) B
           ON (    A.DOWNLOAD_dATE = B.DOWNLOAD_DATE
               AND A.DOWNLOAD_DATE = V_DOWNLOADDATE
               AND A.RESERVED_VARCHAR_28 = B.FUND_CODE)
   WHEN MATCHED
   THEN
      UPDATE SET A.RESERVED_RATE_7 = B.NONGOVRATE * 100;

   COMMIT;
END;