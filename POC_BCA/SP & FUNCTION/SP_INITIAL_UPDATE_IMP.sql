CREATE OR REPLACE PROCEDURE SP_INITIAL_UPDATE_IMP
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
   V_SPNAME     VARCHAR2 (100);
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_MASTER_ACCOUNT';

   /*
   DELETE  FROM IFRS_STATISTIC
   WHERE   DOWNLOAD_DATE = V_CURRDATE
   AND PRC_NAME = 'INIT';
   COMMIT;
   */
   SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;

   SELECT PREVDATE INTO V_PREVDATE FROM IFRS_PRC_DATE;

   INSERT INTO IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                                      DTM,
                                                      OPS,
                                                      PROCNAME,
                                                      REMARK)
        VALUES (V_CURRDATE,
                      SYSTIMESTAMP,
                      'START',
                      'SP_INITIAL_UPDATE_IMP',
                      '');

   COMMIT;

   -----------------------------------------------------------------------------------------------------------------
   --OUTSTANDING FOR MUTFUND
   -----------------------------------------------------------------------------------------------------------------
   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT DEAL_ID, PURCHASE_VALUE_CCY
                 FROM IFRS_STG_TRS_IMASEC
                WHERE DOWNLOAD_DATE = V_CURRDATE) B
           ON (    A.DOWNLOAD_DATE = V_CURRDATE
               AND A.ACCOUNT_NUMBER = B.DEAL_ID
               AND A.DATA_SOURCE = 'KTP'
               AND RESERVED_VARCHAR_26 = 'MUTFUND')
   WHEN MATCHED
   THEN
      UPDATE SET A.OUTSTANDING = B.PURCHASE_VALUE_CCY;

   COMMIT;

   -----------------------------------------------------------------------------------------------------------------
   --GTMP_MASTER_ACCOUNT
   -----------------------------------------------------------------------------------------------------------------

   INSERT INTO IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                                       DTM,
                                                       OPS,
                                                       PROCNAME,
                                                       REMARK)
        VALUES (V_CURRDATE,
                      SYSTIMESTAMP,
                      'START',
                      'SP_INITIAL_UPDATE_IMP',
                      'INSERT INTO TMP');

   COMMIT;

   INSERT INTO GTMP_IFRS_MASTER_ACCOUNT
      SELECT *
        FROM IFRS_MASTER_ACCOUNT
       WHERE DOWNLOAD_DATE IN (V_CURRDATE, V_PREVDATE) AND CREATEDBY <> 'DKP';

   COMMIT;

   INSERT INTO IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                                       DTM,
                                                       OPS,
                                                       PROCNAME,
                                                       REMARK)
        VALUES (V_CURRDATE,
                      SYSTIMESTAMP,
                      'END',
                      'SP_INITIAL_UPDATE_IMP',
                      'INSERT INTO TMP');

   COMMIT;

   ----------------------------------------------------------------------------------------------------------------
   --RATING_CODE
   -----------------------------------------------------------------------------------------------------------------

   INSERT INTO IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                                       DTM,
                                                       OPS,
                                                       PROCNAME,
                                                       REMARK)
        VALUES (V_CURRDATE,
                      SYSTIMESTAMP,
                      'START',
                      'SP_INITIAL_UPDATE_IMP',
                      'UPDATE RATING_CODE');

   COMMIT;

   UPDATE GTMP_IFRS_MASTER_ACCOUNT
      SET RATING_CODE = REPLACE (RATING_CODE, 0),
          RESERVED_VARCHAR_1 = CASE WHEN RESERVED_VARCHAR_1 = 'X'
                                                           THEN NULL
                                                           ELSE RESERVED_VARCHAR_1
                                                  END
    WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'KTP';

   COMMIT;

   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT DOWNLOAD_DATE, CUSTOMER_NUMBER, RATING_CODE_1
                 FROM IFRS_MASTER_CUSTOMER_RATING
                WHERE DOWNLOAD_DATE = V_CURRDATE AND RATING_TYPE_1 = '1') B
           ON (    A.DOWNLOAD_DATE = V_CURRDATE
               AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
               AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = TRIM (B.RATING_CODE_1),
                          A.RESERVED_VARCHAR_22 = B.RATING_CODE_1
              WHERE DATA_SOURCE <> 'CRD'
                    AND NOT (DATA_SOURCE = 'BTRD'
                             AND RESERVED_VARCHAR_23 IN ('0', '1'));

   COMMIT;

   --BOND
   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DOWNLOAD_DATE,
                      MASTERID,
                      RESERVED_VARCHAR_1,
                      RESERVED_VARCHAR_12
                 FROM GTMP_IFRS_MASTER_ACCOUNT
                WHERE DOWNLOAD_DATE = V_CURRDATE
                      AND RESERVED_VARCHAR_26 = 'BOND'
                      AND CUSTOMER_NUMBER NOT IN
                             ('00020409707', '00019597820')) B
           ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = REPLACE (B.RESERVED_VARCHAR_12, 'id', ''),
                          A.RESERVED_VARCHAR_1 = NVL (B.RESERVED_VARCHAR_1, 'CORP');

   COMMIT;

   --GOV BOND
   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DOWNLOAD_DATE,
                      MASTERID,
                      RESERVED_VARCHAR_1,
                      RESERVED_VARCHAR_12
                 FROM GTMP_IFRS_MASTER_ACCOUNT
                WHERE     DOWNLOAD_DATE = V_CURRDATE
                      AND RESERVED_VARCHAR_26 = 'BOND'
                      AND CUSTOMER_NUMBER IN ('00020409707', '00019597820')) B
           ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = NULL,
                          A.RESERVED_VARCHAR_1 = NVL (B.RESERVED_VARCHAR_1, 'GOV');

   COMMIT;



   UPDATE TBLU_RATING_BANK
      SET RATING_FINAL =
             CASE WHEN RATING_FINAL IS NULL THEN RATING ELSE RATING_FINAL END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   /***************************************************************
   UPDATE RATING CODE BANK TRADE FROM ARK RATING FOR IDR
   ****************************************************************/
   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT
                      (SUBSTR (SWIFT_CODE, 1, 8)) SWIFT_CODE,
                      RATING_SOURCE,
                      CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK'
                               ELSE RATING_FINAL
                      END AS RATING_FINAL
                 FROM TBLU_RATING_BANK
                WHERE DOWNLOAD_DATE = (SELECT MAX (DOWNLOAD_DATE)
                                         FROM TBLU_RATING_BANK
                                        WHERE DOWNLOAD_DATE <= V_CURRDATE)
                      AND RATING_SOURCE IN ('PEFINDO', 'PEF')) B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_15, 1, 8)
               AND A.DOWNLOAD_DATE = V_CURRDATE)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = B.RATING_FINAL
              WHERE ( (DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('2', '3'))
                     OR (DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('4', '5') AND RESERVED_FLAG_10 = 1)
                     OR DATA_SOURCE IN ('ILS', 'LIMIT'))
                    AND A.CURRENCY = 'IDR'
                    AND RATING_FINAL IS NOT NULL;

   COMMIT;

   /***************************************************************
   UPDATE RATING CODE BANK TRADE FROM ARK RATING FOR VALAS
   ****************************************************************/
   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT
                      (SUBSTR (B1.SWIFT_CODE, 1, 8)) SWIFT_CODE,
                      B1.RATING_SOURCE,
                      COALESCE (B2.STANDARD_AND_POORS, B3.STANDARD_AND_POORS, B1.RATING_FINAL) AS RATING_FINAL
                 FROM (SELECT SWIFT_CODE,
                              CASE WHEN RATING_SOURCE IN ('PEFINDO', 'PEF', 'FRS_IND')
                                       THEN 'PEFINDO'
                                 ELSE 'STANDARD_AND_POORS'
                              END AS RATING_SOURCE,
                              CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK'
                                       ELSE RATING_FINAL
                              END AS RATING_FINAL
                         FROM TBLU_RATING_BANK
                        WHERE DOWNLOAD_DATE =
                                 (SELECT MAX (DOWNLOAD_DATE)
                                    FROM TBLU_RATING_BANK
                                   WHERE DOWNLOAD_DATE <= V_CURRDATE)) B1
                      LEFT JOIN IFRS_MASTER_RATING_BANK B2
                         ON B1.RATING_FINAL = B2.STANDARD_AND_POORS
                      LEFT JOIN IFRS_MASTER_RATING_BANK B3
                         ON B1.RATING_FINAL = B3.MOODYS
                WHERE B1.RATING_SOURCE = 'STANDARD_AND_POORS') B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_15, 1, 8)
               AND A.DOWNLOAD_DATE = V_CURRDATE)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = B.RATING_FINAL
              WHERE ( (DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('2', '3'))
                     OR (    DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('4', '5') AND RESERVED_FLAG_10 = 1)
                     OR DATA_SOURCE IN ('ILS', 'LIMIT'))
                    AND A.CURRENCY <> 'IDR'
                    AND RATING_FINAL IS NOT NULL;

   COMMIT;

   /***************************************************************
   UPDATE RATING CODE PLACEMENT AND NOSTRO FROM ARK RATING FOR IDR
   ****************************************************************/
   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT
                      (SUBSTR (SWIFT_CODE, 1, 8)) SWIFT_CODE,
                      RATING_SOURCE,
                      CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK'
                               ELSE RATING_FINAL
                      END AS RATING_FINAL
                 FROM TBLU_RATING_BANK
                WHERE DOWNLOAD_DATE = (SELECT MAX (DOWNLOAD_DATE)
                                         FROM TBLU_RATING_BANK
                                        WHERE DOWNLOAD_DATE <= V_CURRDATE)
                      AND RATING_SOURCE IN ('PEFINDO', 'PEF')) B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_1, 1, 8)
               AND A.DOWNLOAD_DATE = V_CURRDATE)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = B.RATING_FINAL
              WHERE ( (A.DATA_SOURCE = 'KTP' AND PRODUCT_CODE LIKE 'PLACEMENT%')
                     OR (A.DATA_SOURCE = 'RKN'))
                    AND A.CURRENCY = 'IDR'
                    AND RATING_FINAL IS NOT NULL;

   COMMIT;

   /***************************************************************
   UPDATE RATING CODE PLACEMENT AND NOSTRO FROM ARK RATING FOR VALAS
   ****************************************************************/
   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT
                      (SUBSTR (B1.SWIFT_CODE, 1, 8)) SWIFT_CODE,
                      B1.RATING_SOURCE,
                      COALESCE (B2.STANDARD_AND_POORS, B3.STANDARD_AND_POORS, B1.RATING_FINAL) AS RATING_FINAL
                 FROM (SELECT SWIFT_CODE,
                              CASE WHEN RATING_SOURCE IN ('PEFINDO', 'PEF', 'FRS_IND')
                                       THEN 'PEFINDO'
                                       ELSE 'STANDARD_AND_POORS'
                              END AS RATING_SOURCE,
                              CASE WHEN RATING_FINAL = 'N/A' THEN 'UNK'
                                       ELSE RATING_FINAL
                              END AS RATING_FINAL
                         FROM TBLU_RATING_BANK
                        WHERE DOWNLOAD_DATE =
                                 (SELECT MAX (DOWNLOAD_DATE)
                                    FROM TBLU_RATING_BANK
                                   WHERE DOWNLOAD_DATE <= V_CURRDATE)) B1
                      LEFT JOIN IFRS_MASTER_RATING_BANK B2
                         ON B1.RATING_FINAL = B2.STANDARD_AND_POORS
                      LEFT JOIN IFRS_MASTER_RATING_BANK B3
                         ON B1.RATING_FINAL = B3.MOODYS
                WHERE B1.RATING_SOURCE = 'STANDARD_AND_POORS') B
           ON (B.SWIFT_CODE = SUBSTR (A.RESERVED_VARCHAR_1, 1, 8)
               AND A.DOWNLOAD_DATE = V_CURRDATE)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = B.RATING_FINAL
              WHERE ( (A.DATA_SOURCE = 'KTP' AND PRODUCT_CODE LIKE 'PLACEMENT%')
                     OR (A.DATA_SOURCE = 'RKN'))
                    AND A.CURRENCY <> 'IDR'
                    AND RATING_FINAL IS NOT NULL;

   COMMIT;

   /*
   UPDATE RATING CODE UNTUK DATA SOURCE KTP BOND UNTUK MENGAMBIL
   RATING INTERNAL TERKAIT CR INTERNAL BOND 20231003
   */

   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT DISTINCT DOWNLOAD_DATE, CUSTOMER_NUMBER, RATING_CODE_1
                 FROM IFRS_MASTER_CUSTOMER_RATING
                WHERE DOWNLOAD_DATE = V_CURRDATE AND RATING_TYPE_1 = '1'
                AND RATING_CODE_1 <> 'UNK'
                AND (RATING_CODE_1 LIKE 'RR%' OR RATING_CODE_1 LIKE 'LOSS')
                ) B
           ON (    A.DOWNLOAD_DATE = V_CURRDATE
               AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
               AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
   WHEN MATCHED
   THEN UPDATE SET A.RATING_CODE = B.RATING_CODE_1
              WHERE     A.DOWNLOAD_DATE = V_CURRDATE
                    AND A.DATA_SOURCE = 'KTP'
                    AND A.RESERVED_VARCHAR_1 = 'CORP'
                    AND A.PRODUCT_TYPE NOT IN ('MUTFUND','REVREPO')
                    AND A.RESERVED_VARCHAR_4 IN ('AFS', 'HTM');





   /***************************************************************
   UPDATE RATING CODE PLACEMENT AND NOSTRO NOT FOUND INTO UNK
   ****************************************************************/
   UPDATE GTMP_IFRS_MASTER_ACCOUNT A
      SET A.RATING_CODE = 'UNK'
    WHERE A.DOWNLOAD_DATE = V_CURRDATE
          AND ( (A.DATA_SOURCE = 'KTP' AND PRODUCT_CODE LIKE 'PLACEMENT%')
               OR (A.DATA_SOURCE = 'KTP' AND RESERVED_VARCHAR_1 = 'CORP'
                     AND PRODUCT_TYPE NOT IN ('MUTFUND','REVREPO')
                     AND RESERVED_VARCHAR_4 IN ('AFS', 'HTM'))---- PENAMBAHAN KONDISI UNTUK KTP BOND TERKAIT CR INTERNAL BOND  20231003
               OR (A.DATA_SOURCE = 'RKN')
               OR (DATA_SOURCE = 'BTRD' AND RESERVED_VARCHAR_23 IN ('2', '3')))
          AND A.RATING_CODE IS NULL;

   COMMIT;

   /*comment by willy teguh (perbaikan isu untuk akun unk)
   UPDATE RATING CODE UNK BILA RERUN  BILA RESERVED_VARCHAR_12 =X
   UNTUK DATA SOURCE KTP BOND UNTUK MENGAMBIL
   RATING INTERNAL TERKAIT CR 20231003

UPDATE GTMP_IFRS_MASTER_ACCOUNT A
   SET A.RATING_CODE = 'UNK'
 WHERE     A.DOWNLOAD_DATE = V_CURRDATE
       AND A.DATA_SOURCE = 'KTP'
       AND RESERVED_VARCHAR_1 = 'CORP'
       AND PRODUCT_TYPE NOT IN ('MUTFUND', 'REVREPO')
       AND RESERVED_VARCHAR_4 IN ('AFS', 'HTM')
       AND RESERVED_VARCHAR_12 = 'X';
       COMMIT;
   */

   /***************************************************************
    BCA Changed 20220125 : UPDATE RATING CODE FOR BANK_BTRD IF THERE
    ARE NO EXTERNAL RATING THEN UNK, NO LONGER INTERNAL RATING.
    THE SUB SEGMENT WILL BE ASSIGNED TO INTERNAL AND USING CALC
    ECL USING PD AVG - RAL
   ****************************************************************/
   UPDATE GTMP_IFRS_MASTER_ACCOUNT A
      SET A.RATING_CODE = 'UNK'
    WHERE 1 = 1 AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATA_SOURCE = 'BTRD'
          AND ( (A.RESERVED_VARCHAR_23 IN ('2', '3'))
               OR (A.RESERVED_VARCHAR_23 IN ('4', '5')
                   AND A.RESERVED_FLAG_10 = 1))
          AND A.RATING_CODE NOT IN
                 ('UNK',
                  'AAA',
                  'AA+',
                  'AA',
                  'AA-',
                  'A+',
                  'A',
                  'A-',
                  'BBB+',
                  'BBB',
                  'BBB-',
                  'BB+',
                  'BB',
                  'BB-',
                  'B+',
                  'B',
                  'B-',
                  'CCC+',
                  'CCC',
                  'CCC-',
                  'CC+',
                  'CC',
                  'CC-',
                  'C+',
                  'C',
                  'C-',
                  'D');

   COMMIT;

   /*
   UPDATE GTMP_IFRS_MASTER_ACCOUNT
   SET RATING_CODE = CASE WHEN RATING_CODE LIKE '%id%' THEN replace(rating_code,'id')
                          ELSE RATING_CODE END
                          WHERE DOWNLOAD_DATE = V_CURRDATE;
                          COMMIT;

   UPDATE GTMP_IFRs_MASTER_ACCOUNT
   SET RATING_CODE = CASE WHEN RATING_CODE LIKE '%0%' THEN replace(rating_code,'0')
                          ELSE RATING_CODE END
                          WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'KTP';
                          COMMIT;
   */

   UPDATE GTMP_IFRS_MASTER_ACCOUNT
      SET RESERVED_VARCHAR_22 = CASE WHEN RATING_CODE IS NULL
                                                               THEN 'UNK'
                                                               ELSE RATING_CODE
                                                      END
    WHERE DOWNLOAD_DATE = V_CURRDATE;

   COMMIT;

   MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
        USING (SELECT *
                 FROM GTMP_IFRS_MASTER_ACCOUNT
                WHERE DOWNLOAD_DATE = V_PREVDATE) B
           ON (A.MASTERID = B.MASTERID AND A.DOWNLOAD_DATE = V_CURRDATE)
   WHEN MATCHED
   THEN UPDATE SET A.RESERVED_VARCHAR_22 = CASE WHEN A.RESERVED_VARCHAR_22 = 'UNK' AND
                                                                                                B.RESERVED_VARCHAR_22 <> 'UNK'
                                                                                      THEN  B.RESERVED_VARCHAR_22
                                                                                      WHEN NVL (A.RESERVED_VARCHAR_22, 'UNK') = 'UNK' AND
                                                                                                        B.RESERVED_VARCHAR_22 = 'UNK'
                                                                                      THEN 'UNK'
                                                                                      ELSE A.RESERVED_VARCHAR_22
                                                                             END;

   COMMIT;

   INSERT INTO IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                                       DTM,
                                                       OPS,
                                                       PROCNAME,
                                                       REMARK)
        VALUES (V_CURRDATE,
                      SYSTIMESTAMP,
                      'END',
                      'SP_INITIAL_UPDATE_IMP',
                      'UPDATE RATING_CODE');

   COMMIT;

   -----------------------------------------------------------------------------------------------------------------
   -- RESERVED_VARCHAR_1 AND RESERVED_VARCHAR_12
   -----------------------------------------------------------------------------------------------------------------

   UPDATE GTMP_IFRS_MASTER_ACCOUNT
      SET RESERVED_VARCHAR_1 = CASE WHEN TRIM (RESERVED_VARCHAR_1) IS NULL
                                                              THEN 'X'
                                                               ELSE RESERVED_VARCHAR_1
                                                     END,
          RESERVED_VARCHAR_12 = CASE WHEN TRIM (RESERVED_VARCHAR_12) IS NULL
                                                             THEN 'X'
                                                             ELSE RESERVED_VARCHAR_12
                                                    END
    WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'KTP';

   COMMIT;


   MERGE INTO IFRS_MASTER_ACCOUNT A
        USING (SELECT *
                 FROM GTMP_IFRS_MASTER_ACCOUNT
                WHERE DOWNLOAD_DATE = V_CURRDATE) B
           ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID)
   WHEN MATCHED THEN
      UPDATE SET A.RATING_CODE = B.RATING_CODE,
                 A.RESERVED_VARCHAR_1 = B.RESERVED_VARCHAR_1,
                 A.RESERVED_VARCHAR_12 = B.RESERVED_VARCHAR_12,
                 A.RESERVED_VARCHAR_22 = B.RESERVED_VARCHAR_22; --RATING_CODE TURUNAN

   COMMIT;

   UPDATE IFRS_MASTER_ACCOUNT
      SET segment_rule_id = 0,
          group_segment = NULL,
          segment = NULL,
          sub_segment = NULL,
          ccf_rule_id = 0,
          ccf_segment = NULL,
          lifetime_rule_id = 0,
          lifetime_segment = NULL,
          prepayment_rule_id = 0,
          prepayment_segment = NULL
    WHERE download_date = V_CURRDATE AND CREATEDBY <> 'DKP';

   COMMIT;

   SP_IFRS_GENERATE_RULE_SEGMENT ('PORTFOLIO_SEG', 'M');
   SP_IFRS_UPDATE_SEGMENT;

   MERGE INTO ifrs_master_account a
        USING IFRS_SEGMENT_MAPPING_DAY1 b
           ON (a.download_date = V_CURRDATE
               AND a.segment_rule_id = b.segment_rule_id)
   WHEN MATCHED THEN UPDATE SET a.ccf_rule_id = b.ccf_rule_id,
                                                          a.ccf_segment = b.ccf_segment,
                                                          a.lifetime_rule_id = b.lifetime_rule_id,
                                                          a.lifetime_segment = b.lifetime_segment,
                                                          a.prepayment_rule_id = b.prepayment_rule_id,
                                                          a.prepayment_segment = b.prepayment_segment
              WHERE CREATEDBY <> 'DKP';

   COMMIT;

   UPDATE IFRS_MASTER_ACCOUNT
      SET PLAFOND = OUTSTANDING,
          RATING_CODE = 'UNK',
          BUCKET_GROUP = 'BR9_1',
          BUCKET_ID = '12',
          SUB_SEGMENT = 'BANK BTRD - EXTERNAL VALAS',
          SEGMENT = 'BANK_BTRD',
          GROUP_SEGMENT = 'BANK_BTRD',
          SEGMENT_RULE_ID = 437
    WHERE     DOWNLOAD_DATE = V_CURRDATE
          AND CREATEDBY = 'DKP'
          AND CURRENCY <> 'IDR';

   COMMIT;

   UPDATE IFRS_MASTER_ACCOUNT
      SET PLAFOND = OUTSTANDING,
          RATING_CODE = 'UNK',
          BUCKET_GROUP = 'BR9_1',
          BUCKET_ID = '12',
          SUB_SEGMENT = 'BANK BTRD - EXTERNAL IDR',
          SEGMENT = 'BANK_BTRD',
          GROUP_SEGMENT = 'BANK_BTRD',
          SEGMENT_RULE_ID = 438
    WHERE     DOWNLOAD_DATE = V_CURRDATE
          AND CREATEDBY = 'DKP'
          AND CURRENCY = 'IDR';

   COMMIT;


   INSERT INTO IFRS_UPDATE_LOG (DOWNLOAD_DATE,
                                                       DTM,
                                                       OPS,
                                                       PROCNAME,
                                                       REMARK)
        VALUES (V_CURRDATE,
                      SYSTIMESTAMP,
                      'END',
                      'SP_INITIAL_UPDATE_IMP',
                      '');

   COMMIT;
END;