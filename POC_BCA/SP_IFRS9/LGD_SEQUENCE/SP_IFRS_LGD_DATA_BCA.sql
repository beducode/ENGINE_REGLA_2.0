CREATE OR REPLACE PROCEDURE      SP_IFRS_LGD_DATA_BCA (V_EFF_DATE DATE)
AS
   V_MAX_DATE   DATE;
BEGIN

    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

   DELETE /*+ PARALLEL(8) */ IFRS.IFRS_LGD_ER_NPL_ACCT
    WHERE EFF_DATE = V_EFF_DATE;

   COMMIT;

   DELETE /*+ PARALLEL(8) */ IFRS.IFRS_LGD_DATA_DETAIL
    WHERE EFF_DATE = V_EFF_DATE;

   COMMIT;

   SELECT MAX (EFF_DATE) INTO V_MAX_DATE FROM IFRS.IFRS_LGD_DATA_DETAIL;

   COMMIT;

   DELETE /*+ PARALLEL(8) */ IFRS.IFRS_LGD
    WHERE EFF_DATE = V_EFF_DATE AND DATA_SOURCE = 'ILS';

   COMMIT;

   INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_LGD (EFF_DATE,
                         DOWNLOAD_DATE,
                         PRODUCT_CODE,
                         PRODUCT_NAME,
                         MASTER_ID,
                         ACCOUNT_NUMBER,
                         CUSTOMER_NUMBER,
                         CUSTOMER_NAME,
                         LGD_CUSTOMER_TYPE,
                         SEGMENTATION_ID,
                         SEGMENTATION_NAME,
                         CURRENCY,
                         ORIGINAL_CURRENCY,
                         NPL_DATE,
                         CLOSED_DATE,
                         BI_COLLECTABILITY_NPL,
                         BI_COLLECTABILITY_CLOSED,
                         TOTAL_LOSS_AMT,
                         RECOV_AMT_BF_NPV,
                         LAST_RECOV_DATE,
                         RECOV_PERCENTAGE,
                         DISCOUNT_RATE,
                         LOSS_RATE,
                         RECOVERY_AMOUNT,
                         DATA_SOURCE,
                         CREATEDBY,
                         CREATEDDATE,
                         CREATEDHOST,
                         UPDATEDBY,
                         UPDATEDDATE,
                         UPDATEDHOST,
                         LGD_RULE_ID,
                         LGD_RULE_NAME,
                         LGD_FLAG)
      SELECT V_EFF_DATE EFF_DATE,
             DOWNLOAD_DATE,
             PRODUCT_CODE,
             PRODUCT_NAME,
             MASTER_ID,
             ACCOUNT_NUMBER,
             CUSTOMER_NUMBER,
             CUSTOMER_NAME,
             LGD_CUSTOMER_TYPE,
             SEGMENTATION_ID,
             SEGMENTATION_NAME,
             CURRENCY,
             ORIGINAL_CURRENCY,
             NPL_DATE,
             CLOSED_DATE,
             BI_COLLECTABILITY_NPL,
             BI_COLLECTABILITY_CLOSED,
             TOTAL_LOSS_AMT,
             RECOV_AMT_BF_NPV,
             LAST_RECOV_DATE,
             RECOV_PERCENTAGE,
             DISCOUNT_RATE,
             LOSS_RATE,
             RECOVERY_AMOUNT,
             DATA_SOURCE,
             CREATEDBY,
             CREATEDDATE,
             CREATEDHOST,
             UPDATEDBY,
             UPDATEDDATE,
             UPDATEDHOST,
             LGD_RULE_ID,
             LGD_RULE_NAME,
             LGD_FLAG
        FROM IFRS.IFRS_LGD
       WHERE EFF_DATE = V_MAX_DATE AND DATA_SOURCE = 'ILS'
             AND ACCOUNT_NUMBER IN
                    (SELECT ACCOUNT_NUMBER
                       FROM IFRS.IFRS_LGD
                      WHERE     EFF_DATE = V_MAX_DATE
                            AND DATA_SOURCE = 'ILS'
                            AND NVL (LGD_FLAG, 'N') = 'N'
                     UNION ALL
                     SELECT A.ACCOUNT_NUMBER
                       FROM    IFRS.IFRS_LGD A
                            JOIN
                               (SELECT DISTINCT ACCOUNT_NUMBER, LGD_FLAG
                                  FROM IFRS.TMP_LGD_IMA    -- WHERE LGD_FLAG != 'N'
                                                  ) B
                            ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                               AND A.LGD_FLAG = B.LGD_FLAG
                      WHERE A.EFF_DATE = V_MAX_DATE AND DATA_SOURCE = 'ILS')
             AND ACCOUNT_NUMBER NOT IN
                    (SELECT ACCOUNT_NUMBER FROM IFRS.TBLU_LGD_EXCLUDED_LOAN); --RAL1022: OLD CALC EXCLUDE

   COMMIT;

   INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_LGD_DATA_DETAIL (EFF_DATE,
                                     MASTER_ID,
                                     ACCOUNT_NUMBER,
                                     CUSTOMER_NUMBER,
                                     ACCOUNT_STATUS,
                                     SEGMENTATION_ID,
                                     PAYMENT_DATE,
                                     CURRENCY,
                                     EXCHANGE_RATE,
                                     RECOVERY_AMOUNT,
                                     RECOVERY_TYPE,
                                     IS_EFFECTIVE,
                                     DATA_SOURCE,
                                     CREATEDBY,
                                     CREATEDDATE,
                                     CREATEDHOST,
                                     UPDATEDBY,
                                     UPDATEDDATE,
                                     UPDATEDHOST)
      SELECT V_EFF_DATE EFF_DATE,
             MASTER_ID,
             ACCOUNT_NUMBER,
             CUSTOMER_NUMBER,
             ACCOUNT_STATUS,
             SEGMENTATION_ID,
             PAYMENT_DATE,
             CURRENCY,
             EXCHANGE_RATE,
             RECOVERY_AMOUNT,
             RECOVERY_TYPE,
             IS_EFFECTIVE,
             DATA_SOURCE,
             CREATEDBY,
             CREATEDDATE,
             CREATEDHOST,
             UPDATEDBY,
             UPDATEDDATE,
             UPDATEDHOST
        FROM IFRS.IFRS_LGD_DATA_DETAIL
       WHERE EFF_DATE = V_MAX_DATE
             AND ACCOUNT_NUMBER IN
                    (SELECT ACCOUNT_NUMBER
                       FROM IFRS.IFRS_LGD
                      WHERE     EFF_DATE = V_MAX_DATE
                            AND DATA_SOURCE = 'ILS'
                            AND NVL (LGD_FLAG, 'N') = 'N'
                     UNION ALL
                     SELECT A.ACCOUNT_NUMBER
                       FROM    IFRS.IFRS_LGD A
                            JOIN
                               (SELECT DISTINCT ACCOUNT_NUMBER, LGD_FLAG
                                  FROM IFRS.TMP_LGD_IMA) B
                            ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                               AND A.LGD_FLAG = B.LGD_FLAG
                      WHERE A.EFF_DATE = V_MAX_DATE AND DATA_SOURCE = 'ILS')
             AND ACCOUNT_NUMBER NOT IN
                    (SELECT ACCOUNT_NUMBER FROM IFRS.TBLU_LGD_EXCLUDED_LOAN); --RAL1022: OLD CALC EXCLUDE

   COMMIT;


   --RAL1022: OLD CALC ADJUSTMENT IF ANY, SHOULD BE TREATED AS A NEW ACCOUNT LGD, DELETED FROM IFRS_LGD_DATA_DETAIL CURRENT
   DELETE /*+ PARALLEL(8) */ FROM IFRS.IFRS_LGD_DATA_DETAIL
         WHERE 1 = 1 AND EFF_DATE = V_EFF_DATE
               AND ACCOUNT_NUMBER IN
                      (SELECT A.ACCOUNT_NUMBER
                         FROM    IFRS.IFRS_LGD A
                              INNER JOIN
                                 IFRS.TBLU_LGD_ADJ_REC_AMOUNT_BF_NPV B
                              ON A.ACCOUNT_NUMBER =
                                    B.ACCOUNT_NUMBER
                                 AND nvl(A.RECOV_AMT_BF_NPV,0) !=
                                        nvl(B.ADJ_REC_AMOUNT_BF_NPV,0)
                        WHERE     1 = 1
                              AND A.EFF_DATE = V_EFF_DATE
                              AND A.DATA_SOURCE = 'ILS');

   COMMIT;

   --RAL1022: OLD CALC ADJUSTMENT IF ANY, SHOULD BE TREATED AS A NEW ACCOUNT LGD, DELETED FROM IFRS_LGD CURRENT
   DELETE /*+ PARALLEL(8) */ FROM IFRS.IFRS_LGD
         WHERE 1 = 1 AND EFF_DATE = V_EFF_DATE
               AND ACCOUNT_NUMBER IN
                      (SELECT A.ACCOUNT_NUMBER
                         FROM    IFRS.IFRS_LGD A
                              INNER JOIN
                                 IFRS.TBLU_LGD_ADJ_REC_AMOUNT_BF_NPV B
                              ON A.ACCOUNT_NUMBER =
                                    B.ACCOUNT_NUMBER
                                 AND nvl(A.RECOV_AMT_BF_NPV,0) !=
                                        nvl(B.ADJ_REC_AMOUNT_BF_NPV,0)
                        WHERE     1 = 1
                              AND A.EFF_DATE = V_EFF_DATE
                              AND A.DATA_SOURCE = 'ILS');

   COMMIT;


   INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_LGD_ER_NPL_ACCT (EFF_DATE,
                                     DOWNLOAD_DATE,
                                     MASTER_ID,
                                     ACCOUNT_NUMBER,
                                     CUSTOMER_NUMBER,
                                     CUSTOMER_NAME,
                                     NPL_DATE,
                                     CLOSED_DATE,
                                     BI_COLLECTABILITY_NPL,
                                     BI_COLLECTABILITY_CLOSED,
                                     OUTSTANDING_DEFAULT,
                                     LGD_CUSTOMER_TYPE,
                                     SEGMENTATION_ID,
                                     SEGMENTATION_NAME,
                                     LGD_RULE_ID,
                                     LGD_RULE_NAME,
                                     PRODUCT_CODE,
                                     CURRENCY,
                                     ORIGINAL_CURRENCY,
                                     INTEREST_RATE,
                                     DATA_SOURCE,
                                     LGD_FLAG)
        SELECT DISTINCT
               V_EFF_DATE,
               LAST_DAY (A.FIRST_NPL_DATE) AS DOWNLOAD_DATE,
               NVL (B.PKID, 0) AS MASTER_ID,
               A.ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
               A.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
               MAX (A.CUSTOMER_NAME) AS CUSTOMER_NAME,
               A.FIRST_NPL_DATE AS NPL_DATE,
               A.CLOSED_DATE AS CLOSED_DATE,
               ' ' AS BI_COLLECTABILITY_NPL,
               ' ' AS BI_COLLECTABILITY_CLOSED,
               ROUND (A.FIRST_NPL_OS * C.RATE_AMOUNT, 2) AS OUTSTANDING_DEFAULT,
               ' ' AS LGD_CUSTOMER_TYPE,
               E.SEGMENTATION_ID AS SEGMENTATION_ID,
               F.SEGMENT AS SEGMENTATION_NAME,
               E.PKID AS LGD_RULE_ID,
               E.LGD_RULE_NAME AS LGD_RULE_NAME,
               A.PRODUCT_CODE AS PRODUCT_CODE,
               'IDR' AS CURRENCY,
               A.CURRENCY AS ORIGINAL_CURRENCY,
               A.INTEREST_RATE AS INTEREST_RATE,
               'ILS',
               A.LGD_FLAG
          FROM IFRS.TMP_LGD_IMA A
               LEFT JOIN IFRS.IFRS_MASTERID B
                  ON A.ACCOUNT_NUMBER = B.MASTER_ACCOUNT_CODE
               JOIN (SELECT CASE
                               WHEN download_date < '31 JAN 2011'
                               THEN
                                  LAST_DAY (download_date)
                               ELSE
                                  download_date
                            END
                               download_date,
                            currency,
                            rate_amount
                       FROM IFRS.ifrs_master_exchange_rate) c
                  ON LAST_DAY (A.first_npl_date) = c.download_date
                     AND A.currency = c.currency
               INNER JOIN (SELECT a.pkid AS LGD_SEGMENTATION_ID,
                                  b.pkid AS SEGMENT_RULE_ID --, a.sub_segment, b.sub_segment
                             FROM    IFRS.ifrs_mstr_segment_rules_header a
                                  JOIN
                                     IFRS.ifrs_mstr_segment_rules_header b
                                  ON     a.segment_type = 'LGD_SEG'
                                     AND b.segment_type = 'PORTFOLIO_SEG'
                                     AND a.group_Segment = b.group_segment) D
                  ON A.SEGMENT_RULE_ID = D.SEGMENT_RULE_ID
               INNER JOIN IFRS.IFRS_LGD_RULES_CONFIG E
                  ON D.LGD_SEGMENTATION_ID = E.SEGMENTATION_ID
               INNER JOIN IFRS.IFRS_MSTR_SEGMENT_RULES_HEADER F
                  ON E.SEGMENTATION_ID = F.PKID
         WHERE                               --A.FLAG <> 'L' OR A.FLAG IS NULL
               (NVL (A.LGD_FLAG, 'L') <> 'L')
               AND A.ACCOUNT_NUMBER NOT IN
                      (SELECT ACCOUNT_NUMBER FROM IFRS.TBLU_LGD_EXCLUDED_LOAN)
               AND A.ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER
                                              FROM IFRS.TBLU_LGD_OVERRIDE_FLAG
                                             WHERE OVERRIDE_FLAG = 'L')
               AND A.ACCOUNT_NUMBER NOT IN
                      (SELECT ACCOUNT_NUMBER
                         FROM IFRS.IFRS_LGD
                        WHERE EFF_DATE = V_EFF_DATE AND DATA_SOURCE = 'ILS')
               AND NVL (A.SEGMENT_RULE_ID, 0) != 0
      GROUP BY B.PKID,
               A.ACCOUNT_NUMBER,
               A.CUSTOMER_NUMBER,
               A.FIRST_NPL_DATE,
               A.CLOSED_DATE,
               A.FIRST_NPL_OS,
               C.RATE_AMOUNT,
               E.SEGMENTATION_ID,
               F.SEGMENT,
               E.PKID,
               E.LGD_RULE_NAME,
               A.PRODUCT_CODE,
               A.CURRENCY,
               A.INTEREST_RATE,
               A.LGD_FLAG --        OR (NVL(A.FLAG,' ') = 'L' AND NVL(E.WORKOUT_PERIOD,0) > 0 AND V_CURRDATE - A.CLOSED_DATE >= NVL(E.WORKOUT_PERIOD,0))
                         ;

   COMMIT;

   --UPDATE /INSERT TABLE IFRS_LGD_DATA_DETAIL_K


DELETE IFRS.IFRS_LGD_ER_NPL_ACCT K
 WHERE EXISTS
 (SELECT 1
          FROM (select NPLO.*
                  from IFRS.IFRS_LGD_ER_NPL_ACCT Nplo
                  join (select npl.*,
                                              CASE WHEN npl.segmentation_id = 144 AND
                                                     UPPER(NVL(kkb.FLAG, ' ')) = 'BEKAS'
                                                   then '524'
                                                WHEN npl.segmentation_id = 144 AND
                                                     UPPER(NVL(kkb.FLAG, ' ')) <> 'BEKAS'
                                                   then '523'
                                                WHEN npl.segmentation_id = 145 AND
                                                     UPPER(NVL(kkb.FLAG, ' ')) = 'BEKAS'
                                                   then '526'
                                                WHEN npl.segmentation_id = 145 AND
                                                     UPPER(NVL(kkb.FLAG, ' ')) <> 'BEKAS'
                                                   then '525'
                                              end NEW_SEGMENT_ID
                         from IFRS.IFRS_LGD_ER_NPL_ACCT npl
                         left join (
                         select a.account_number,
                                          upper(a.FLAG) FLAG
                                     from IFRS.ifrs_kkb_flag a join
                                          (SELECT account_number,
                                                  max(download_date) download_date
                                             FROM IFRS.ifrs_kkb_flag
                                            group by account_number) mxdt
                                       on a.account_number =
                                          mxdt.account_number
                                      and a.download_date =
                                          mxdt.download_date
                                          ) kkb
                           on trim(npl.account_number) =
                              substr(kkb.account_number, 1, 16)
                        where npl.eff_date = V_EFF_DATE
                          and npl.closed_date < '30-oct-2024'
                          and npl.SEGMENTATION_ID in ('144', '145')) olds
                    on Nplo.account_number = olds.account_number
                   and nplo.eff_date = olds.eff_date
                 where nplo.eff_date = V_EFF_DATE
                   and nplo.SEGMENTATION_ID in ('523', '524', '525', '526')
                   and nplo.SEGMENTATION_ID <> olds.NEW_SEGMENT_ID) DELE
--) DELE
         WHERE K.MASTER_ID = DELE.MASTER_ID
           AND K.ACCOUNT_NUMBER = DELE.ACCOUNT_NUMBER
           AND K.EFF_DATE = DELE.EFF_DATE
           AND K.EFF_DATE = V_EFF_DATE
           AND K.SEGMENTATION_ID = DELE.SEGMENTATION_ID);
COMMIT;


DELETE IFRS.IFRS_LGD_ER_NPL_ACCT K
 WHERE EXISTS
 (SELECT 1
          FROM (SELECT distinct D.NEW_SEGMENT_ID, C.*
                  FROM IFRS.IFRS_LGD_ER_NPL_ACCT C,
                       (SELECT CASE
                 WHEN B.RESERVED_VARCHAR_6 IN ('O', '0', '1', '2') AND
                      B.PRODUCT_CODE IN (310, 311, 313, 316)
                     THEN 523
                 WHEN B.RESERVED_VARCHAR_6 IN ('3', '4') AND
                      B.PRODUCT_CODE IN (310, 311, 313, 316)
                     THEN 524
                 WHEN B.RESERVED_VARCHAR_6 IN ('O', '0', '1', '2') AND
                      B.PRODUCT_CODE IN (312, 314)
                     THEN 525
                 WHEN B.RESERVED_VARCHAR_6 IN ('3', '4') AND
                      B.PRODUCT_CODE IN (312, 314)
                     THEN 526
                 END NEW_SEGMENT_ID,
             A.*
      FROM IFRS.IFRS_LGD_ER_NPL_ACCT A,
           (SELECT mo.*
            FROM IFRS.IFRS_MASTER_ACCOUNT_MONTHLY mo JOIN
                 (select MASTER_ID,
                         ACCOUNT_NUMBER,
                         CLOSED_DATE
                  from IFRS.IFRS_LGD_ER_NPL_ACCT
                  where SEGMENTATION_ID in
                        ('523', '524', '525', '526')
                    and EFF_DATE = V_EFF_DATE
                    AND CLOSED_DATE >= '30-OCT-2024'
                  group by MASTER_ID,
                           ACCOUNT_NUMBER,
                           CLOSED_DATE
                  having count(MASTER_ID) = 2) "CL" ON mo.MASTERID = cl.MASTER_ID
                                   and mo.ACCOUNT_NUMBER = cl.ACCOUNT_NUMBER
                                   and mo.DOWNLOAD_DATE = cl.CLOSED_DATE) B
      WHERE A.EFF_DATE = V_EFF_DATE
        AND A.MASTER_ID = B.MASTERID
        AND A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.SEGMENTATION_ID IN
            ('523', '524', '525', '526')) D
WHERE C.MASTER_ID = D.MASTER_ID
  AND C.ACCOUNT_NUMBER = D.ACCOUNT_NUMBER
  AND C.EFF_DATE = D.EFF_DATE
  AND C.SEGMENTATION_ID <> D.NEW_SEGMENT_ID
  AND C.SEGMENTATION_ID = D.SEGMENTATION_ID
  AND C.SEGMENTATION_ID IN ('523', '524', '525', '526')) DELE
         WHERE K.MASTER_ID = DELE.MASTER_ID
           AND K.ACCOUNT_NUMBER = DELE.ACCOUNT_NUMBER
           AND K.EFF_DATE = DELE.EFF_DATE
           AND K.EFF_DATE = V_EFF_DATE
           AND K.SEGMENTATION_ID = DELE.SEGMENTATION_ID);

COMMIT;


   INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_LGD_DATA_DETAIL (EFF_DATE,
                                     MASTER_ID,
                                     ACCOUNT_NUMBER,
                                     CUSTOMER_NUMBER,
                                     ACCOUNT_STATUS,
                                     SEGMENTATION_ID,
                                     PAYMENT_DATE,
                                     CURRENCY,
                                     RECOVERY_AMOUNT,
                                     IS_EFFECTIVE,
                                     DATA_SOURCE)
      SELECT V_EFF_DATE,
             A.MASTER_ID AS MASTER_ID,
             A.ACCOUNT_NUMBER,
             A.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
             B.ACCOUNT_STATUS,
             A.SEGMENTATION_ID AS SEGMENTATION_ID,
             NVL ( (SELECT EOW
                      FROM IFRS.FINS_ILS_VAL_DATE
                     WHERE LAST_DAY (EOW) = B.DOWNLOAD_DATE),
                  B.DOWNLOAD_DATE)
                AS PAYMENT_DATE,
             A.CURRENCY,
             ROUND (B.RECOVERY_AMOUNT * c.rate_amount, 2) RECOVERY_AMOUNT,
             1 IS_EFFECTIVE,
             A.DATA_SOURCE
        FROM IFRS.IFRS_LGD_ER_NPL_ACCT A
             JOIN IFRS.TMP_LGD_IMA B
                ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
             JOIN (SELECT CASE
                             WHEN download_date < '31 JAN 2011'
                             THEN
                                LAST_DAY (download_date)
                             ELSE
                                download_date
                          END
                             download_date,
                          currency,
                          rate_amount
                     FROM IFRS.ifrs_master_exchange_rate) c
                ON LAST_DAY (b.first_npl_date) = c.download_date
                   AND b.currency = c.currency
       WHERE                              --(A.FLAG <> 'L' OR A.FLAG IS NULL);
            A    .EFF_DATE = V_EFF_DATE
             AND B.LGD_FLAG <> 'L'
             AND B.RECOVERY_AMOUNT > 0 --        OR (NVL(A.FLAG,' ') = 'L' AND NVL(F.WORKOUT_PERIOD,0) > 0 AND V_CURRDATE - A.CLOSED_DATE >= NVL(F.WORKOUT_PERIOD,0))
                                      ;

   COMMIT;
END;