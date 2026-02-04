CREATE OR REPLACE PROCEDURE RESOLVE_PD_DATA_STEP_1 AS

   dtCurrDate   DATE;
   dtPrevDate   DATE;
   V_EFF_DATE   DATE;
   V_SPNAME     VARCHAR(100);
BEGIN
   dtCurrdate := '31-Jul-2024';

   WHILE dtCurrdate <= '30-Jun-2025'
   LOOP

      V_EFF_DATE := dtCurrdate;
      dtPrevDate := LAST_DAY (ADD_MONTHS (-1, dtCurrdate));

      UPDATE ifrs.IFRS_DATE_DAY1
         SET currdate = dtCurrdate, prevdate = dtPrevDate;

      commit;


      MERGE INTO IFRS.IFRS_PD_MIGRATION_DETAIL A USING (
SELECT NEW_PD_RULE_ID, P.* FROM IFRS.IFRS_PD_MIGRATION_DETAIL P JOIN(
SELECT CASE WHEN PD_RULE_ID = 4 AND UPPER(NVL(FLAG,'BARU')) = 'BEKAS' THEN 56
            WHEN PD_RULE_ID = 4 AND UPPER(NVL(FLAG,'BARU')) <> 'BEKAS' THEN 55
            WHEN PD_RULE_ID = 5 AND UPPER(NVL(FLAG,'BARU')) = 'BEKAS' THEN 58
            WHEN PD_RULE_ID = 5 AND UPPER(NVL(FLAG,'BARU')) <> 'BEKAS' THEN 57
END NEW_PD_RULE_ID , PD.*
FROM IFRS.IFRS_PD_MIGRATION_DETAIL PD FULL JOIN (
SELECT KKB.* FROM IFRS.IFRS_KKB_FLAG KKB JOIN (
SELECT ACCOUNT_NUMBER , MAX(DOWNLOAD_DATE) DOWNLOAD_DATE FROM IFRS.IFRS_KKB_FLAG
GROUP BY ACCOUNT_NUMBER) NX ON NX.ACCOUNT_NUMBER =KKB.ACCOUNT_NUMBER
AND NX.DOWNLOAD_DATE =KKB.DOWNLOAD_DATE) KK ON SUBSTR(PD.PD_UNIQUE_ID,1,16) = KK.ACCOUNT_NUMBER
WHERE PD.EFF_DATE = dtCurrdate
AND PD_RULE_ID IN (4,5)) R ON P.PD_UNIQUE_ID = R.PD_UNIQUE_ID
WHERE P.EFF_DATE = dtCurrdate
AND P.PD_RULE_ID IN (55,56,57,58)
AND P.PD_RULE_ID <> R.NEW_PD_RULE_ID
    ) B ON (A.PD_UNIQUE_ID = B.PD_UNIQUE_ID
    AND A.EFF_DATE = B.EFF_DATE
    AND A.PKID = B.PKID)
WHEN MATCHED THEN  UPDATE SET PD_RULE_ID = NEW_PD_RULE_ID
WHERE EFF_DATE = dtCurrdate
AND PD_RULE_ID IN (55,56,57,58);


COMMIT;

DELETE ifrs.IFRS_PD_MIG_ENR
WHERE EFF_DATE = dtCurrdate AND  PD_RULE_ID IN (55,56,57,58);

COMMIT;



 INSERT                                                   /*+ PARALLEL(4) */
         INTO  ifrs.IFRS_PD_MIG_ENR (EFF_DATE,
                                BASE_DATE,
                                PD_RULE_ID,
                                BUCKET_GROUP,
                                BUCKET_FROM,
                                BUCKET_TO,
                                CALC_AMOUNT)
SELECT                                              /*+ PARALLEL(4) */
              A.EFF_DATE,
               A.BASE_DATE,
               A.PD_RULE_ID,
               A.BUCKET_GROUP,
               A.BUCKET_FROM,
               A.BUCKET_TO,
               SUM (A.CALC_AMOUNT)
          FROM    ifrs.IFRS_PD_MIGRATION_DETAIL A
WHERE A.EFF_DATE = dtCurrdate
AND A.PD_RULE_ID IN (55,56,57,58)
               GROUP BY A.EFF_DATE,
               A.BASE_DATE,
               A.PD_RULE_ID,
               A.BUCKET_GROUP,
               A.BUCKET_FROM,
               A.BUCKET_TO;

COMMIT;


 INSERT                                                   /*+ PARALLEL(4) */
         INTO  ifrs.IFRS_PD_MIG_ENR (EFF_DATE,
                                BASE_DATE,
                                PD_RULE_ID,
                                BUCKET_GROUP,
                                BUCKET_FROM,
                                BUCKET_TO,
                                CALC_AMOUNT)
      SELECT                                                /*+ PARALLEL(4) */
            DISTINCT dtCurrdate AS EFF_DATE,
             LAST_DAY (ADD_MONTHS (dtCurrdate, A.INCREMENT_PERIOD * -1))
                AS BASE_DATE,
                    A.PKID,
                     A.BUCKET_GROUP,
                     B.BUCKET_ID BUCKET_FROM,
                     D.BUCKET_ID BUCKET_TO,
                     0 AS CALC_AMOUNT
        FROM ifrs.IFRS_PD_RULES_CONFIG A
             JOIN ifrs.IFRS_BUCKET_DETAIL B
                ON A.BUCKET_GROUP = B.BUCKET_GROUP
             JOIN VW_IFRS_MAX_BUCKET C
                ON B.BUCKET_GROUP = C.BUCKET_GROUP
                   AND B.BUCKET_ID <= C.MAX_BUCKET_ID
             CROSS JOIN IFRS_BUCKET_DETAIL D
       WHERE D.BUCKET_GROUP = C.BUCKET_GROUP
             AND D.BUCKET_ID <= C.MAX_BUCKET_ID
             AND NVL (A.ACTIVE_FLAG, 0) = 1
             AND IS_DELETED = 0
             AND PD_METHOD = 'MIG'
             AND DERIVED_PD_MODEL IS NULL
             AND A.PKID IN (55,56,57,58)
             AND NOT EXISTS
                        (SELECT 1
                           FROM ifrs.IFRS_PD_MIG_ENR E
                          WHERE     E.PD_RULE_ID = A.PKID
                                AND E.EFF_DATE = dtCurrdate
                                AND E.BUCKET_FROM = B.BUCKET_ID
                                AND E.BUCKET_TO = D.BUCKET_ID);

COMMIT;

          ifrs.SP_IFRS_PD_MIG_FLOWRATE (dtCurrdate);
          ifrs.SP_IFRS_PD_MIG_FLOW_TO_LOSS (dtCurrdate);
          ifrs.SP_IFRS_PD_MIG_TTC (dtCurrdate);
          ifrs.SP_IFRS_PD_MIG_ODR (dtCurrdate);
          ifrs.SP_IFRS_PD_MIG_LOGIT_ODR (dtCurrdate);


      dtCurrdate := LAST_DAY (ADD_MONTHS (dtCurrdate,1));
   END LOOP;
END;