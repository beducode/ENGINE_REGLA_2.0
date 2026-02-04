CREATE OR REPLACE PROCEDURE      SP_IFRS_PD_MIG_ENR (V_EFF_DATE DATE)
AS
    v_COUNT   NUMBER;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.TMP_IFRS_PD_MIG_AVG_MONTH';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.TMP_IFRS_PD_RUNNING_DATE';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.TMP_IFRS_PD_SCENARIO_DATA';

    -- GET PD_RULE_ID FOR MAA
    INSERT INTO IFRS.TMP_IFRS_PD_RUNNING_DATE (EFF_DATE,
                                          BASE_DATE,
                                          PD_RULE_ID,
                                          BUCKET_GROUP,
                                          HISTORICAL_DATA,
                                          POPULATION_MONTH,
                                          TRANSITION_START_DATE)
        SELECT V_EFF_DATE
                   AS EFF_DATE,
               LAST_DAY (ADD_MONTHS (V_EFF_DATE, A.INCREMENT_PERIOD * -1))
                   AS BASE_DATE,
               A.PKID
                   AS PD_RULE_ID,
               A.BUCKET_GROUP,
               A.HISTORICAL_DATA,
               A.POPULATION_MONTH,
               LAST_DAY (ADD_MONTHS (V_EFF_DATE, A.TRANSITION_DATA * -1))
                   AS TRANSITION_START_DATE
          FROM IFRS.IFRS_PD_RULES_CONFIG A
         WHERE     NVL (A.ACTIVE_FLAG, 0) = 1
               AND IS_DELETED = 0
               AND PD_METHOD = 'MIG'
               AND DERIVED_PD_MODEL IS NULL;

    COMMIT;


    INSERT INTO IFRS.TMP_IFRS_PD_MIG_AVG_MONTH
        SELECT PD_RULE_ID, POPULATIONMONTH
          FROM IFRS.TMP_IFRS_PD_RUNNING_DATE
               CROSS APPLY (    SELECT REGEXP_SUBSTR (POPULATION_MONTH,
                                                      '[^,]+',
                                                      1,
                                                      LEVEL)    AS POPULATIONMONTH
                                  FROM DUAL
                            CONNECT BY REGEXP_SUBSTR (POPULATION_MONTH,
                                                      '[^,]+',
                                                      1,
                                                      LEVEL)
                                           IS NOT NULL);

    COMMIT;

    -- GET DATA FROM IFRS_PD_SCENARIO_DATA
    INSERT INTO IFRS.TMP_IFRS_PD_SCENARIO_DATA
          SELECT A.EFF_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.PD_UNIQUE_ID,
                 A.CALC_METHOD,
                 MAX (CUSTOMER_NAME)                          AS CUSTOMER_NAME,
                 SUM (CALC_AMOUNT)                            AS CALC_AMOUNT,
                 MAX (BUCKET_ID)                              AS BUCKET_ID,
                 SUM (OUTSTANDING)                            AS OUTSTANDING,
                 TO_CHAR (A.PD_RULE_ID || A.PD_UNIQUE_ID)     AS KEY_TMP
            FROM IFRS.IFRS_PD_SCENARIO_DATA A
                 JOIN IFRS.TMP_IFRS_PD_MIG_AVG_MONTH B
                     ON     EXTRACT (MONTH FROM A.EFF_DATE) = B.POPULATION_MONTH
                        AND A.PD_RULE_ID = B.PD_RULE_ID
        GROUP BY A.EFF_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.PD_UNIQUE_ID,
                 A.CALC_METHOD;

    COMMIT;

    DELETE IFRS.IFRS_PD_MIGRATION_DETAIL
     WHERE     EFF_DATE = V_EFF_DATE
           AND PD_RULE_ID IN (SELECT DISTINCT PD_RULE_ID
                                FROM IFRS.TMP_IFRS_PD_RUNNING_DATE);

    COMMIT;

    SELECT COUNT (*)
      INTO v_COUNT
      FROM USER_INDEXES
     WHERE INDEX_NAME = 'IDX_IFRS_PD_MIGRATION_DETAIL';

    IF v_COUNT > 0
    THEN
        EXECUTE IMMEDIATE 'DROP INDEX IDX_IFRS_PD_MIGRATION_DETAIL';
    END IF;

    /*Start inserting ENR population*/
    INSERT INTO IFRS.IFRS_PD_MIGRATION_DETAIL (EFF_DATE,
                                          BASE_DATE,
                                          PD_RULE_ID,
                                          BUCKET_GROUP,
                                          PD_UNIQUE_ID,
                                          BUCKET_FROM,
                                          BUCKET_TO,
                                          CALC_AMOUNT,
                                          CUSTOMER_NAME,
                                          OUTSTANDING)
        SELECT B.EFF_DATE      EFF_DATE,
               A.EFF_DATE      BASE_DATE,
               A.PD_RULE_ID,
               A.BUCKET_GROUP,
               A.PD_UNIQUE_ID,
               A.BUCKET_ID     BUCKET_FROM,
               --        CASE WHEN A.BUCKET_ID = D.MAX_BUCKET_ID THEN
               --            D.MAX_BUCKET_ID
               --        ELSE
               B.BUCKET_ID,
               --        END BUCKET_TO,
               A.CALC_AMOUNT,
               B.CUSTOMER_NAME,
               A.OUTSTANDING
          FROM IFRS.TMP_IFRS_PD_SCENARIO_DATA  A
               JOIN IFRS.TMP_IFRS_PD_SCENARIO_DATA B
                   ON     A.PD_RULE_ID = B.PD_RULE_ID
                      AND A.PD_UNIQUE_ID = B.PD_UNIQUE_ID
               JOIN IFRS.TMP_IFRS_PD_RUNNING_DATE C
                   ON     A.PD_RULE_ID = C.PD_RULE_ID
                      AND B.PD_RULE_ID = C.PD_RULE_ID
                      AND A.EFF_DATE = C.BASE_DATE
                      AND B.EFF_DATE = C.EFF_DATE
               JOIN IFRS.VW_IFRS_MAX_BUCKET D
                   ON     D.BUCKET_GROUP = A.BUCKET_GROUP
                      AND D.BUCKET_GROUP = B.BUCKET_GROUP;

    COMMIT;



MERGE INTO IFRS.IFRS_PD_MIGRATION_DETAIL UPD
     USING (SELECT B.NEW_PD_RULE_ID, A.*
              FROM IFRS.IFRS_PD_MIGRATION_DETAIL A
                   JOIN
                   (SELECT CASE WHEN PD.PD_RULE_ID = 4 AND UPPER (NVL (FLAG, 'BARU')) = 'BEKAS'
                                THEN 56
                                WHEN PD.PD_RULE_ID = 4 AND UPPER (NVL (FLAG, 'BARU')) <> 'BEKAS'
                                THEN 55
                                WHEN PD.PD_RULE_ID = 5 AND UPPER (NVL (FLAG, 'BARU')) = 'BEKAS'
                                THEN 58
                                WHEN PD.PD_RULE_ID = 5 AND UPPER (NVL (FLAG, 'BARU')) <> 'BEKAS'
                                THEN 57
                           END NEW_PD_RULE_ID,
                           FLAG,
                           PD.*
                      FROM (SELECT DET.*
                              FROM IFRS.IFRS_PD_MIGRATION_DETAIL DET
                             WHERE     DET.EFF_DATE = V_EFF_DATE
                                   AND PD_RULE_ID IN (4, 5)) PD
                           LEFT JOIN
                           (SELECT 'KKB'             SOURCE,
                                   DET1.PD_UNIQUE_ID ACCOUNT_NUMBER,
                                   UPPER (FLAG)      FLAG
                              FROM IFRS.IFRS_KKB_FLAG KKB
                                   JOIN
                                   (  SELECT ACCOUNT_NUMBER,
                                             MAX (DOWNLOAD_DATE) DOWNLOAD_DATE
                                        FROM IFRS.IFRS_KKB_FLAG
                                    GROUP BY ACCOUNT_NUMBER) NX
                                      ON     NX.ACCOUNT_NUMBER = KKB.ACCOUNT_NUMBER
                                         AND NX.DOWNLOAD_DATE = KKB.DOWNLOAD_DATE
                                   FULL JOIN
                                   (SELECT PD_UNIQUE_ID, BASE_DATE
                                      FROM IFRS.IFRS_PD_MIGRATION_DETAIL
                                     WHERE     PD_RULE_ID IN (4, 5)
                                           AND EFF_DATE = V_EFF_DATE) DET1
                                      ON KKB.ACCOUNT_NUMBER = SUBSTR (DET1.PD_UNIQUE_ID, 1, 16)
                             WHERE DET1.PD_UNIQUE_ID IS NOT NULL
                            UNION
                            SELECT 'IMAM' SOURCE,
                                   ACCOUNT_NUMBER,
                                   CASE WHEN RESERVED_VARCHAR_6 IN ('O', '0', '1', '2')
                                        THEN 'BARU'
                                        WHEN RESERVED_VARCHAR_6 IN ('3', '4')
                                        THEN 'BEKAS'
                                   END FLAG
                              FROM IFRS.IFRS_MASTER_ACCOUNT_MONTHLY IMAM
                                   JOIN
                                   (SELECT PD_UNIQUE_ID, BASE_DATE
                                      FROM IFRS.IFRS_PD_MIGRATION_DETAIL
                                     WHERE     PD_RULE_ID IN (4, 5)
                                           AND EFF_DATE = V_EFF_DATE) PD
                                      ON PD.PD_UNIQUE_ID = IMAM.ACCOUNT_NUMBER
                             WHERE     NOT EXISTS
                                          (SELECT 1
                                             FROM IFRS.IFRS_KKB_FLAG KKB
                                            WHERE KKB.ACCOUNT_NUMBER = SUBSTR (IMAM.ACCOUNT_NUMBER, 1, 16))
                                   AND IMAM.DOWNLOAD_DATE >= '31-OCT-2024'
                                   AND IMAM.DOWNLOAD_DATE = PD.BASE_DATE) KK
                              ON KK.ACCOUNT_NUMBER = PD.PD_UNIQUE_ID
                     WHERE PD.PD_UNIQUE_ID IS NOT NULL) B
                      ON     A.PD_UNIQUE_ID = B.PD_UNIQUE_ID
                         AND A.EFF_DATE = B.EFF_DATE
                         AND A.EFF_DATE = V_EFF_DATE
                         AND A.PD_RULE_ID IN (55, 56, 57, 58)
                         AND A.PD_RULE_ID <> B.NEW_PD_RULE_ID) DAT
        ON (    DAT.PKID = UPD.PKID
            AND DAT.PD_UNIQUE_ID = UPD.PD_UNIQUE_ID
            AND DAT.EFF_DATE = UPD.EFF_DATE)
WHEN MATCHED THEN UPDATE SET PD_RULE_ID = NEW_PD_RULE_ID
           WHERE     UPD.EFF_DATE = V_EFF_DATE
                 AND UPD.PD_RULE_ID IN (55, 56, 57, 58);

COMMIT;



MERGE INTO IFRS.IFRS_PD_MIGRATION_DETAIL UPD USING (
SELECT PREV.PD_RULE_ID PREV_PD_RULE_ID,CURR.* FROM (
SELECT * FROM IFRS.IFRS_PD_MIGRATION_DETAIL
WHERE EFF_DATE = V_EFF_DATE
AND PD_RULE_ID IN (55,56,57,58)) CURR JOIN (
select * from IFRS.IFRS_PD_MIGRATION_DETAIL
where eff_date = (SELECT MAX(EFF_DATE)EFF_DATE FROM IFRS.IFRS_PD_MIGRATION_DETAIL
WHERE EFF_DATE < V_EFF_DATE
AND PD_RULE_ID IN (55,56,57,58))
AND PD_RULE_ID IN (55,56,57,58)) PREV ON CURR.PD_UNIQUE_ID = PREV.PD_UNIQUE_ID
WHERE CURR.PD_RULE_ID <> PREV.PD_RULE_ID) DAT ON (UPD.PD_UNIQUE_ID = DAT.PD_UNIQUE_ID
AND UPD.EFF_DATE = DAT.EFF_DATE)
WHEN MATCHED THEN UPDATE SET PD_RULE_ID = PREV_PD_RULE_ID
WHERE EFF_DATE = V_EFF_DATE
AND PD_RULE_ID IN (55,56,57,58);

COMMIT;


    --EXECUTE IMMEDIATE 'CREATE INDEX IDX_IFRS_PD_MIGRATION_DETAIL ON IFRS_PD_MIGRATION_DETAIL(CURR_DATE, PD_RULE_ID, PD_UNIQUE_ID)';
    /*End inserting ENR population*/

    DELETE IFRS.IFRS_PD_MIG_ENR
     WHERE     EFF_DATE = V_EFF_DATE
           AND PD_RULE_ID IN
                   (SELECT PD_RULE_ID FROM IFRS.TMP_IFRS_PD_RUNNING_DATE);

    COMMIT;

    -- COUNT FOR BUCKET MOVEMENT
    INSERT INTO IFRS.IFRS_PD_MIG_ENR (EFF_DATE,
                                 BASE_DATE,
                                 PD_RULE_ID,
                                 BUCKET_GROUP,
                                 BUCKET_FROM,
                                 BUCKET_TO,
                                 CALC_AMOUNT)
          SELECT B.EFF_DATE,
                 B.BASE_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.BUCKET_FROM,
                 A.BUCKET_TO,
                 SUM (A.CALC_AMOUNT)
            FROM IFRS.IFRS_PD_MIGRATION_DETAIL A
                 JOIN IFRS.TMP_IFRS_PD_RUNNING_DATE B
                     ON A.PD_RULE_ID = B.PD_RULE_ID AND A.EFF_DATE = B.EFF_DATE
        GROUP BY B.EFF_DATE,
                 B.BASE_DATE,
                 A.PD_RULE_ID,
                 A.BUCKET_GROUP,
                 A.BUCKET_FROM,
                 A.BUCKET_TO;

    COMMIT;

    INSERT INTO IFRS.IFRS_PD_MIG_ENR (EFF_DATE,
                                 BASE_DATE,
                                 PD_RULE_ID,
                                 BUCKET_GROUP,
                                 BUCKET_FROM,
                                 BUCKET_TO,
                                 CALC_AMOUNT)
        SELECT DISTINCT A.EFF_DATE,
                        A.BASE_DATE,
                        PD_RULE_ID,
                        A.BUCKET_GROUP,
                        B.BUCKET_ID     BUCKET_FROM,
                        D.BUCKET_ID     BUCKET_TO,
                        0               AS CALC_AMOUNT
          FROM IFRS.TMP_IFRS_PD_RUNNING_DATE  A
               JOIN IFRS.IFRS_BUCKET_DETAIL B ON A.BUCKET_GROUP = B.BUCKET_GROUP
               JOIN VW_IFRS_MAX_BUCKET C
                   ON     B.BUCKET_GROUP = C.BUCKET_GROUP
                      AND B.BUCKET_ID <= C.MAX_BUCKET_ID
               CROSS JOIN IFRS.IFRS_BUCKET_DETAIL D
         WHERE     D.BUCKET_GROUP = C.BUCKET_GROUP
               AND D.BUCKET_ID <= C.MAX_BUCKET_ID
               AND NOT EXISTS
                       (SELECT 1
                          FROM IFRS.IFRS_PD_MIG_ENR E
                         WHERE     E.PD_RULE_ID = A.PD_RULE_ID
                               AND E.EFF_DATE = V_EFF_DATE
                               AND E.BUCKET_FROM = B.BUCKET_ID
                               AND E.BUCKET_TO = D.BUCKET_ID);

    COMMIT;
END;