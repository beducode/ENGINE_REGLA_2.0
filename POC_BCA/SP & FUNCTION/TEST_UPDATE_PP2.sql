CREATE OR REPLACE PROCEDURE TEST_UPDATE_PP2
as
  v_date date;
  V_PREV DATE;
begin

-------------------------------------------------------------------------------- Perbaikan CCF Process - Header jan 20 - jun 25
    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    v_date := '31-JAN-2020';
    V_PREV := '31-JAN-2019';
--

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_HEADER
    WHERE DOWNLOAD_DATE >= V_DATE
    AND SEGMENTATION_ID NOT IN (410,411);
    commit;

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
    WHERE DOWNLOAD_DATE >= V_DATE AND PRODUCT_CODE_L IN ('CARDS','00541');
   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_UNPROCESS
    WHERE DOWNLOAD_DATE >= V_DATE AND PRODUCT_CODE_L IN ('CARDS','00541');
    COMMIT;

--
    while v_date <= '30-jun-2025' loop
--
INSERT /*+ PARALLEL(12) */ INTO IFRS.IFRS_CCF_DETAIL_PROCESS (PKID,
                                        DOWNLOAD_DATE,
                                        CURRENT_DATE,
                                        PREVIOUS_DATE,
                                        FACILITY_NUMBER,
                                        ACCOUNT_STATUS,
                                        SEGMENTATION_ID,
                                        CCF_RULE_ID,
                                        FIRST_NPL_DATE,
                                        OS_CUR,
                                        OS_PREV,
                                        LIMIT_CUR,
                                        LIMIT_PREV,
                                        USED_AMOUNT_CUR,
                                        USED_AMOUNT_PREV,
                                        AVAILABLE_AMT_CUR,
                                        AVAILABLE_AMT_PREV,
                                        REVOLVING_FLAG_L,
                                        REVOLVING_FLAG_I,
                                        PRODUCT_CODE_L,
                                        PRODUCT_CODE_I)
        SELECT 0,
               DOWNLOAD_DATE,
               CURRENT_DATE,
               PREVIOUS_DATE,
               ACCOUNT_NUMBER,
               ACCOUNT_STATUS,
               SEGMENTATION_ID,
               CCF_RULE_ID,
               FIRST_NPL_DATE,
               SUM (OS_CUR),
               SUM (OS_PREV),
               LIMIT_CUR,
               LIMIT_PREV,
               USED_AMOUNT_CUR,
               USED_AMOUNT_PREV,
               AVAILABLE_AMT_CUR,
               AVAILABLE_AMT_PREV,
               REVOLVING_FLAG_L,
               REVOLVING_FLAG_I,
               PRODUCT_CODE_L,
               PRODUCT_CODE_I
          FROM IFRS.IFRS_CCF_DETAIL
         WHERE DOWNLOAD_DATE = V_DATE
               AND ACCOUNT_NUMBER NOT IN
                      (SELECT NVL (FACILITY_NUMBER, 0)
                         FROM IFRS.IFRS_CCF_DETAIL_PROCESS)
               AND PRODUCT_CODE_L = 'CARDS'
               AND LAST_DAY (FIRST_NPL_DATE) = DOWNLOAD_DATE
      GROUP BY DOWNLOAD_DATE,
               "CURRENT_DATE",
               PREVIOUS_DATE,
               ACCOUNT_NUMBER,
               ACCOUNT_STATUS,
               SEGMENTATION_ID,
               CCF_RULE_ID,
               FIRST_NPL_DATE,
               LIMIT_CUR,
               LIMIT_PREV,
               USED_AMOUNT_CUR,
               USED_AMOUNT_PREV,
               AVAILABLE_AMT_CUR,
               AVAILABLE_AMT_PREV,
               REVOLVING_FLAG_L,
               REVOLVING_FLAG_I,
               PRODUCT_CODE_L,
               PRODUCT_CODE_I,
               CCF_RULE_ID;

   COMMIT;


   UPDATE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
      SET OS_SELISIH =
             CASE
                WHEN (OS_CUR - OS_PREV) < 0 THEN 0
                ELSE OS_CUR - OS_PREV
             END,
          LIMIT_SELISIH = LIMIT_CUR - OS_PREV
    WHERE DOWNLOAD_DATE = V_DATE
    AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO IFRS.IFRS_CCF_DETAIL_UNPROCESS
      SELECT *
        FROM IFRS.IFRS_CCF_DETAIL_PROCESS
       WHERE DOWNLOAD_DATE = V_DATE AND ( (LIMIT_CUR - OS_PREV) <= 0 --OR OS_CUR > LIMIT_CUR
                                             OR OS_PREV > LIMIT_CUR)
                                             AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
    WHERE DOWNLOAD_DATE = V_DATE AND ( (LIMIT_CUR - OS_PREV) <= 0 --OR OS_CUR > LIMIT_CUR
                                          OR OS_PREV > LIMIT_CUR)
                                          AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;

   INSERT /*+ PARALLEL(12) */ INTO IFRS.IFRS_CCF_DETAIL_UNPROCESS
      SELECT *
        FROM IFRS.IFRS_CCF_DETAIL_PROCESS
       WHERE DOWNLOAD_DATE = V_DATE AND (LIMIT_SELISIH < 0)
       AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
    WHERE DOWNLOAD_DATE = V_DATE AND (LIMIT_SELISIH < 0)
    AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;

   UPDATE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
      SET CCF_RESULT =
             CASE
                WHEN (OS_CUR - OS_PREV) / (NVL (LIMIT_CUR, 0) - OS_PREV) > 1
                THEN
                   1
                WHEN (OS_CUR - OS_PREV) / (NVL (LIMIT_CUR, 0) - OS_PREV) < 0
                THEN
                   0
                ELSE
                   (OS_CUR - OS_PREV) / (NVL (LIMIT_CUR, 0) - OS_PREV)
             END
    WHERE DOWNLOAD_DATE = V_DATE
    AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;

   UPDATE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
      SET CCF_RESULT =
             CASE WHEN (OS_CUR - OS_PREV) <= 0 THEN 0 ELSE CCF_RESULT END
    WHERE DOWNLOAD_DATE = V_DATE
    AND PRODUCT_CODE_L IN ('CARDS','00541');

   COMMIT;


   INSERT /*+ PARALLEL(12) */ INTO IFRS.IFRS_CCF_HEADER (PKID,
                                DOWNLOAD_DATE,
                                SEGMENTATION,
                                SEGMENTATION_ID,
                                CCF_RULE_ID,
                                CCF_RATE,
                                AVERAGE_METHOD)
      SELECT 0,
             V_DATE,
             B.CCF_RULE_NAME,
             A.SEGMENTATION_ID,
             A.CCF_RULE_ID,
             --CASE WHEN NVL(CCF_OVERRIDE,0) <> 0 THEN CCF_OVERRIDE ELSE
             CASE WHEN B.AVERAGE_METHOD = 'Simple' THEN C1 ELSE C2 --END
             END,
             B.AVERAGE_METHOD
        FROM    (  SELECT SEGMENTATION_ID,
                          CCF_RULE_ID,
                          C1,
                          C2
                     FROM (  SELECT SEGMENTATION_ID,
                                    CCF_RULE_ID,
                                    AVG (CCF_RESULT) C1,
                                    (SUM (CASE WHEN OS_SELISIH > LIMIT_SELISIH THEN LIMIT_SELISIH ELSE OS_SELISIH END))
                                    / (SUM (LIMIT_SELISIH))
                                       C2
                               FROM IFRS.IFRS_CCF_DETAIL_PROCESS
                           GROUP BY SEGMENTATION_ID, CCF_RULE_ID)
                 GROUP BY SEGMENTATION_ID,
                          CCF_RULE_ID,
                          C1,
                          C2) A
             JOIN
                IFRS.IFRS_CCF_RULES_CONFIG B
             ON A.SEGMENTATION_ID = B.SEGMENTATION_ID AND V_DATE >= B.CUT_OFF_DATE;

   COMMIT;

            v_date := ADD_MONTHS(V_DATE,1);
         V_PREV := ADD_MONTHS(V_PREV,1);

       update IFRS.IFRS_DATE_DAY1
       set currdate = v_date,
           PREVDATE = V_PREV;
       commit;

    end loop;

end;